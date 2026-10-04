# ByteArena: Phased Build Plan

Each phase has a goal, tasks and **Done when** checks. Do not start the next phase until the checks pass. Tick boxes in `PROGRESS.md`, not here.

Estimated effort: about 7 to 9 focused days in total.

---

## Phase 0: Scaffold and infrastructure (0.5 to 1 day)

**Goal:** an empty but correct monorepo, with Kafka and Postgres running healthy.

Tasks:
1. Root `package.json` and `tsconfig.base.json` already exist. Create workspaces: `packages/shared`, `services/submission-service`, `services/runner`, `services/gateway`, each with its own `package.json`, `tsconfig.json` (extends base) and `src/`.
2. In `packages/shared` add: `config.ts` (zod-validated env), `logger.ts` (pino), `kafka.ts` (client factory, producer and consumer helpers), `events.ts` (zod schemas from ARCHITECTURE.md), `db.ts` (pg Pool), `grpc.ts` (proto loader helpers).
3. Add `proto:gen` script that runs `proto-loader-gen-types` on `proto/judge.proto` into `packages/shared/generated/`.
4. Add ESLint + Prettier config and a `vitest` config.
5. Copy `.env.example` to `.env`.

Done when:
- `docker compose up -d postgres kafka kafka-init` then `docker compose ps` shows postgres and kafka **healthy** and kafka-init **exited 0**.
- `docker compose exec postgres psql -U bytearena -d bytearena -c "select id from problems;"` lists the 3 seeded problems.
- `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` shows `submissions.queued`, `submissions.results`, `submissions.dlq`.
- `npm run typecheck` and `npm run lint` pass.

---

## Phase 1: Submission service, gRPC and transactional create (1 to 1.5 days)

**Goal:** `CreateSubmission` stores the submission and the outbox row atomically, with idempotency.

Tasks:
1. `services/submission-service/src/server.ts`: gRPC server implementing `SubmissionService` and `JudgeService` from `judge.proto`.
2. `CreateSubmission`:
   - validate input (language enum, `code` length 1..65536, known `problem_id`, `handle` 1..32 chars `[a-zA-Z0-9_-]`)
   - one transaction: `INSERT INTO submissions`, then `INSERT INTO outbox` (topic `submissions.queued`, key = submission id, payload per ARCHITECTURE.md)
   - idempotency: if `(handle, idempotency_key)` already exists, return the existing submission with `created = false`. Handle the race with the unique index (`ON CONFLICT DO NOTHING`, then select).
3. `GetSubmission`, `ListSubmissions`, `ListProblems`, `GetProblem` (samples only, never hidden tests).
4. `JudgeService.GetJudgingJob`: returns code, limits, **all** test cases and `already_final` (true if status is COMPLETED or SYSTEM_ERROR).
5. Dockerfile for the service.

Done when:
- A script `scripts/smoke-grpc.ts` creates a submission, then `select count(*) from outbox where published_at is null` returns 1.
- Calling it twice with the same handle + idempotency key returns the same id and still only 1 submission row and 1 outbox row.
- A unit or integration test proves a failure after the submission insert (simulated throw) leaves **no** submission and **no** outbox row (rollback).

---

## Phase 2: Outbox publisher (0.5 to 1 day)

**Goal:** committed outbox rows reach Kafka reliably.

Tasks:
1. `src/outbox-publisher.ts`: loop every ~200 ms. In a transaction: `SELECT ... WHERE published_at IS NULL ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED`, publish the batch (producer with `idempotent: true`), then `UPDATE ... SET published_at = now()`, commit.
2. If publishing fails, roll back and retry with backoff. Never mark rows as published before the broker acknowledged.
3. Graceful shutdown on SIGTERM.

Done when:
- After Phase 1's smoke script, the message is visible via `kafka-console-consumer.sh --topic submissions.queued --from-beginning` and the outbox row has `published_at` set.
- **Crash test:** stop Kafka, create 3 submissions (they succeed, outbox grows), start Kafka, and all 3 messages arrive. Nothing lost.
- Running 2 publisher instances at once does not publish the same row twice under normal operation (SKIP LOCKED). Note that duplicates after a crash between publish and commit are possible and expected (at-least-once).

---

## Phase 3: Sandbox library (1.5 to 2 days), the critical phase

**Goal:** a standalone, well-tested function `runInSandbox({language, code, stdin, timeLimitMs, memoryLimitMb})` in `services/runner/src/sandbox/`.

Tasks:
1. Pre-pull images: `python:3.12-alpine` and `node:20-alpine`.
2. Implement with dockerode, following the rules in AGENTS.md. Details in ARCHITECTURE.md ("Sandbox design"). Summary:
   - create the container (stopped) with the full HostConfig, the label, and the code in an env var `SUBMISSION_CODE`
   - the command is a tiny bootstrap that reads and removes `SUBMISSION_CODE` and executes it (Python and Node variants are listed in ARCHITECTURE.md)
   - attach stdin, write the test input, close stdin; demux stdout and stderr; cap output at 64 KiB
   - wall-clock timer, kill on expiry, then remove the container in `finally`
   - after exit, inspect: `OOMKilled` means MLE, non-zero exit means RE, timer fired means TLE
   - return `{ outcome, stdout, stderr (truncated), timeMs, memoryKb (best effort) }`
3. Startup cleanup: remove any container labelled `bytearena.submission` that is older than a few minutes.

Done when `npm run test:sandbox -w services/runner` passes these cases (write them as vitest tests):
- prints expected output for a normal program (Python and JavaScript)
- infinite loop returns `TIME_LIMIT_EXCEEDED` within about `limit + 500 ms` and the container is gone
- memory bomb (allocate GBs) returns `MEMORY_LIMIT_EXCEEDED`
- fork bomb does not take down the host (pids limit holds) and returns within the time limit
- network attempt (`socket.connect`/`fetch` to 1.1.1.1) fails
- writing a file to `/` fails, writing to `/tmp` is allowed, `/tmp` is not executable
- printing 100 MB of output is killed at the cap and returns `OUTPUT_LIMIT_EXCEEDED`
- reading `os.environ` / `process.env` does not reveal anything useful (the code variable is removed)
- `docker ps -a --filter label=bytearena.submission` shows nothing left after the tests

---

## Phase 4: Runner worker (1 day)

**Goal:** consume queued submissions, judge them, publish results.

Tasks:
1. `services/runner/src/main.ts`: KafkaJS consumer in group `runner-group`, `autoCommit: false`, one message at a time per partition.
2. Per message: validate with zod, call `GetJudgingJob` over gRPC. If `already_final`, commit and skip. Else publish `JUDGING_STARTED`, then for each test case run the sandbox, compare output (rule in AGENTS.md), publish a `TEST_RESULT` event, call `heartbeat()`. Stop at the first failing test? **No.** Run all tests (simpler, and the live stream looks better). The final verdict is the first non-accepted verdict in test order, otherwise ACCEPTED.
3. Publish `FINAL_VERDICT`, **then** commit the offset.
4. Infrastructure failure (Docker down, gRPC unreachable): retry up to 3 times with backoff. After that, publish to `submissions.dlq`, publish `FINAL_VERDICT` with `INTERNAL_ERROR`, commit.
5. Never put hidden-test inputs or expected outputs in any event or log.
6. Dockerfile. In compose, mount `/var/run/docker.sock` for this service only.

Done when:
- `scripts/smoke.sh` submits a correct solution, a wrong one, an infinite loop and a runtime error for `sum-two`, and `kafka-console-consumer` on `submissions.results` shows `JUDGING_STARTED`, `TEST_RESULT` x N and `FINAL_VERDICT` with the right verdicts.
- Two runner replicas (`docker compose up -d --scale runner=2`) split the work without errors.

---

## Phase 5: Result writer (0.5 day)

**Goal:** persist the stream idempotently.

Tasks:
1. `services/submission-service/src/result-writer.ts`, group `result-writer-group`.
2. Idempotency rules from ARCHITECTURE.md:
   - `JUDGING_STARTED`: `UPDATE ... SET status='JUDGING' WHERE status='QUEUED'`
   - `TEST_RESULT`: `INSERT ... ON CONFLICT (submission_id, test_index) DO NOTHING`
   - `FINAL_VERDICT`: `UPDATE ... SET status, verdict, judged_at WHERE status IN ('QUEUED','JUDGING')`
3. Commit offsets after the DB write.

Done when:
- After the smoke script, `select status, verdict from submissions` shows the right final state and `test_results` has exactly one row per test per submission.
- Replaying the whole `submissions.results` topic (reset the group offsets) leaves the data unchanged (no duplicates, no state flip).

---

## Phase 6: GraphQL gateway with live subscriptions (1 day)

**Goal:** the public API.

Tasks:
1. `services/gateway`: graphql-yoga serving `schema/schema.graphql`, resolvers call `SubmissionService` over gRPC.
2. `submitSolution` resolver. Basic per-handle rate limit in memory (5 submissions per 10 seconds, return a GraphQL error).
3. `kafka-bridge.ts`: a consumer with a **unique group id per instance** (`gateway-${HOSTNAME}`), `fromBeginning: false`, feeding an in-memory pub/sub keyed by `submission_id`.
4. `submissionProgress` subscription: subscribe to the bus **first**, then fetch the current snapshot via gRPC (replay already-finished tests), then stream live events, dedupe by `(type, testIndex)`, complete after `FINAL_VERDICT`.
5. Validation limits: max request body about 128 KB, query depth limit 6.
6. Dockerfile and compose service on port 4000.

Done when:
- Open `http://localhost:4000/graphql`, run `submitSolution` and get a submission id.
- `curl -N -H "accept: text/event-stream" -X POST http://localhost:4000/graphql -H "content-type: application/json" -d '{"query":"subscription { submissionProgress(submissionId: \"<ID>\") { type testIndex verdict timeMs } }"}'` prints events as tests complete, then closes after `FINAL_VERDICT`.
- Subscribing **after** the submission already finished still returns all results and completes.

---

## Phase 7: Crash recovery and hardening (1 to 1.5 days), the part that makes the project stand out

**Goal:** prove it survives failures.

Tasks and scripts (`scripts/chaos/`):
1. `kill-runner.sh`: submit a slow-ish but valid solution, `docker kill` the runner mid-judging, restart it. The submission must still finish **once**: one final verdict, one row per test.
2. `kill-publisher.sh`: kill the outbox publisher between create and publish. After restart the event is delivered.
3. `kafka-down.sh`: stop Kafka while submitting, start it, all submissions get judged.
4. `duplicate-delivery.sh`: re-send the same `queued` event manually. The final state is unchanged and not double-counted.
5. Reduce consumer `sessionTimeout` to 10 s via env for faster recovery in demos.
6. Add the DLQ path test (simulate Docker failure, see message in `submissions.dlq` and `INTERNAL_ERROR`).

Done when every chaos script ends with a printed `PASS` or `FAIL` and all pass. Record in `PROGRESS.md` the measured recovery time.

---

## Phase 8: CI, benchmarks, docs, demo (1 day)

Tasks:
1. `.github/workflows/ci.yml`: install, lint, typecheck, unit tests. Optionally a compose-based integration job that runs `scripts/smoke.sh`.
2. `scripts/bench/`: submit N correct solutions concurrently (for example 50 total, concurrency 5) and report p50/p95 judge latency and throughput. Record machine specs and the exact command.
3. Fill in `README.md`: what it is, architecture diagram (Mermaid from ARCHITECTURE.md), how to run in one command, API examples, sandbox security model and its known limits, chaos test results, measured benchmark table.
4. Record a short GIF of: submit, live per-test results, an infinite loop getting killed, a runner kill and recovery.

Done when a fresh clone runs with `docker compose up -d --build` followed by `scripts/smoke.sh`, and the README has only measured numbers.

---

## Stretch (only after Phase 8)

- Live leaderboard (Redis ZSET fed by `FINAL_VERDICT`)
- gRPC server-streaming `WatchSubmission` instead of the gateway reading Kafka
- A third language, or compile-then-run languages (C++)
- Stronger isolation (gVisor `runsc` runtime, or rootless Docker) with notes in the README
- OpenTelemetry tracing across GraphQL, gRPC and Kafka headers
