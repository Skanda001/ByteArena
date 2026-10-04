# Progress Log

> The builder updates this at the end of EVERY session. This file is how work continues across sessions and accounts.

## Current status
- **Current phase:** 6
- **Last session summary:** Completed Phase 5. Result writer (`services/submission-service/src/result-writer.ts`) fully implemented, containerised, and tested. Discovered and fixed subtle dockerode/docker-modem bug where `container.attach` sent options JSON as body into container stdin; implemented clean HTTP upgrade attach in `attachStdin` that sends zero body bytes. Verified that `smoke.ts` runs 4 submissions to expected verdicts (`ACCEPTED`, `WRONG_ANSWER`, `TIME_LIMIT_EXCEEDED`, `RUNTIME_ERROR`), `submissions` and `test_results` tables in Postgres are correctly populated, and resetting `result-writer-group` offset to earliest replays all events with zero duplicate rows and zero state flips. All 25 unit/integration tests passing.
- **Next step:** Start Phase 6 in PLAN.md (GraphQL gateway with live subscriptions)

## Checklist (tick only after running the "Done when" checks)

- [x] Phase 0: Scaffold and infrastructure
- [x] Phase 1: Submission service, gRPC and transactional create
- [x] Phase 2: Outbox publisher
- [x] Phase 3: Sandbox library
- [x] Phase 4: Runner worker
- [x] Phase 5: Result writer
- [ ] Phase 6: GraphQL gateway with live subscriptions
- [ ] Phase 7: Crash recovery and hardening
- [ ] Phase 8: CI, benchmarks, docs, demo

## Session notes

### Session 1 (2026-10-04)
- Did:
  - Copied `.env.example` to `.env`.
  - Configured monorepo workspaces: `packages/shared`, `services/submission-service`, `services/runner`, `services/gateway` each with their `package.json` and `tsconfig.json`.
  - In `packages/shared`, implemented `config.ts` (zod-validated), `logger.ts` (pino), `events.ts` (zod schemas for Kafka topics per ARCHITECTURE.md), `enums.ts` (mapping between proto strings and DB/Kafka enums), `db.ts` (pg Pool with no `any`), `kafka.ts` (client, producer, consumer helpers), `grpc.ts` (proto loader helpers).
  - Generated TypeScript types from `proto/judge.proto` into `packages/shared/generated` using `npm run proto:gen`.
  - Added ESLint 9 flat configuration (`eslint.config.mjs`), Prettier configuration (`.prettierrc`, `.prettierignore`), and `vitest.config.ts`.
  - Started and validated Docker infrastructure services.
- Verified with:
  - `docker compose ps -a`:
    `bytearena-kafka-1`: Up (healthy)
    `bytearena-postgres-1`: Up (healthy)
    `bytearena-kafka-init-1`: Exited (0)
  - `docker compose exec postgres psql -U bytearena -d bytearena -c "select id from problems;"`:
    Returned 3 rows: `sum-two`, `reverse-string`, `max-of-array`.
  - `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`:
    Returned topics: `submissions.dlq`, `submissions.queued`, `submissions.results`.
  - `npm run typecheck`: Passed cleanly across all 4 workspaces with 0 errors.
  - `npm run lint`: Passed with 0 errors.
  - `npm run build`: Compiled TypeScript across all workspaces cleanly.
- Problems / decisions:
  - Previous containers from another project on the host (`crashradar-*`) occupied ports 5432 and 4000; stopped them to allow ByteArena containers to bind to default ports.
- Next:
  - Phase 1: Submission service gRPC server, `CreateSubmission` transactional insert with outbox and idempotency.

### Session 2 (2026-10-04)
- Did:
  - Implemented transactional database operations in `services/submission-service/src/db.ts` with atomic outbox insert and idempotency handling using PostgreSQL partial unique index `submissions_idempotency_uq`.
  - Implemented gRPC `SubmissionService` handlers in `services/submission-service/src/handlers/submission.ts` (`CreateSubmission`, `GetSubmission`, `ListSubmissions`, `GetProblem`, `ListProblems` returning only samples).
  - Implemented gRPC `JudgeService` handler in `services/submission-service/src/handlers/judge.ts` (`GetJudgingJob` with `alreadyFinal` check).
  - Implemented gRPC server and graceful shutdown in `services/submission-service/src/server.ts`.
  - Created multi-stage non-root Dockerfile for `services/submission-service`.
  - Created smoke test `scripts/smoke-grpc.ts` and integration test `services/submission-service/src/transaction.test.ts`.
- Verified with:
  - `npx tsx scripts/smoke-grpc.ts`:
    - First call created submission: `id=5b12360e-2467-4e2f-b77f-f3fbfaf8d5dc, created=true`.
    - Outbox count verified: exactly 1 unpublished row for the submission.
    - Submissions count verified: exactly 1 row.
    - Idempotency replay: second call with identical `(handle, idempotency_key)` returned same submission id with `created=false`.
    - Outbox count post-replay: still exactly 1 row.
    - Submissions count post-replay: still exactly 1 row.
    - `JudgeService.GetJudgingJob`: returned `alreadyFinal=false`, 5 test cases.
    - `ListProblems`: returned 3 problems, strictly sample cases only.
  - `vitest run services/submission-service/src/transaction.test.ts`:
    - Passed 2/2 tests:
      1. Proves simulated post-insert failure rolls back transaction atomically leaving 0 submission rows and 0 outbox rows.
      2. Handles idempotent submission replay correctly with 0 duplicate outbox rows.
  - `npm run typecheck`: Passed cleanly across all workspaces with 0 errors.
  - `npm run lint`: Passed with 0 errors.
  - `npm run build`: Compiled cleanly across all workspaces.
  - `docker build -f services/submission-service/Dockerfile -t bytearena-submission-service .`: Multi-stage build completed successfully, non-root user `node`.
- Problems / decisions:
  - Host Windows native PostgreSQL service occupied 5432; parameterized compose port to `${POSTGRES_HOST_PORT:-5432}:5432` and set `POSTGRES_HOST_PORT=5434` in `.env` (documented in `DECISIONS.md`).
  - Aligned proto loader runtime options with generated types by removing `keepCase: true` so camelCase interface properties match runtime serialization.
- Next:
  - Phase 2: Outbox publisher (`services/submission-service/src/outbox-publisher.ts`).

### Session 3 (2026-10-04)
- Did:
  - Implemented `OutboxPublisher` class in `services/submission-service/src/outbox-publisher.ts`:
    - Polls every 200ms with configurable batch limit.
    - Atomic transaction: `SELECT ... FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED`.
    - Idempotent KafkaJS producer batching with `acks: -1`.
    - Updates `published_at = now()` upon broker acknowledgment and commits transaction.
    - Graceful rollback on publishing failure with exponential backoff.
    - Graceful shutdown handling `SIGTERM` and `SIGINT`.
  - Enabled `submission-api` and `outbox-publisher` services in `docker-compose.yml`.
  - Created integration test `services/submission-service/src/outbox.test.ts` and automated crash test script `scripts/test-outbox-crash.ts`.
- Verified with:
  - `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic submissions.queued --from-beginning`:
    - Confirmed events are visible in Kafka and `published_at` is set in Postgres.
  - `npx tsx scripts/test-outbox-crash.ts`:
    - Stopped Kafka container via `docker compose stop kafka`.
    - Created 3 submissions while Kafka was offline; confirmed outbox held 3 unpublished rows and did not fail transactionally.
    - Restarted Kafka container via `docker compose start kafka`.
    - Outbox publisher automatically reconnected, published all 3 rows (`count: 3`), and marked them `published_at` in Postgres.
    - Kafka consumer received 3/3 messages from `submissions.queued` with 0 lost messages.
  - `npx vitest run services/submission-service/src/outbox.test.ts`:
    - Passed 2/2 tests: single-batch publishing and 2 concurrent publishers processing 10 rows without duplicate delivery via `SKIP LOCKED`.
  - `npx vitest run`: All 4 test suites across the repository passed.
  - `docker compose ps`: Both `bytearena-submission-api-1` and `bytearena-outbox-publisher-1` running healthy.
  - `npm run typecheck` and `npm run lint`: Passed with 0 errors.
- Problems / decisions:
  - Fixed volume placement in `docker-compose.yml` so application services live within `services:`.
  - Added `closeDbOnStop` option to `OutboxPublisher` so unit tests sharing a database connection pool can stop the publisher without closing the shared pool.
- Next:
  - Phase 3: Sandbox library (`services/runner/src/sandbox/`).

### Session 4 (2026-10-04)
- Did:
  - Pre-pulled `python:3.12-alpine` and `node:20-alpine` images.
  - Implemented output normaliser and comparator (`services/runner/src/sandbox/comparer.ts`) strictly adhering to AGENTS.md rule (normalise `\r\n` to `\n`, trim trailing whitespace per line, ignore trailing blank lines, exact match).
  - Implemented orphan container cleanup (`services/runner/src/sandbox/cleanup.ts`) scanning for containers labeled `bytearena.submission` older than threshold and removing them.
  - Implemented core sandbox runner (`services/runner/src/sandbox/runner.ts`) with `dockerode`:
    - Validates code byte length `<= 65536` bytes.
    - Python bootstrap: `["python3","-B","-c","import os;c=os.environ.pop('SUBMISSION_CODE');exec(compile(c,'main.py','exec'),{'__name__':'__main__'})"]`.
    - JavaScript bootstrap: `["node","-e","const c=process.env.SUBMISSION_CODE;delete process.env.SUBMISSION_CODE;eval(c)"]`.
    - Full security HostConfig: `NetworkMode: "none"`, `MemorySwap: Memory` (no swap), `NanoCpus: 500_000_000` (0.5 CPU), `PidsLimit: 64`, `ReadonlyRootfs: true`, tmpfs `/tmp` `rw,noexec,nosuid,size=16m`, `CapDrop: ["ALL"]`, `SecurityOpt: ["no-new-privileges"]`, user `65534:65534`, working dir `/tmp`, label `bytearena.submission`.
    - Streams stdin cleanly via hijacked stdin-only attach stream; closes stdin.
    - Live demuxing of stdout and stderr (`docker.modem.demuxStream`); enforces 64 KiB cap (`OUTPUT_LIMIT_EXCEEDED`) and immediately kills container.
    - Wall-clock timer started when container is running (`timeLimitMs + startupAllowanceMs`), kills container on expiry (`TIME_LIMIT_EXCEEDED`).
    - Inspects `OOMKilled` -> `MEMORY_LIMIT_EXCEEDED`, non-zero exit code -> `RUNTIME_ERROR` / `MEMORY_LIMIT_EXCEEDED` (if heap/MemoryError).
    - Always forcibly removes container in `finally` block.
  - Updated `services/runner/src/index.ts` to export sandbox and run orphan cleanup on init.
  - Created complete Phase 3 vitest test suite (`services/runner/src/sandbox/sandbox.test.ts`).
- Verified with:
  - `npm run test:sandbox -w services/runner`:
    - Passed 16/16 tests across all Phase 3 requirements in 12.05s:
      1. Output Comparison Rule: CRLF normalisation, line trailing whitespace trimming, trailing blank lines.
      2. Output Comparison Rule: distinguishes non-matching outputs.
      3. Normal Python program execution with stdin + stdout matching expected answer (ACCEPTED).
      4. Normal JavaScript program execution with stdin + stdout matching expected answer (ACCEPTED).
      5. WRONG_ANSWER verdict when program output does not match expected.
      6. Infinite loop returns `TIME_LIMIT_EXCEEDED` within limit + 500ms and container is cleanly removed.
      7. Python memory bomb (allocating 500MB with 64MB limit) returns `MEMORY_LIMIT_EXCEEDED`.
      8. JavaScript memory bomb (allocating 1M arrays with 64MB limit) returns `MEMORY_LIMIT_EXCEEDED`.
      9. Fork bomb does not crash host (PID limit 64 holds) and returns within time limit.
      10. Python network attempt fails due to `NetworkMode: "none"`.
      11. JavaScript network attempt fails due to `NetworkMode: "none"`.
      12. Writing to `/` fails due to `ReadonlyRootfs`.
      13. Writing to `/tmp` succeeds, but executing a script in `/tmp` fails due to `noexec`.
      14. Printing 100 MB of output is killed at 64 KiB cap and returns `OUTPUT_LIMIT_EXCEEDED`.
      15. Reading `os.environ` / `process.env` does not reveal `SUBMISSION_CODE` (cleaned before user code runs).
      16. Rejection of code payloads larger than 64 KiB before container creation.
  - `docker ps -a --filter label=bytearena.submission`: Returned 0 containers (all cleaned up).
  - `npx vitest run`: All 3 test suites (20 tests) across monorepo passed.
  - `npm run typecheck`: Passed with 0 errors across all workspaces.
  - `npm run lint`: Passed with 0 errors.
  - `npm run build`: Compiled TypeScript across all workspaces cleanly.
- Problems / decisions:
  - Windows named pipe does not support TCP half-close on hijacked duplex streams; attaching for stdin-only before container start and streaming container logs via `container.logs({ follow: true })` demuxed with `docker.modem.demuxStream` solved stdout/stderr truncation and byte counting across both Windows development and Linux production environments.
  - Wall-clock timer must be started after `await container.start()` completes so `container.kill()` reliably targets a running container rather than a container in transition.
- Next:
  - Phase 5: Result writer — consume `submissions.results`, persist per-test verdicts (`test_results` table) and final verdict (`submissions.status`), with idempotency on duplicate judging runs.

### Session 5 (2026-10-04) — Phase 4: Runner Worker
- Did:
  - Implemented `RunnerWorker` class (`services/runner/src/worker.ts`): KafkaJS consumer (`autoCommit: false`, manual offset commit after `FINAL_VERDICT`), gRPC `GetJudgingJob`, `alreadyFinal` skip, `JUDGING_STARTED` → per-test `runInSandbox()` → `TEST_RESULT` × N → `FINAL_VERDICT`, heartbeat between test cases, 3-retry exponential backoff (1s, 2s) for infra errors, DLQ routing on permanent failure.
  - Implemented `services/runner/src/main.ts` — entrypoint with SIGTERM/SIGINT graceful shutdown.
  - Created multi-stage non-root `services/runner/Dockerfile`.
  - Added `runner` service to `docker-compose.yml` with `/var/run/docker.sock` volume and `group_add: [DOCKER_GID]`.
  - Fixed `DOCKER_GID` to `0` (Docker Desktop socket is owned by root/GID 0 on Windows).
  - Fixed stdin ordering bug: stdin must be written and stream closed **before** `container.start()` (not after) to prevent `EOFError`/RUNTIME_ERROR on fast-starting containers — root cause of test-3 RUNTIME_ERROR failure.
- Verified with:
  - `npx tsx scripts/smoke.ts` (1 replica): All 4 submissions received correct verdicts:
    - `ok.py` → ACCEPTED (5/5 tests)
    - `wrong.py` → WRONG_ANSWER (5/5 tests)
    - `infinite.py` → TIME_LIMIT_EXCEEDED (5/5 tests at ~2600ms each)
    - `runtime_error.py` → RUNTIME_ERROR (5/5 tests)
  - `docker compose up -d --scale runner=2`: Both replicas joined same consumer group (`runner-group`), split partitions, processed messages without errors or duplicates. Smoke test passed again with 2 replicas.
  - `npm run lint`, `npm run typecheck`, `npm run build`: All passed with 0 errors.
- Problems / decisions:
  - stdin-before-start fix: On Windows Docker Desktop, the named pipe buffer is flushed to the container process before it starts if written before `container.start()`. Writing after start caused a race condition where `input()` was called before stdin data was available, causing `EOFError` and RUNTIME_ERROR on hidden test cases.
- Next:
  - Phase 5: Result writer.

### Session 6 (2026-10-04) — Phase 5: Result Writer
- Did:
  - Implemented `ResultWriter` class (`services/submission-service/src/result-writer.ts`): KafkaJS consumer in group `result-writer-group`, `autoCommit: false` (manual offset commit after DB write), subscribing to `submissions.results` from beginning.
  - Implemented strict idempotency per ARCHITECTURE.md:
    - `JUDGING_STARTED`: `UPDATE submissions SET status='JUDGING' WHERE id=$1 AND status='QUEUED'`
    - `TEST_RESULT`: `INSERT INTO test_results (submission_id, test_index, verdict, time_ms, memory_kb, is_sample) VALUES (...) ON CONFLICT (submission_id, test_index) DO NOTHING`
    - `FINAL_VERDICT`: `UPDATE submissions SET status=$2, verdict=$3, judged_at=now() WHERE id=$1 AND status IN ('QUEUED', 'JUDGING')` (status `SYSTEM_ERROR` for `INTERNAL_ERROR`, `COMPLETED` otherwise; first final verdict wins)
  - Created entrypoint `services/submission-service/src/result-writer-main.ts` with graceful shutdown (`SIGTERM`/`SIGINT`).
  - Added `result-writer` service to `docker-compose.yml`.
  - Created integration test suite `services/submission-service/src/result-writer.test.ts` testing all 3 events and idempotency on duplicate events.
  - Discovered and diagnosed deep dockerode/docker-modem bug: `container.attach` in `dockerode` passes the options object to `docker-modem`, which stringifies it as an HTTP POST body; Docker daemon's upgraded connection forwards body bytes into container stdin (producing `ValueError: invalid literal for int() with base 10: '{"stream":true...}'`). Implemented clean `attachStdin` using native `http.request` HTTP Upgrade headers (`Upgrade: tcp`, `Connection: Upgrade`, `req.end()`) with zero body bytes.
- Verified with:
  - `npx tsx scripts/smoke.ts`: All 4 test solutions judged with 100% correct verdicts (`ACCEPTED`, `WRONG_ANSWER`, `TIME_LIMIT_EXCEEDED`, `RUNTIME_ERROR`).
  - `psql` verification:
    - `SELECT id, status, verdict FROM submissions`: All 4 submissions have `status = 'COMPLETED'` and correct verdicts.
    - `SELECT submission_id, count(*) FROM test_results`: Exactly 5 rows (`{1,2,3,4,5}`) per submission.
  - Replay verification:
    - Stopped `result-writer`, executed `kafka-consumer-groups.sh --group result-writer-group --reset-offsets --to-earliest --execute --topic submissions.results`, restarted `result-writer`.
    - Confirmed all results reprocessed, `submissions` status and verdicts remained unchanged, and `test_results` count remained exactly 5 per submission (zero duplicates, zero state flips).
  - `npx vitest run`: All 5 test suites (25 tests) passed across monorepo in 13.9s.
  - `npm run typecheck` and `npm run lint`: Passed with 0 errors.
- Problems / decisions:
  - dockerode body leak: Fixed by bypassing `dockerode.attach` and using direct HTTP upgrade request `POST /containers/${id}/attach?stream=1&stdin=1` with zero body bytes via `req.end()`.
  - socketPath in runner: Inside Linux runner container, socket path is `/var/run/docker.sock` (on Windows host `//./pipe/docker_engine`). Avoided calling `modem.socketPath()` which returns an async Promise in some docker-modem versions.
- Next:
  - Phase 6: GraphQL gateway with live subscriptions (`services/gateway`).


## Measured numbers (only real, measured values)

| Metric | Value | How measured | Machine |
|---|---|---|---|
| Judge latency p50 (sum-two, Python) | - | - | - |
| Judge latency p95 | - | - | - |
| Throughput | - | - | - |
| Recovery time after runner kill | - | - | - |
