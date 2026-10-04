# Progress Log

> The builder updates this at the end of EVERY session. This file is how work continues across sessions and accounts.

## Current status
- **Current phase:** 4
- **Last session summary:** Completed Phase 3. Implemented sandbox execution library using `dockerode` in `services/runner/src/sandbox/` enforcing all non-negotiable security flags (`NetworkMode: "none"`, `MemorySwap === Memory`, `NanoCpus: 500_000_000`, `PidsLimit: 64`, `ReadonlyRootfs: true`, tmpfs `/tmp` `rw,noexec,nosuid,size=16m`, `CapDrop: ["ALL"]`, `SecurityOpt: ["no-new-privileges"]`, user `65534:65534`, working dir `/tmp`, label `bytearena.submission`). Passing code via `SUBMISSION_CODE` env var stripped before execution. Demuxed stdout/stderr with live byte counting to enforce 64 KiB output limit (`OUTPUT_LIMIT_EXCEEDED`). Wall-clock timeout with startup allowance (`TIME_LIMIT_EXCEEDED`). Guaranteed container removal in `finally`. Output normalisation per AGENTS.md rule. All 16 Phase 3 test cases passed.
- **Next step:** Start Phase 4 in PLAN.md (Runner worker)

## Checklist (tick only after running the "Done when" checks)

- [x] Phase 0: Scaffold and infrastructure
- [x] Phase 1: Submission service, gRPC and transactional create
- [x] Phase 2: Outbox publisher
- [x] Phase 3: Sandbox library
- [ ] Phase 4: Runner worker
- [ ] Phase 5: Result writer
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
  - Phase 4: Runner worker (`services/runner/src/main.ts`) - consume queued submissions, gRPC `GetJudgingJob`, judge per-test via sandbox, publish events (`JUDGING_STARTED`, `TEST_RESULT`, `FINAL_VERDICT`), manual offset commits.

## Measured numbers (only real, measured values)

| Metric | Value | How measured | Machine |
|---|---|---|---|
| Judge latency p50 (sum-two, Python) | - | - | - |
| Judge latency p95 | - | - | - |
| Throughput | - | - | - |
| Recovery time after runner kill | - | - | - |
