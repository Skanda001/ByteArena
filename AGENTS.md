# ByteArena: Builder Instructions

You are building **ByteArena**, a distributed online judge (LeetCode-style). Users submit code through GraphQL, it is queued through Kafka, run inside a locked-down Docker container, and per-test verdicts stream back live.

This file is the rulebook. Follow it exactly.

## Read order (every session)

1. `AGENTS.md` (this file)
2. `PROGRESS.md` (find out where the last session stopped)
3. `PLAN.md` (read **only the current phase**)
4. `docs/ARCHITECTURE.md` (when you need details on topics, flows, sandbox)

The human may switch between AI accounts mid-project. Never rely on chat memory. The repo files are the only memory.

## Fixed stack (do not substitute)

- Node 20, TypeScript with `strict: true`, npm workspaces monorepo
- Kafka via **KafkaJS**, broker in KRaft mode (no Zookeeper)
- gRPC via `@grpc/grpc-js` + `@grpc/proto-loader` (generate types with `proto-loader-gen-types`, which needs no `protoc` install)
- GraphQL via **graphql-yoga** (subscriptions over SSE, built in)
- PostgreSQL 16 via `pg` (no ORM, plain SQL)
- `zod` for validating every Kafka message and every external input
- `pino` for logging, `vitest` for tests, `tsx` for running TS in dev
- `dockerode` in the runner to drive the sandbox containers

Do not add NestJS, Prisma, TypeORM, Express or other frameworks. If you truly need a new dependency, add one line to `DECISIONS.md` explaining why.

## Source-of-truth contracts

These files are already written. Do **not** rewrite or silently change them:

- `proto/judge.proto`
- `schema/schema.graphql`
- `db/init.sql`
- `docs/ARCHITECTURE.md` (topics, event shapes, delivery semantics)

If a contract genuinely has to change: make the smallest change, add an entry to `DECISIONS.md` (what, why), and mention it in `PROGRESS.md`.

## Non-negotiable rules

### Sandbox security (the most important part)
Every untrusted submission runs in its own short-lived container created by the runner with ALL of:
- `NetworkMode: "none"`
- memory limit set, and `MemorySwap` equal to `Memory` (no swap)
- `NanoCpus` limit (0.5 CPU)
- `PidsLimit` (64)
- `ReadonlyRootfs: true` plus a small `/tmp` tmpfs (`noexec,nosuid`)
- `CapDrop: ["ALL"]`, `SecurityOpt: ["no-new-privileges"]`
- non-root user `65534:65534`
- a wall-clock timeout enforced by the runner, which kills the container
- an output cap of 64 KiB, after which the container is killed
- label `bytearena.submission=<id>` so leftovers can be cleaned up

Also:
- **Never** mount the Docker socket, any host path, or any secret into a sandbox container.
- **Never** build shell command strings from user data. Use dockerode or argument arrays only.
- The runner container itself holds the Docker socket (it is the trusted component). Document this trade-off in the README. Never give that socket to anything that runs user code.
- Sandbox containers are always removed in a `finally` block.

### Reliability
- Creating a submission writes the `submissions` row **and** the `outbox` row in **one DB transaction**. Never publish to Kafka directly from the request path.
- The outbox publisher uses `SELECT ... FOR UPDATE SKIP LOCKED`, publishes, then marks `published_at`. Delivery is **at-least-once**. Say so honestly in docs. Do not claim "exactly-once".
- The runner uses **manual offset commits**: commit only after judging is complete and results are published. Call `heartbeat()` between test cases.
- All consumers must be idempotent (see ARCHITECTURE.md, "Idempotency rules").
- A submission may be judged twice after a crash. The result must be the same, with no duplicate rows.

### Code quality
- No `any`. Validate all Kafka payloads with zod before use.
- Shared code (config, logger, kafka helpers, event schemas) lives in `packages/shared`.
- Config comes from environment variables, validated at startup. No hardcoded hosts.
- Every service handles `SIGTERM` gracefully (stop consuming, finish or abandon the current job, close connections).
- Every service has a `Dockerfile` (multi-stage, runs as non-root) and, where it makes sense, a healthcheck.

### Workflow
- Work on **one phase at a time**. Stop when its "Done when" checks pass.
- Run the "Done when" commands yourself and paste the real output summary into `PROGRESS.md`. Never tick a box you have not verified.
- Commit-sized steps. Keep `PROGRESS.md` updated at the end of every session, even a short one.
- If something in the plan is impossible or wrong, **do not improvise silently**. Write it in `DECISIONS.md`, pick the simplest alternative, and continue.
- Keep explanations of tricky code in short comments. The human must be able to read and explain this code in an interview.

### Honesty
- README and docs may only contain performance numbers that were measured by scripts in `scripts/bench/`. Record the method and machine.
- No claims like "millions of events per minute", "sub-millisecond", "exactly-once", or "secure against all attacks".

## Repository layout

```
bytearena/
├─ AGENTS.md  PLAN.md  PROGRESS.md  PROMPTS.md  README.md
├─ DECISIONS.md                       (create when first needed)
├─ docker-compose.yml  .env.example
├─ package.json  tsconfig.base.json
├─ proto/judge.proto
├─ schema/schema.graphql
├─ db/init.sql
├─ packages/shared/                   config, logger, kafka helpers, zod events, db pool, grpc loaders
├─ services/
│  ├─ submission-service/             gRPC server + outbox-publisher + result-writer (3 entrypoints, 1 package)
│  ├─ runner/                         Kafka consumer + sandbox + comparer
│  └─ gateway/                        GraphQL Yoga + Kafka-to-subscription bridge
├─ scripts/                           smoke.sh, chaos/, bench/
├─ tests/e2e/
├─ docs/                              ARCHITECTURE.md, INTERVIEW_NOTES.md
└─ .github/workflows/ci.yml
```

## Output comparison rule

Compare judge output to expected output after: normalising `\r\n` to `\n`, trimming trailing whitespace on every line, and ignoring trailing blank lines. Everything else must match exactly.

## Scope guard

Leaderboard, extra languages beyond Python and JavaScript, user accounts, admin UI and a web frontend are **out of scope** until Phase 8 is done and the human asks for them.
