# Interview Notes (read before you talk about this project)

You must be able to explain this system without the code open. Practise answering these out loud.

## 30-second pitch
"ByteArena is a distributed online judge. Users submit code over GraphQL. The submission is saved in Postgres together with an outbox row in one transaction, an outbox publisher moves it into Kafka, runner workers execute it in locked-down Docker containers, and per-test verdicts stream back live over GraphQL subscriptions. I tested it by killing workers mid-judging and showing no submission is lost or double-scored."

## Likely questions and the honest answers

**Why the transactional outbox?**
Writing to the DB and publishing to Kafka are two systems, so a crash between them either loses the event or publishes something that never committed (the dual-write problem). The outbox row is committed in the same transaction as the submission, so either both exist or neither does. A separate publisher then moves rows to Kafka.

**Is it exactly-once?**
No. It is at-least-once delivery plus idempotent consumers, which gives the same visible result. A crash after publishing but before marking the row can send a duplicate; primary keys and conditional updates make duplicates harmless.

**Why `FOR UPDATE SKIP LOCKED`?**
So several publisher instances can take different batches without blocking or double-publishing under normal operation.

**Why manual offset commit in the runner?**
Auto-commit could mark a message done before judging finished, so a crash would lose the submission. I commit only after `FINAL_VERDICT` is published. The price is that a crash causes a re-judge, which is why results are idempotent.

**What if a runner dies mid-judging?**
The offset was not committed, the consumer group rebalances after the session timeout, another runner gets the message, and judges it again. Duplicate `TEST_RESULT` events are dropped by the primary key. I measured recovery time with `scripts/chaos/kill-runner.sh` (quote the number from PROGRESS.md).

**Why gRPC between services and GraphQL at the edge?**
gRPC gives a typed, contract-first internal API (protobuf, HTTP/2). GraphQL gives clients flexible queries and subscriptions. The gateway translates between them.

**Why does the Kafka event not carry the code?**
Small events, one source of truth in Postgres, and hidden test data never travels through the broker. The runner fetches the job over gRPC.

**Explain each sandbox flag.**
- no network: user code cannot exfiltrate data or attack internal services
- memory limit with swap equal to memory: runaway allocation is OOM-killed instead of swapping the host
- CPU limit: one submission cannot starve others
- pids limit: stops fork bombs
- read-only rootfs + noexec tmpfs: cannot persist or run dropped binaries
- drop all capabilities + no-new-privileges + non-root user: cannot gain privileges inside the container
- wall-clock timeout + output cap: stops infinite loops and output floods
- label + cleanup: no leaked containers after crashes

**Why is the Docker socket in the runner? Isn't that dangerous?**
Yes, it is effectively root on the host. The runner is the trusted component and never executes user code itself; the socket is never mounted into a sandbox container. It is a known trade-off. Stronger options are gVisor, Firecracker microVMs or rootless Docker.

**How is time measured fairly?**
The timer starts when the container starts and allows a small startup allowance for the interpreter. It is wall-clock, so noisy neighbours can affect it. A production judge would pin CPUs and use cgroup CPU accounting.

**How does it scale?**
Add runner replicas; Kafka partitions (3) spread submissions across them, so useful parallelism is bounded by partition count. The gateway scales horizontally because each instance has its own consumer group. Postgres is the single stateful bottleneck.

**Why does each gateway instance use its own consumer group?**
Every gateway needs every result event to serve its own subscribers. A shared group would split events between instances.

**How do subscriptions avoid missing early results?**
Subscribe to the in-memory bus first, then fetch the DB snapshot, then stream live events, deduplicating by `(type, testIndex)`. That ordering closes the gap between snapshot and live stream.

**What would you do next?**
Stronger isolation (gVisor), compile-then-run languages, a leaderboard on a Redis sorted set, OpenTelemetry tracing across GraphQL, gRPC and Kafka headers, and CPU pinning for fairer timing.

## What not to claim
Anything you did not measure: throughput, "scales to thousands", "secure against all attacks", "exactly-once". Use only numbers from `scripts/bench/` and `PROGRESS.md`.
