# ByteArena

A distributed online judge. Submit code over GraphQL, it is queued through Kafka, executed in locked-down Docker containers, and per-test verdicts stream back live.

**Stack:** TypeScript, GraphQL (Yoga), gRPC, Apache Kafka (KRaft), PostgreSQL, Docker Compose.

> Status: under construction. This README is completed in Phase 8 (see `PLAN.md`). Only measured numbers belong here.

## Planned sections (Phase 8)

- Architecture diagram (from `docs/ARCHITECTURE.md`)
- Run it: `docker compose up -d --build` then `scripts/smoke.sh`
- API examples (mutation, query, subscription)
- Sandbox security model and its known limits
- Reliability: transactional outbox, at-least-once delivery with idempotent consumers
- Chaos test results (runner kill, Kafka down, duplicate delivery)
- Measured benchmark table (method and machine recorded)
- Demo GIF
