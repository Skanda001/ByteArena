# Architectural & Technical Decisions Log

This file records any deviations, operational trade-offs, or configuration decisions made during implementation, as required by `AGENTS.md`.

---

## 1. Parameterize PostgreSQL host port in `docker-compose.yml`

- **Date:** 2026-10-04
- **Context:** On Windows development environments, a host-native PostgreSQL 18 service (`postgresql-x64-18`) was running and bound to port 5432. The process could not be stopped without system administrator privileges.
- **Decision:** Parameterized the container port mapping in `docker-compose.yml` to `${POSTGRES_HOST_PORT:-5432}:5432`. Set `POSTGRES_HOST_PORT=5434` and `DATABASE_URL=postgres://bytearena:bytearena@localhost:5434/bytearena` in `.env`.
- **Impact:** Inside the Docker Compose network, services still talk to `postgres:5432`. Host development scripts (e.g. `smoke-grpc.ts`, vitest) connect cleanly to port 5434 without colliding with the native Windows PostgreSQL service.
