# Architectural & Technical Decisions Log

This file records any deviations, operational trade-offs, or configuration decisions made during implementation, as required by `AGENTS.md`.

---

## 1. Parameterize PostgreSQL host port in `docker-compose.yml`

- **Date:** 2026-10-04
- **Context:** On Windows development environments, a host-native PostgreSQL 18 service (`postgresql-x64-18`) was running and bound to port 5432. The process could not be stopped without system administrator privileges.
- **Decision:** Parameterized the container port mapping in `docker-compose.yml` to `${POSTGRES_HOST_PORT:-5432}:5432`. Set `POSTGRES_HOST_PORT=5434` and `DATABASE_URL=postgres://bytearena:bytearena@localhost:5434/bytearena` in `.env`.
- **Impact:** Inside the Docker Compose network, services still talk to `postgres:5432`. Host development scripts (e.g. `smoke-grpc.ts`, vitest) connect cleanly to port 5434 without colliding with the native Windows PostgreSQL service.

---

## 2. DOCKER_GID=0 for Docker Desktop on Windows / WSL2

- **Date:** 2026-10-04
- **Context:** In Docker Desktop (WSL2 backend), `/var/run/docker.sock` has `uid: 0 (root)`, `gid: 0 (root)` and permissions `0660`. The default Linux Docker GID (typically 999) caused `connect EACCES /var/run/docker.sock` when runner ran as non-root user `node`.
- **Decision:** Set `DOCKER_GID=0` in `.env` so `group_add` grants the runner container's `node` user permission to communicate with Docker over `/var/run/docker.sock`.
- **Impact:** Runner can create and drive sandbox containers securely while still executing within the container as non-root user `node`.

---

## 3. Native HTTP Upgrade `attachStdin` bypassing docker-modem body leak

- **Date:** 2026-10-04
- **Context:** Calling `container.attach({ stream: true, stdin: true, hijack: true })` in `dockerode` delegates to `docker-modem`, which stringifies the options object as an HTTP POST body. Because Docker daemon upgrades the socket to TCP stream mode, any body bytes in the HTTP request (`{"stream":true...}`) get piped directly into the container's stdin, corrupting the first test input and causing intermittent `ValueError: invalid literal for int() with base 10` or syntax errors.
- **Decision:** Implemented `attachStdin` using native Node.js `http.request` with `Upgrade: tcp` and `Connection: Upgrade`, ending the HTTP handshake with `req.end()` (zero body bytes).
- **Impact:** Stdin input to sandbox containers is completely clean and deterministic across all executions.

