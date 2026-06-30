# Oracle to Oracle Migration Coordinator

This repository contains the active migration coordinator stack:

- `backend/` - Flask API, state-machine orchestrator, State DB migrations,
  and the React static host.
- `frontend/` - Vite/React UI.
- `workers/` - universal worker process.
- `docker-compose.yml` - local/runtime composition for PostgreSQL,
  coordinator, and workers.

## Current Runtime Contract

The supported worker entrypoint is:

```bash
python workers/worker.py
```

Workers connect directly to the PostgreSQL State DB through `STATE_DB_DSN`.
They do not call Flask `/api/worker/*` endpoints; that legacy HTTP worker API
has been removed from the active backend.

The universal worker handles:

- bulk load chunks
- baseline publish chunks
- CDC apply migrations
- data-compare chunks
- DDL apply jobs

## Configuration API

`/api/config` masks stored secrets when returning configuration to the UI.
The fields `password` and `owner_password` are returned as `********`.

Set `CONFIG_API_TOKEN` to require `X-Config-Token` for config reads, writes,
and connection tests:

```bash
CONFIG_API_TOKEN=change-me
```

Leave it empty for local development without config API protection.

## State DB Migrations

The backend uses Alembic for State DB migrations. The first baseline revision
delegates to the previous idempotent bootstrap, so existing deployments keep
the same schema behavior.

Manual upgrade:

```bash
cd backend
alembic upgrade head
```

Emergency fallback:

```bash
STATE_DB_USE_ALEMBIC=false python app.py
```

## Tests

Backend smoke tests:

```bash
cd backend
pip install -r requirements-dev.txt
python -m pytest tests
```

Frontend build:

```bash
cd frontend
npm run build
```

CDC runtime smoke check:

```powershell
.\scripts\cdc-smoke-check.ps1 -Strict
```

The check is read-only. It verifies that the coordinator API is reachable, the
State DB is available, the universal worker heartbeat is fresh with CDC
capability, and the configured Oracle/Kafka/Kafka Connect services respond.

## Experimental Prototype

`db_m/` is a separate experimental prototype with its own nested Git metadata,
Flask/SQLAlchemy/Alembic stack, and database URL. It is not part of the active
root runtime unless code is explicitly ported.

## Release Artifacts

Root-level release binaries/scripts such as `m-coordinator`, `m-worker`, and
`1.bat` are release artifacts, not source entrypoints. Keep source changes and
release artifacts in separate commits when possible.
