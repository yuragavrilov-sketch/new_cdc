# Changelog

## Unreleased

- Consolidated workers around the direct State DB universal worker
  (`workers/worker.py`).
- Removed the legacy Flask `/api/worker/*` runtime surface.
- Replaced legacy standalone `bulk_worker.py` and `cdc_apply_worker.py` with
  fail-fast compatibility stubs.
- Added optional `CONFIG_API_TOKEN` protection for `/api/config` reads, writes,
  and connection tests.
- Masked stored config secrets in API responses and preserved existing secrets
  when the UI submits `********`.
- Added Alembic State DB migration support with a baseline revision.
- Added backend smoke tests for config masking/token behavior, strategy parsing,
  and State DB serialization helpers.
- Documented `db_m/` as an experimental prototype outside the active runtime.
- Made `Strategy` compatible with Python 3.10 by using `str, Enum`.
