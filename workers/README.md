# Workers

The supported worker entrypoint is:

```bash
python worker.py
```

`worker.py` connects directly to the PostgreSQL state DB using `STATE_DB_DSN`.
It claims and processes:

- bulk load chunks
- baseline publish chunks
- CDC apply migrations
- data-compare chunks
- DDL apply jobs

The worker does not call Flask worker HTTP endpoints. Those endpoints and the
old `bulk_worker.py` / `cdc_apply_worker.py` names are legacy compatibility
surface only. The legacy scripts intentionally exit with an explanatory error
so a wrong process does not run half-configured.

Docker already uses the supported entrypoint through `Dockerfile.worker`.
