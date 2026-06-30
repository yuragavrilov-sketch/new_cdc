# Backend

## State DB Migrations

The application runs Alembic migrations from `backend/migrations` during
startup through `db.state_db.init_db()`.

Manual upgrade:

```bash
cd backend
alembic upgrade head
```

The first revision, `0001_state_db_baseline`, delegates to the previous
idempotent bootstrap code so existing installations keep the same schema
behavior. New schema changes should be added as separate Alembic revisions.

Emergency fallback:

```bash
STATE_DB_USE_ALEMBIC=false python app.py
```

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest tests
```
