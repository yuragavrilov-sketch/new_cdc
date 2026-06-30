"""Flask application entry-point — thin composition root.

Structure
---------
backend/
  app.py                  ← this file
  db/
    state_db.py           ← PostgreSQL helpers (config + migrations schema)
    oracle_browser.py     ← Oracle schema/table/column introspection
  routes/
    sse.py                ← SSE event bus
    config.py             ← service config + status API
    migrations.py         ← migrations CRUD + phase transitions
    oracle_db.py          ← Oracle DB browser API (wizard)
  services/
    checkers.py           ← Oracle / Kafka / Kafka-Connect availability probes
    status_poller.py      ← background status-poll thread
"""

import os
import sys
import threading
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, send_from_directory

load_dotenv(Path(__file__).parent / ".env")

sys.path.insert(0, os.path.dirname(__file__))   # make local packages importable

from services.kafka_logging import install_kafka_log_filters

install_kafka_log_filters()

# ── Static dir (React build) ──────────────────────────────────────────────────
STATIC_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path="")

# ── Database ──────────────────────────────────────────────────────────────────
from db.state_db import init_db, get_conn, row_to_dict, load_configs, save_config

_db_available = {"value": False}   # mutable dict — lets blueprints read live value


def _try_init_db() -> bool:
    try:
        init_db()
        _db_available["value"] = True
        print("[db] PostgreSQL state DB ready")
        return True
    except Exception as _e:
        print(f"[db] PostgreSQL unavailable ({_e})")
        return False


def _db_retry_loop() -> None:
    """Background thread: retry init_db every 5 s until success, then start orchestrator."""
    import time
    attempt = 1
    while not _try_init_db():
        print(f"[db] retry #{attempt} in 5 s...")
        attempt += 1
        time.sleep(5)
    # DB became available — start orchestrator if not already running
    if not orchestrator_mod.is_running():
        orchestrator_mod.start_orchestrator()


if not _try_init_db():
    print("[db] will retry in background every 5 s")

# ── Service checkers ──────────────────────────────────────────────────────────
from services.checkers import check_oracle, check_kafka, check_kafka_connect

_service_status: dict = {
    "oracle_source": {"status": "unknown", "message": "Not yet checked"},
    "oracle_target": {"status": "unknown", "message": "Not yet checked"},
    "kafka":         {"status": "unknown", "message": "Not yet checked"},
    "kafka_connect": {"status": "unknown", "message": "Not yet checked"},
}
_status_lock = threading.Lock()

_checkers = {
    "oracle_source": check_oracle,
    "oracle_target": check_oracle,
    "kafka":         check_kafka,
    "kafka_connect": check_kafka_connect,
}

# ── Convenience wrappers ──────────────────────────────────────────────────────
def _load_cfg():       return load_configs(_db_available["value"])
def _save_cfg(s, c):   save_config(s, c, _db_available["value"])

# ── SSE blueprint ─────────────────────────────────────────────────────────────
import routes.sse as sse_mod
from routes.sse import bp as sse_bp, broadcast, subscriber_count

sse_mod.init(_service_status, _status_lock)
app.register_blueprint(sse_bp)

# ── Config blueprint ──────────────────────────────────────────────────────────
import routes.config as config_mod
from routes.config import bp as config_bp

config_mod.init(
    db_available_ref=_db_available,
    mem_configs={},
    service_status=_service_status,
    status_lock=_status_lock,
    broadcast_fn=broadcast,
    load_configs_fn=_load_cfg,
    save_config_fn=_save_cfg,
    checkers=_checkers,
)
app.register_blueprint(config_bp)

# ── Migrations blueprint ──────────────────────────────────────────────────────
import routes.migrations as mig_mod
from routes.migrations import bp as mig_bp

mig_mod.init(
    get_conn_fn=get_conn,
    row_to_dict_fn=row_to_dict,
    db_available_ref=_db_available,
    broadcast_fn=broadcast,
    load_configs_fn=_load_cfg,
)
app.register_blueprint(mig_bp)

# ── Oracle DB browser blueprint ───────────────────────────────────────────────
import routes.oracle_db as oracle_mod
from routes.oracle_db import bp as oracle_bp

oracle_mod.init(load_configs_fn=_load_cfg)
app.register_blueprint(oracle_bp)

# ── Service-health metrics (Settings page) ────────────────────────────────────
from routes.services_metrics import bp as services_metrics_bp
app.register_blueprint(services_metrics_bp)

# ── Checklist blueprint ───────────────────────────────────────────────────────
import routes.checklist as checklist_mod
from routes.checklist import bp as checklist_bp

checklist_mod.init(get_conn_fn=get_conn, db_available_ref=_db_available)
app.register_blueprint(checklist_bp)

# ── Target preparation blueprint ──────────────────────────────────────────────
import routes.target_prep as target_prep_mod
from routes.target_prep import bp as target_prep_bp

target_prep_mod.init(load_configs_fn=_load_cfg)
app.register_blueprint(target_prep_bp)

# ── Data comparison blueprint ────────────────────────────────────────────────
import routes.data_compare as data_compare_mod
from routes.data_compare import bp as data_compare_bp

data_compare_mod.init(
    get_conn_fn=get_conn,
    load_configs_fn=_load_cfg,
    broadcast_fn=broadcast,
)
app.register_blueprint(data_compare_bp)

# ── Connector Groups blueprint ───────────────────────────────────────────────
import routes.connector_groups as cg_mod
from routes.connector_groups import bp as cg_bp

cg_mod.init(
    get_conn_fn=get_conn,
    row_to_dict_fn=row_to_dict,
    load_configs_fn=_load_cfg,
    broadcast_fn=broadcast,
)
app.register_blueprint(cg_bp)

# ── Planner blueprint ────────────────────────────────────────────────────────
import routes.planner as planner_mod
from routes.planner import bp as planner_bp

planner_mod.init(
    get_conn_fn=get_conn,
    row_to_dict_fn=row_to_dict,
    load_configs_fn=_load_cfg,
    broadcast_fn=broadcast,
)
app.register_blueprint(planner_bp)

# ── Catalog blueprint ────────────────────────────────────────────────────────
import routes.catalog as catalog_mod
from routes.catalog import bp as catalog_bp

catalog_mod.init(
    get_conn_fn=get_conn,
    row_to_dict_fn=row_to_dict,
    load_configs_fn=_load_cfg,
    broadcast_fn=broadcast,
)
app.register_blueprint(catalog_bp)

# ── Schema migrations (dashboard) ─────────────────────────────────────────────
import routes.schema_migrations as schema_mig_mod
from routes.schema_migrations import bp as schema_mig_bp

schema_mig_mod.init(
    get_conn_fn=get_conn,
    db_available_ref=_db_available,
    broadcast_fn=broadcast,
    load_configs_fn=_load_cfg,
)
app.register_blueprint(schema_mig_bp)

# ── Worker liveness ──────────────────────────────────────────────────────────
import routes.workers as workers_mod
from routes.workers import bp as workers_bp

workers_mod.init(
    get_conn_fn=get_conn,
    db_available_ref=_db_available,
)
app.register_blueprint(workers_bp)

# ── Status poller ─────────────────────────────────────────────────────────────
from services.status_poller import start_poller

start_poller(
    load_configs_fn=_load_cfg,
    checkers=_checkers,
    service_status=_service_status,
    status_lock=_status_lock,
    broadcast_fn=broadcast,
)

# ── Migration orchestrator ────────────────────────────────────────────────────
import services.debezium    as _debezium_mod
import services.orchestrator as orchestrator_mod

_debezium_mod.init(load_configs_fn=_load_cfg)
orchestrator_mod.init(
    get_conn_fn=get_conn,
    load_configs_fn=_load_cfg,
    broadcast_fn=broadcast,
)
# Wire manual trigger into migrations routes (late binding — orchestrator must be
# initialised first so trigger_indexes_enabling can reference _state["get_conn"]).
mig_mod._state["enable_indexes"]    = orchestrator_mod.trigger_indexes_enabling
mig_mod._state["enable_triggers"]  = orchestrator_mod.trigger_enable_triggers
mig_mod._state["list_trigger_jobs"] = orchestrator_mod.list_trigger_jobs
mig_mod._state["create_trigger_job"] = orchestrator_mod.create_trigger_job
mig_mod._state["run_trigger_job"] = orchestrator_mod.run_trigger_job
mig_mod._state["restart_baseline"] = orchestrator_mod.trigger_baseline_restart

if _db_available["value"]:
    orchestrator_mod.start_orchestrator()
else:
    print("[orchestrator] skipped — DB unavailable, will start when DB reconnects")
    threading.Thread(target=_db_retry_loop, daemon=True, name="db-retry").start()

# ── SPA catch-all ─────────────────────────────────────────────────────────────

@app.get("/")
@app.get("/<path:path>")
def spa(path: str = ""):
    target = os.path.join(STATIC_DIR, path)
    if path and os.path.isfile(target):
        return send_from_directory(STATIC_DIR, path)
    return send_from_directory(STATIC_DIR, "index.html")


@app.get("/api/health")
def health():
    return jsonify({
        "status":      "ok",
        "subscribers": subscriber_count(),
        "db":          _db_available["value"],
    })


# ── Dev server ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(
        host=os.environ.get("FLASK_HOST", "0.0.0.0"),
        port=int(os.environ.get("FLASK_PORT", "5000")),
        debug=os.environ.get("FLASK_DEBUG", "true").lower() == "true",
        threaded=True,
        use_reloader=False,
    )
