"""Routes for service-health metrics shown in Settings (and elsewhere)."""

from flask import Blueprint, jsonify

import services.services_metrics as svc

bp = Blueprint("services_metrics", __name__)


@bp.get("/api/services/metrics")
def get_metrics():
    """Aggregate status + lightweight metrics for the four configured services.

    Response shape: {
      oracle_source: {ok, host, version, cpu_pct, redo_bps, active_sessions, ...},
      oracle_target: {...},
      kafka:         {ok, brokers, topics, ...},
      kafka_connect: {ok, connectors: {total, running, failed, paused, ...}, ...},
    }
    Each side is best-effort — a failure on one doesn't sink the whole call.
    """
    return jsonify(svc.get_all_services_metrics())
