"""Aggregate health + lightweight metrics for the four services exposed in
Settings: oracle_source, oracle_target, kafka, kafka_connect.

Each probe is wrapped so a single failure (config missing, network down, etc.)
just shows `{ok: false, error: ...}` on that tab — the others still render.
Timeouts kept tight (3–5s) so the Settings page never hangs.
"""

import time
from typing import Any


def _oracle_metrics(side: str) -> dict[str, Any]:
    t0 = time.time()
    try:
        from db.state_db import load_configs
        from db.oracle_browser import (
            get_oracle_conn, get_oracle_version,
            get_v_sysmetric,
        )

        configs = load_configs(True)
        cfg = configs.get(f"oracle_{side}") or {}
        if not cfg.get("host") or not cfg.get("service_name") or not cfg.get("user"):
            return {"ok": False, "error": "not configured"}

        conn = get_oracle_conn(side, configs)
        try:
            version = get_oracle_version(conn)
            metrics = get_v_sysmetric(conn)

            sessions = None
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(*) FROM v$session
                        WHERE  type = 'USER' AND status = 'ACTIVE'
                    """)
                    sessions = cur.fetchone()[0]
            except Exception:
                pass

            instance = None
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT instance_name, host_name, status FROM v$instance")
                    row = cur.fetchone()
                    if row:
                        instance = {"name": row[0], "host": row[1], "status": row[2]}
            except Exception:
                pass

            return {
                "ok": True,
                "host":         cfg.get("host"),
                "port":         cfg.get("port"),
                "service_name": cfg.get("service_name"),
                "version":      version.get("short", "unknown"),
                "instance":     instance,
                "cpu_pct":      round(float(metrics.get("Host CPU Utilization (%)") or 0), 1),
                "redo_bps":     int(metrics.get("Redo Generated Per Sec") or 0),
                "network_bps":  int(metrics.get("Network Traffic Volume Per Sec") or 0),
                "active_sessions": sessions,
                "rtt_ms":       int((time.time() - t0) * 1000),
            }
        finally:
            try: conn.close()
            except Exception: pass
    except Exception as exc:
        return {"ok": False, "error": str(exc), "rtt_ms": int((time.time() - t0) * 1000)}


def _kafka_metrics() -> dict[str, Any]:
    t0 = time.time()
    try:
        from db.state_db import load_configs

        configs = load_configs(True)
        cfg = configs.get("kafka") or {}
        bootstrap_str = (cfg.get("bootstrap_servers") or "").strip()
        if not bootstrap_str:
            return {"ok": False, "error": "not configured"}

        servers = [s.strip() for s in bootstrap_str.split(",") if s.strip()]

        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(
            bootstrap_servers=servers,
            request_timeout_ms=5000,
        )
        try:
            cluster = admin.describe_cluster()
            broker_count = len(cluster.get("brokers") or []) if isinstance(cluster, dict) else 0
            cluster_id = cluster.get("cluster_id") if isinstance(cluster, dict) else None
            controller = cluster.get("controller_id") if isinstance(cluster, dict) else None

            try:
                topics = admin.list_topics()
                topic_count = len(topics)
            except Exception:
                topic_count = 0
        finally:
            try: admin.close()
            except Exception: pass

        return {
            "ok": True,
            "bootstrap":   bootstrap_str,
            "brokers":     broker_count,
            "cluster_id":  cluster_id,
            "controller":  controller,
            "topics":      topic_count,
            "rtt_ms":      int((time.time() - t0) * 1000),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "rtt_ms": int((time.time() - t0) * 1000)}


def _kafka_connect_metrics() -> dict[str, Any]:
    t0 = time.time()
    try:
        import requests
        from db.state_db import load_configs

        configs = load_configs(True)
        cfg = configs.get("kafka_connect") or {}
        url = (cfg.get("url") or "").strip().rstrip("/")
        if not url:
            return {"ok": False, "error": "not configured"}

        root = requests.get(url, timeout=5).json()
        kc_version = root.get("version")
        cluster    = root.get("kafka_cluster_id")

        resp = requests.get(f"{url}/connectors?expand=status", timeout=5)
        resp.raise_for_status()
        data = resp.json()

        running = failed = paused = unassigned = 0
        if isinstance(data, dict):
            for _, info in data.items():
                state = (info.get("status") or {}).get("connector", {}).get("state", "UNKNOWN")
                if   state == "RUNNING":    running += 1
                elif state == "FAILED":     failed += 1
                elif state == "PAUSED":     paused += 1
                elif state == "UNASSIGNED": unassigned += 1
            total = len(data)
        elif isinstance(data, list):
            total = len(data)
        else:
            total = 0

        return {
            "ok": True,
            "url":       url,
            "version":   kc_version,
            "cluster_id": cluster,
            "connectors": {
                "total":      total,
                "running":    running,
                "failed":     failed,
                "paused":     paused,
                "unassigned": unassigned,
            },
            "rtt_ms": int((time.time() - t0) * 1000),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "rtt_ms": int((time.time() - t0) * 1000)}


def get_all_services_metrics() -> dict:
    """Aggregate response for /api/services/metrics."""
    return {
        "oracle_source": _oracle_metrics("source"),
        "oracle_target": _oracle_metrics("target"),
        "kafka":         _kafka_metrics(),
        "kafka_connect": _kafka_connect_metrics(),
    }
