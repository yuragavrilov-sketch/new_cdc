"""Availability probes for Oracle, Kafka, and Kafka Connect."""


def check_oracle(cfg: dict) -> tuple[str, str]:
    host         = cfg.get("host", "").strip()
    port         = cfg.get("port", 1521)
    service_name = cfg.get("service_name", "").strip()
    user         = cfg.get("user", "").strip()
    password     = cfg.get("password", "")
    if not host or not service_name or not user:
        return "unknown", "Not configured"
    try:
        import oracledb
        conn = oracledb.connect(user=user, password=password, dsn=f"{host}:{port}/{service_name}")
        conn.close()
        return "up", "Connected"
    except ImportError:
        return "unknown", "oracledb not installed"
    except Exception as exc:
        return "down", str(exc)[:150]


def check_kafka(cfg: dict) -> tuple[str, str]:
    servers = cfg.get("bootstrap_servers", "").strip()
    if not servers:
        return "unknown", "Not configured"
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            bootstrap_servers=[s.strip() for s in servers.split(",")],
            request_timeout_ms=5000,
            connections_max_idle_ms=8000,
            enable_auto_commit=False,
        )
        consumer.topics()
        consumer.close()
        return "up", "Connected"
    except ImportError:
        return "unknown", "kafka-python not installed"
    except Exception as exc:
        return "down", str(exc)[:150]


def check_kafka_connect(cfg: dict) -> tuple[str, str]:
    url = cfg.get("url", "").strip()
    if not url:
        return "unknown", "Not configured"
    try:
        import requests as req
        r = req.get(f"{url.rstrip('/')}/", timeout=5)
        return ("up", f"HTTP {r.status_code}") if r.ok else ("down", f"HTTP {r.status_code}")
    except ImportError:
        return "unknown", "requests not installed"
    except Exception as exc:
        return "down", str(exc)[:150]
