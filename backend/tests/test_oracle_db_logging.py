from __future__ import annotations

import logging

from flask import Flask

from routes import oracle_db


def test_oracle_503_logs_context_without_passwords(monkeypatch, caplog):
    app = Flask(__name__)
    app.register_blueprint(oracle_db.bp)

    oracle_db.init(lambda: {
        "oracle_source": {
            "host": "10.200.103.50",
            "port": 1521,
            "service_name": "PAYDB",
            "user": "APP_USER",
            "password": "plain-secret",
            "owner_user": "OWNER_USER",
            "owner_password": "owner-secret",
        }
    })

    def fail_connect(db, configs, *, prefer_owner=False):
        assert db == "source"
        assert prefer_owner is True
        assert configs["oracle_source"]["host"] == "10.200.103.50"
        raise RuntimeError("ORA-01017: invalid username/password; logon denied")

    monkeypatch.setattr(oracle_db, "get_oracle_conn", fail_connect)
    caplog.set_level(logging.ERROR, logger="routes.oracle_db")

    response = app.test_client().get("/api/db/source/schemas")
    body = response.get_json()

    assert response.status_code == 503
    assert body["error"] == "ORA-01017: invalid username/password; logon denied"
    assert body["error_id"]

    logs = caplog.text
    assert body["error_id"] in logs
    assert "endpoint=/api/db/source/schemas" in logs
    assert "credential_mode': 'owner_user" in logs
    assert "10.200.103.50" in logs
    assert "PAYDB" in logs
    assert "OWNER_USER" in logs
    assert "plain-secret" not in logs
    assert "owner-secret" not in logs
