from __future__ import annotations

import threading

from flask import Flask

from routes import config as config_routes


def make_client(monkeypatch, *, token: str = ""):
    if token:
        monkeypatch.setenv("CONFIG_API_TOKEN", token)
    else:
        monkeypatch.delenv("CONFIG_API_TOKEN", raising=False)

    store = {
        "oracle_source": {
            "host": "src-db",
            "user": "debezium",
            "password": "source-secret",
            "owner_password": "owner-secret",
        },
        "oracle_target": {},
        "kafka": {"bootstrap_servers": "broker:9092"},
        "kafka_connect": {"url": "http://connect:8083"},
    }

    def load_configs():
        return {k: dict(v) for k, v in store.items()}

    def save_config(service: str, body: dict):
        store[service] = dict(body)

    app = Flask(__name__)
    config_routes.init(
        db_available_ref={"value": True},
        mem_configs={},
        service_status={},
        status_lock=threading.Lock(),
        broadcast_fn=lambda _event: None,
        load_configs_fn=load_configs,
        save_config_fn=save_config,
        checkers={"oracle_source": lambda cfg: ("up", cfg["password"])},
    )
    app.register_blueprint(config_routes.bp)
    return app.test_client(), store


def test_get_config_masks_secrets(monkeypatch):
    client, _store = make_client(monkeypatch)

    response = client.get("/api/config")

    assert response.status_code == 200
    body = response.get_json()
    assert body["oracle_source"]["password"] == "********"
    assert body["oracle_source"]["owner_password"] == "********"
    assert body["oracle_source"]["host"] == "src-db"


def test_config_token_is_required_when_enabled(monkeypatch):
    client, _store = make_client(monkeypatch, token="secret-token")

    assert client.get("/api/config").status_code == 401

    response = client.get("/api/config", headers={"X-Config-Token": "secret-token"})
    assert response.status_code == 200


def test_save_preserves_existing_secret_when_mask_is_submitted(monkeypatch):
    client, store = make_client(monkeypatch, token="secret-token")

    response = client.post(
        "/api/config/oracle_source",
        json={"host": "new-src", "password": "********", "owner_password": "new-owner"},
        headers={"X-Config-Token": "secret-token"},
    )

    assert response.status_code == 200
    assert store["oracle_source"]["host"] == "new-src"
    assert store["oracle_source"]["password"] == "source-secret"
    assert store["oracle_source"]["owner_password"] == "new-owner"


def test_connection_test_resolves_secret_mask_before_checker(monkeypatch):
    client, _store = make_client(monkeypatch, token="secret-token")

    response = client.post(
        "/api/config/oracle_source/test",
        json={"password": "********"},
        headers={"X-Config-Token": "secret-token"},
    )

    assert response.status_code == 200
    assert response.get_json() == {"status": "up", "message": "source-secret"}
