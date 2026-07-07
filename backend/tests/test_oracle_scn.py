from __future__ import annotations

import sys
import types

from services import oracle_scn


def test_open_oracle_conn_applies_session_context(monkeypatch):
    class OracleConnStub:
        module = ""
        action = ""
        client_identifier = ""

    conn = OracleConnStub()
    monkeypatch.setitem(
        sys.modules,
        "oracledb",
        types.SimpleNamespace(connect=lambda **_kwargs: conn),
    )

    result = oracle_scn.open_oracle_conn(
        {
            "host": "db-host",
            "port": 1521,
            "service_name": "svc",
            "user": "usr",
            "password": "pwd",
        },
        {
            "module": "new_cdc.coordinator",
            "action": "prepare mid-1",
            "client_identifier": "migration=mid-1 table=TCBPAY.ORDERS",
        },
    )

    assert result is conn
    assert conn.module == "new_cdc.coordinator"
    assert conn.action == "prepare mid-1"
    assert conn.client_identifier == "migration=mid-1 table=TCBPAY.ORDERS"
