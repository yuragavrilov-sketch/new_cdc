from services import schema_migrations


def test_cdc_apply_starting_is_active_cdc_phase():
    assert schema_migrations._phase_to_object_status("CDC_APPLY_STARTING", has_error=False) == "running"
    assert schema_migrations._aggregate_status(["CDC_APPLY_STARTING"], any_failed=False, paused=False) == "cdc"
    assert schema_migrations._aggregate_stage(["CDC_APPLY_STARTING"], any_failed=False) == "cdc"


def test_build_ddl_table_marks_column_diff_from_diff_json():
    obj = schema_migrations._build_ddl_object(
        "TABLE",
        "ORDERS",
        "VALID",
        "VALID",
        "DIFF",
        {"ok": False, "cols_missing": ["NEW_COL"], "cols_extra": [], "cols_type": ["AMOUNT"]},
    )

    assert obj["columnsDiff"] is True
    assert obj["columnDiffCounts"] == {
        "missing": 1,
        "extra": 0,
        "type": 1,
        "total": 2,
    }


def test_build_ddl_table_does_not_mark_non_column_diff():
    obj = schema_migrations._build_ddl_object(
        "TABLE",
        "ORDERS",
        "VALID",
        "VALID",
        "DIFF",
        {"ok": False, "idx_missing": ["IX_ORDERS"]},
    )

    assert obj["columnsDiff"] is False
    assert obj["columnDiffCounts"]["total"] == 0
