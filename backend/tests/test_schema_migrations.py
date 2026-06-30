from services import schema_migrations


def test_cdc_apply_starting_is_active_cdc_phase():
    assert schema_migrations._phase_to_object_status("CDC_APPLY_STARTING", has_error=False) == "running"
    assert schema_migrations._aggregate_status(["CDC_APPLY_STARTING"], any_failed=False, paused=False) == "cdc"
    assert schema_migrations._aggregate_stage(["CDC_APPLY_STARTING"], any_failed=False) == "cdc"
