from __future__ import annotations

from services import ddl_apply_jobs


def test_job_type_for_frontend_aliases():
    assert ddl_apply_jobs.job_type_for_object_type("MVIEW") == "SYNC_MVIEW"
    assert ddl_apply_jobs.job_type_for_object_type("DBLINK") == "SYNC_DBLINK"
    assert ddl_apply_jobs.job_type_for_object_type("PACKAGE") == "SYNC_CODE"
    assert ddl_apply_jobs.job_type_for_object_type("INDEX") == "SYNC_INDEX"
