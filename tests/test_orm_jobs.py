"""构建队列只保存生命周期与产物引用，不保存执行结果。"""

import inspect

from dbt_metricflow_service.storage.entities import ArtifactSet, Base, RuntimeJob, RuntimeProject
from dbt_metricflow_service.storage.jobs import JobStore


def test_runtime_schema_has_no_obsolete_result_or_publication_storage():
    assert not ({"runtime_job_result", "runtime_branch", "runtime_branch_event",
                 "runtime_release", "runtime_release_relation"} & set(Base.metadata.tables))
    assert "current_output_set_id" not in RuntimeProject.__table__.c
    assert "kind" not in RuntimeJob.__table__.c
    assert "kind" in ArtifactSet.__table__.c
    assert not ({"branch_id", "input_mode", "pinned_instance_id", "input_lease_expires_at"}
                & set(RuntimeJob.__table__.c.keys()))
    assert not ({"catalog_json", "metadata"} & set(ArtifactSet.__table__.c.keys()))


def test_finish_accepts_status_and_artifact_reference_only():
    assert set(inspect.signature(JobStore.finish).parameters) == {
        "self", "job_id", "token", "succeeded", "error_code", "output_set_id", "seal"
    }
    assert not hasattr(JobStore, "result")
