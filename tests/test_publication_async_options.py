"""冷选项先返回持久任务身份，完成后复用原 optionId。"""

from types import MethodType
from uuid import uuid4

from dbt_metricflow_service.publications.models import QueryOptionsRequest
from dbt_metricflow_service.runtime.service import Runtime
from tests.test_publication_queries import query_service
from tests.test_publication_storage import store as store


def test_options_admission_deduplicates_and_reads_same_mapping(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    assert hasattr(service, "submit_options"), "asynchronous query options are not implemented"
    # 保留真实 Runtime 队列逻辑，只有已发布目录采用现有受控 fixture。
    runtime = service.runtime
    for name in ("submit_options", "_submit_child", "_run"):
        setattr(runtime, name, MethodType(getattr(Runtime, name), runtime))
    request = QueryOptionsRequest(releaseId=release["release_id"], metricResourceIds=["metric.sample.orders"])
    accepted = service.submit_options(job["project_id"], request)
    assert accepted["state"] == "QUEUED"
    assert service.submit_options(job["project_id"], request)["optionsJobId"] == accepted["optionsJobId"]
    child = runtime.jobs.claim(str(uuid4()), toolchain_version=job["toolchain_version"], kinds=["QUERY_OPTIONS"])
    runtime.jobs.finish(child["job_id"], child["lease_token"], runtime.options(None, ("orders",)))
    result = service.get_options(job["project_id"], accepted["optionsJobId"])
    assert result["state"] == "READY"
    assert result["options"] == service.query_options(job["project_id"], request)["options"]
    import pytest

    from dbt_metricflow_service.publications.service import ReleaseGone
    from tests.test_publication_result_pages import queued
    from tests.test_publication_transaction import prepared

    with pytest.raises(KeyError):
        service.get_options("wrong-project", accepted["optionsJobId"])
    query = queued(service, job, release)
    jobs, candidate, _, output, _ = prepared(store, tmp_path, job["project_id"])
    jobs.finish(candidate["job_id"], candidate["lease_token"], output_set_id=output)
    with pytest.raises(ReleaseGone):
        service.get_options(job["project_id"], accepted["optionsJobId"])
    assert service.query_status(job["project_id"], query)["queryId"] == query
