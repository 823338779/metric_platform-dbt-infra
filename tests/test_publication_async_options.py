"""异步选项身份和结果在后续构建后保持稳定。"""
from uuid import uuid4

import pytest

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.models.queries import OptionsRequest
from dbt_metricflow_service.runtime.completion import complete_job
from tests.test_publication_queries import query_service
from tests.test_publication_storage import store as store
from tests.test_v3_catalog_queries import ready_build


def test_options_admission_deduplicates_and_reads_same_mapping(store, tmp_path):
    service, job, build = query_service(store, tmp_path)
    request = OptionsRequest(idempotencyKey=uuid4().hex, metricResourceIds=["metric.sample.orders"])
    accepted = service.submit_options(build["build_id"], request, "platform")
    assert accepted.state == "QUEUED"
    assert service.submit_options(build["build_id"], request, "platform").options_task_id == accepted.options_task_id
    child = service.jobs.claim(str(uuid4()), toolchain_version=job["toolchain_version"], kinds=["QUERY_OPTIONS"])
    existing = service.jobs.result(str(service.fixture_options.options_task_id))["payload_json"]
    assert complete_job(service.jobs, child["job_id"], child["lease_token"], existing)
    result = service.get_options(accepted.options_task_id)
    assert result.state == "SUCCEEDED"
    assert result.options == service.get_options(service.fixture_options.options_task_id).options
    with pytest.raises(ServiceError):
        service.get_options(uuid4())
    ready_build(store, tmp_path)
    assert service.get_options(accepted.options_task_id).options == result.options
