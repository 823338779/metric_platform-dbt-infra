"""同步选项在后续构建后仍保持固定构建和指标集合的映射。"""
from uuid import uuid4

import pytest

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.models.queries import AvailableDimensionsRequest
from tests.test_publication_queries import query_service
from tests.test_publication_storage import store as store
from tests.test_v3_catalog_queries import ready_build


def test_synchronous_options_reuse_same_mapping(store, tmp_path):
    service, job, build = query_service(store, tmp_path)
    request = AvailableDimensionsRequest(metricResourceIds=["metric.sample.orders"])
    result = service.available_dimensions(build["build_id"], request, "platform")
    assert result.dimensions == service.fixture_options.dimensions
    assert service.available_dimensions(build["build_id"], request, "platform") == result
    assert service.jobs.claim(str(uuid4()), toolchain_version=job["toolchain_version"], kinds=["QUERY_OPTIONS"]) is None
    with pytest.raises(ServiceError):
        service.available_dimensions(uuid4(), request, "platform")
    ready_build(store, tmp_path)
    assert service.available_dimensions(build["build_id"], request, "platform").dimensions == result.dimensions
