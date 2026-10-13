"""可用维度在后续构建后仍保持固定构建和指标集合的映射。"""
from uuid import uuid4

import pytest

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.models.queries import AvailableDimensionsRequest
from tests.test_catalog_queries import ready_build
from tests.test_publication_queries import query_service
from tests.test_publication_storage import store as store


def test_recomputed_available_dimensions_keep_stable_option_ids(store, tmp_path):
    service, job, build = query_service(store, tmp_path)
    request = AvailableDimensionsRequest(metricResourceIds=["metric.sample.orders"])
    result = service.available_dimensions(build["build_id"], request, "platform")
    assert result.dimensions == service.fixture_available_dimensions.dimensions
    count = len(service.execute.calls)
    assert service.available_dimensions(build["build_id"], request, "platform") == result
    assert len(service.execute.calls) == count + 1
    with pytest.raises(ServiceError):
        service.available_dimensions(uuid4(), request, "platform")
    ready_build(store, tmp_path)
    assert service.available_dimensions(build["build_id"], request, "platform").dimensions == result.dimensions
