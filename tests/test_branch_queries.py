"""查询幂等按分支隔离，已受理查询保留固定版本和 run。"""

from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dbt_metricflow_service.publications.models import PublishedQueryRequest, QueryOptionsRequest
from dbt_metricflow_service.publications.service import PublicationService, ReleaseGone
from tests.test_branch_catalog import published_pair
from tests.test_branch_publication import SQL_DELETE, SelectedStore
from tests.test_publication_catalog import build
from tests.test_publication_storage import store as store
from tests.test_publication_transaction import prepared

METRIC = "metric.sample.orders"
IDENTITY = "platform"
MODE = "QUERY"
OPTIONS = {"metrics": [{"name": "orders"}], "dimensions": [
    {"token": "order__region", "name": "region", "type": "categorical"}],
    "timeDimensions": [], "allowedFilters": ["="]}


def selected(runtime, branch_id):
    # 保留真实版本检查，模型目录 fixture 提供受支持指标和维度。
    runtime.settings = SimpleNamespace(command_timeout_seconds=60)
    runtime.options = lambda *_: deepcopy(OPTIONS)
    service = PublicationService(runtime, branch_id=branch_id)
    original = service._catalog

    def catalog(project, release):
        row, _ = original(project, release)
        content = build().model_dump(mode="json", by_alias=True)
        content.update(projectId=project, releaseId=release)
        return row, content

    service._catalog = catalog
    return service


def request(release, key):
    return PublishedQueryRequest(releaseId=release["release_id"], idempotencyKey=key,
                                 mode=MODE, metricResourceIds=[METRIC])


def test_same_key_in_two_branches_is_independent(store, tmp_path):
    runtime, project, a, b, ra, rb = published_pair(store, tmp_path)
    sa, sb = selected(runtime, a), selected(runtime, b)
    key = uuid4().hex
    qa = sa.submit_query(project, request(ra, key), IDENTITY)
    qb = sb.submit_query(project, request(rb, key), IDENTITY)
    assert qa["queryId"] != qb["queryId"]
    assert runtime.jobs.get(qa["queryId"])["parent_run_id"] == ra["run_id"]
    assert runtime.jobs.get(qb["queryId"])["parent_run_id"] == rb["run_id"]
    with pytest.raises(KeyError):
        sa.query_status(project, qb["queryId"])


def test_retry_after_release_switch_returns_original_query(store, tmp_path):
    runtime, project, a, _, ra, _ = published_pair(store, tmp_path)
    service = selected(runtime, a)
    body = request(ra, uuid4().hex)
    accepted = service.submit_query(project, body, IDENTITY)
    jobs, job, _, output, _ = prepared(SelectedStore(store.db, a), tmp_path, project)
    jobs.finish(job["job_id"], job["lease_token"], output_set_id=output)
    assert service.submit_query(project, body, IDENTITY) == accepted
    assert service.query_status(project, accepted["queryId"])["releaseId"] == ra["release_id"]
    with pytest.raises(ReleaseGone):
        service.submit_query(project, request(ra, uuid4().hex), IDENTITY)


def test_cross_branch_resource_and_option_are_rejected(store, tmp_path):
    runtime, project, a, b, ra, rb = published_pair(store, tmp_path)
    sa, sb = selected(runtime, a), selected(runtime, b)
    foreign = sb.query_options(project, QueryOptionsRequest(releaseId=rb["release_id"], metricResourceIds=[METRIC]))
    body = request(ra, uuid4().hex).model_dump(mode="json", by_alias=True)
    body["groupBy"] = [{"optionId": foreign["options"][0]["optionId"]}]
    with pytest.raises(ValueError):
        sa.submit_query(project, PublishedQueryRequest.model_validate(body), IDENTITY)
    with pytest.raises(KeyError):
        sa.resource(project, rb["release_id"], METRIC)


def test_deleted_branch_keeps_accepted_results_readable(store, tmp_path):
    runtime, project, a, _, ra, _ = published_pair(store, tmp_path)
    service = selected(runtime, a)
    body = request(ra, uuid4().hex)
    accepted = service.submit_query(project, body, IDENTITY)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(SQL_DELETE, (a,))
    assert service.query_status(project, accepted["queryId"])["releaseId"] == ra["release_id"]
    assert service.submit_query(project, body, IDENTITY) == accepted
    with pytest.raises(ReleaseGone):
        service.submit_query(project, request(ra, uuid4().hex), IDENTITY)
