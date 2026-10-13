"""新协议只描述引擎输入，不接受调用方项目、草稿或交付身份。"""

import pytest
from pydantic import ValidationError

REPOSITORY = "https://git.example.com/metrics.git"
SHA = "a" * 40
PREVIEW = "PREVIEW"
MAIN = "main"
FEATURE = "feature/revenue"
NONE = "NONE"
ON_SUCCESS = "ON_SUCCESS"
PRODUCTION = "PRODUCTION"
LEGACY_FIELDS = ("projectId", "branchId", "workspaceId", "projectSubdir", "releaseId", "runId", "jobId")


def build_body(**changes):
    import os
    from uuid import NAMESPACE_URL, uuid5

    from tests.test_execution_snapshot_storage import snapshot

    # 每个测试独立物理环境；同一用例内所有重试/并发仍使用相同环境。
    identity = uuid5(NAMESPACE_URL, os.environ.get("PYTEST_CURRENT_TEST", "fixture").split(" (")[0])
    execution = snapshot(environmentId=str(identity), configVersion=1, schema="test_" + identity.hex)
    return {
        "repository": REPOSITORY, "branchName": FEATURE, "commitSha": SHA,
        "execution": execution.model_dump(mode="json", by_alias=True),
        "idempotencyKey": "build-001", **changes,
    }


def test_build_defaults_and_full_sha():
    from dbt_metricflow_service.models.execution import SnapshotBuildRequest as BuildRequest

    assert BuildRequest.model_validate(build_body()).deployment_policy == NONE
    assert BuildRequest.model_validate(build_body(commitSha="b" * 64)).commit_sha == "b" * 64
    assert BuildRequest.model_validate(build_body(branchName=MAIN)).branch_name == MAIN


@pytest.mark.parametrize("field", LEGACY_FIELDS)
def test_legacy_fields_are_rejected(field):
    from dbt_metricflow_service.models.execution import SnapshotBuildRequest as BuildRequest

    with pytest.raises(ValidationError):
        BuildRequest.model_validate(build_body(**{field: "old"}))


@pytest.mark.parametrize(
    "changes",
    [
        {"commitSha": "abc"},
        {"branchName": "refs/heads/feature/a"},
        {"branchName": "../main"},
        {"executionBinding": "warehouse"},
        {"environment": PRODUCTION},
        {"environment": PRODUCTION, "branchName": MAIN, "commitSha": None},
        {"branchName": None, "deploymentPolicy": ON_SUCCESS},
        {"branchName": None, "commitSha": None},
        {"idempotencyKey": ""},
        {"idempotencyKey": "a" * 257},
    ],
)
def test_invalid_build_inputs(changes):
    from dbt_metricflow_service.models.execution import SnapshotBuildRequest as BuildRequest

    with pytest.raises(ValidationError):
        BuildRequest.model_validate(build_body(**changes))


def test_query_modes_are_strict_and_build_is_outside_body():
    from dbt_metricflow_service.models.queries import QueryRequest

    query = {"mode": "QUERY", "metricResourceIds": ["metric.revenue"]}
    assert QueryRequest.model_validate(query).limit == 1000
    for changed in (
        {"limit": 10001},
        {"offset": 0},
        {"buildId": SHA},
        {"releaseId": SHA},
        {"datasetResourceId": "table.a"},
        {"metricResourceIds": []},
    ):
        with pytest.raises(ValidationError):
            QueryRequest.model_validate({**query, **changed})
    with pytest.raises(ValidationError):
        QueryRequest.model_validate({**query, "mode": "PREVIEW", "datasetResourceId": "table.a"})


def test_available_dimensions_require_nonempty_metrics_without_task_key():
    from dbt_metricflow_service.models.queries import AvailableDimensionsRequest

    with pytest.raises(ValidationError):
        AvailableDimensionsRequest.model_validate({"metricResourceIds": []})
    assert AvailableDimensionsRequest.model_validate({"metricResourceIds": ["m", "m"]})


def test_exchange_fixture_matches_models():
    import json
    from pathlib import Path

    from dbt_metricflow_service.models import builds, catalog, deployments, execution, queries

    fixture = json.loads((Path(__file__).parent / "fixtures/contract.json").read_text(encoding="utf-8"))
    registry = {
        name: model
        for module in (builds, catalog, deployments, queries, execution)
        for name, model in vars(module).items()
        if isinstance(model, type) and hasattr(model, "model_validate")
    }
    for item in fixture["examples"]:
        assert (
            registry[item["model"]].model_validate(item["value"]).model_dump(mode="json", by_alias=True)
            == item["value"]
        )
    for body in fixture["invalidBuilds"]:
        with pytest.raises(ValidationError):
            execution.SnapshotBuildRequest.model_validate(body)
