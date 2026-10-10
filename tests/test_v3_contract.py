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
    # 固定请求样本不含连接秘密或 Agent 业务对象。
    return {
        "repository": REPOSITORY,
        "branchName": FEATURE,
        "commitSha": SHA,
        "environment": PREVIEW,
        "executionBinding": "warehouse",
        "configVersion": "1",
        "idempotencyKey": "build-001",
        **changes,
    }


def test_build_defaults_and_full_sha():
    from dbt_metricflow_service.models.builds import BuildRequest

    assert BuildRequest.model_validate(build_body()).deployment_policy == NONE
    assert BuildRequest.model_validate(build_body(commitSha="b" * 64)).commit_sha == "b" * 64
    assert BuildRequest.model_validate(build_body(branchName=None)).branch_name is None


@pytest.mark.parametrize("field", LEGACY_FIELDS)
def test_legacy_fields_are_rejected(field):
    from dbt_metricflow_service.models.builds import BuildRequest

    with pytest.raises(ValidationError):
        BuildRequest.model_validate(build_body(**{field: "old"}))


@pytest.mark.parametrize(
    "changes",
    [
        {"commitSha": "abc"},
        {"branchName": "refs/heads/feature/a"},
        {"branchName": "../main"},
        {"branchName": MAIN},
        {"environment": PRODUCTION},
        {"environment": PRODUCTION, "branchName": MAIN, "commitSha": None},
        {"branchName": None, "deploymentPolicy": ON_SUCCESS},
        {"branchName": None, "commitSha": None},
        {"idempotencyKey": ""},
        {"idempotencyKey": "a" * 257},
    ],
)
def test_invalid_build_inputs(changes):
    from dbt_metricflow_service.models.builds import BuildRequest

    with pytest.raises(ValidationError):
        BuildRequest.model_validate(build_body(**changes))


def test_query_modes_are_strict_and_version_is_in_path():
    from dbt_metricflow_service.models.queries import QueryRequest

    query = {"mode": "QUERY", "metricResourceIds": ["metric.revenue"], "idempotencyKey": "q"}
    assert QueryRequest.model_validate(query).limit == 1000
    for changed in (
        {"limit": 10001},
        {"buildId": SHA},
        {"releaseId": SHA},
        {"datasetResourceId": "table.a"},
        {"metricResourceIds": []},
    ):
        with pytest.raises(ValidationError):
            QueryRequest.model_validate({**query, **changed})
    with pytest.raises(ValidationError):
        QueryRequest.model_validate({**query, "mode": "PREVIEW", "datasetResourceId": "table.a"})


def test_options_require_idempotency_and_nonempty_metrics():
    from dbt_metricflow_service.models.queries import OptionsRequest

    with pytest.raises(ValidationError):
        OptionsRequest.model_validate({"idempotencyKey": "o", "metricResourceIds": []})
    assert OptionsRequest.model_validate({"idempotencyKey": "o", "metricResourceIds": ["m", "m"]})


def test_exchange_fixture_matches_models():
    import json
    from pathlib import Path

    from dbt_metricflow_service.models import builds, catalog, deployments, queries

    fixture = json.loads((Path(__file__).parent / "fixtures/v3/contract.json").read_text(encoding="utf-8"))
    registry = {
        name: model
        for module in (builds, catalog, deployments, queries)
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
            builds.BuildRequest.model_validate(body)
