from __future__ import annotations

import logging

import pytest
from pydantic import ValidationError

from dbt_metricflow_service.models import DbtJobRequest, MetricFlowJobRequest

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("raw", [{}, {"orders.yml": ""}, {"orders.yml": " \n\t\u3000"}])
def test_blank_resources_keep_default_behavior(raw: dict[str, str]) -> None:
    assert DbtJobRequest(project="sales", command="parse", resources=raw).resources == {}


@pytest.mark.parametrize("raw", [" \nversion: [broken\n", "# comment", "{}"])
def test_nonblank_content_preserved(raw: str) -> None:
    request = DbtJobRequest(project="sales", command="parse", resources={"a.yml": raw})
    assert request.resources == {"a.yml": raw}


def test_mixed_resources_filter_only_blank_entries() -> None:
    request = MetricFlowJobRequest(
        project="sales",
        command="list_metrics",
        resources={"a.yml": " \n", "b.yml": "metrics: []\n"},
    )
    assert request.resources == {"b.yml": "metrics: []\n"}


@pytest.mark.parametrize(
    "raw",
    [
        None, [], {"a.yml": 1}, {"a.yml": {}}, {1: "x"}, {"../a.yml": "x"},
        {"a/b.yml": "x"}, {"a\\b.yml": "x"}, {"CON.yml": "x"}, {"a:Y.yml": "x"},
        {"a.YML": "x"}, {"A.yml": "", "a.yml": ""}, {"a.yml": "\ud800"},
        {"a" * 125 + ".yml": ""}, {f"a{i}.yml": "" for i in range(101)},
        {"a.yml": " " * (1024 * 1024 + 1)},
        {f"a{i}.yml": "x" * 1024 * 1024 for i in range(6)},
    ],
)
def test_rejects_invalid_resources(raw: object) -> None:
    with pytest.raises(ValidationError):
        DbtJobRequest(project="sales", command="parse", resources=raw)


def test_size_boundaries_and_input_copy() -> None:
    raw = {"a" * 124 + ".yml": "中" * 349525 + "x"}
    request = DbtJobRequest(project="sales", command="parse", resources=raw)
    assert request.resources == raw
    raw.clear()
    assert len(request.resources) == 1
    with pytest.raises(ValidationError):
        DbtJobRequest(project="sales", command="parse", resources={"a.yml": "中" * 349526})


def test_debug_accepts_only_blank_resources() -> None:
    assert DbtJobRequest(project="sales", command="debug", resources={"a.yml": " "}).resources == {}
    with pytest.raises(ValidationError):
        DbtJobRequest(project="sales", command="debug", resources={"a.yml": "{}"})


def test_worker_protocol_validates_kind_and_resources() -> None:
    from dbt_metricflow_service.resource_protocol import WorkerRequest

    payload = {"kind": "dbt", "request": {"project": "sales", "command": "parse", "resources": {"a.yml": "{}"}}}
    assert WorkerRequest.model_validate(payload).kind == "dbt"
    invalid_payloads = [
        dict(payload, kind="metricflow"),
        dict(payload, path="elsewhere"),
        {"kind": "dbt", "request": {"project": "sales", "command": "parse"}},
    ]
    for invalid in invalid_payloads:
        with pytest.raises(ValidationError):
            WorkerRequest.model_validate(invalid)
