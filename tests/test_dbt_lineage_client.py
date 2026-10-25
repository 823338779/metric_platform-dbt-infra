"""血缘使用已有 dbt ls，并将 selector 作为原生参数传递。"""

import json
from types import SimpleNamespace

import pytest
from dbt.cli.main import cli, dbtRunner
from dbt.exceptions import InvalidSelectorError

from dbt_metricflow_service.delegates.clients import InvalidDimensionsRequest
from dbt_metricflow_service.delegates.clients.dbt_client import DbtClient
from tests.test_publication_catalog import package


def test_custom_lineage_command_is_removed():
    assert "lineage" not in cli.commands
    assert "ls" in cli.commands


@pytest.mark.parametrize("selector", ["+orders+", "1+orders+1", "--help"])
def test_lineage_uses_native_ls_json_without_interpreting_manifest(tmp_path, monkeypatch, selector):
    nodes = [{"unique_id": "model.sample.orders", "depends_on": {"nodes": []}}]

    def invoke(self, args):
        assert self.manifest is not None
        assert args == ["ls", "--project-dir", str(tmp_path), "--profiles-dir", str(tmp_path),
                        "--select=" + selector, "--output", "json"]
        return SimpleNamespace(success=True, exception=None, result=[json.dumps(node) for node in nodes])

    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    assert DbtClient(json.dumps(package()).encode()).lineage(
        selector, project=tmp_path, profiles=tmp_path,
    ) == nodes


def test_lineage_empty_native_selection_returns_empty_list(tmp_path, monkeypatch):
    monkeypatch.setattr(dbtRunner, "invoke", lambda *_: SimpleNamespace(success=True, exception=None, result=[]))
    assert DbtClient(json.dumps(package()).encode()).lineage(
        "missing", project=tmp_path, profiles=tmp_path,
    ) == []


def test_lineage_preserves_native_failure(tmp_path, monkeypatch):
    failure = ValueError("native selector failure")
    monkeypatch.setattr(dbtRunner, "invoke", lambda *_: SimpleNamespace(success=False, exception=failure))
    with pytest.raises(ValueError) as caught:
        DbtClient(json.dumps(package()).encode()).lineage("bad", project=tmp_path, profiles=tmp_path)
    assert caught.value is failure


def test_lineage_maps_native_selector_error_to_request_error(tmp_path, monkeypatch):
    failure = InvalidSelectorError("unknown selector method")
    monkeypatch.setattr(dbtRunner, "invoke", lambda *_: SimpleNamespace(success=False, exception=failure))
    with pytest.raises(InvalidDimensionsRequest) as caught:
        DbtClient(json.dumps(package()).encode()).lineage("bogus:orders", project=tmp_path, profiles=tmp_path)
    assert caught.value.__cause__ is failure
