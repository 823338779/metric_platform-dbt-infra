"""草稿输入的边界必须先于 Git / worker 副作用检查。"""

import importlib
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from tests.test_request_limits import _invoke


def contract():
    # 延迟加载使缺失实现显示为测试失败，后续断言仍测试实际公开契约。
    spec = importlib.util.find_spec("dbt_metricflow_service.draft_validation_models")
    assert spec is not None, "draft validation contract is not implemented"
    return importlib.import_module(spec.name)


def request(changes, **extra):
    return contract().DraftValidationRequest.model_validate({
        "baseCommitSha": "a" * 40, "idempotencyKey": "draft-1", "changes": changes, **extra,
    })


def change(path="models/订单.yml", **extra):
    return {"path": path, "operation": "CREATE", "content": "version: 2\n", **extra}


@pytest.mark.parametrize("path", ["../x.yml", "/models/x.yml", "models\\x.yml", "a//x.yml",
                                      "models/./x.yml", "models/x.sql", "a/\nx.yml", "C:/x.yml"])
def test_unsafe_paths_rejected(path):
    with pytest.raises(ValidationError):
        request([change(path)])


@pytest.mark.parametrize("changes", [
    [change("models/x.yml"), change("models/x.yml")],
    [change("models/X.yml"), change("models/x.yml")],
    [change(operation="DELETE", expectedSha256="a" * 64)],
    [change(operation="UPDATE")],
    [change(expectedSha256="a" * 64)],
    [change("models/x.yml", content="你" * (512 * 1024 // 3 + 1))],
    [change(f"models/{i}.yml") for i in range(101)],
    [change(f"models/{i}.yml", content="x" * (512 * 1024)) for i in range(11)],
])
def test_invalid_operation_sets_rejected(changes):
    with pytest.raises(ValidationError):
        request(changes)


def test_exact_byte_limits_and_empty_update_are_valid():
    assert len(request([change(content="x" * (512 * 1024))]).changes) == 1
    parsed = request([change(operation="UPDATE", expectedSha256="a" * 64, content="")])
    assert parsed.changes[0].content == ""
    with pytest.raises(ValidationError):
        request([change()], remote="https://untrusted.invalid")


def test_digest_is_order_independent_and_preserves_content_bytes():
    models = contract()
    changes = request([change(), change("models/z.yaml", operation="DELETE", content=None,
                                      expectedSha256="b" * 64)]).changes
    assert models.changes_digest(changes) == models.changes_digest(list(reversed(changes)))
    changed = [changes[0].model_copy(update={"content": "version: 2\r\n"}), changes[1]]
    assert models.changes_digest(changes) != models.changes_digest(changed)


def test_cross_language_digest_fixture():
    fixture = json.loads((Path(__file__).parent / "fixtures/agent_contract/changes-v1.json").read_text("utf-8"))
    assert contract().changes_digest(request(fixture["changes"]).changes) == fixture["digest"]


def test_validation_result_keeps_binding_and_validity_distinct_from_state():
    result = contract().ValidationResult(validation_id="11111111-1111-1111-1111-111111111111",
        state="SUCCEEDED", valid=False, base_commit_sha="a" * 40, changes_digest="b" * 64,
        project_subdir=".", config_version="1", toolchain_version="test", checked_levels=["YAML"])
    body = result.model_dump(mode="json", by_alias=True)
    assert body["valid"] is False
    assert body["baseCommitSha"] == "a" * 40
    assert body["checkedLevels"] == ["YAML"]


@pytest.mark.parametrize("path", ["/v2/projects/project/validations", "/v2/projects/project/queries",
                                 "/v2/projects/project/query-option-jobs"])
async def test_v2_oversize_body_is_rejected_before_application(path):
    called, sent, _ = await _invoke([], path=path, max_bytes=8 * 1024 * 1024,
                                    headers=[(b"content-length", b"8388609")])
    assert called is False
    assert sent[0]["status"] == 413
