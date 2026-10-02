"""新增 agent 路由必须有可导出的响应契约。"""

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI

from dbt_metricflow_service.draft_validation_models import ValidationResult
from dbt_metricflow_service.publication_api import create_publication_router
from dbt_metricflow_service.publication_models import OptionsTask, QueryResultPage, QueryStatus


def test_shared_fixture_matches_typed_contract_and_hash_manifest():
    directory = Path(__file__).parent / "fixtures/agent_contract"
    manifest = json.loads((directory / "manifest.json").read_text("utf-8"))
    assert manifest["protocolVersion"] == "agent-dbt-v1"
    for name, digest in manifest["sha256"].items():
        assert hashlib.sha256((directory / name).read_bytes()).hexdigest() == digest
    fixture = json.loads((directory / "protocol-v1.json").read_text("utf-8"))
    for key, model in [("options", OptionsTask), ("status", QueryStatus), ("page", QueryResultPage),
                       ("validation", ValidationResult)]:
        model.model_validate(fixture[key])


def test_new_routes_publish_typed_response_schemas():
    app = FastAPI()
    app.include_router(create_publication_router(SimpleNamespace(db=None)))
    paths = app.openapi()["paths"]
    for path, method, status in [
        ("/v2/projects/{project_id}/query-option-jobs", "post", "202"),
        ("/v2/projects/{project_id}/query-option-jobs/{options_job_id}", "get", "200"),
        ("/v2/projects/{project_id}/queries/{query_id}/status", "get", "200"),
        ("/v2/projects/{project_id}/queries/{query_id}/results", "get", "200"),
    ]:
        schema = paths[path][method]["responses"][status]["content"]["application/json"]["schema"]
        assert schema, path
