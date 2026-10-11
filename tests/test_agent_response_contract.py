"""新增 agent 路由必须有可导出的响应契约。"""

from types import SimpleNamespace

from fastapi import FastAPI

from dbt_metricflow_service.api.queries import router


def test_new_routes_publish_typed_response_schemas():
    app = FastAPI()
    runtime = SimpleNamespace(db=None, jobs=SimpleNamespace(db=None),
                              settings=SimpleNamespace(service_token=None))
    app.include_router(router(runtime, lambda: "service"))
    paths = app.openapi()["paths"]
    for path, method, status in [
        ("/v3/query/available-dimensions", "post", "200"),
        ("/v3/query", "post", "200"),
    ]:
        schema = paths[path][method]["responses"][status]["content"]["application/json"]["schema"]
        assert schema, path
