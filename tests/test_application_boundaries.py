"""应用用例保持具体依赖，HTTP 和引擎执行不得反向渗透。"""

import ast
from pathlib import Path

PACKAGE = Path(__file__).parents[1] / "src" / "dbt_metricflow_service"


def imported_modules(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            parts = list(path.relative_to(PACKAGE).with_suffix("").parts[:-1])
            if node.level:
                parts = parts[: len(parts) - node.level + 1]
                yield ".".join(["dbt_metricflow_service", *parts, node.module or ""])
            else:
                yield node.module or ""


def test_application_dependency_graph_excludes_http_and_execution():
    queue = list((PACKAGE / "application").glob("*.py"))
    visited = set()
    forbidden = ("fastapi", "dbt_metricflow_service.bootstrap", "dbt_metricflow_service.workers",
                 "dbt_metricflow_service.execution.build", "dbt_metricflow_service.execution.query",
                 "subprocess", "dbt.cli")
    while queue:
        path = queue.pop()
        if path in visited:
            continue
        visited.add(path)
        for module in imported_modules(path):
            assert not any(module == prefix or module.startswith(prefix + ".") for prefix in forbidden), (path, module)
            # client 是明确的工具边界，业务层可以调用其方法，但不穿透到 SDK 实现。
            if module.startswith("dbt_metricflow_service.delegates.clients"):
                continue
            if module.startswith("dbt_metricflow_service."):
                dependency = PACKAGE.joinpath(*module.split(".")[1:]).with_suffix(".py")
                if dependency.is_file():
                    queue.append(dependency)


def test_engine_entrypoints_are_owned_by_clients():
    clients = PACKAGE / "delegates" / "clients"
    for path in PACKAGE.rglob("*.py"):
        imports = set(imported_modules(path))
        if any(module.startswith("dbt.cli") or module == "dbt_metricflow.cli.cli_configuration"
               for module in imports):
            assert path.is_relative_to(clients), path
        if path.is_relative_to(PACKAGE / "delegates"):
            assert not any(module.startswith((
                "fastapi", "dbt_metricflow_service.bootstrap", "dbt_metricflow_service.workers",
            )) for module in imports), path


def test_http_routes_have_no_sql_and_models_have_no_io():
    for path in (PACKAGE / "api").glob("*.py"):
        assert "exec_driver_sql" not in path.read_text(encoding="utf-8")
    for path in (PACKAGE / "models").glob("*.py"):
        imports = set(imported_modules(path))
        assert not imports.intersection({"os", "pathlib", "subprocess", "sqlalchemy", "fastapi"})
        tree = ast.parse(path.read_text(encoding="utf-8"))
        assert not any(
            isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"open", "exec", "eval"}
            for node in ast.walk(tree)
        )


def test_legacy_route_factories_are_removed():
    assert not (PACKAGE / "api/runtime.py").exists()
    assert not (PACKAGE / "publications/api.py").exists()
    assert not (PACKAGE / "validation/api.py").exists()


def test_execution_and_workers_do_not_depend_on_bootstrap():
    for directory in ("execution", "workers"):
        for path in (PACKAGE / directory).glob("*.py"):
            assert "dbt_metricflow_service.bootstrap" not in set(imported_modules(path)), path


def test_shared_execution_context_does_not_depend_on_orchestrators():
    for name in ("dbt", "embedded", "environment", "workspace", "threads", "log_writer", "errors"):
        imports = set(imported_modules(PACKAGE / "execution" / (name + ".py")))
        assert not imports & {
            "dbt_metricflow_service.execution.build", "dbt_metricflow_service.execution.query",
            "dbt_metricflow_service.workers.build", "dbt_metricflow_service.bootstrap",
        }
