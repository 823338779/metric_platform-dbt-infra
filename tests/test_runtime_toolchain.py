"""包拆分后，工具链指纹仍覆盖整个服务源码。"""

import pytest

import dbt_metricflow_service.runtime.service as runtime_service

SOURCE_PATHS = ("publications/service.py", "storage/jobs.py", "__init__.py")
RUNTIME_PATH = "runtime/service.py"
UTF8 = "utf-8"
INITIAL_SOURCE = "VALUE = 1\n"
UPDATED_SOURCE = "VALUE = 2\n"
PACKAGE_VERSION = "fixture"


@pytest.mark.parametrize("relative_path", SOURCE_PATHS)
def test_toolchain_detects_changes_outside_runtime_package(monkeypatch, tmp_path, relative_path):
    # 模拟分包后的源码目录，避免修改真实文件或依赖本机安装版本。
    runtime_file = tmp_path / RUNTIME_PATH
    runtime_file.parent.mkdir()
    runtime_file.write_text(INITIAL_SOURCE, encoding=UTF8)
    source = tmp_path / relative_path
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(INITIAL_SOURCE, encoding=UTF8)
    monkeypatch.setattr(runtime_service, "__file__", str(runtime_file))
    monkeypatch.setattr(runtime_service, "version", lambda _: PACKAGE_VERSION)

    # 其他业务包或包根变化也必须使 worker 的兼容标识失效。
    original = runtime_service.current_toolchain()
    assert runtime_service.current_toolchain() == original
    source.write_text(UPDATED_SOURCE, encoding=UTF8)
    assert runtime_service.current_toolchain() != original
