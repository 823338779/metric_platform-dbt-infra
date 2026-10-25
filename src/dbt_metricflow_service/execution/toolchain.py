"""基于安装依赖和服务源码计算 worker 兼容标识。"""
import hashlib
from importlib.metadata import version
from pathlib import Path

from dbt_metricflow_service import constants as strings

TOOLCHAIN_PACKAGES = (
    strings.DBT_CORE_PACKAGE,
    strings.METRICFLOW_PACKAGE,
    strings.DBT_STARROCKS_PACKAGE,
    strings.DBT_DUCKDB_PACKAGE,
    strings.DBT_POSTGRES_PACKAGE,
)


def current_toolchain() -> str:
    """安装版本和服务代码共同决定执行兼容标识，避免不同代码误领旧任务。"""
    digest = hashlib.sha256()
    for package in TOOLCHAIN_PACKAGES:
        digest.update(f"{package}={version(package)}\n".encode())
    # 兼容标识覆盖整个服务源码。
    source_root = Path(__file__).resolve().parents[1]
    for source in sorted(source_root.rglob(strings.PYTHON_GLOB)):
        digest.update(source.relative_to(source_root).as_posix().encode())
        digest.update(source.read_bytes().replace(b"\r\n", b"\n"))
    return digest.hexdigest()
