"""dbt 与 MetricFlow 共用的执行环境配置。"""
import os
from pathlib import Path

from dbt_metricflow_service import constants as strings

TARGET_DIRECTORY = strings.TARGET_DIRECTORY

def build_engine_environment(project: Path, profiles: Path, schema: str, target: str) -> dict[str, str]:
    """构建 dbt 与 MetricFlow 共用的引擎环境。"""

    return {
        **os.environ,
        strings.DBT_PROJECT_DIR: str(project),
        strings.DBT_PROFILES_DIR: str(profiles),
        strings.DBT_TARGET_PATH: str(project / TARGET_DIRECTORY),
        strings.DBT_PLATFORM_SCHEMA: schema,
        strings.DBT_SEND_ANONYMOUS_USAGE_STATS: strings.DBT_TELEMETRY_DISABLED,
        strings.DBT_TARGET: target,
        strings.PYTHONUTF8: strings.UTF8_ENV_ENABLED,
    }
