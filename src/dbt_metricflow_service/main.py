from __future__ import annotations

import logging

import uvicorn

from dbt_metricflow_service.api import create_app
from dbt_metricflow_service.jobs import JobRunner
from dbt_metricflow_service.projects import ProjectRegistry
from dbt_metricflow_service.settings import Settings

logger = logging.getLogger(__name__)

def run() -> None:
    """加载启动配置并创建运行依赖与 HTTP 服务。"""
    # 服务和管理命令使用相同配置来源，环境变量可覆盖部署参数。
    settings = Settings.from_file()
    app = create_app(
        settings,
        ProjectRegistry(settings.projects_root),
        JobRunner(
            settings.command_timeout_seconds,
            settings.max_output_bytes,
            job_artifacts_root=settings.job_artifacts_root,
        ),
    )
    # 监听参数随配置加载，避免端口仅能通过修改源码变更。
    uvicorn.run(app, host=settings.server_host, port=settings.server_port)


if __name__ == "__main__":
    run()
