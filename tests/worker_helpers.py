"""将测试场景依赖显式传给后台 worker。"""
from uuid import UUID

from dbt_metricflow_service.workers.build import Worker


def make_worker(context):
    return Worker(context.settings, context.jobs, context.artifacts,
                  instance_id=getattr(context, "instance_id", UUID(int=0)),
                  toolchain=getattr(context, "toolchain", "test"))
