"""ORM 部署认领保持数据库跳锁行为并持久化扫描时间。"""

from uuid import uuid4

from sqlalchemy import select

from dbt_metricflow_service.storage.deployments import DeploymentStore
from dbt_metricflow_service.storage.entities import DeploymentAttempt
from tests.test_publication_storage import store as store
from tests.test_v3_build_acceptance import CALLER, request, service


def test_pending_skips_locked_attempts_and_persists_scan_time(store):
    app = service(store)
    build = app.submit(request(branchName="orm/" + uuid4().hex, deploymentPolicy="ON_SUCCESS"), CALLER)
    deployments = DeploymentStore(store.db)
    # 占住其他候选的行锁，使认领结果不依赖测试库已有记录的数量和先后顺序。
    with store.db.session() as session:
        session.scalars(
            select(DeploymentAttempt).where(DeploymentAttempt.build_id != str(build.build_id)).with_for_update()
        ).all()
        pending = deployments.pending()
    assert len(pending) == 1
    assert pending[0]["build_id"] == str(build.build_id)
    assert pending[0]["last_checked_at"] is not None
    assert deployments.initial(build.build_id)["last_checked_at"] == pending[0]["last_checked_at"]
    # 本候选被锁住时，应跳过它而不是等待该行锁。
    with store.db.session() as session:
        session.scalar(
            select(DeploymentAttempt).where(DeploymentAttempt.build_id == str(build.build_id)).with_for_update()
        )
        assert all(row["build_id"] != str(build.build_id) for row in deployments.pending())
