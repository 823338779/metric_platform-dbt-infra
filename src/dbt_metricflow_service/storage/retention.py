"""同一环境只发布一个构建，事务内解除旧引用并回收独占快照。"""

from typing import Final

from sqlalchemy import exists, select, update
from sqlalchemy.orm import Session

from dbt_metricflow_service import constants as strings

from .artifacts import LIVE_ATTEMPT_STATES, ArtifactStore
from .entities import ArtifactSet, Build, DeploymentAttempt, DeploymentTarget, RuntimeAttempt, RuntimeJob

PENDING_STATUSES: Final = (strings.WAITING_FOR_BUILD, strings.PENDING, strings.CHECKING)
TERMINAL_BUILDS: Final = (strings.SUCCEEDED, strings.FAILED, strings.CANCELLED, strings.OUTCOME_UNKNOWN)


def retain_deployment(session: Session, environment: str, winner: str) -> None:
    # 同一事实事务内清除其余活动指针；版本变化使并发 Git 观察失效。
    session.execute(update(DeploymentTarget).where(
        DeploymentTarget.environment == environment, DeploymentTarget.active_build_id != winner,
    ).values(active_build_id=None, source_state=strings.UNDEPLOYED, version=DeploymentTarget.version + 1))

    # 当前构建、候选部署和仍未确认停止的执行需要其快照，直至成功切换或执行终结。
    jobs = session.scalars(select(RuntimeJob).join(Build, Build.run_id == RuntimeJob.job_id).where(
        Build.environment_id == environment, Build.build_id != winner, Build.build_status.in_(TERMINAL_BUILDS),
        RuntimeJob.status.in_((strings.SUCCEEDED, strings.FAILED)),
        # 失败终态无法部署；存量 WAITING_FOR_BUILD 不应永久持有已停止任务的输入。
        (Build.build_status != strings.SUCCEEDED) | ~exists().where(
            DeploymentAttempt.build_id == Build.build_id, DeploymentAttempt.deployment_status.in_(PENDING_STATUSES)),
        ~exists().where(RuntimeAttempt.job_id == RuntimeJob.job_id, RuntimeAttempt.state.in_(LIVE_ATTEMPT_STATES)),
    ).with_for_update(of=RuntimeJob)).all()
    if not jobs:
        return

    # 收集历史任务的输入、输出及重试暂存集合。任务触发器同步解除构建的输出引用。
    job_ids = [job.job_id for job in jobs]
    references = {ref for job in jobs for ref in (job.input_set_id, job.output_set_id) if ref}
    produced = select(RuntimeAttempt.attempt_id).where(RuntimeAttempt.job_id.in_(job_ids))
    sets = session.scalars(select(ArtifactSet).where(
        ArtifactSet.set_id.in_(references) | ArtifactSet.producer_attempt_id.in_(produced),
    ).order_by(ArtifactSet.kind, ArtifactSet.set_id)).all()
    for job in jobs:
        job.input_set_id = job.output_set_id = None
    session.flush()

    # EXECUTION 排在 SOURCE 前，先解除输出对源码的外键，再按全库引用核实独占性。
    for artifact in sets:
        ArtifactStore.delete_in_transaction(session, artifact.set_id)
