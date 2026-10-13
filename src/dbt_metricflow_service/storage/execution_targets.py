"""物理目标占用属于执行事实，复用任务和 attempt，不维护环境注册表。"""

from sqlalchemy import func, select

from dbt_metricflow_service import constants as strings

from .entities import RuntimeAttempt, RuntimeJob
from .jobs import JobStore
from .postgres import Database

ACTIVE_STATES = (strings.EXECUTING, strings.EXPIRED_UNCONFIRMED)


class ExecutionTargetStore:
    def __init__(self, db: Database) -> None:
        self.db = db

    def claim(self, job_id: str, lease_token: str) -> bool:
        # 先沿用事实事务锁顺序，再锁物理目标；占用跨进程持久化。
        with self.db.fact_session() as session:
            if not JobStore(self.db)._authorized(session, job_id, lease_token):
                return False
            job = session.get(RuntimeJob, job_id)
            if not job.physical_target_key:
                return True
            session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(job.physical_target_key, 0))))
            owners = session.scalars(select(RuntimeJob).where(
                RuntimeJob.physical_target_key == job.physical_target_key,
                RuntimeJob.target_claimed.is_(True), RuntimeJob.job_id != job_id,
            ).with_for_update()).all()
            for owner in owners:
                unsafe = session.scalar(select(RuntimeAttempt.attempt_id).where(
                    RuntimeAttempt.job_id == owner.job_id, RuntimeAttempt.state.in_(ACTIVE_STATES),
                ).limit(1))
                if unsafe:
                    return False
                owner.target_claimed = False
            job.target_claimed = True
            return True

    def release(self, job_id: str, lease_token: str, stopped: bool) -> None:
        # 调用者声明 stopped 仍不足以释放，必须有对应 attempt 的持久停止证据。
        if not stopped:
            return
        with self.db.fact_session() as session:
            job = session.get(RuntimeJob, job_id, with_for_update=True)
            if not job or not job.target_claimed:
                return
            attempt = session.scalar(select(RuntimeAttempt).where(
                RuntimeAttempt.job_id == job_id, RuntimeAttempt.lease_token == lease_token,
            ))
            if (attempt and attempt.attempt_id == job.current_attempt_id
                    and attempt.state not in ACTIVE_STATES and attempt.stop_confirmed_at is not None):
                job.target_claimed = False
