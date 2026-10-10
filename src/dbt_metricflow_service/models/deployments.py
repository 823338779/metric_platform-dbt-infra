"""版本部署的自然键、意图与当前指针。"""

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import Field, field_validator

from .builds import Contract, Environment, validate_branch


class DeploymentStatus(StrEnum):
    WAITING_FOR_BUILD = "WAITING_FOR_BUILD"
    PENDING = "PENDING"
    CHECKING = "CHECKING"
    DEPLOYED = "DEPLOYED"
    STALE = "STALE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class DeploymentKey(Contract):
    repository: str = Field(description="规范仓库地址")
    environment: Environment = Field(description="物理执行环境")
    branch_name: str = Field(description="目标分支短名")
    _branch = field_validator("branch_name")(validate_branch)


class DeploymentRequest(Contract):
    build_id: UUID = Field(description="已成功构建的版本")
    branch_name: str = Field(description="来源核对及部署目标分支")
    expected_target_version: int = Field(ge=0, description="受理前观察到的目标版本")
    idempotency_key: str = Field(min_length=1, max_length=256, description="部署请求幂等键")
    _branch = field_validator("branch_name")(validate_branch)


class DeploymentAttemptView(DeploymentKey):
    generation: int = Field(ge=1, description="受理顺序，不按完成时间分配")
    build_id: UUID = Field(description="该意图请求的构建")
    deployment_status: DeploymentStatus = Field(description="独立于构建的部署状态")
    reason: str | None = Field(default=None, description="未部署或过期原因")
    created_at: datetime = Field(description="意图受理时间")
    deployed_at: datetime | None = Field(default=None, description="实际切换时间")


class DeploymentTargetView(DeploymentKey):
    version: int = Field(default=0, description="目标乐观锁版本")
    desired_generation: int = Field(default=0, description="最新受理意图序号")
    active_build_id: UUID | None = Field(default=None, description="当前实际生效构建")
    active_commit_sha: str | None = Field(default=None, description="当前实际生效提交")
    latest_attempt: DeploymentAttemptView | None = Field(default=None, description="最新部署意图")
    observed_head_sha: str | None = Field(default=None, description="最近观察远端 head")
    head_observed_at: datetime | None = Field(default=None, description="远端观察时间")
    source_state: str = Field(default="UNDEPLOYED", description="远端观察结果，不替代部署终态")
