"""分支开发公开协议；执行凭据和绑定配置不对外暴露。"""

from enum import StrEnum
from uuid import UUID

from pydantic import Field

from ..publications.models import Contract

COMMIT_PATTERN = "^[a-f0-9]{40}$"


class BranchMode(StrEnum):
    """生产与开发预览的执行用途。"""

    PRODUCTION = "PRODUCTION"
    PREVIEW = "PREVIEW"


class BranchStatus(StrEnum):
    """持久化生命周期，支持外部 Git 操作中断恢复。"""

    PROVISIONING = "PROVISIONING"
    ACTIVE = "ACTIVE"
    DELETING = "DELETING"
    DELETED = "DELETED"
    FAILED = "FAILED"


class BranchView(Contract):
    # 项目与分支共同定位资源；git_ref 仅用于展示和受控 Git 操作。
    project_id: str
    branch_id: UUID
    git_ref: str
    # 模式和状态决定能否受理新的开发或发布请求。
    mode: BranchMode
    status: BranchStatus
    # 固定基线用于差异查看；未发布过的生产分支允许基线为空。
    base_commit_sha: str | None
    base_release_id: UUID | None
    # 已核实提交、成功发布与最新候选是三个独立事实。
    observed_head_sha: str | None
    active_release_id: UUID | None
    latest_release_id: UUID | None
    # 生命周期 CAS 版本；合并地址由服务端受控 Web 基址生成。
    version: int
    merge_request_url: str | None = None


class CreateBranchRequest(Contract):
    # 短分支名只允许生成 refs/heads 下的新引用。
    name: str = Field(min_length=1, max_length=200)
    # 来源分支与不可变提交共同约束创建基线。
    source_branch_id: UUID
    source_commit_sha: str = Field(pattern=COMMIT_PATTERN)
    # 重试必须携带相同键和相同输入。
    idempotency_key: str = Field(min_length=1, max_length=200)


class RegisterBranchRequest(Contract):
    # 显式登记远端已有分支，不能隐式接管同名引用。
    name: str = Field(min_length=1, max_length=200)
    # 用户确认的固定差异基线，必须在该分支历史中。
    base_commit_sha: str = Field(pattern=COMMIT_PATTERN)
    # 项目内操作键避免重复登记产生多份身份。
    idempotency_key: str = Field(min_length=1, max_length=200)
