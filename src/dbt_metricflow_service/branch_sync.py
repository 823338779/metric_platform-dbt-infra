"""用短数据库租约核对受控 Git，事件本身不携带可信发布输入。"""

import hashlib
import json
import logging
from uuid import uuid4

from psycopg2 import Error as DatabaseError

from .branch_models import CreateBranchRequest, RegisterBranchRequest
from .branches import OP_CREATE, OP_KIND, OP_REQUEST, BranchService
from .platform_bindings import ProjectBinding, observe_revision, validate_git_ref
from .publication import PublicationService

logger = logging.getLogger(__name__)
SCAN_LIMIT = 10
SCAN_LEASE_SECONDS = 300
KEY_PREFIX = "branch-input:"
ACTIVE = "ACTIVE"
PREVIEW = "PREVIEW"
PROVISIONING = "PROVISIONING"
DELETING = "DELETING"
FAILURE_LOG = "Branch synchronization failed: %s"
SQL_SIGNAL = """UPDATE runtime_branch SET signal_version=signal_version+1,
 scan_expires_at=CASE WHEN scan_token IS NULL THEN NULL ELSE scan_expires_at END
 WHERE project_id=%s AND git_ref=%s AND status='ACTIVE'"""
SQL_CANDIDATES = """SELECT project_id,branch_id FROM runtime_branch
 WHERE status IN ('ACTIVE','PROVISIONING','DELETING')
 AND (%s::text IS NULL OR project_id=%s) AND (scan_expires_at IS NULL OR scan_expires_at<clock_timestamp())
 AND binding_config ? 'remote' ORDER BY scan_expires_at NULLS FIRST,created_at LIMIT %s"""
SQL_CLAIM = """UPDATE runtime_branch SET scan_token=%s,
 scan_expires_at=clock_timestamp()+%s*interval '1 second'
 WHERE project_id=%s AND branch_id=%s AND status IN ('ACTIVE','PROVISIONING','DELETING')
 AND (scan_expires_at IS NULL OR scan_expires_at<clock_timestamp()) RETURNING *"""
SQL_RELEASE_LEASE = """UPDATE runtime_branch SET scan_token=NULL,
 processed_signal_version=CASE WHEN %s THEN GREATEST(processed_signal_version,%s) ELSE processed_signal_version END,
 scan_expires_at=CASE WHEN signal_version>%s THEN NULL ELSE clock_timestamp()+%s*interval '1 second' END
 WHERE branch_id=%s AND scan_token=%s"""
SQL_OBSERVED = """UPDATE runtime_branch SET observed_head_sha=%s,
 base_commit_sha=CASE WHEN mode='PRODUCTION' THEN COALESCE(base_commit_sha,%s) ELSE base_commit_sha END
 WHERE branch_id=%s AND scan_token=%s AND scan_expires_at>clock_timestamp() AND status='ACTIVE'
 AND publication_sequence=%s"""
SQL_EXTERNAL_DELETE = """UPDATE runtime_branch SET status='DELETED',version=version+1
 WHERE branch_id=%s AND scan_token=%s AND scan_expires_at>clock_timestamp() AND mode='PREVIEW' AND status='ACTIVE'"""


class BranchSynchronizer:
    """多实例只核对已登记分支，不按事件自动接管未知远端引用。"""

    def __init__(self, runtime):
        self.runtime = runtime
        self.service = BranchService(runtime)

    def signal(self, project_id: str, git_ref: str) -> None:
        # 持久提交信号版本后才能响应事件 202；未知和删除身份不会被复活。
        validate_git_ref(git_ref)
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_SIGNAL, (project_id, git_ref))

    def scan(self, *, project_id: str | None = None) -> None:
        # 每轮有界枚举，逐个取得租约；Git I/O 始终发生在事务之外。
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_CANDIDATES, (project_id, project_id, SCAN_LIMIT))
            candidates = cursor.fetchall()
        for candidate in candidates:
            token = str(uuid4())
            with self.runtime.db.transaction() as cursor:
                cursor.execute(SQL_CLAIM, (token, SCAN_LEASE_SECONDS, candidate["project_id"], candidate["branch_id"]))
                row = cursor.fetchone()
            if row is None:
                continue
            succeeded = False
            try:
                self._reconcile(row, token)
                succeeded = True
            except (ValueError, KeyError, RuntimeError, DatabaseError) as error:
                # 错误类型足够诊断可重试失败，不能泄漏远端凭据或数据库地址。
                logger.warning(FAILURE_LOG, type(error).__name__)
            finally:
                with self.runtime.db.transaction() as cursor:
                    cursor.execute(SQL_RELEASE_LEASE, (succeeded, row["signal_version"], row["signal_version"],
                                                        self.runtime.settings.branch_poll_seconds,
                                                        row["branch_id"], token))

    def _reconcile(self, row, token):
        # 中断的创建/删除先恢复原操作，绝不按新事件 payload 猜测状态。
        project_id, branch_id = row["project_id"], row["branch_id"]
        if row["status"] == PROVISIONING:
            operation = row["operation_json"]
            if operation[OP_KIND] == OP_CREATE:
                self.service.create(project_id, CreateBranchRequest.model_validate(operation[OP_REQUEST]))
            else:
                self.service.register(project_id, RegisterBranchRequest.model_validate(operation[OP_REQUEST]))
            return
        if row["status"] == DELETING:
            self.service.delete(project_id, branch_id, row["version"])
            return
        binding = row["binding_config"]
        configured = ProjectBinding(project_id, binding["remote"], binding["projectSubdir"],
                                    binding["profileBindingId"])
        head = self.service._remote_head(configured, row["git_ref"])
        if head is None:
            if row["mode"] != PREVIEW:
                raise ValueError("生产分支暂不可读取")
            with self.runtime.db.transaction() as cursor:
                cursor.execute(SQL_EXTERNAL_DELETE, (branch_id, token))
            return
        sha, digest = observe_revision(configured, self.runtime.settings.temp_root, git_ref=row["git_ref"])
        # 确定性身份包含全部构建输入；同一次失败输入不会被定时器无限重试。
        identity = [branch_id, sha, digest, row["config_version"], self.runtime.toolchain]
        key = KEY_PREFIX + hashlib.sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()
        release = PublicationService(self.runtime).submit(project_id, key, branch_id=branch_id,
                                                _observed=(sha, digest), _scan_token=token,
                                                _expected_sequence=row["publication_sequence"])
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_OBSERVED, (sha, sha, branch_id, token, release["sequence"]))
