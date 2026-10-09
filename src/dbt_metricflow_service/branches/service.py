"""受控 Git 分支生命周期；外部操作有持久意图，网络调用不占数据库事务。"""

import shutil
import tempfile
from pathlib import Path
from urllib.parse import quote, urlsplit
from uuid import uuid4

from psycopg2.extras import Json

from ..platform.bindings import (
    GIT_BARE,
    GIT_FETCH,
    GIT_HEAD,
    GIT_INIT,
    GIT_NO_TAGS,
    GIT_SEPARATOR,
    HEADS_PREFIX,
    ProjectBinding,
    _git,
    resolve_draft_revision,
    validate_git_ref,
)
from ..storage.artifacts import ArtifactStore
from ..storage.branches import SQL_BRANCH_LOCK, BranchStore
from ..storage.jobs import StoreConflict
from .models import BranchMode, BranchStatus, BranchView, CreateBranchRequest, RegisterBranchRequest

SQL_OPERATION = "SELECT * FROM runtime_branch WHERE project_id=%s AND operation_key=%s"
SQL_LOCK_PROJECT = "SELECT * FROM runtime_project WHERE project_id=%s FOR UPDATE"
SQL_LIVE_REF = "SELECT 1 FROM runtime_branch WHERE project_id=%s AND git_ref=%s AND status<>'DELETED'"
SQL_INSERT = """INSERT INTO runtime_branch(branch_id,project_id,git_ref,mode,status,base_commit_sha,
 base_release_id,production_base_release_id,base_input_set_id,binding_config,config_version,operation_key,operation_json)
 VALUES(%s,%s,%s,'PREVIEW','PROVISIONING',%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"""
SQL_BASE_RELEASE = """SELECT release_id FROM runtime_release WHERE project_id=%s
 AND request_json->>'commitSha'=%s AND artifact_set_id IS NOT NULL AND state='PUBLISHED'
 AND (%s::uuid IS NULL OR branch_id=%s::uuid) ORDER BY published_at DESC LIMIT 1"""
SQL_ACTIVATE = """UPDATE runtime_branch SET status='ACTIVE',version=version+1,
 observed_head_sha=%s,signal_version=signal_version+1 WHERE branch_id=%s AND status='PROVISIONING' RETURNING *"""
SQL_DELETING = """UPDATE runtime_branch SET status='DELETING',version=version+1,observed_head_sha=%s
 WHERE branch_id=%s RETURNING *"""
SQL_DELETED = """UPDATE runtime_branch SET status='DELETED',version=version+1
 WHERE branch_id=%s AND status='DELETING' RETURNING *"""
JSON_MODE = "json"
OP_CREATE = "CREATE"
OP_REGISTER = "REGISTER"
OP_KIND = "kind"
OP_REQUEST = "request"
GIT_LS_REMOTE = "ls-remote"
GIT_REFS = "--refs"
GIT_PUSH = "push"
GIT_LEASE = "--force-with-lease="
GIT_ATOMIC = "--atomic"
OWNERSHIP_PREFIX = "refs/agent-branches/"
GIT_MERGE_BASE = "merge-base"
GIT_ANCESTOR = "--is-ancestor"
REFSPEC_SEPARATOR = ":"
EMPTY = ""
TEMP_PREFIX = "branch-"
SCHEMA_PREFIX = "dbt_dev_"
WEB_URL = "gitWebUrl"
WEB_SCHEMES = frozenset({"http", "https"})
COMPARE_PATH = "/compare/main..."
SLASH = "/"


def branch_view(row: dict) -> BranchView:
    # 只拣选公开字段，内部绑定和操作记录不能序列化到 API。
    value = {key: row[key] for key in BranchView.model_fields if key in row}
    base = row["binding_config"].get(WEB_URL)
    if base and row["mode"] == BranchMode.PREVIEW:
        parsed = urlsplit(base)
        if (parsed.scheme in WEB_SCHEMES and parsed.hostname and not parsed.username and not parsed.password
                and not parsed.query and not parsed.fragment):
            value["merge_request_url"] = (base.rstrip(SLASH) + COMPARE_PATH
                                          + quote(row["git_ref"].removeprefix(HEADS_PREFIX), safe=EMPTY))
    return BranchView.model_validate(value)


class BranchService:
    """会话不拥有分支；分支由项目控制并有可恢复生命周期。"""

    def __init__(self, runtime):
        # 复用现有存储，不启动独立调度器。
        self.runtime = runtime
        self.store = BranchStore(runtime.db)

    def list(self, project_id: str) -> list[BranchView]:
        # 先核实项目存在，避免未知项目返回假空列表。
        self.store.production(project_id)
        return [branch_view(row) for row in self.store.list(project_id)]

    def get(self, project_id: str, branch_id: str) -> BranchView:
        return branch_view(self.store.get(project_id, branch_id))

    def _binding(self, project_id):
        # Git remote 只从受控项目配置读取，客户端没有覆盖入口。
        project = self.runtime.jobs.project(project_id)
        if not project:
            raise KeyError(project_id)
        config = project["binding_config"]
        return ProjectBinding(project_id, config["remote"], config["projectSubdir"], config["profileBindingId"])

    def _remote_head(self, binding, git_ref, *, ownership=False):
        # ls-remote 成功但无记录才算不存在；网络错误绝不能当成删除。
        root = self.runtime.settings.temp_root
        root.mkdir(parents=True, exist_ok=True)
        ref = git_ref if ownership and git_ref.startswith(OWNERSHIP_PREFIX) else validate_git_ref(git_ref)
        output = _git(root, GIT_LS_REMOTE, GIT_REFS, GIT_SEPARATOR, binding.remote, ref)
        for line in output.decode().splitlines():
            sha, ref = line.split()
            if ref == git_ref:
                return sha
        return None

    def _prior(self, project_id, key, operation):
        # 同键重试先恢复固定操作，不受随后 ref 或生产指针推进影响。
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_OPERATION, (project_id, key))
            row = cursor.fetchone()
        if row and row["operation_json"] != operation:
            raise StoreConflict("分支操作键已用于不同输入")
        return row

    def _intent(self, project_id, request, operation, ref, base_sha, base_input):
        # 项目锁只保护短暂登记；有效 ref 唯一索引防止并发接管。
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_LOCK_PROJECT, (project_id,))
            project = cursor.fetchone()
            if not project:
                raise KeyError(project_id)
            cursor.execute(SQL_OPERATION, (project_id, request.idempotency_key))
            prior = cursor.fetchone()
            if prior:
                if prior["operation_json"] != operation:
                    raise StoreConflict("分支操作键已用于不同输入")
                return prior
            cursor.execute(SQL_LIVE_REF, (project_id, ref))
            if cursor.fetchone():
                raise StoreConflict("分支已登记")
            profile = self.runtime.settings.branch_preview_profile_binding_id
            if not profile:
                raise ValueError("未配置开发分支的受控 profile")
            identifier = uuid4()
            config = {**project["binding_config"], "profileBindingId": profile,
                      "schemaName": SCHEMA_PREFIX + identifier.hex}
            cursor.execute(SQL_BRANCH_LOCK, (project_id, None, None))
            main = cursor.fetchone()
            source_id = str(request.source_branch_id) if isinstance(request, CreateBranchRequest) else None
            cursor.execute(SQL_BASE_RELEASE, (project_id, base_sha, source_id, source_id))
            baseline = cursor.fetchone()
            cursor.execute(SQL_INSERT, (str(identifier), project_id, ref, base_sha,
                                        baseline["release_id"] if baseline else None,
                                        main["active_release_id"], base_input,
                                        Json(config), project["config_version"], request.idempotency_key,
                                        Json(operation)))
            return cursor.fetchone()

    def _validate_baseline(self, binding, sha, ref):
        # 固定基线封存到数据库；ref 删除后仍可审计，外键阻止 GC 回收。
        directory, _ = resolve_draft_revision(binding, sha, self.runtime.settings.temp_root, git_ref=ref)
        try:
            return ArtifactStore(self.runtime.db).capture(binding.project_id, directory,
                                                          metadata={"source_commit_sha": sha})
        finally:
            shutil.rmtree(directory)

    def _activate(self, row, head):
        # 激活和首次扫描信号同事务，重试不会多发信号。
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_ACTIVATE, (head, row["branch_id"]))
            activated = cursor.fetchone()
        return activated or self.store.get(row["project_id"], row["branch_id"])

    def create(self, project_id: str, request: CreateBranchRequest) -> BranchView:
        # 首次创建确认目标不存在，之后只恢复已保存的创建意图。
        ref = validate_git_ref(HEADS_PREFIX + request.name)
        operation = {OP_KIND: OP_CREATE, OP_REQUEST: request.model_dump(mode=JSON_MODE, by_alias=True)}
        row = self._prior(project_id, request.idempotency_key, operation)
        binding = self._binding(project_id)
        if row is None:
            source = self.store.get(project_id, str(request.source_branch_id))
            if source["status"] != BranchStatus.ACTIVE:
                raise StoreConflict("来源分支不可用")
            baseline = self._validate_baseline(binding, request.source_commit_sha, source["git_ref"])
            if self._remote_head(binding, ref) is not None:
                raise StoreConflict("远端已有同名分支，请显式登记")
            row = self._intent(project_id, request, operation, ref, request.source_commit_sha, baseline)
        return self._resume_create(row, binding)

    def _resume_create(self, row, binding):
        # 已完成或已删除操作直接返回原身份；不创建新分支、不复活旧分支。
        if row["status"] != BranchStatus.PROVISIONING:
            return branch_view(row)
        ref, sha = row["git_ref"], row["base_commit_sha"]
        # 归属引用和目标引用一次原子创建，不能把相同 SHA 当作创建归属证据。
        marker = OWNERSHIP_PREFIX + row["branch_id"]
        owned = self._remote_head(binding, marker, ownership=True) == sha
        current = self._remote_head(binding, ref)
        if current is None:
            if owned:
                raise StoreConflict("已创建的远端分支已删除，不能恢复创建使其复活")
            source_id = row["operation_json"][OP_REQUEST]["sourceBranchId"]
            source = self.store.get(row["project_id"], source_id)
            with tempfile.TemporaryDirectory(prefix=TEMP_PREFIX, dir=self.runtime.settings.temp_root) as temporary:
                cache = Path(temporary)
                _git(cache, GIT_INIT, GIT_BARE)
                _git(cache, GIT_FETCH, GIT_NO_TAGS, GIT_SEPARATOR, binding.remote, source["git_ref"])
                _git(cache, GIT_MERGE_BASE, GIT_ANCESTOR, sha, GIT_HEAD)
                # 空 expected ref 的租约只允许创建不存在的引用，绝不覆盖已有提交。
                _git(cache, GIT_PUSH, GIT_ATOMIC, GIT_LEASE + ref + REFSPEC_SEPARATOR,
                     GIT_LEASE + marker + REFSPEC_SEPARATOR, GIT_SEPARATOR, binding.remote,
                     sha + REFSPEC_SEPARATOR + ref, sha + REFSPEC_SEPARATOR + marker)
            owned = True
            current = self._remote_head(binding, ref)
        if not owned or current is None:
            raise StoreConflict("远端分支缺少当前创建操作的归属证明")
        # 已证明归属后允许普通后继 push；固定创建提交仍必须在其历史中。
        directory, _ = resolve_draft_revision(binding, sha, self.runtime.settings.temp_root, git_ref=ref)
        shutil.rmtree(directory)
        return branch_view(self._activate(row, current))

    def register(self, project_id: str, request: RegisterBranchRequest) -> BranchView:
        # 已存在引用仅通过显式登记接管，固定基线必须属于目标分支。
        ref = validate_git_ref(HEADS_PREFIX + request.name)
        operation = {OP_KIND: OP_REGISTER, OP_REQUEST: request.model_dump(mode=JSON_MODE, by_alias=True)}
        row = self._prior(project_id, request.idempotency_key, operation)
        if row and row["status"] != BranchStatus.PROVISIONING:
            return branch_view(row)
        binding = self._binding(project_id)
        baseline = (self._validate_baseline(binding, request.base_commit_sha, ref)
                    if row is None else row["base_input_set_id"])
        head = self._remote_head(binding, ref)
        if head is None:
            raise StoreConflict("远端分支不存在")
        row = row or self._intent(project_id, request, operation, ref, request.base_commit_sha, baseline)
        return branch_view(self._activate(row, head))

    def delete(self, project_id: str, branch_id: str, expected_version: int) -> BranchView:
        # 先关闭新请求和发布入口；失败保留 DELETING，重试原 version 可以恢复。
        binding = self._binding(project_id)
        before = self.store.get(project_id, branch_id)
        observed = self._remote_head(binding, before["git_ref"]) if before["status"] == BranchStatus.ACTIVE else None
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_BRANCH_LOCK, (project_id, branch_id, branch_id))
            row = cursor.fetchone()
            if not row:
                raise KeyError(branch_id)
            if row["mode"] == BranchMode.PRODUCTION:
                raise StoreConflict("生产分支不能删除")
            if row["status"] == BranchStatus.DELETED:
                return branch_view(row)
            if row["status"] == BranchStatus.DELETING:
                if expected_version not in (row["version"], row["version"] - 1):
                    raise StoreConflict("分支版本已变化")
            elif row["status"] != BranchStatus.ACTIVE or row["version"] != expected_version:
                raise StoreConflict("分支版本已变化")
            else:
                cursor.execute(SQL_DELETING, (observed, branch_id))
                row = cursor.fetchone()
        head = self._remote_head(binding, row["git_ref"])
        if head is not None:
            if head != row["observed_head_sha"]:
                raise StoreConflict("待删除远端分支已变化，原删除意图不能应用到新提交")
            # 按刚核实的 head 比较删除，期间若有 push 则失败并等待显式恢复。
            with tempfile.TemporaryDirectory(prefix=TEMP_PREFIX, dir=self.runtime.settings.temp_root) as temporary:
                cache = Path(temporary)
                _git(cache, GIT_INIT, GIT_BARE)
                _git(cache, GIT_PUSH, GIT_LEASE + row["git_ref"] + REFSPEC_SEPARATOR + head,
                     GIT_SEPARATOR, binding.remote, REFSPEC_SEPARATOR + row["git_ref"])
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_DELETED, (branch_id,))
            deleted = cursor.fetchone()
        return branch_view(deleted or self.store.get(project_id, branch_id))
