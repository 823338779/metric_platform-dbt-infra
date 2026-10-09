"""数据库队列、执行租约与发布事务；外部 SQL 的停止必须单独确认。"""

import hashlib
import json
from contextlib import nullcontext
from uuid import uuid4

from psycopg2.extras import Json

from dbt_metricflow_service.storage.rows import row_dict

from .branches import BranchStore
from .postgres import Database

# SQL 统一作为静态常量，数据全部由绑定参数传入。
SQL_SELECT_PARENT_RUN_ID_FROM = "SELECT parent_run_id FROM runtime_job WHERE job_id=%s"
SQL_SELECT_J_A = (
    "SELECT j.*,a.attempt_id,a.execution_stage,a.lease_token FROM runtime_job j JOIN runtime_attempt a "
    "ON a.attempt_id=j.current_attempt_id WHERE j.job_id=%s AND j.status='RUNNING' AND "
    "a.state='EXECUTING' AND a.lease_token=%s AND a.lease_expires_at>clock_timestamp() AND "
    "j.deadline_at>clock_timestamp() AND (j.input_mode='DURABLE' OR "
    "j.input_lease_expires_at>clock_timestamp()) FOR UPDATE OF j,a"
)
SQL_UPDATE_RUNTIME_PROJECT_SET = (
    "UPDATE runtime_project SET busy_job_id=NULL WHERE busy_job_id=%s AND NOT EXISTS (SELECT 1 FROM "
    "runtime_attempt WHERE job_id=%s AND execution_stage='EXTERNAL' AND stop_confirmed_at IS NULL)"
)
SQL_INSERT_INTO_RUNTIME_PROJECT = (
    "INSERT INTO runtime_project(project_id,binding_config,config_version) VALUES(%s,%s,%s) ON "
    "CONFLICT(project_id) DO NOTHING"
)
SQL_SELECT_FROM_RUNTIME_PROJECT = "SELECT * FROM runtime_project WHERE project_id=%s FOR UPDATE"
SQL_UPDATE_RUNTIME_PROJECT_SET_2 = (
    "UPDATE runtime_project SET "
    "binding_config=%s,config_version=%s,source_set_id=COALESCE(%s,source_set_id),current_output_set_id=C"
    "ASE WHEN %s THEN NULL ELSE current_output_set_id END,revision=revision+1 WHERE project_id=%s "
    "RETURNING *"
)
SQL_SELECT_FROM_RUNTIME_PROJECT_2 = """SELECT p.*,
 COALESCE(b.publication_sequence,0) AS publication_sequence,
 b.active_release_id AS active_published_release_id
 FROM runtime_project p LEFT JOIN runtime_branch b ON b.project_id=p.project_id AND b.mode='PRODUCTION'
 WHERE p.project_id=%s"""
SQL_SELECT_FROM_RUNTIME_JOB = "SELECT * FROM runtime_job WHERE job_id=%s"
SQL_SELECT_FROM_RUNTIME_JOB_2 = "SELECT * FROM runtime_job WHERE idempotency_scope=%s AND idempotency_key=%s"
SQL_SELECT_FROM_RUNTIME_JOB_RESULT = "SELECT * FROM runtime_job_result WHERE job_id=%s"
SQL_INSERT_INTO_RUNTIME_JOB = (
    "INSERT INTO "
    "runtime_job(job_id,kind,project_id,parent_run_id,idempotency_scope,idempotency_key,request_fingerpri"
    "nt,request_json,input_mode,pinned_instance_id,input_lease_expires_at,input_set_id,config_version,too"
    "lchain_version,schema_name,profile_binding_id,run_lifecycle,deadline_at,retry_policy,max_attempts) "
    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CASE WHEN %s='VOLATILE' THEN clock_timestamp()+%s*interval '1 "
    "second' ELSE NULL END,%s,%s,%s,%s,%s,%s,clock_timestamp()+%s*interval '1 second',%s,%s) RETURNING *"
)
SQL_SELECT_FROM_RUNTIME_JOB_3 = (
    "SELECT * FROM runtime_job WHERE status='QUEUED' AND available_at<=clock_timestamp() AND "
    "deadline_at>clock_timestamp() AND config_version=ANY(%s) AND toolchain_version=%s AND (%s IS NULL "
    "OR kind=ANY(%s)) AND (input_mode='DURABLE' OR (pinned_instance_id=%s AND "
    "input_lease_expires_at>clock_timestamp())) ORDER BY available_at,created_at LIMIT 1 FOR UPDATE SKIP "
    "LOCKED"
)
SQL_INSERT_INTO_RUNTIME_ATTEMPT = (
    "INSERT INTO runtime_attempt(attempt_id,job_id,attempt_no,worker_id,lease_token,lease_expires_at) "
    "VALUES(%s,%s,%s,%s,%s,clock_timestamp()+%s*interval '1 second')"
)
SQL_UPDATE_RUNTIME_JOB_SET = (
    "UPDATE runtime_job SET "
    "status='RUNNING',attempt_no=attempt_no+1,current_attempt_id=%s,started_at=COALESCE(started_at,clock_"
    "timestamp()) WHERE job_id=%s RETURNING *"
)
SQL_SELECT_JOB_ID_FROM = "SELECT job_id FROM runtime_job WHERE job_id=%s FOR UPDATE"
SQL_UPDATE_RUNTIME_ATTEMPT_SET = (
    "UPDATE runtime_attempt SET "
    "heartbeat_at=clock_timestamp(),lease_expires_at=clock_timestamp()+%s*interval '1 second' WHERE "
    "attempt_id=%s"
)
SQL_UPDATE_RUNTIME_JOB_SET_2 = (
    "UPDATE runtime_job SET input_lease_expires_at=clock_timestamp()+%s*interval '1 second' WHERE "
    "input_mode='VOLATILE' AND pinned_instance_id=%s AND status IN ('QUEUED','RUNNING') AND "
    "input_lease_expires_at>clock_timestamp() AND deadline_at>clock_timestamp() AND (%s IS NULL OR "
    "job_id=ANY(%s::uuid[]))"
)
SQL_UPDATE_RUNTIME_JOB_SET_3 = "UPDATE runtime_job SET phase=%s WHERE job_id=%s"
SQL_SELECT_FROM_RUNTIME_ARTIFACT_SET = "SELECT * FROM runtime_artifact_set WHERE set_id=%s"
SQL_UPDATE_RUNTIME_JOB_SET_4 = "UPDATE runtime_job SET input_set_id=%s WHERE job_id=%s"
SQL_INSERT_INTO_RUNTIME_JOB_RESULT = (
    "INSERT INTO "
    "runtime_job_result(job_id,attempt_id,payload_json,stdout_tail,stderr_tail,exit_code,output_truncated"
    ") VALUES(%s,%s,%s,%s,%s,%s,%s)"
)
SQL_UPDATE_RUNTIME_ATTEMPT_SET_2 = (
    "UPDATE runtime_attempt SET "
    "state='SUCCEEDED',stop_confirmed_at=clock_timestamp(),finished_at=clock_timestamp() WHERE "
    "attempt_id=%s"
)
SQL_UPDATE_RUNTIME_JOB_SET_5 = (
    "UPDATE runtime_job SET "
    "status='SUCCEEDED',output_set_id=%s,finished_at=clock_timestamp(),error_code=NULL,error_detail=NULL "
    "WHERE job_id=%s"
)
SQL_UPDATE_RUNTIME_ATTEMPT_SET_3 = (
    "UPDATE runtime_attempt SET state=CASE WHEN %s THEN 'FAILED' ELSE 'EXPIRED_UNCONFIRMED' "
    "END,stop_confirmed_at=CASE WHEN %s THEN clock_timestamp() ELSE NULL "
    "END,finished_at=clock_timestamp() WHERE attempt_id=%s"
)
SQL_UPDATE_RUNTIME_JOB_SET_6 = (
    "UPDATE runtime_job SET status='FAILED',error_code=%s,error_detail=%s,finished_at=clock_timestamp() WHERE job_id=%s"
)
SQL_SELECT_J_FROM = (
    "SELECT j.* FROM runtime_job j WHERE status IN ('QUEUED','RUNNING') AND "
    "(deadline_at<=clock_timestamp() OR (input_mode='VOLATILE' AND "
    "input_lease_expires_at<=clock_timestamp()) OR EXISTS (SELECT 1 FROM runtime_attempt a WHERE "
    "a.attempt_id=j.current_attempt_id AND a.lease_expires_at<=clock_timestamp())) ORDER BY created_at "
    "FOR UPDATE OF j SKIP LOCKED"
)
SQL_SELECT_JOB_ID_FROM_2 = "SELECT job_id FROM runtime_attempt WHERE attempt_id=%s AND lease_token=%s"
SQL_SELECT_FROM_RUNTIME_JOB_4 = "SELECT * FROM runtime_job WHERE job_id=%s FOR UPDATE"
SQL_UPDATE_RUNTIME_ATTEMPT_SET_4 = (
    "UPDATE runtime_attempt SET state='STOPPED',stop_confirmed_at=clock_timestamp() WHERE attempt_id=%s "
    "AND state='EXPIRED_UNCONFIRMED'"
)
SQL_SELECT_FROM_RUNTIME_JOB_5 = "SELECT * FROM runtime_job WHERE kind='RUN_CLEANUP' AND parent_run_id=%s"
SQL_SELECT_FROM_RUNTIME_JOB_6 = (
    "SELECT 1 FROM runtime_job WHERE (job_id=%s OR parent_run_id=%s) AND status IN ('QUEUED','RUNNING') LIMIT 1"
)
SQL_SELECT_FROM_RUNTIME_ATTEMPT = (
    "SELECT 1 FROM runtime_attempt a JOIN runtime_job j ON j.job_id=a.job_id WHERE (j.job_id=%s OR "
    "j.parent_run_id=%s) AND a.execution_stage='EXTERNAL' AND a.stop_confirmed_at IS NULL LIMIT 1"
)
SQL_UPDATE_RUNTIME_JOB_SET_7 = "UPDATE runtime_job SET run_lifecycle='CLEANING' WHERE job_id=%s"
SQL_INSERT_INTO_RUNTIME_JOB_2 = (
    "INSERT INTO "
    "runtime_job(job_id,kind,project_id,parent_run_id,idempotency_scope,idempotency_key,request_fingerpri"
    "nt,config_version,toolchain_version,schema_name,profile_binding_id,input_set_id,deadline_at,retry_po"
    "licy) VALUES(%s,'RUN_CLEANUP',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp()+interval '10 "
    "minutes','PREPARATION_ONLY') RETURNING *"
)
SQL_SELECT_PG_ADVISORY_XACT_LOCK_HASHTEXTEXTENDED = "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))"
SQL_SELECT_STATE_PROJECT_ID = "SELECT state,project_id FROM runtime_artifact_set WHERE set_id=%s"
SQL_UPDATE_RUNTIME_PROJECT_SET_3 = "UPDATE runtime_project SET busy_job_id=%s WHERE project_id=%s"
SQL_UPDATE_RUNTIME_ATTEMPT_SET_5 = (
    "UPDATE runtime_attempt SET execution_stage='EXTERNAL',external_execution_refs=%s WHERE attempt_id=%s"
)
SQL_SELECT_FROM_RUNTIME_ARTIFACT_SET_2 = "SELECT * FROM runtime_artifact_set WHERE set_id=%s FOR UPDATE"
SQL_UPDATE_RUNTIME_JOB_SET_8 = "UPDATE runtime_job SET run_lifecycle='CLEANED' WHERE job_id=%s"
SQL_UPDATE_RUNTIME_PROJECT_SET_4 = (
    "UPDATE runtime_project SET current_output_set_id=%s,revision=revision+1 WHERE project_id=%s AND "
    "config_version=%s AND (source_set_id=%s OR source_set_id=(SELECT source_set_id FROM "
    "runtime_artifact_set WHERE set_id=%s))"
)
SQL_SELECT_CLOCK_TIMESTAMP_AS = "SELECT clock_timestamp() AS now"
SQL_SELECT_RELATIVE_PATH_FROM = "SELECT relative_path FROM runtime_artifact_file WHERE set_id=%s"
SQL_SELECT_FROM_RUNTIME_ATTEMPT_2 = "SELECT * FROM runtime_attempt WHERE attempt_id=%s FOR UPDATE"
SQL_UPDATE_RUNTIME_ATTEMPT_SET_6 = (
    "UPDATE runtime_attempt SET state=%s,finished_at=clock_timestamp(),stop_confirmed_at=CASE WHEN %s "
    "THEN NULL ELSE clock_timestamp() END WHERE attempt_id=%s"
)
SQL_UPDATE_RUNTIME_JOB_SET_9 = (
    "UPDATE runtime_job SET "
    "status='QUEUED',current_attempt_id=NULL,available_at=clock_timestamp()+%s*interval '1 second' WHERE "
    "job_id=%s"
)
SQL_SELECT_FROM_RUNTIME_ATTEMPT_3 = (
    "SELECT 1 FROM runtime_attempt WHERE job_id=%s AND execution_stage='EXTERNAL' AND stop_confirmed_at IS NULL"
)
SQL_UPDATE_RUNTIME_JOB_SET_10 = (
    "UPDATE runtime_job SET "
    "status='QUEUED',current_attempt_id=NULL,available_at=clock_timestamp(),deadline_at=clock_timestamp()"
    "+interval '10 minutes',finished_at=NULL,error_code=NULL WHERE job_id=%s RETURNING *"
)

QUEUED = "QUEUED"
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
BUILD_RUN = "BUILD_RUN"
RUN_CLEANUP = "RUN_CLEANUP"
DBT_COMMAND = "DBT_COMMAND"
MF_COMMAND = "MF_COMMAND"
VOLATILE = "VOLATILE"
DURABLE = "DURABLE"
READ_ONLY = "READ_ONLY"
NEVER = "NEVER"
EXTERNAL = "EXTERNAL"
INPUT_LOST = "INPUT_LOST"
OUTCOME_UNKNOWN = "EXECUTION_OUTCOME_UNKNOWN"
TIMEOUT = "TASK_TIMEOUT"
LEASE_EXPIRED = "LEASE_EXPIRED"
ACTIVE = "ACTIVE"
CLEANING = "CLEANING"
CLEANED = "CLEANED"
SEALED = "SEALED"
EXECUTION = "EXECUTION"
SOURCE = "SOURCE"
EXPIRED_UNCONFIRMED = "EXPIRED_UNCONFIRMED"
STOPPED = "STOPPED"
CLEANUP_SCOPE = "RUN_CLEANUP"
REQUIRED_BUILD_FILES = frozenset(
    {"target/manifest.json", "target/semantic_manifest.json", "target/run_results.json", "target/catalog.json"}
)
REQUIRED_VALIDATION_FLAGS = ("allTestsPassed", "representativeQueryPassed", "relationsVerified")
MAX_DIAGNOSTIC = 1024 * 1024
MAX_RESULT = 16 * 1024 * 1024
SQL_OPTIONS_RETRY = """UPDATE runtime_job j SET status='QUEUED',current_attempt_id=NULL,
 available_at=clock_timestamp(),finished_at=NULL,error_code=NULL,error_detail=NULL
 WHERE job_id=%s AND kind='QUERY_OPTIONS' AND status='FAILED' AND attempt_no<max_attempts
 AND deadline_at>clock_timestamp() AND error_code<>'INVALID_QUERY'
 AND NOT EXISTS(SELECT 1 FROM runtime_attempt a WHERE a.job_id=j.job_id
 AND a.execution_stage='EXTERNAL' AND a.stop_confirmed_at IS NULL)"""
SQL_RELEASE_RUN_ARTIFACTS = """UPDATE runtime_job SET input_set_id=NULL,output_set_id=NULL
 WHERE job_id=%s OR parent_run_id=%s"""
SQL_EXTERNAL_ATTEMPT = "SELECT 1 FROM runtime_attempt WHERE job_id=%s AND execution_stage='EXTERNAL' LIMIT 1"
FORBIDDEN_REQUEST_KEYS = frozenset({"resources", "credentials", "password", "token", "secret", "environment", "env"})
LEGACY_IMPORT_DIGEST = "legacyImportDigest"
BINDING_FIELD = "binding"
RELEASE_FIELD = "releaseId"
SQL_RELEASE_BRANCH = "SELECT branch_id FROM runtime_release WHERE project_id=%s AND release_id=%s"
SQL_ASSIGN_BRANCH = "UPDATE runtime_job SET branch_id=%s WHERE job_id=%s RETURNING *"


class StoreConflict(ValueError):
    """幂等键已用于不同请求。"""


class ProjectBusy(ValueError):
    """项目存在尚未确认结束的写操作。"""


class CleanupBlocked(ValueError):
    """run 仍被活动任务或未知外部执行引用。"""


def _safe_request(value):
    # resources 和凭据不进入持久请求；调用者必须另外在内存保留 VOLATILE 输入。
    if isinstance(value, dict):
        return {key: _safe_request(item) for key, item in value.items() if key.lower() not in FORBIDDEN_REQUEST_KEYS}
    if isinstance(value, list):
        return [_safe_request(item) for item in value]
    return value


class JobStore:
    def __init__(
        self,
        db: Database,
        lease_seconds: int = 90,
        *,
        max_result_bytes: int = MAX_RESULT,
        max_diagnostic_bytes: int = MAX_DIAGNOSTIC,
    ):
        # 数据库共享控制状态；租约长度只决定后续新租约的有效期。
        self.db = db
        self.lease_seconds = lease_seconds
        # 序列化后的结果和诊断分别设限，避免单条记录撑满内容存储。
        self.max_result_bytes = max_result_bytes
        self.max_diagnostic_bytes = max_diagnostic_bytes

    def register_project(self, project_id, binding_config=None, config_version="1", source_set_id=None,
                         preview_profile=None):
        # 项目导入与普通任务受理锁同一项目行，换源时立即移除旧输出指针。
        with self.db.transaction() as connection:
            sql_result = connection.exec_driver_sql(
                SQL_INSERT_INTO_RUNTIME_PROJECT,
                (project_id, Json(binding_config or {}), config_version),
            )
            sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_PROJECT, (project_id,))
            current = row_dict(sql_result)
            if source_set_id is not None:
                sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_ARTIFACT_SET, (source_set_id,))
                source = row_dict(sql_result)
                if (
                    not source
                    or source["state"] != SEALED
                    or source["kind"] != SOURCE
                    or source["project_id"] != project_id
                ):
                    raise ValueError("Project source must be a sealed source of the same project")
            changed = source_set_id is not None and current["source_set_id"] != str(source_set_id)
            sql_result = connection.exec_driver_sql(
                SQL_UPDATE_RUNTIME_PROJECT_SET_2,
                (
                    Json(binding_config if binding_config is not None else current["binding_config"]),
                    config_version,
                    source_set_id,
                    changed or current["config_version"] != config_version,
                    project_id,
                ),
            )
            result = dict(row_dict(sql_result))
            BranchStore.ensure_production(connection, project_id, preview_profile)
            return result

    def project(self, project_id):
        with self.db.transaction() as connection:
            sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_PROJECT_2, (project_id,))
            return row_dict(sql_result)

    def get(self, job_id):
        with self.db.transaction() as connection:
            sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_JOB, (str(job_id),))
            return row_dict(sql_result)

    def by_key(self, scope, key):
        with self.db.transaction() as connection:
            sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_JOB_2, (scope, key))
            return row_dict(sql_result)

    def result(self, job_id):
        with self.db.transaction() as connection:
            sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_JOB_RESULT, (str(job_id),))
            return row_dict(sql_result)

    def reserve(
        self,
        kind,
        project_id,
        request_json,
        *,
        job_id=None,
        fingerprint=None,
        idempotency_scope=None,
        idempotency_key=None,
        parent_run_id=None,
        input_set_id=None,
        input_mode=DURABLE,
        pinned_instance_id=None,
        config_version="1",
        toolchain_version="default",
        schema_name=None,
        profile_binding_id=None,
        retry_policy="PREPARATION_ONLY",
        max_attempts=3,
        timeout_seconds=600,
        write=False,
        expected_revision=None,
        branch_id=None,
        _cursor=None,
    ):
        # 幂等作用域先串行化；parent 锁统一先于项目行和子任务，避免清理受理穿透。
        with nullcontext(_cursor) if _cursor is not None else self.db.transaction() as connection:
            if idempotency_key is not None:
                idempotency_scope = idempotency_scope or kind
                sql_result = connection.exec_driver_sql(
                    SQL_SELECT_PG_ADVISORY_XACT_LOCK_HASHTEXTEXTENDED, (idempotency_scope + ":" + idempotency_key,)
                )
            parent = None
            if parent_run_id is not None:
                sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_JOB_4, (str(parent_run_id),))
                parent = row_dict(sql_result)
                if not parent or parent["kind"] != BUILD_RUN or parent["project_id"] != project_id:
                    raise ValueError("Parent must be a BUILD_RUN of this project")
            sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_PROJECT, (project_id,))
            project = row_dict(sql_result)
            if not project:
                raise ValueError("Project is not registered")
            # 前置 manifest 校验之后若项目发生导入，拒绝混用旧输入与新配置。
            if expected_revision is not None and (
                project["revision"] != expected_revision or project["config_version"] != config_version
            ):
                raise StoreConflict("Project changed during request validation; retry")
            if input_set_id is None:
                if parent:
                    input_set_id = parent["output_set_id"]
                elif kind in (DBT_COMMAND, MF_COMMAND):
                    input_set_id = project["current_output_set_id"] or project["source_set_id"]
            safe_request = _safe_request(request_json)
            encoded = json.dumps(
                [
                    {"input": fingerprint or safe_request, "branchId": branch_id} if branch_id else
                    fingerprint or safe_request,
                    kind,
                    project_id,
                    str(parent_run_id) if parent_run_id else None,
                    input_set_id,
                    input_mode,
                    profile_binding_id,
                    config_version,
                    toolchain_version,
                ],
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            )
            fingerprint = hashlib.sha256(encoded.encode()).hexdigest()
            if idempotency_key is not None:
                sql_result = connection.exec_driver_sql(
                    SQL_SELECT_FROM_RUNTIME_JOB_2,
                    (idempotency_scope, idempotency_key),
                )
                prior = row_dict(sql_result)
                if prior:
                    # 旧库摘要算法不同；迁移任务按原公开请求比较，保留旧幂等语义。
                    if (prior["error_detail"] or {}).get(LEGACY_IMPORT_DIGEST):
                        comparable = {key: value for key, value in safe_request.items() if key != BINDING_FIELD}
                        if (prior["kind"] == kind and prior["project_id"] == project_id
                                and prior["request_json"] == comparable):
                            return prior
                    if prior["request_fingerprint"] != fingerprint:
                        raise StoreConflict("Idempotency key belongs to another request")
                    return prior
            if parent and (parent["status"] != SUCCEEDED or parent["run_lifecycle"] != ACTIVE):
                raise CleanupBlocked("Run is not ready and active")
            if input_set_id is not None:
                sql_result = connection.exec_driver_sql(SQL_SELECT_STATE_PROJECT_ID, (input_set_id,))
                artifact = row_dict(sql_result)
                if not artifact or artifact["state"] != SEALED or artifact["project_id"] != project_id:
                    raise ValueError("Input artifact set is not available")
            elif parent:
                raise ValueError("Parent has no published artifact set")
            if write and project["busy_job_id"] is not None:
                raise ProjectBusy("project_busy")
            identifier = str(job_id or uuid4())
            sql_result = connection.exec_driver_sql(
                SQL_INSERT_INTO_RUNTIME_JOB,
                (
                    identifier,
                    kind,
                    project_id,
                    parent_run_id,
                    idempotency_scope,
                    idempotency_key,
                    fingerprint,
                    Json(safe_request),
                    input_mode,
                    pinned_instance_id,
                    input_mode,
                    self.lease_seconds,
                    input_set_id,
                    config_version,
                    toolchain_version,
                    schema_name,
                    profile_binding_id,
                    ACTIVE if kind == BUILD_RUN else None,
                    timeout_seconds,
                    retry_policy,
                    max_attempts,
                ),
            )
            result = row_dict(sql_result)
            # 发布类任务继承固定候选或父 run 的分支，普通任务保持无分支。
            if parent and branch_id is not None and parent["branch_id"] != branch_id:
                raise ValueError("父任务与分支归属不匹配")
            branch_id = parent["branch_id"] if parent else branch_id
            if safe_request.get(RELEASE_FIELD):
                sql_result = connection.exec_driver_sql(SQL_RELEASE_BRANCH, (project_id, safe_request[RELEASE_FIELD]))
                release = row_dict(sql_result)
                if not release or branch_id is not None and branch_id != release["branch_id"]:
                    raise ValueError("任务与发布分支归属不匹配")
                branch_id = release["branch_id"]
            if branch_id is not None:
                sql_result = connection.exec_driver_sql(SQL_ASSIGN_BRANCH, (branch_id, identifier))
                result = row_dict(sql_result)
            if write:
                sql_result = connection.exec_driver_sql(SQL_UPDATE_RUNTIME_PROJECT_SET_3, (identifier, project_id))
            return result

    def requeue_options(self, job_id):
        # 同步选项的明确终止失败可重试，沿用原截止时间和总尝试上限。
        with self.db.transaction() as connection:
            sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_JOB, (str(job_id),))
            row = row_dict(sql_result)
            if row and row["parent_run_id"]:
                sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_JOB_4, (row["parent_run_id"],))
                parent = row_dict(sql_result)
                if parent["run_lifecycle"] != ACTIVE:
                    return False
            sql_result = connection.exec_driver_sql(SQL_OPTIONS_RETRY, (str(job_id),))
            return sql_result.rowcount == 1

    def claim(self, worker_id, *, config_version="1", config_versions=None, toolchain_version="default", kinds=None):
        # 短事务领取一个任务，跳过其他 worker 已锁住的任务；VOLATILE 输入必须仍有效。
        with self.db.transaction() as connection:
            versions = config_versions if config_versions is not None else [config_version]
            sql_result = connection.exec_driver_sql(
                SQL_SELECT_FROM_RUNTIME_JOB_3,
                (versions, toolchain_version, kinds, kinds, str(worker_id)),
            )
            job = row_dict(sql_result)
            if not job:
                return None
            attempt_id, token = str(uuid4()), str(uuid4())
            sql_result = connection.exec_driver_sql(
                SQL_INSERT_INTO_RUNTIME_ATTEMPT,
                (attempt_id, job["job_id"], job["attempt_no"] + 1, str(worker_id), token, self.lease_seconds),
            )
            sql_result = connection.exec_driver_sql(
                SQL_UPDATE_RUNTIME_JOB_SET,
                (attempt_id, job["job_id"]),
            )
            result = dict(row_dict(sql_result))
            result.update(attempt_id=attempt_id, lease_token=token)
            return result

    def _authorized(self, connection, job_id, token):
        # 清理和子任务更新都先锁 parent；每次修改同时核对 current attempt、token 和数据库时间。
        sql_result = connection.exec_driver_sql(SQL_SELECT_PARENT_RUN_ID_FROM, (str(job_id),))
        reference = row_dict(sql_result)
        if reference and reference["parent_run_id"]:
            sql_result = connection.exec_driver_sql(SQL_SELECT_JOB_ID_FROM, (reference["parent_run_id"],))
        sql_result = connection.exec_driver_sql(
            SQL_SELECT_J_A,
            (str(job_id), str(token)),
        )
        return row_dict(sql_result)

    def heartbeat(self, job_id, token):
        with self.db.transaction() as connection:
            job = self._authorized(connection, job_id, token)
            if not job:
                return False
            connection.exec_driver_sql(
                SQL_UPDATE_RUNTIME_ATTEMPT_SET,
                (self.lease_seconds, job["attempt_id"]),
            )
            return True

    def heartbeat_inputs(self, worker_id, job_ids=None):
        # 排队等待期也需要输入续租；已经过期的输入租约不能通过迟到心跳复活。
        with self.db.transaction() as connection:
            sql_result = connection.exec_driver_sql(
                SQL_UPDATE_RUNTIME_JOB_SET_2,
                (self.lease_seconds, str(worker_id), job_ids, job_ids),
            )
            return sql_result.rowcount

    def phase(self, job_id, token, phase, *, external=False, external_execution_refs=None):
        with self.db.transaction() as connection:
            job = self._authorized(connection, job_id, token)
            if not job:
                return False
            connection.exec_driver_sql(SQL_UPDATE_RUNTIME_JOB_SET_3, (phase, str(job_id)))
            if job["request_json"].get("releaseId"):
                from .publications import SQL_PHASE

                connection.exec_driver_sql(SQL_PHASE, (phase, str(job_id)))
            if external:
                connection.exec_driver_sql(
                    SQL_UPDATE_RUNTIME_ATTEMPT_SET_5,
                    (Json(external_execution_refs or {}), job["attempt_id"]),
                )
            return True

    def attach_input(self, job_id, token, set_id):
        # 首次构建的源码在外部写入前固定，后续准备阶段重试可复用该集合。
        with self.db.transaction() as connection:
            job = self._authorized(connection, job_id, token)
            if not job:
                return False
            sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_ARTIFACT_SET, (set_id,))
            source = row_dict(sql_result)
            if not source or source["state"] != SEALED or source["project_id"] != job["project_id"]:
                raise ValueError("Invalid source artifact set")
            if job["input_set_id"] is not None and job["input_set_id"] != str(set_id):
                raise ValueError("Job input is immutable after admission")
            sql_result = connection.exec_driver_sql(SQL_UPDATE_RUNTIME_JOB_SET_4, (set_id, str(job_id)))
            return True

    def _release_project(self, connection, job_id):
        # 只有所有外部执行都确认结束，才释放通用写锁。
        connection.exec_driver_sql(
            SQL_UPDATE_RUNTIME_PROJECT_SET,
            (str(job_id), str(job_id)),
        )

    def finish(
        self,
        job_id,
        token,
        payload=None,
        *,
        output_set_id=None,
        stdout_tail="",
        stderr_tail="",
        exit_code=0,
        output_truncated=False,
        seal=None,
    ):
        # 发布产物、结果和终态共用一个事务；过期 worker 无权发布任何内容。
        encoded = json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":"))
        if len(encoded.encode()) > self.max_result_bytes:
            raise ValueError("Job result exceeds configured size limit")
        with self.db.transaction() as connection:
            job = self._authorized(connection, job_id, token)
            if not job:
                return False
            if job["kind"] == BUILD_RUN and output_set_id is None:
                raise ValueError("BUILD_RUN requires a complete validated output artifact set")
            if output_set_id is not None:
                if job["input_mode"] == VOLATILE:
                    raise ValueError("Volatile resources cannot publish durable project artifacts")
                sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_ARTIFACT_SET_2, (output_set_id,))
                output = row_dict(sql_result)
                if not output or output["producer_attempt_id"] != job["attempt_id"] or output["kind"] != EXECUTION:
                    raise ValueError("Output must belong to the authorized attempt")
                if output["project_id"] != job["project_id"]:
                    raise ValueError("Output belongs to a different project")
                if job["kind"] == BUILD_RUN:
                    sql_result = connection.exec_driver_sql(SQL_SELECT_RELATIVE_PATH_FROM, (output_set_id,))
                    files = {row["relative_path"] for row in sql_result.mappings()}
                    if not REQUIRED_BUILD_FILES.issubset(files):
                        raise ValueError("BUILD_RUN output is missing required native artifacts")
                    if not all(output["validation_json"].get(flag) is True for flag in REQUIRED_VALIDATION_FLAGS):
                        raise ValueError("BUILD_RUN output lacks successful validation evidence")
                if seal is None:
                    from .artifacts import ArtifactStore

                    seal = ArtifactStore(self.db).seal
                seal(output_set_id, connection)
            sql_result = connection.exec_driver_sql(
                SQL_INSERT_INTO_RUNTIME_JOB_RESULT,
                (
                    str(job_id),
                    job["attempt_id"],
                    Json(payload or {}),
                    stdout_tail.encode()[-self.max_diagnostic_bytes :].decode(errors="ignore"),
                    stderr_tail.encode()[-self.max_diagnostic_bytes :].decode(errors="ignore"),
                    exit_code,
                    output_truncated
                    or len(stdout_tail.encode()) > self.max_diagnostic_bytes
                    or len(stderr_tail.encode()) > self.max_diagnostic_bytes,
                ),
            )
            # 业务发布与封存共用事务；兼容 BUILD_RUN 没有 releaseId 时仍只报告 READY。
            if job["kind"] == BUILD_RUN and job["request_json"].get("releaseId"):
                from .publications import PublicationStore

                PublicationStore(self.db).publish_in_transaction(
                    connection, job_id=job_id, attempt_token=token,
                    release_id=job["request_json"]["releaseId"], output_set_id=output_set_id,
                )
            # 封存校验可能耗时；提交前重新 fencing，失效时连同已封存文件状态一起回滚。
            if not self._authorized(connection, job_id, token):
                connection.rollback()
                return False
            sql_result = connection.exec_driver_sql(
                SQL_UPDATE_RUNTIME_ATTEMPT_SET_2,
                (job["attempt_id"],),
            )
            sql_result = connection.exec_driver_sql(
                SQL_UPDATE_RUNTIME_JOB_SET_5,
                (output_set_id, str(job_id)),
            )
            if job["kind"] == RUN_CLEANUP:
                sql_result = connection.exec_driver_sql(SQL_UPDATE_RUNTIME_JOB_SET_8, (job["parent_run_id"],))
                # schema 删除已经确认；保留任务/结果与幂等墓碑，将无引用文件交给 GC。
                sql_result = connection.exec_driver_sql(
                    SQL_RELEASE_RUN_ARTIFACTS, (job["parent_run_id"], job["parent_run_id"])
                )
            if job["kind"] == DBT_COMMAND and output_set_id is not None:
                sql_result = connection.exec_driver_sql(
                    SQL_UPDATE_RUNTIME_PROJECT_SET_4,
                    (output_set_id, job["project_id"], job["config_version"], job["input_set_id"], job["input_set_id"]),
                )
            self._release_project(connection, job_id)
            return True

    def fail(self, job_id, token, error_code, detail=None, *, stopped=True):
        # 错误详情也有字节预算，禁止在失败路径写入无限增长的异常堆栈。
        if len(json.dumps(detail or {}, ensure_ascii=False).encode()) > self.max_diagnostic_bytes:
            detail = {"message": "Diagnostic exceeded configured size limit"}
        with self.db.transaction() as connection:
            job = self._authorized(connection, job_id, token)
            if not job:
                return False
            connection.exec_driver_sql(
                SQL_UPDATE_RUNTIME_ATTEMPT_SET_3,
                (stopped, stopped, job["attempt_id"]),
            )
            connection.exec_driver_sql(
                SQL_UPDATE_RUNTIME_JOB_SET_6,
                (error_code, Json(detail or {}), str(job_id)),
            )
            from .publications import SQL_FAIL

            connection.exec_driver_sql(SQL_FAIL, (error_code, str(job_id)))
            self._release_project(connection, job_id)
            return True

    def recover(self):
        # 恢复只处理过期租约/截止时间，健康实例的 RUNNING 任务不会被启动流程打断。
        with self.db.transaction() as connection:
            sql_result = connection.exec_driver_sql(SQL_SELECT_J_FROM)
            jobs = [dict(row) for row in sql_result.mappings()]
            for job in jobs:
                sql_result = connection.exec_driver_sql(SQL_SELECT_CLOCK_TIMESTAMP_AS)
                now = row_dict(sql_result)["now"]
                attempt = None
                if job["current_attempt_id"]:
                    sql_result = connection.exec_driver_sql(
                        SQL_SELECT_FROM_RUNTIME_ATTEMPT_2, (job["current_attempt_id"],)
                    )
                    attempt = row_dict(sql_result)
                external = bool(attempt and attempt["execution_stage"] == EXTERNAL)
                lost = job["input_mode"] == VOLATILE and job["input_lease_expires_at"] <= now
                timed_out = job["deadline_at"] <= now
                if attempt:
                    sql_result = connection.exec_driver_sql(
                        SQL_UPDATE_RUNTIME_ATTEMPT_SET_6,
                        (EXPIRED_UNCONFIRMED if external else STOPPED, external, attempt["attempt_id"]),
                    )
                retry = (
                    not lost
                    and not timed_out
                    and job["input_mode"] == DURABLE
                    and job["attempt_no"] < job["max_attempts"]
                    and job["retry_policy"] != NEVER
                    and (not external or job["retry_policy"] == READ_ONLY)
                )
                if retry:
                    delay = 5 if job["attempt_no"] <= 1 else 15
                    sql_result = connection.exec_driver_sql(
                        SQL_UPDATE_RUNTIME_JOB_SET_9,
                        (delay, job["job_id"]),
                    )
                else:
                    code = (
                        INPUT_LOST if lost else TIMEOUT if timed_out else OUTCOME_UNKNOWN if external else LEASE_EXPIRED
                    )
                    sql_result = connection.exec_driver_sql(
                        SQL_UPDATE_RUNTIME_JOB_SET_6,
                        (code, Json({"externalOutcomeUnknown": external}), job["job_id"]),
                    )
                    from .publications import SQL_FAIL

                    sql_result = connection.exec_driver_sql(SQL_FAIL, (code, job["job_id"]))
                    self._release_project(connection, job["job_id"])
            return len(jobs)

    def confirm_stopped(self, attempt_id, token):
        # 失效执行者仅可凭自己的 token 确认停止，不能改写公开结果。
        with self.db.transaction() as connection:
            sql_result = connection.exec_driver_sql(
                SQL_SELECT_JOB_ID_FROM_2,
                (str(attempt_id), str(token)),
            )
            attempt = row_dict(sql_result)
            if not attempt:
                return False
            sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_JOB_4, (attempt["job_id"],))
            job = row_dict(sql_result)
            sql_result = connection.exec_driver_sql(
                SQL_UPDATE_RUNTIME_ATTEMPT_SET_4,
                (str(attempt_id),),
            )
            if job["status"] in (SUCCEEDED, FAILED):
                self._release_project(connection, job["job_id"])
            return True

    def reserve_cleanup(self, parent_run_id, toolchain_version="default"):
        # 与查询受理锁同一 parent；阻止所有排队/执行任务和任何未确认停止的历史 attempt。
        with self.db.transaction() as connection:
            sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_JOB_4, (str(parent_run_id),))
            parent = row_dict(sql_result)
            if not parent or parent["kind"] != BUILD_RUN:
                raise ValueError("Run does not exist")
            # 首版保留发布历史，连同复用来源一起保护；旧 cleanup 接口不能绕过。
            from .publications import SQL_PROTECTED_RUN

            sql_result = connection.exec_driver_sql(SQL_PROTECTED_RUN, (str(parent_run_id), str(parent_run_id)))
            if row_dict(sql_result):
                raise CleanupBlocked("run_cleanup_blocked")
            sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_JOB_5, (str(parent_run_id),))
            existing = row_dict(sql_result)
            if existing:
                if existing["status"] == FAILED:
                    sql_result = connection.exec_driver_sql(
                        SQL_SELECT_FROM_RUNTIME_ATTEMPT_3,
                        (existing["job_id"],),
                    )
                    if row_dict(sql_result):
                        raise CleanupBlocked("Cleanup outcome is still unknown")
                    sql_result = connection.exec_driver_sql(
                        SQL_UPDATE_RUNTIME_JOB_SET_10,
                        (existing["job_id"],),
                    )
                    return row_dict(sql_result)
                return existing
            sql_result = connection.exec_driver_sql(
                SQL_SELECT_FROM_RUNTIME_JOB_6,
                (str(parent_run_id), str(parent_run_id)),
            )
            if row_dict(sql_result):
                raise CleanupBlocked("run_cleanup_blocked")
            sql_result = connection.exec_driver_sql(
                SQL_SELECT_FROM_RUNTIME_ATTEMPT,
                (str(parent_run_id), str(parent_run_id)),
            )
            if row_dict(sql_result):
                raise CleanupBlocked("run_cleanup_blocked")
            if not (parent["output_set_id"] or parent["input_set_id"]):
                sql_result = connection.exec_driver_sql(SQL_EXTERNAL_ATTEMPT, (str(parent_run_id),))
                if row_dict(sql_result):
                    raise CleanupBlocked("Restore source artifacts before cleaning a run that executed externally")
            sql_result = connection.exec_driver_sql(SQL_UPDATE_RUNTIME_JOB_SET_7, (str(parent_run_id),))
            sql_result = connection.exec_driver_sql(
                SQL_INSERT_INTO_RUNTIME_JOB_2,
                (
                    str(uuid4()),
                    parent["project_id"],
                    str(parent_run_id),
                    CLEANUP_SCOPE,
                    str(parent_run_id),
                    parent["request_fingerprint"],
                    parent["config_version"],
                    parent["toolchain_version"],
                    parent["schema_name"],
                    parent["profile_binding_id"],
                    parent["output_set_id"] or parent["input_set_id"],
                ),
            )
            return row_dict(sql_result)
