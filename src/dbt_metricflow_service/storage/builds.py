"""固定构建的具体存储；与现有任务队列共用短事务。"""

import hashlib
import json
from uuid import uuid4

from psycopg2.extras import Json

from .jobs import JobStore, StoreConflict
from .rows import row_dict

JSON_MODE = "json"
BUILD_KIND = "BUILD_RUN"
BUILD_FIELD = "buildId"
SCHEMA_PREFIX = "run_"
PROJECT_PREFIX = "engine:"
SQL_LOGS = """SELECT sequence,summary FROM engine_change
 WHERE object_type='engine_build' AND object_id=%s AND sequence>%s ORDER BY sequence LIMIT 100"""
SQL_LOCK = "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))"
SQL_BINDING = """SELECT config_json FROM engine_execution_binding WHERE repository=%s AND execution_binding=%s AND
 config_version=%s"""
SQL_REGISTER = "INSERT INTO engine_execution_binding VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING"
SQL_PROJECT = "INSERT INTO runtime_project(project_id,binding_config,config_version) VALUES(%s,%s,%s)"
SQL_KEY = "SELECT * FROM engine_build WHERE repository=%s AND caller=%s AND idempotency_key=%s"
SQL_GET = (
    "SELECT b.*,j.run_lifecycle FROM engine_build b LEFT JOIN runtime_job j ON j.job_id=b.run_id WHERE b.build_id=%s"
)
SQL_INSERT = """INSERT INTO engine_build(build_id,run_id,repository,branch_name,environment,
 execution_binding,config_version,toolchain_version,caller,idempotency_key,request_digest,request_json,
 config_snapshot,requested_commit_sha,commit_sha) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"""
SQL_LIST = """SELECT b.*,j.run_lifecycle FROM engine_build b LEFT JOIN runtime_job j ON j.job_id=b.run_id WHERE
 repository=%s ORDER BY created_at DESC,build_id DESC"""
SQL_PIN = """UPDATE engine_build SET commit_sha=%s,version=version+1,updated_at=clock_timestamp()
 WHERE build_id=%s AND (commit_sha IS NULL OR commit_sha=%s) RETURNING *"""
SQL_JOB_SOURCE = "UPDATE runtime_job SET request_json=request_json || %s::jsonb WHERE job_id=%s"
SQL_CANCEL = (
    "UPDATE engine_build SET cancel_requested=true,version=version+1,updated_at=clock_timestamp() WHERE build_id=%s"
)
SQL_JOB_LOCK = "SELECT * FROM runtime_job WHERE job_id=%s FOR UPDATE"
SQL_CANCEL_QUEUED = """UPDATE runtime_job SET status='FAILED',error_code='CANCELLED',finished_at=clock_timestamp()
 WHERE job_id=%s AND status='QUEUED'"""


def digest(value):
    """默认值已由协议模型补齐，摘要不依赖 JSON 键顺序。"""
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


class BuildStore:
    def __init__(self, db):
        # 只接收具体持久依赖，不拥有 Runtime 或 worker。
        self.db = db
        self.jobs = JobStore(db)

    def register_binding(self, repository, binding, version, config):
        # 同版本禁止修改语义，凭据仅由已有 profile 环境解析。
        from urllib.parse import urlsplit
        from zoneinfo import ZoneInfo

        from ..models.builds import ConfigSnapshot

        parsed = urlsplit(repository)
        if (
            not repository
            or any(char in repository for char in ("\n", "\r"))
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.scheme in {"http", "https"}
            and parsed.username
        ):
            raise ValueError("repository must not contain credentials or URL parameters")
        config = ConfigSnapshot.model_validate(config).model_dump(mode=JSON_MODE, by_alias=True, exclude_none=True)
        ZoneInfo(config["businessTimezone"])
        with self.db.transaction() as connection:
            connection.exec_driver_sql(SQL_LOCK, (repository + binding + version,))
            prior = connection.exec_driver_sql(SQL_BINDING, (repository, binding, version)).scalar_one_or_none()
            if (
                prior is not None
                and ConfigSnapshot.model_validate(prior).model_dump(mode=JSON_MODE, by_alias=True, exclude_none=True)
                != config
            ):
                raise StoreConflict("execution binding version is immutable")
            connection.exec_driver_sql(SQL_REGISTER, (repository, binding, version, Json(config)))

    def get(self, build_id):
        with self.db.transaction() as connection:
            return row_dict(connection.exec_driver_sql(SQL_GET, (str(build_id),)))

    def accept(self, request, caller, toolchain, timeout):
        from .deployments import DeploymentStore

        body = request.model_dump(mode=JSON_MODE, by_alias=True)
        fingerprint = digest(body)
        with self.db.fact_transaction() as connection:
            # 查旧请求先于读取配置；Git 不在受理路径内。
            scope = json.dumps([request.repository, caller, request.idempotency_key])
            connection.exec_driver_sql(SQL_LOCK, (scope,))
            prior = row_dict(connection.exec_driver_sql(SQL_KEY, (request.repository, caller, request.idempotency_key)))
            if prior:
                if prior["request_digest"] != fingerprint:
                    raise StoreConflict("idempotency key already binds another input")
                return prior
            config = connection.exec_driver_sql(
                SQL_BINDING, (request.repository, request.execution_binding, request.config_version)
            ).scalar_one_or_none()
            if config is None or request.environment not in config.get("environments", []):
                raise ValueError("repository and execution binding are not allowed")
            build_id, run_id = str(uuid4()), uuid4()
            # 旧队列表的 project 列只是内部执行容器；每次构建独立，不是公开业务项目。
            project = PROJECT_PREFIX + build_id
            connection.exec_driver_sql(SQL_PROJECT, (project, Json(config), request.config_version))
            payload = {**body, BUILD_FIELD: build_id, "binding": config}
            job = self.jobs.reserve_in_transaction(
                connection,
                BUILD_KIND,
                project,
                payload,
                job_id=str(run_id),
                config_version=request.config_version,
                toolchain_version=toolchain,
                profile_binding_id=config["profileBindingId"],
                schema_name=config.get("schemaName") or SCHEMA_PREFIX + run_id.hex,
                timeout_seconds=timeout,
            )
            row = row_dict(
                connection.exec_driver_sql(
                    SQL_INSERT,
                    (
                        build_id,
                        job["job_id"],
                        request.repository,
                        request.branch_name,
                        request.environment,
                        request.execution_binding,
                        request.config_version,
                        toolchain,
                        caller,
                        request.idempotency_key,
                        fingerprint,
                        Json(body),
                        Json(config),
                        request.commit_sha,
                        request.commit_sha,
                    ),
                )
            )
            if request.deployment_policy == "ON_SUCCESS":
                DeploymentStore(self.db).accept_in_transaction(
                    connection, row, caller, "build:" + build_id, fingerprint, request.branch_name, operation="BUILD"
                )
            return row

    def logs(self, build_id, cursor):
        # 游标绑定构建，不依赖实例内存中的日志缓冲区。
        after = 0
        if cursor is not None:
            prefix, separator, sequence = cursor.partition(":")
            if prefix != build_id or not separator or not sequence.isascii() or not sequence.isdecimal():
                raise ValueError("invalid build log cursor")
            after = int(sequence)
            if after > 9223372036854775807:
                raise ValueError("invalid build log cursor")
        with self.db.transaction() as connection:
            rows = [dict(row) for row in connection.exec_driver_sql(SQL_LOGS, (build_id, after)).mappings()]
        return rows, build_id + ":" + str(rows[-1]["sequence"] if rows else after)

    def list(self, repository):
        with self.db.transaction() as connection:
            return [dict(row) for row in connection.exec_driver_sql(SQL_LIST, (repository,)).mappings()]

    def pin_source(self, build_id, commit_sha, token):
        row = self.get(build_id)
        with self.db.fact_transaction() as connection:
            # 与完成/取消统一先锁执行记录；过期 worker 不能变更固定输入。
            if not row or not self.jobs._authorized(connection, row["run_id"], token):
                raise StoreConflict("execution lease is no longer valid")
            result = row_dict(connection.exec_driver_sql(SQL_PIN, (commit_sha, str(build_id), commit_sha)))
            if not result:
                raise ValueError("commitSha is immutable after resolution")
            connection.exec_driver_sql(SQL_JOB_SOURCE, (Json({"commitSha": commit_sha}), row["run_id"]))
            return result

    def cancel(self, build_id):
        row = self.get(build_id)
        if row is None:
            raise KeyError(build_id)
        with self.db.fact_transaction() as connection:
            job = row_dict(connection.exec_driver_sql(SQL_JOB_LOCK, (row["run_id"],)))
            if job["error_code"] == "CANCELLED":
                return 200
            if job["status"] not in {"QUEUED", "RUNNING"}:
                raise StoreConflict("terminal build cannot be cancelled")
            connection.exec_driver_sql(SQL_CANCEL, (str(build_id),))
            connection.exec_driver_sql(SQL_CANCEL_QUEUED, (row["run_id"],))
            return 202
