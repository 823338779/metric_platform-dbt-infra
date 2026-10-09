"""项目的唯一发布存储。"""

from uuid import UUID, uuid4

from psycopg2.extras import Json
from sqlalchemy import Connection

from dbt_metricflow_service.storage.rows import row_dict

from ..publications.models import BindingMode, PublishedCatalog
from .artifacts import MAX_FILE_BYTES, SQL_FILE, SQL_SET, _decode
from .branches import SQL_BRANCH_LOCK, SQL_BRANCH_READ, SQL_PARENT_LOCK
from .jobs import JobStore, StoreConflict

# 兼容旧入口的生产视图；项目遗留指针不再参与运行时决策。
SQL_PROJECT_LOCK = """SELECT p.*,b.publication_sequence,b.active_release_id AS active_published_release_id
 FROM runtime_project p JOIN runtime_branch b USING(project_id)
 WHERE project_id=%s AND mode='PRODUCTION' FOR UPDATE OF p,b"""
SQL_BY_KEY = """SELECT r.* FROM runtime_release r JOIN runtime_branch b USING(project_id,branch_id)
 WHERE project_id=%s AND idempotency_key=%s AND b.mode='PRODUCTION'"""
SQL_BRANCH_BY_KEY = "SELECT * FROM runtime_release WHERE project_id=%s AND branch_id=%s AND idempotency_key=%s"
SQL_RELEASE = """SELECT * FROM runtime_release WHERE project_id=%s AND release_id=COALESCE(
 (SELECT target_id FROM runtime_legacy_identity WHERE project_id=runtime_release.project_id
 AND kind='RELEASE' AND legacy_id=%s),%s)"""
SQL_SEQUENCE = """UPDATE runtime_branch SET publication_sequence=publication_sequence+1,latest_release_id=%s
 ,observed_head_sha=COALESCE(%s,observed_head_sha),
 base_commit_sha=CASE WHEN mode='PRODUCTION' THEN COALESCE(base_commit_sha,%s) ELSE base_commit_sha END
 WHERE project_id=%s AND branch_id=%s"""
SQL_CANDIDATE = """INSERT INTO runtime_release
 (release_id,project_id,sequence,idempotency_key,request_json,baseline_release_id,branch_id)
 VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING *"""
SQL_ATTACH_RUN = "UPDATE runtime_release SET run_id=%s WHERE release_id=%s"
SQL_BY_RUN = "SELECT * FROM runtime_release WHERE run_id=%s FOR UPDATE"
SQL_SUPERSEDE = "UPDATE runtime_release SET state='SUPERSEDED' WHERE release_id=%s"
SQL_PUBLISH = """UPDATE runtime_release SET state='PUBLISHED',artifact_set_id=%s,catalog_digest=%s,build_mode=%s,
 published_at=clock_timestamp() WHERE release_id=%s"""
SQL_POINTER = "UPDATE runtime_branch SET active_release_id=%s WHERE project_id=%s AND branch_id=%s"
SQL_RELATION = """INSERT INTO runtime_release_relation(release_id,native_id,creator_run_id,binding_json)
 VALUES(%s,%s,%s,%s)"""
SQL_RUN = "SELECT * FROM runtime_job WHERE job_id=%s"
SQL_PROTECTED_RUN = """SELECT 1 WHERE EXISTS(SELECT 1 FROM runtime_release WHERE run_id=%s)
 OR EXISTS(SELECT 1 FROM runtime_release_relation WHERE creator_run_id=%s)"""
SQL_FAIL = """UPDATE runtime_release SET state='FAILED',error_code=%s
 WHERE run_id=%s AND state NOT IN ('PUBLISHED','SUPERSEDED')"""
SQL_PHASE = """UPDATE runtime_release SET state=%s
 WHERE run_id=%s AND state IN ('PREPARING','BUILDING','VALIDATING')"""
PUBLISHED = "PUBLISHED"
SEALED = "SEALED"
CATALOG_PATH = "target/published_catalog.json"
INPUT_FIELDS = {"commitSha": "source_commit_sha", "projectDigest": "project_digest",
                "configVersion": "config_version", "toolchainVersion": "toolchain_version"}
BUILD_RUN = "BUILD_RUN"
ACTIVE = "ACTIVE"
SUCCEEDED = "SUCCEEDED"


class PublicationStore:
    """与任务存储共用数据库，活动版本仅在封存事务内推进。"""

    def __init__(self, db):
        # 连接池由 Runtime 拥有；store 不单独创建连接或提交外部事务。
        self.db = db

    def by_key(self, project_id, key):
        with self.db.transaction() as connection:
            return row_dict(connection.exec_driver_sql(SQL_BY_KEY, (project_id, key)))

    def project_ids(self):
        with self.db.transaction() as connection:
            return [row[0] for row in connection.exec_driver_sql(
                "SELECT project_id FROM runtime_project ORDER BY project_id")]

    def releases(self, project_id):
        with self.db.transaction() as connection:
            return [dict(row) for row in connection.exec_driver_sql(
                "SELECT * FROM runtime_release WHERE project_id=%s ORDER BY created_at DESC", (project_id,)
            ).mappings()]

    def reserve_build(self, jobs, project, key, snapshot, source_id, timeout_seconds):
        from ..platform.namespace import validate_schema_name
        from .jobs import SQL_SELECT_PG_ADVISORY_XACT_LOCK_HASHTEXTEXTENDED

        project_id = project["project_id"]
        scope = "PUBLICATION:" + project_id
        with self.db.transaction() as connection:
            connection.exec_driver_sql(SQL_SELECT_PG_ADVISORY_XACT_LOCK_HASHTEXTEXTENDED, (scope + ":" + key,))
            connection.exec_driver_sql(SQL_PARENT_LOCK, (project_id,))
            prior = row_dict(connection.exec_driver_sql(SQL_BY_KEY, (project_id, key)))
            if prior:
                if prior["request_json"]["commitSha"] != snapshot["commitSha"]:
                    raise StoreConflict("release key already binds another commit")
                return prior
            current = row_dict(connection.exec_driver_sql(
                "SELECT * FROM runtime_project WHERE project_id=%s", (project_id,)))
            if (current["binding_config"] != project["binding_config"]
                    or current["config_version"] != snapshot["configVersion"]):
                raise StoreConflict("project changed while reading commit")
            release = self.create_candidate_in_transaction(connection, project_id, snapshot, key)
            run_id = uuid4()
            binding = project["binding_config"]
            schema = validate_schema_name(binding["schemaName"]) if binding.get("schemaName") else "run_" + run_id.hex
            job = jobs.reserve_in_transaction(
                connection, BUILD_RUN, project_id, {**snapshot, "binding": binding, "releaseId": release["release_id"]},
                job_id=str(run_id), input_set_id=source_id, idempotency_scope=scope, idempotency_key=key,
                config_version=snapshot["configVersion"], toolchain_version=snapshot["toolchainVersion"],
                profile_binding_id=snapshot["profileBindingId"], schema_name=schema,
                timeout_seconds=timeout_seconds, expected_revision=project["revision"],
            )
            connection.exec_driver_sql(SQL_ATTACH_RUN, (job["job_id"], release["release_id"]))
            return {**release, "run_id": job["job_id"]}

    def create_candidate(self, project_id: str, request: dict, idempotency_key: str, *,
                         branch_id: str | None = None) -> dict:
        # 分支行串行化候选序号与请求幂等；不同分支互不淘汰。
        with self.db.transaction() as connection:
            return self.create_candidate_in_transaction(
                connection, project_id, request, idempotency_key,
                branch_id=branch_id,
            )

    def create_candidate_in_transaction(
        self,
        connection: Connection,
        project_id: str,
        request: dict,
        idempotency_key: str,
        *,
        branch_id: str | None = None,
    ) -> dict:
        # 分支行串行化候选序号与请求幂等；不同分支互不淘汰。
        if not idempotency_key:
            raise ValueError("候选幂等键不能为空")
        sql_result = connection.exec_driver_sql(SQL_PARENT_LOCK, (project_id,))
        sql_result = connection.exec_driver_sql(SQL_BRANCH_LOCK, (project_id, branch_id, branch_id))
        project = row_dict(sql_result)
        if not project:
            raise KeyError(project_id)
        sql_result = connection.exec_driver_sql(
            SQL_BRANCH_BY_KEY, (project_id, project["branch_id"], idempotency_key)
        )
        prior = row_dict(sql_result)
        if prior:
            if prior["request_json"] != request:
                raise StoreConflict("候选幂等键已用于不同输入")
            return prior
        if project["status"] != ACTIVE:
            raise StoreConflict("分支当前不接受新候选")
        sql_result = connection.exec_driver_sql(
            SQL_CANDIDATE,
            (
                str(uuid4()),
                project_id,
                project["publication_sequence"] + 1,
                idempotency_key,
                Json(request),
                project["active_release_id"],
                project["branch_id"],
            ),
        )
        result = row_dict(sql_result)
        sql_result = connection.exec_driver_sql(
            SQL_SEQUENCE,
            (
                result["release_id"],
                request.get("commitSha"),
                request.get("commitSha"),
                project_id,
                project["branch_id"],
            ),
        )
        return result

    def get_release(self, project_id: str, release_id) -> dict:
        with self.db.transaction() as connection:
            sql_result = connection.exec_driver_sql(SQL_RELEASE, (project_id, str(release_id), str(release_id)))
            row = row_dict(sql_result)
            if not row:
                raise KeyError(str(release_id))
            return row

    def get_publication(self, project_id: str, *, branch_id: str | None = None) -> dict:
        # 在单一事务读取指针和记录，发布身份一旦取定就不替换成后续版本。
        with self.db.transaction() as connection:
            sql_result = connection.exec_driver_sql(SQL_BRANCH_READ, (project_id, branch_id, branch_id))
            project = row_dict(sql_result)
            if not project:
                raise KeyError(project_id)
            active = None
            if project["active_release_id"]:
                active_id = project["active_release_id"]
                sql_result = connection.exec_driver_sql(SQL_RELEASE, (project_id, active_id, active_id))
                active = dict(row_dict(sql_result))
            return {"projectId": project_id, "activePublication": active}

    def publish_in_transaction(self, connection: Connection, *, job_id: UUID, attempt_token: str,
                               release_id: UUID, output_set_id: UUID) -> None:
        # 调用者持有 job/attempt 锁；与受理及清理保持 job → project → release 顺序。
        job = JobStore(self.db)._authorized(connection, job_id, attempt_token)
        if not job:
            raise ValueError("发布租约已失效")
        sql_result = connection.exec_driver_sql(SQL_PARENT_LOCK, (job["project_id"],))
        # 从不可变候选读取分支；先锁分支再锁候选，与受理顺序一致。
        sql_result = connection.exec_driver_sql(SQL_RELEASE, (job["project_id"], str(release_id), str(release_id)))
        identity = row_dict(sql_result)
        if not identity:
            raise ValueError("发布与执行身份不匹配")
        sql_result = connection.exec_driver_sql(
            SQL_BRANCH_LOCK, (job["project_id"], identity["branch_id"], identity["branch_id"])
        )
        project = row_dict(sql_result)
        sql_result = connection.exec_driver_sql(SQL_BY_RUN, (str(job_id),))
        release = row_dict(sql_result)
        if (not release or release["release_id"] != str(release_id)
                or job["branch_id"] != release["branch_id"]):
            raise ValueError("发布与执行身份不匹配")
        if release["state"] == PUBLISHED:
            return
        if (project["status"] != ACTIVE or project["publication_sequence"] != release["sequence"]
                or project["active_release_id"] != release["baseline_release_id"]
                or project["config_version"] != job["config_version"]):
            sql_result = connection.exec_driver_sql(SQL_SUPERSEDE, (str(release_id),))
            return
        sql_result = connection.exec_driver_sql(SQL_SET, (str(output_set_id),))
        output = row_dict(sql_result)
        if (not output or output["state"] != SEALED or output["project_id"] != job["project_id"]
                or output["producer_attempt_id"] != job["current_attempt_id"]
                or output["validation_json"].get("publicationValidated") is not True):
            raise ValueError("缺少已封存的完整发布证明")
        # 源码、配置、工具链以及源产物均必须来自这个候选的固定输入。
        if (any(not release["request_json"].get(field)
                or release["request_json"][field] != job["request_json"].get(field)
                or release["request_json"][field] != output[column]
                for field, column in INPUT_FIELDS.items())
                or output["source_set_id"] != job["input_set_id"]):
            raise ValueError("发布产物与候选输入不匹配")
        sql_result = connection.exec_driver_sql(SQL_FILE, (str(output_set_id), CATALOG_PATH))
        file = row_dict(sql_result)
        if not file:
            raise ValueError("发布展示文件未入库")
        catalog = PublishedCatalog.model_validate_json(_decode(file, MAX_FILE_BYTES))
        if catalog.project_id != job["project_id"] or str(catalog.release_id) != str(release_id):
            raise ValueError("展示产物身份不匹配")
        for binding in catalog.relation_bindings:
            if ((binding.mode == BindingMode.EXTERNAL) != (binding.creator_run_id is None)
                    or binding.mode == BindingMode.BUILT and str(binding.creator_run_id) != str(job_id)
                    or binding.mode == BindingMode.REUSED and str(binding.creator_run_id) == str(job_id)):
                raise ValueError("物理绑定创建者身份不匹配")
            if binding.creator_run_id:
                sql_result = connection.exec_driver_sql(SQL_RUN, (str(binding.creator_run_id),))
                creator = row_dict(sql_result)
                if (not creator or creator["project_id"] != job["project_id"]
                        or creator["branch_id"] != release["branch_id"]
                        or creator["kind"] != BUILD_RUN or creator["run_lifecycle"] != ACTIVE
                        or binding.mode == BindingMode.REUSED and creator["status"] != SUCCEEDED):
                    raise ValueError("物理绑定所属项目不匹配")
            sql_result = connection.exec_driver_sql(SQL_RELATION, (str(release_id), binding.native_id,
                                         str(binding.creator_run_id) if binding.creator_run_id else None,
                                         Json(binding.model_dump(mode="json", by_alias=True))))
        sql_result = connection.exec_driver_sql(SQL_PUBLISH, (str(output_set_id), file["raw_sha256"],
                                    output["validation_json"].get("buildMode", "FULL_BUILD"), str(release_id)))
        sql_result = connection.exec_driver_sql(SQL_POINTER, (str(release_id), job["project_id"], project["branch_id"]))
