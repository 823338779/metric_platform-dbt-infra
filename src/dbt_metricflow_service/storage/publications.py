"""项目的唯一发布存储。"""

from contextlib import nullcontext
from uuid import UUID, uuid4

from psycopg2.extras import Json

from ..publication_models import BindingMode, PublishedCatalog
from .artifacts import MAX_FILE_BYTES, SQL_FILE, SQL_SET, _decode
from .jobs import JobStore, StoreConflict

SQL_PROJECT_LOCK = "SELECT * FROM runtime_project WHERE project_id=%s FOR UPDATE"
SQL_PROJECT = "SELECT * FROM runtime_project WHERE project_id=%s"
SQL_BY_KEY = "SELECT * FROM runtime_release WHERE project_id=%s AND idempotency_key=%s"
SQL_RELEASE = """SELECT * FROM runtime_release WHERE project_id=%s AND release_id=COALESCE(
 (SELECT target_id FROM runtime_legacy_identity WHERE project_id=runtime_release.project_id
 AND kind='RELEASE' AND legacy_id=%s),%s)"""
SQL_SEQUENCE = "UPDATE runtime_project SET publication_sequence=publication_sequence+1 WHERE project_id=%s"
SQL_CANDIDATE = """INSERT INTO runtime_release
 (release_id,project_id,sequence,idempotency_key,request_json,baseline_release_id)
 VALUES(%s,%s,%s,%s,%s,%s) RETURNING *"""
SQL_ATTACH_RUN = "UPDATE runtime_release SET run_id=%s WHERE release_id=%s"
SQL_BY_RUN = "SELECT * FROM runtime_release WHERE run_id=%s FOR UPDATE"
SQL_SUPERSEDE = "UPDATE runtime_release SET state='SUPERSEDED' WHERE release_id=%s"
SQL_PUBLISH = """UPDATE runtime_release SET state='PUBLISHED',artifact_set_id=%s,catalog_digest=%s,build_mode=%s,
 published_at=clock_timestamp() WHERE release_id=%s"""
SQL_POINTER = "UPDATE runtime_project SET active_published_release_id=%s WHERE project_id=%s"
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

    def create_candidate(self, project_id: str, request: dict, idempotency_key: str, *, _cursor=None) -> dict:
        # 项目行串行化候选序号与请求幂等；同一输入的重试不创建新版本。
        if not idempotency_key:
            raise ValueError("候选幂等键不能为空")
        with nullcontext(_cursor) if _cursor is not None else self.db.transaction() as cursor:
            cursor.execute(SQL_PROJECT_LOCK, (project_id,))
            project = cursor.fetchone()
            if not project:
                raise KeyError(project_id)
            cursor.execute(SQL_BY_KEY, (project_id, idempotency_key))
            prior = cursor.fetchone()
            if prior:
                if prior["request_json"] != request:
                    raise StoreConflict("候选幂等键已用于不同输入")
                return prior
            cursor.execute(SQL_SEQUENCE, (project_id,))
            cursor.execute(SQL_CANDIDATE, (str(uuid4()), project_id, project["publication_sequence"] + 1,
                                          idempotency_key, Json(request), project["active_published_release_id"]))
            return cursor.fetchone()

    def get_release(self, project_id: str, release_id) -> dict:
        with self.db.transaction() as cursor:
            cursor.execute(SQL_RELEASE, (project_id, str(release_id), str(release_id)))
            row = cursor.fetchone()
            if not row:
                raise KeyError(str(release_id))
            return row

    def get_publication(self, project_id: str) -> dict:
        # 在单一事务读取指针和记录，发布身份一旦取定就不替换成后续版本。
        with self.db.transaction() as cursor:
            cursor.execute(SQL_PROJECT, (project_id,))
            project = cursor.fetchone()
            if not project:
                raise KeyError(project_id)
            active = None
            if project["active_published_release_id"]:
                active_id = project["active_published_release_id"]
                cursor.execute(SQL_RELEASE, (project_id, active_id, active_id))
                active = dict(cursor.fetchone())
            return {"projectId": project_id, "activePublication": active}

    def publish_in_transaction(self, cursor, *, job_id: UUID, attempt_token: str,
                               release_id: UUID, output_set_id: UUID) -> None:
        # 调用者持有 job/attempt 锁；与受理及清理保持 job → project → release 顺序。
        job = JobStore(self.db)._authorized(cursor, job_id, attempt_token)
        if not job:
            raise ValueError("发布租约已失效")
        cursor.execute(SQL_PROJECT_LOCK, (job["project_id"],))
        project = cursor.fetchone()
        cursor.execute(SQL_BY_RUN, (str(job_id),))
        release = cursor.fetchone()
        if not release or release["release_id"] != str(release_id):
            raise ValueError("发布与执行身份不匹配")
        if release["state"] == PUBLISHED:
            return
        if (project["publication_sequence"] != release["sequence"]
                or project["active_published_release_id"] != release["baseline_release_id"]
                or project["config_version"] != job["config_version"]):
            cursor.execute(SQL_SUPERSEDE, (str(release_id),))
            return
        cursor.execute(SQL_SET, (str(output_set_id),))
        output = cursor.fetchone()
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
        cursor.execute(SQL_FILE, (str(output_set_id), CATALOG_PATH))
        file = cursor.fetchone()
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
                cursor.execute(SQL_RUN, (str(binding.creator_run_id),))
                creator = cursor.fetchone()
                if (not creator or creator["project_id"] != job["project_id"]
                        or creator["kind"] != BUILD_RUN or creator["run_lifecycle"] != ACTIVE
                        or binding.mode == BindingMode.REUSED and creator["status"] != SUCCEEDED):
                    raise ValueError("物理绑定所属项目不匹配")
            cursor.execute(SQL_RELATION, (str(release_id), binding.native_id,
                                         str(binding.creator_run_id) if binding.creator_run_id else None,
                                         Json(binding.model_dump(mode="json", by_alias=True))))
        cursor.execute(SQL_PUBLISH, (str(output_set_id), file["raw_sha256"],
                                    output["validation_json"].get("buildMode", "FULL_BUILD"), str(release_id)))
        cursor.execute(SQL_POINTER, (str(release_id), job["project_id"]))
