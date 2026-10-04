"""旧平台公开身份的受控接管。"""

from uuid import UUID

from .publication_models import PublishedCatalog
from .storage.publications import CATALOG_PATH

SQL_PROJECT_LOCK = "SELECT project_id FROM runtime_project WHERE project_id=%s FOR UPDATE"
SQL_RELEASE_BY_RUN = """SELECT r.* FROM runtime_release r JOIN runtime_branch b USING(project_id,branch_id)
 WHERE project_id=%s AND run_id=%s AND r.state='PUBLISHED' AND b.mode='PRODUCTION'"""
SQL_QUERY = "SELECT * FROM runtime_job WHERE project_id=%s AND job_id=%s AND kind='METRIC_QUERY'"
SQL_ALIAS = "SELECT target_id FROM runtime_legacy_identity WHERE project_id=%s AND kind=%s AND legacy_id=%s"
SQL_INSERT_ALIAS = """INSERT INTO runtime_legacy_identity(project_id,kind,legacy_id,target_id)
 VALUES(%s,%s,%s,%s) ON CONFLICT(project_id,kind,legacy_id) DO NOTHING"""
RELEASE = "RELEASE"
QUERY = "QUERY"
DOCUMENT_KEYS = frozenset({"projectId", "legacyReleaseId", "runId", "queries"})


def import_publication(db, artifacts, document: dict, *, dry_run: bool = True) -> dict:
    """仅接管已经通过服务发布验证的 run；未验证旧产物必须先执行服务新发布。"""
    if set(document) != DOCUMENT_KEYS or not isinstance(document["queries"], list):
        raise ValueError("迁移文件格式无效")
    project_id = document["projectId"]
    legacy_id, run_id = str(UUID(document["legacyReleaseId"])), str(UUID(document["runId"]))
    with db.transaction() as cursor:
        cursor.execute(SQL_RELEASE_BY_RUN, (project_id, run_id))
        release = cursor.fetchone()
        if not release:
            raise ValueError("旧 run 尚无服务验证发布；请先完成服务新发布，旧平台记录保留只读历史")
    catalog = PublishedCatalog.model_validate_json(artifacts.read_file(release["artifact_set_id"], CATALOG_PATH))
    if catalog.project_id != project_id or str(catalog.release_id) != release["release_id"]:
        raise ValueError("迁移产物身份不一致")
    identities = [(RELEASE, legacy_id, release["release_id"])]
    # 同项目查询映射必须指向该固定 run，绝不能通过迁移重执行旧查询。
    with db.transaction() as cursor:
        cursor.execute(SQL_PROJECT_LOCK, (project_id,))
        if not cursor.fetchone():
            raise KeyError(project_id)
        for item in document["queries"]:
            old, target = str(UUID(item["legacyQueryId"])), str(UUID(item["queryId"]))
            cursor.execute(SQL_QUERY, (project_id, target))
            query = cursor.fetchone()
            if not query or query["parent_run_id"] != run_id:
                raise ValueError("历史查询没有对应固定 run")
            identities.append((QUERY, old, target))
        for kind, old, target in identities:
            cursor.execute(SQL_ALIAS, (project_id, kind, old))
            existing = cursor.fetchone()
            if existing and existing["target_id"] != target:
                raise ValueError("旧身份已映射到不同目标")
        if not dry_run:
            for kind, old, target in identities:
                cursor.execute(SQL_INSERT_ALIAS, (project_id, kind, old, target))
    return {"projectId": project_id, "releaseId": release["release_id"], "mapped": not dry_run,
            "identityCount": len(identities)}
