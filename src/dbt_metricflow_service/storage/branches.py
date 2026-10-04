"""分支登记与执行绑定存储；不执行 Git 网络操作。"""

from uuid import uuid4

SQL_ENSURE_MAIN = """INSERT INTO runtime_branch
 (branch_id,project_id,git_ref,mode,status,binding_config,config_version)
 SELECT %s,project_id,'refs/heads/main','PRODUCTION','ACTIVE',binding_config,config_version
 FROM runtime_project WHERE project_id=%s
 ON CONFLICT(project_id) WHERE mode='PRODUCTION' DO UPDATE
 SET binding_config=excluded.binding_config,config_version=excluded.config_version"""
SQL_BRANCH = "SELECT * FROM runtime_branch WHERE project_id=%s AND branch_id=%s"
SQL_PRODUCTION = "SELECT * FROM runtime_branch WHERE project_id=%s AND mode='PRODUCTION'"
SQL_BRANCHES = "SELECT * FROM runtime_branch WHERE project_id=%s ORDER BY created_at,branch_id"
SQL_BRANCH_LOCK = """SELECT b.* FROM runtime_branch b WHERE project_id=%s
 AND ((%s::uuid IS NULL AND mode='PRODUCTION') OR branch_id=%s::uuid) FOR UPDATE"""
SQL_BRANCH_READ = SQL_BRANCH_LOCK.removesuffix(" FOR UPDATE")
SQL_PARENT_LOCK = "SELECT project_id FROM runtime_project WHERE project_id=%s FOR UPDATE"
SQL_REFRESH_PREVIEWS = """UPDATE runtime_branch b SET
 binding_config=p.binding_config || jsonb_build_object('schemaName',b.binding_config->>'schemaName',
 'profileBindingId',COALESCE(%s,b.binding_config->>'profileBindingId')),
 config_version=p.config_version,signal_version=b.signal_version+1,
 scan_token=NULL,scan_expires_at=NULL
 FROM runtime_project p WHERE b.project_id=p.project_id AND p.project_id=%s
 AND b.mode='PREVIEW' AND b.status IN ('ACTIVE','PROVISIONING')
 AND (b.config_version<>p.config_version OR b.binding_config IS DISTINCT FROM
 p.binding_config || jsonb_build_object('schemaName',b.binding_config->>'schemaName',
 'profileBindingId',COALESCE(%s,b.binding_config->>'profileBindingId')))"""


class BranchStore:
    """调用方共用 Runtime 连接池，执行绑定始终来自服务配置。"""

    def __init__(self, db):
        # 连接池生命周期由 Runtime 统一管理。
        self.db = db

    @staticmethod
    def ensure_production(cursor, project_id, preview_profile=None):
        # 项目注册事务内更新生产绑定，已存在分支的身份和指针不变。
        cursor.execute(SQL_ENSURE_MAIN, (str(uuid4()), project_id))
        # 新任务沿用分支 schema 并取得最新受控配置，旧候选的配置检查将失败。
        cursor.execute(SQL_REFRESH_PREVIEWS, (preview_profile, project_id, preview_profile))

    def get(self, project_id: str, branch_id: str) -> dict:
        # 项目和分支必须同时匹配，防止其他项目的 UUID 穿透。
        with self.db.transaction() as cursor:
            cursor.execute(SQL_BRANCH, (project_id, branch_id))
            row = cursor.fetchone()
            if not row:
                raise KeyError(branch_id)
            return dict(row)

    def production(self, project_id: str) -> dict:
        # 无分支旧接口总是定位固定生产分支。
        with self.db.transaction() as cursor:
            cursor.execute(SQL_PRODUCTION, (project_id,))
            row = cursor.fetchone()
            if not row:
                raise KeyError(project_id)
            return dict(row)

    def list(self, project_id: str) -> list[dict]:
        # 保留已删除身份，供旧工作区诊断和历史结果读取。
        with self.db.transaction() as cursor:
            cursor.execute(SQL_BRANCHES, (project_id,))
            return [dict(row) for row in cursor.fetchall()]

    def execution_binding(self, project_id: str, branch_id: str) -> dict:
        # 只供内部任务封存使用，公开 BranchView 永远不包含这些配置。
        row = self.get(project_id, branch_id)
        return {"binding_config": row["binding_config"], "config_version": row["config_version"],
                "git_ref": row["git_ref"]}
