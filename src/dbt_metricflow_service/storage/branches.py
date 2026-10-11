"""分支登记与执行绑定存储；不执行 Git 网络操作。"""

from __future__ import annotations

from uuid import uuid4

from sqlalchemy import func, literal, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.storage.entities import Branch, RuntimeProject
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.rows import entity_dict

PRODUCTION = "PRODUCTION"
PREVIEW = "PREVIEW"
ACTIVE = "ACTIVE"
PROVISIONING = "PROVISIONING"
MAIN_REF = "refs/heads/main"
SCHEMA_NAME = "schemaName"
PROFILE_BINDING_ID = "profileBindingId"


class BranchStore:
    """调用方共用 Runtime 连接池，执行绑定始终来自服务配置。"""

    def __init__(self, db: Database) -> None:
        # 连接池生命周期由 Runtime 统一管理。
        self.db = db

    @staticmethod
    def ensure_production(session: Session, project_id: str, preview_profile: str | None=None) -> None:
        # 项目注册事务内更新生产绑定，已存在分支的身份和指针不变。
        statement = insert(Branch).from_select(
            [Branch.branch_id, Branch.project_id, Branch.git_ref, Branch.mode, Branch.status,
             Branch.binding_config, Branch.config_version],
            select(literal(str(uuid4())), RuntimeProject.project_id, literal(MAIN_REF), literal(PRODUCTION),
                   literal(ACTIVE), RuntimeProject.binding_config, RuntimeProject.config_version)
            .where(RuntimeProject.project_id == project_id),
        )
        session.execute(statement.on_conflict_do_update(
            index_elements=[Branch.project_id], index_where=Branch.mode == PRODUCTION,
            set_={Branch.binding_config: statement.excluded.binding_config,
                  Branch.config_version: statement.excluded.config_version},
        ))
        # 新任务沿用分支 schema 并取得最新受控配置，旧候选的配置检查将失败。
        binding = RuntimeProject.binding_config.op("||")(func.jsonb_build_object(
            SCHEMA_NAME, Branch.binding_config[SCHEMA_NAME].astext,
            PROFILE_BINDING_ID, func.coalesce(preview_profile, Branch.binding_config[PROFILE_BINDING_ID].astext),
        ))
        session.execute(update(Branch).where(
            Branch.project_id == RuntimeProject.project_id, RuntimeProject.project_id == project_id,
            Branch.mode == PREVIEW, Branch.status.in_((ACTIVE, PROVISIONING)),
            (Branch.config_version != RuntimeProject.config_version) | Branch.binding_config.is_distinct_from(binding),
        ).values(binding_config=binding, config_version=RuntimeProject.config_version,
                 signal_version=Branch.signal_version + 1, scan_token=None, scan_expires_at=None)
            .execution_options(synchronize_session=False))

    def get(self, project_id: str, branch_id: str) -> JsonObject:
        # 项目和分支必须同时匹配，防止其他项目的 UUID 穿透。
        with self.db.session() as session:
            row = session.scalar(select(Branch).where(
                Branch.project_id == project_id, Branch.branch_id == branch_id))
            if row is None:
                raise KeyError(branch_id)
            return entity_dict(row)

    def production(self, project_id: str) -> JsonObject:
        # 无分支旧接口总是定位固定生产分支。
        with self.db.session() as session:
            row = session.scalar(select(Branch).where(Branch.project_id == project_id, Branch.mode == PRODUCTION))
            if row is None:
                raise KeyError(project_id)
            return entity_dict(row)

    def list(self, project_id: str) -> list[JsonObject]:
        # 保留已删除身份，供旧工作区诊断和历史结果读取。
        with self.db.session() as session:
            return [entity_dict(row) for row in session.scalars(
                select(Branch).where(Branch.project_id == project_id).order_by(Branch.created_at, Branch.branch_id))]

    def execution_binding(self, project_id: str, branch_id: str) -> JsonObject:
        # 只供内部任务封存使用，公开 BranchView 永远不包含这些配置。
        row = self.get(project_id, branch_id)
        return {"binding_config": row["binding_config"], "config_version": row["config_version"],
                "git_ref": row["git_ref"]}
