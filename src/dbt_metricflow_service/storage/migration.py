"""旧封存记录映射为引擎历史；不修改原始产物、审计和物理引用。"""

from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import Connection, insert

from dbt_metricflow_service.models.payloads import HistoryMigrationReport

from .build_tables import BUILD
from .builds import digest
from .postgres import SQL_LOCK_FACTS
from .rows import row_dict

SQL_HISTORY = """SELECT r.*,p.binding_config AS project_config,b.binding_config AS branch_config,
 b.git_ref,b.mode,b.active_release_id,j.status AS job_status,j.toolchain_version,j.config_version,
 j.profile_binding_id,j.finished_at FROM runtime_release r JOIN runtime_project p USING(project_id)
 JOIN runtime_branch b USING(project_id,branch_id) LEFT JOIN runtime_job j ON j.job_id=r.run_id
 ORDER BY r.created_at,r.release_id"""
SQL_EXISTING = "SELECT repository,run_id FROM engine_build WHERE build_id=%s"
SQL_UPDATE = """UPDATE engine_build SET version=version+1,build_status=%s,output_set_id=%s,catalog_digest=%s,
 source_incomplete=%s,error_code=%s,created_at=%s,updated_at=%s,finished_at=%s,phase=%s WHERE build_id=%s"""
SQL_TARGET_INSERT = """INSERT INTO engine_deployment_target(repository,environment,branch_name,version,
 desired_generation,active_build_id) VALUES(%s,%s,%s,1,1,%s) ON CONFLICT DO NOTHING RETURNING *"""
SQL_ATTEMPT_INSERT = """INSERT INTO engine_deployment_attempt(repository,environment,branch_name,generation,
 build_id,caller,idempotency_key,request_digest,deployment_status,created_at,deployed_at)
 VALUES(%s,%s,%s,1,%s,'migration',%s,%s,'DEPLOYED',%s,%s)"""
MIGRATION = "migration"
SOURCE_INCOMPLETE = "SOURCE_INCOMPLETE"
UNRESOLVED_PREFIX = "urn:unresolved:"


def migrate_history(connection: Connection, bindings: dict[str, str], dry_run: bool=False) -> HistoryMigrationReport:
    """调用方拥有事务；同源冲突阻止提交，不静默覆盖已映射来源。"""
    if not dry_run:
        connection.exec_driver_sql(SQL_LOCK_FACTS)
    rows = [dict(row) for row in connection.exec_driver_sql(SQL_HISTORY).mappings()]
    report: HistoryMigrationReport = {"mapping": {}, "sourceIncomplete": [], "conflicts": [], "dryRun": dry_run}
    prepared = []
    for row in rows:
        old = row["release_id"]
        identifier = str(uuid5(NAMESPACE_URL, "dbt-service/build/" + old))
        config = row["branch_config"] or row["project_config"]
        configured = config.get("remote")
        mapped = bindings.get(row["project_id"])
        if mapped and configured and mapped != configured:
            report["conflicts"].append(old)
            continue
        repository = mapped or configured or UNRESOLVED_PREFIX + digest(row["project_id"])
        sha = row["request_json"].get("commitSha")
        incomplete = not (mapped or configured) or not sha or not row["run_id"]
        prior = row_dict(connection.exec_driver_sql(SQL_EXISTING, (identifier,)))
        # 已确认的映射也是来源证据；分批增量导入无需每次重传此前所有映射。
        if prior and not (mapped or configured):
            repository = prior["repository"]
            incomplete = repository.startswith(UNRESOLVED_PREFIX) or not sha or not row["run_id"]
        if prior and (prior["repository"] != repository or prior["run_id"] != row["run_id"]):
            report["conflicts"].append(old)
            continue
        report["mapping"][old] = identifier
        if incomplete:
            report["sourceIncomplete"].append(old)
        prepared.append((row, identifier, repository, sha, config, incomplete, prior))
    if report["conflicts"] or dry_run:
        return report
    for row, identifier, repository, sha, config, incomplete, prior in prepared:
        if prior:
            continue
        branch = row["git_ref"].removeprefix("refs/heads/")
        snapshot = {
            key: config[key]
            for key in ("profileBindingId", "businessTimezone", "schemaName", "queryRetrySafe")
            if key in config
        }
        version = row["config_version"] or "unknown"
        toolchain = row["toolchain_version"] or "unknown"
        payload = {"repository": repository, "branchName": branch, "commitSha": sha}
        # 与新构建共享列类型和具名插入，保留历史身份及来源快照。
        connection.execute(
            insert(BUILD).values(
                build_id=identifier,
                run_id=row["run_id"],
                repository=repository,
                branch_name=branch,
                environment=row["mode"],
                execution_binding=row["profile_binding_id"] or "unknown",
                config_version=version,
                toolchain_version=toolchain,
                caller=MIGRATION,
                idempotency_key=row["release_id"],
                request_digest=digest(payload),
                request_json=payload,
                config_snapshot=snapshot,
                requested_commit_sha=sha,
                commit_sha=sha,
            ).returning(BUILD)
        )
        status = "SUCCEEDED" if row["state"] in {"PUBLISHED", "SUPERSEDED"} else row["job_status"] or "FAILED"
        connection.exec_driver_sql(
            SQL_UPDATE,
            (
                status,
                row["artifact_set_id"],
                row["catalog_digest"],
                incomplete,
                SOURCE_INCOMPLETE if incomplete else row["error_code"],
                row["created_at"],
                row["created_at"],
                row["finished_at"],
                "COMPLETE" if status == "SUCCEEDED" else row["state"],
                identifier,
            ),
        )
        # 只恢复原本有效的指针，不把历史成功版本自动激活或覆盖已存在的新指针。
        if not incomplete and row["active_release_id"] == row["release_id"] and status == "SUCCEEDED":
            target = row_dict(
                connection.exec_driver_sql(SQL_TARGET_INSERT, (repository, row["mode"], branch, identifier))
            )
            if target:
                connection.exec_driver_sql(
                    SQL_ATTEMPT_INSERT,
                    (
                        repository,
                        row["mode"],
                        branch,
                        identifier,
                        row["release_id"],
                        digest(payload),
                        row["created_at"],
                        row["published_at"],
                    ),
                )
    return report
