"""增加引擎构建和部署事实；不改写历史产物和旧迁移。"""

from alembic import op

revision = "0002_build_deployment_contract"
down_revision = "0001_runtime_adoption"
branch_labels = None
depends_on = None

DDL = """
CREATE TABLE engine_execution_binding (
 repository text NOT NULL, execution_binding text NOT NULL, config_version text NOT NULL,
 config_json jsonb NOT NULL, PRIMARY KEY(repository,execution_binding,config_version)
);
CREATE TABLE engine_build (
 build_id uuid PRIMARY KEY, run_id uuid UNIQUE REFERENCES runtime_job(job_id),
 repository text NOT NULL, branch_name text, environment text NOT NULL,
 execution_binding text NOT NULL, config_version text NOT NULL, toolchain_version text NOT NULL,
 caller text NOT NULL, idempotency_key text NOT NULL, request_digest text NOT NULL,
 request_json jsonb NOT NULL, config_snapshot jsonb NOT NULL,
 requested_commit_sha text, commit_sha text, build_status text NOT NULL DEFAULT 'QUEUED',
 phase text NOT NULL DEFAULT 'RESOLVING_SOURCE', cancel_requested boolean NOT NULL DEFAULT false,
 output_set_id uuid REFERENCES runtime_artifact_set(set_id), catalog_digest text,
 error_code text, source_incomplete boolean NOT NULL DEFAULT false, version bigint NOT NULL DEFAULT 1,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
 UNIQUE(repository,caller,idempotency_key),
 CHECK(environment IN ('PREVIEW','PRODUCTION')),
 CHECK(build_status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED','OUTCOME_UNKNOWN'))
);
CREATE INDEX engine_build_history ON engine_build(repository,created_at,build_id);
CREATE TABLE engine_deployment_target (
 repository text NOT NULL, environment text NOT NULL, branch_name text NOT NULL,
 version bigint NOT NULL DEFAULT 0, desired_generation bigint NOT NULL DEFAULT 0,
 active_build_id uuid REFERENCES engine_build(build_id), observed_head_sha text,
 head_observed_at timestamptz, source_state text NOT NULL DEFAULT 'UNDEPLOYED',
 PRIMARY KEY(repository,environment,branch_name)
);
CREATE TABLE engine_deployment_attempt (
 repository text NOT NULL, environment text NOT NULL, branch_name text NOT NULL,
 generation bigint NOT NULL, build_id uuid NOT NULL REFERENCES engine_build(build_id),
 caller text NOT NULL, idempotency_key text NOT NULL, request_digest text NOT NULL,
 operation text NOT NULL DEFAULT 'DEPLOYMENT',
 deployment_status text NOT NULL, reason text, version bigint NOT NULL DEFAULT 1,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), deployed_at timestamptz, last_checked_at timestamptz,
 PRIMARY KEY(repository,environment,branch_name,generation),
 UNIQUE(repository,caller,operation,idempotency_key),
 FOREIGN KEY(repository,environment,branch_name)
 REFERENCES engine_deployment_target(repository,environment,branch_name)
);
CREATE TABLE engine_change_counter (singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), value bigint NOT NULL);
INSERT INTO engine_change_counter VALUES(true,0);
CREATE TABLE engine_change (
 sequence bigint PRIMARY KEY, repository text NOT NULL, object_type text NOT NULL,
 object_id text NOT NULL, object_version bigint NOT NULL, summary jsonb NOT NULL
);
CREATE FUNCTION engine_record_change() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE next_sequence bigint; identity text; record_version bigint; projection jsonb; lifecycle text;
BEGIN
 UPDATE engine_change_counter SET value=value+1 WHERE singleton RETURNING value INTO next_sequence;
 IF TG_TABLE_NAME='engine_build' THEN
  identity := NEW.build_id::text; record_version := NEW.version;
 ELSIF TG_TABLE_NAME='engine_deployment_target' THEN
  identity := jsonb_build_array(NEW.repository,NEW.environment,NEW.branch_name)::text;
  -- 观察事实有自己的变化版本，不能复用不随观察更新的目标 CAS version。
  record_version := next_sequence;
 ELSE
  identity := jsonb_build_array(NEW.repository,NEW.environment,NEW.branch_name,NEW.generation)::text;
  record_version := NEW.version;
 END IF;
 projection := to_jsonb(NEW)-'request_json'-'config_snapshot'-'caller'-'idempotency_key'
               -'request_digest'-'last_checked_at';
 IF TG_TABLE_NAME='engine_build' THEN
  SELECT run_lifecycle INTO lifecycle FROM runtime_job WHERE job_id=NEW.run_id;
  projection := projection || jsonb_build_object(
   'catalog_available',NEW.build_status='SUCCEEDED' AND NEW.output_set_id IS NOT NULL,
   'query_available',NEW.build_status='SUCCEEDED' AND NEW.output_set_id IS NOT NULL
      AND lifecycle='ACTIVE' AND NOT NEW.source_incomplete,
   'query_unavailable_reason',CASE
      WHEN NEW.build_status<>'SUCCEEDED' THEN 'BUILD_NOT_SUCCEEDED'
      WHEN NEW.output_set_id IS NULL THEN 'ARTIFACT_UNAVAILABLE'
      WHEN lifecycle IN ('CLEANING','CLEANED') THEN 'PHYSICAL_OBJECTS_REMOVED'
      WHEN NEW.source_incomplete THEN 'EXECUTION_ENVIRONMENT_UNAVAILABLE' ELSE NULL END);
 END IF;
 INSERT INTO engine_change VALUES(next_sequence,NEW.repository,TG_TABLE_NAME,identity,record_version,projection);
 RETURN NEW;
END $$;
CREATE TRIGGER engine_build_change AFTER INSERT OR UPDATE ON engine_build
 FOR EACH ROW EXECUTE FUNCTION engine_record_change();
CREATE TRIGGER engine_target_change AFTER INSERT OR UPDATE ON engine_deployment_target
 FOR EACH ROW EXECUTE FUNCTION engine_record_change();
CREATE TRIGGER engine_attempt_change AFTER INSERT OR UPDATE OF deployment_status,reason,version,deployed_at
 ON engine_deployment_attempt
 FOR EACH ROW EXECUTE FUNCTION engine_record_change();
CREATE FUNCTION engine_sync_job() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 UPDATE engine_build SET
  build_status=CASE
   WHEN NEW.error_code='CANCELLED' THEN 'CANCELLED'
   WHEN NEW.error_code='EXECUTION_OUTCOME_UNKNOWN' OR NEW.error_detail->>'externalOutcomeUnknown'='true'
    THEN 'OUTCOME_UNKNOWN'
   ELSE NEW.status END,
  phase=CASE WHEN NEW.status='SUCCEEDED' THEN 'COMPLETE'
             WHEN NEW.phase='PREPARING' THEN 'RESOLVING_SOURCE' ELSE NEW.phase END,
  output_set_id=NEW.output_set_id,error_code=NEW.error_code,
  catalog_digest=(SELECT raw_sha256 FROM runtime_artifact_file
                  WHERE set_id=NEW.output_set_id AND relative_path='target/published_catalog.json'),
  finished_at=NEW.finished_at,updated_at=clock_timestamp(),version=version+1
 WHERE run_id=NEW.job_id;
 RETURN NEW;
END $$;
CREATE TRIGGER engine_job_change AFTER UPDATE OF status,phase,output_set_id,error_code,run_lifecycle ON runtime_job
 FOR EACH ROW EXECUTE FUNCTION engine_sync_job();
"""

# 每个新增列均给出可审计含义；表名和字段为静态迁移常量。
COMMENTS = {
    "repository": "受控规范仓库地址",
    "execution_binding": "可复用执行配置名称",
    "config_version": "不可变执行配置版本",
    "config_json": "执行配置，不含凭据正文",
    "build_id": "一次构建身份",
    "run_id": "内部执行记录关联，不公开为构建身份",
    "branch_name": "Git 短分支名",
    "environment": "物理执行环境",
    "toolchain_version": "固定工具链版本",
    "caller": "可信服务调用来源",
    "operation": "自动部署与独立部署的幂等操作域",
    "idempotency_key": "调用方稳定请求键",
    "request_digest": "规范化输入摘要",
    "request_json": "原始规范化请求",
    "config_snapshot": "受理时固定的执行语义配置",
    "requested_commit_sha": "请求显式指定的提交",
    "commit_sha": "实际固定源码提交",
    "build_status": "构建终态，不代表部署成功",
    "phase": "执行进度阶段",
    "cancel_requested": "持久取消意图",
    "output_set_id": "可靠封存产物引用",
    "catalog_digest": "封存目录字节摘要",
    "error_code": "脱敏稳定错误码",
    "source_incomplete": "历史来源证据是否不足",
    "version": "对象递增版本",
    "created_at": "受理时间",
    "updated_at": "最近事实变更时间",
    "finished_at": "执行终态时间",
    "desired_generation": "最新受理的部署意图序号",
    "active_build_id": "当前有效构建指针",
    "observed_head_sha": "最近观察的远端提交",
    "head_observed_at": "远端观察时间",
    "source_state": "远端相对部署的观察状态",
    "generation": "按受理顺序分配的部署序号",
    "deployment_status": "部署意图状态",
    "reason": "稳定状态原因",
    "deployed_at": "实际切换时间",
    "last_checked_at": "内部公平调度的最近扫描时间，不生成业务变化",
    "singleton": "变化流计数器唯一行",
    "value": "最后同事务分配的变化序号",
    "sequence": "提交顺序可重放变化序号",
    "object_type": "引擎事实类型",
    "object_id": "变化对象自然身份",
    "object_version": "变化对应对象版本",
    "summary": "完整有界事实摘要，不含执行秘密",
}
TABLES = (
    "engine_execution_binding",
    "engine_build",
    "engine_deployment_target",
    "engine_deployment_attempt",
    "engine_change_counter",
    "engine_change",
)
SQL_COLUMNS = "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=%s"


def upgrade():
    connection = op.get_bind()
    connection.exec_driver_sql(DDL, execution_options={"no_parameters": True})
    # 列名称来自本迁移创建的表；注释不拼入用户输入。
    for table in TABLES:
        for column in connection.exec_driver_sql(SQL_COLUMNS, (table,)).scalars():
            connection.exec_driver_sql(f"COMMENT ON COLUMN {table}.{column} IS '{COMMENTS[column]}'")


def downgrade():
    raise RuntimeError("downgrade: " + "Restore a verified backup; build history must not be discarded")
