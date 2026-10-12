-- 增量增加外部执行身份，保留旧构建、执行配置和全部产物。
ALTER TABLE engine_build ADD COLUMN IF NOT EXISTS protocol_version integer NOT NULL DEFAULT 3;
ALTER TABLE engine_build ADD COLUMN IF NOT EXISTS environment_id uuid;
ALTER TABLE engine_build ADD COLUMN IF NOT EXISTS physical_target_key text;
ALTER TABLE engine_build ALTER COLUMN execution_binding DROP NOT NULL;
ALTER TABLE engine_build DROP CONSTRAINT IF EXISTS engine_build_environment_check;
ALTER TABLE engine_build ADD CONSTRAINT engine_build_environment_check CHECK (
    (protocol_version = 3 AND environment IN ('PREVIEW', 'PRODUCTION') AND environment_id IS NULL)
    OR (protocol_version = 4 AND environment_id IS NOT NULL AND environment = environment_id::text
        AND physical_target_key IS NOT NULL AND execution_binding IS NULL)
);
COMMENT ON COLUMN engine_build.protocol_version IS '构建输入协议版本，区分旧绑定与外部执行快照';
COMMENT ON COLUMN engine_build.environment_id IS '调用方环境 UUID，旧构建不猜测归属';
COMMENT ON COLUMN engine_build.physical_target_key IS '规范化实例、catalog 和 schema 的物理写入目标摘要';
CREATE INDEX IF NOT EXISTS engine_build_environment_history ON engine_build(environment_id, created_at DESC, build_id DESC);
CREATE INDEX IF NOT EXISTS engine_build_physical_target ON engine_build(physical_target_key);
ALTER TABLE runtime_job ADD COLUMN IF NOT EXISTS physical_target_key text;
ALTER TABLE runtime_job ADD COLUMN IF NOT EXISTS target_claimed boolean NOT NULL DEFAULT false;
COMMENT ON COLUMN runtime_job.physical_target_key IS '本执行的规范化物理写入目标';
COMMENT ON COLUMN runtime_job.target_claimed IS '本执行是否仍持有目标写入权，未知结果不得释放';
CREATE INDEX IF NOT EXISTS runtime_job_target_owner ON runtime_job(physical_target_key) WHERE target_claimed;

-- 部署自然键继续保留来源维度，新环境使用 UUID 字符串而非旧枚举。
ALTER TABLE engine_deployment_target DROP CONSTRAINT IF EXISTS engine_deployment_target_environment_check;
ALTER TABLE engine_deployment_target ADD CONSTRAINT engine_deployment_target_environment_check CHECK (
    environment IN ('PREVIEW', 'PRODUCTION') OR environment ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
);
ALTER TABLE engine_deployment_attempt DROP CONSTRAINT IF EXISTS engine_deployment_attempt_environment_check;
ALTER TABLE engine_deployment_attempt ADD CONSTRAINT engine_deployment_attempt_environment_check CHECK (
    environment IN ('PREVIEW', 'PRODUCTION') OR environment ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
);
