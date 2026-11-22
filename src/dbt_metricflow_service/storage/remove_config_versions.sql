-- 停止服务及 worker 后，在目标服务 schema 执行。历史构建和待执行任务保留各自的快照。
BEGIN;
ALTER TABLE engine_build DROP COLUMN IF EXISTS config_version;
ALTER TABLE runtime_project DROP COLUMN IF EXISTS config_version;
ALTER TABLE runtime_job DROP COLUMN IF EXISTS config_version;
ALTER TABLE runtime_artifact_set DROP COLUMN IF EXISTS config_version;

-- 公开请求和持久化快照使用 camelCase；仅删除已停用的字段，其余输入保持原值。
UPDATE engine_build
SET config_snapshot = config_snapshot - 'configVersion',
    request_json = request_json #- '{execution,configVersion}'
WHERE config_snapshot ? 'configVersion' OR (request_json->'execution') ? 'configVersion';

UPDATE runtime_project
SET binding_config = binding_config - 'configVersion'
WHERE binding_config ? 'configVersion';

UPDATE runtime_job
SET request_json = request_json #- '{execution,configVersion}' #- '{binding,configVersion}'
WHERE (request_json->'execution') ? 'configVersion' OR (request_json->'binding') ? 'configVersion';
COMMENT ON COLUMN runtime_job.request_fingerprint IS '规范化任务请求摘要';
COMMIT;
