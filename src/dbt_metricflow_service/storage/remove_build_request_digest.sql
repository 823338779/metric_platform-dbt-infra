-- 停止服务和 worker 后，在目标服务 schema 执行。
BEGIN;
ALTER TABLE engine_build DROP COLUMN IF EXISTS request_digest;
ALTER TABLE engine_deployment_attempt DROP COLUMN IF EXISTS request_digest;
ALTER TABLE runtime_job ALTER COLUMN request_fingerprint DROP NOT NULL;
COMMENT ON COLUMN runtime_job.request_fingerprint IS '通用队列的幂等请求摘要';
COMMIT;
