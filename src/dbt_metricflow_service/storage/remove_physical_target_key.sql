-- 停止服务和 worker 后，在目标服务 schema 执行。PostgreSQL 会随列删除依赖它们的索引。
BEGIN;
ALTER TABLE engine_build DROP COLUMN IF EXISTS physical_target_key;
ALTER TABLE runtime_job DROP COLUMN IF EXISTS physical_target_key, DROP COLUMN IF EXISTS target_claimed;
COMMIT;
