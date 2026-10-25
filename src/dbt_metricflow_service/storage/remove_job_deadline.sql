-- 停止全部旧版本服务/worker，确认 search_path 为目标服务 schema 后，在事务中执行。
-- 仅删除已停用的业务超时字段，保留任务、构建及执行租约。
ALTER TABLE runtime_job DROP COLUMN IF EXISTS deadline_at;
