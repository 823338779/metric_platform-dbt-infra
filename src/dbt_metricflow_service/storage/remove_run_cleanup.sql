-- 停止全部服务和 worker，确认 search_path 为目标服务 schema 后，在事务中执行。
-- 保留历史任务、attempt、构建和产物；旧清理任务不再被领取。
DROP TRIGGER IF EXISTS engine_job_change ON runtime_job;
ALTER TABLE runtime_job DROP COLUMN IF EXISTS run_lifecycle;
ALTER TABLE runtime_job DROP CONSTRAINT IF EXISTS runtime_job_kind_check;
CREATE TRIGGER engine_job_change AFTER UPDATE OF status, phase, output_set_id, error_code
ON runtime_job FOR EACH ROW EXECUTE FUNCTION engine_sync_job();
UPDATE runtime_job SET status='FAILED', error_code='CLEANUP_REMOVED', finished_at=clock_timestamp()
WHERE kind='RUN_CLEANUP' AND status IN ('QUEUED', 'RUNNING');
UPDATE runtime_project SET busy_job_id=NULL
WHERE busy_job_id IN (SELECT job_id FROM runtime_job WHERE kind='RUN_CLEANUP');
-- NOT VALID 保留历史 RUN_CLEANUP 记录，同时禁止新增此类任务。
ALTER TABLE runtime_job ADD CONSTRAINT runtime_job_kind_check CHECK (kind = 'BUILD_RUN') NOT VALID;
COMMENT ON COLUMN runtime_job.job_id IS '内部任务标识';
COMMENT ON COLUMN runtime_job.kind IS '任务类型；新任务仅支持构建';
COMMENT ON COLUMN runtime_job.parent_run_id IS '历史父任务引用';
