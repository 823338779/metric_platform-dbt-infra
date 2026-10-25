-- 停止全部服务和 worker，在目标服务 schema 的事务内执行。
-- 若仍有 run_lifecycle，先执行 remove_run_cleanup.sql；历史记录和关联关系保留。
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_attribute
               WHERE attrelid = 'runtime_job'::regclass AND attname = 'kind' AND NOT attisdropped) THEN
        UPDATE runtime_job SET status='FAILED', error_code='CLEANUP_REMOVED', finished_at=clock_timestamp()
        WHERE kind <> 'BUILD_RUN' AND status IN ('QUEUED', 'RUNNING');
        UPDATE runtime_project SET busy_job_id=NULL
        WHERE busy_job_id IN (SELECT job_id FROM runtime_job WHERE kind <> 'BUILD_RUN');
    END IF;
END $$;
DROP INDEX IF EXISTS runtime_job_queue;
ALTER TABLE runtime_job DROP COLUMN IF EXISTS kind;
CREATE INDEX runtime_job_queue ON runtime_job (available_at, created_at) WHERE status='QUEUED';
