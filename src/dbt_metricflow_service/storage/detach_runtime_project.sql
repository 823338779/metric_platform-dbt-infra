-- 停止服务和 worker 后，在目标服务 schema 执行。历史项目行和任务、产物的归属标识均保留。
BEGIN;
ALTER TABLE runtime_job DROP CONSTRAINT IF EXISTS runtime_job_project_id_fkey;
ALTER TABLE runtime_artifact_set DROP CONSTRAINT IF EXISTS runtime_artifact_set_project_id_fkey;
-- 旧项目上的指针也要解除，否则可能阻止回收已结束的任务或产物。
ALTER TABLE IF EXISTS runtime_project
    DROP CONSTRAINT IF EXISTS runtime_project_busy_fk,
    DROP CONSTRAINT IF EXISTS runtime_project_source_fk;
COMMIT;
