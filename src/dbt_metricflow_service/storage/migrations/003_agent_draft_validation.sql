-- 定义验证复用队列租约，但既不占用写任务 busy 指针，也不发布默认产物。
ALTER TABLE runtime_job DROP CONSTRAINT runtime_job_kind_check;
ALTER TABLE runtime_job ADD CONSTRAINT runtime_job_kind_check
    CHECK(kind IN ('BUILD_RUN','METRIC_QUERY','DBT_COMMAND','MF_COMMAND','QUERY_OPTIONS','RUN_CLEANUP','DRAFT_VALIDATION'));
ALTER TABLE runtime_artifact_set DROP CONSTRAINT runtime_artifact_set_kind_check;
ALTER TABLE runtime_artifact_set ADD CONSTRAINT runtime_artifact_set_kind_check
    CHECK(kind IN ('SOURCE','EXECUTION','VALIDATION_INPUT'));
COMMENT ON COLUMN runtime_job.kind IS '执行种类；DRAFT_VALIDATION 仅解析和语义验证，不改变发布或默认产物';
COMMENT ON COLUMN runtime_artifact_set.kind IS 'SOURCE 项目源、EXECUTION 执行产物、VALIDATION_INPUT 封存的有界 YAML 操作正文';
UPDATE runtime_schema_version SET version=3;
