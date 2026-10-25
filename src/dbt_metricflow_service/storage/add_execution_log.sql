-- 仅针对已有当前服务结构执行一次；由调用方设置 search_path 并在事务中执行。
CREATE TABLE runtime_execution_log (
    log_id bigserial PRIMARY KEY,
    build_id uuid REFERENCES engine_build(build_id) ON DELETE CASCADE,
    job_id uuid NOT NULL REFERENCES runtime_job(job_id) ON DELETE CASCADE,
    attempt_id uuid NOT NULL REFERENCES runtime_attempt(attempt_id) ON DELETE CASCADE,
    phase text NOT NULL,
    command text NOT NULL,
    event_time timestamp with time zone NOT NULL,
    level text NOT NULL,
    event_name text NOT NULL,
    event_code text NOT NULL,
    invocation_id text NOT NULL,
    node_id text NOT NULL,
    thread_name text NOT NULL,
    message text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL
);
COMMENT ON TABLE runtime_execution_log IS '脱敏后的 dbt 内部运行事件；独立于构建阶段变化记录';
CREATE INDEX runtime_execution_log_build_idx ON runtime_execution_log(build_id, log_id);
CREATE INDEX runtime_execution_log_attempt_idx ON runtime_execution_log(job_id, attempt_id, log_id);
