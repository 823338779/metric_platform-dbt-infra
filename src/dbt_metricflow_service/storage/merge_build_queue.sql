-- 停止服务和 worker，并先执行 detach_runtime_project.sql。整个迁移在目标 schema 内原子提交。
BEGIN;
DO $merge$
BEGIN
    IF to_regclass(format('%I.runtime_job', current_schema())) IS NULL THEN
        IF EXISTS (SELECT FROM information_schema.columns WHERE table_schema = current_schema()
                   AND table_name = 'engine_build' AND column_name = 'current_attempt_id') THEN
            RETURN;
        END IF;
        RAISE EXCEPTION 'Expected the build and queue tables in the current schema';
    END IF;
    LOCK TABLE engine_build, runtime_job, runtime_attempt, runtime_execution_log IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (SELECT FROM runtime_job j LEFT JOIN engine_build b ON b.run_id = j.job_id
               WHERE b.build_id IS NULL OR j.parent_run_id IS NOT NULL) THEN
        RAISE EXCEPTION 'Unmapped or parent-linked legacy jobs require reconciliation before merging';
    END IF;
    IF EXISTS (SELECT FROM engine_build b JOIN runtime_job j ON j.job_id = b.run_id
               WHERE b.config_snapshot IS DISTINCT FROM (b.request_json->'execution')
                  OR b.requested_commit_sha IS DISTINCT FROM (b.request_json->>'commitSha')
                  OR j.schema_name IS DISTINCT FROM (b.config_snapshot->>'schema')
                  OR j.profile_binding_id IS DISTINCT FROM 'execution'
                  OR j.request_json->'execution' IS DISTINCT FROM b.config_snapshot) THEN
        RAISE EXCEPTION 'Inconsistent execution snapshots require reconciliation before merging';
    END IF;
    IF EXISTS (SELECT FROM runtime_execution_log l JOIN engine_build b ON b.run_id = l.job_id
               WHERE l.build_id IS NOT NULL AND l.build_id <> b.build_id) THEN
        RAISE EXCEPTION 'Execution log refers to a different build than its job';
    END IF;

    ALTER TABLE engine_build
        ADD COLUMN project_id text,
        ADD COLUMN input_set_id uuid REFERENCES runtime_artifact_set(set_id),
        ADD COLUMN project_digest text,
        ADD COLUMN attempt_no integer DEFAULT 0 NOT NULL,
        ADD COLUMN current_attempt_id uuid REFERENCES runtime_attempt(attempt_id),
        ADD COLUMN available_at timestamptz DEFAULT clock_timestamp() NOT NULL,
        ADD COLUMN max_attempts integer DEFAULT 3 NOT NULL CHECK (max_attempts BETWEEN 1 AND 3),
        ADD COLUMN retry_policy text DEFAULT 'PREPARATION_ONLY' NOT NULL
            CHECK (retry_policy IN ('PREPARATION_ONLY', 'NEVER')),
        ADD COLUMN started_at timestamptz,
        ADD COLUMN error_detail jsonb;
    -- 构建表已保存对外状态、阶段及输出引用。这里只补入领取、恢复和验证源码所需的字段。
    UPDATE engine_build b SET
        project_id = j.project_id, input_set_id = j.input_set_id, project_digest = j.request_json->>'projectDigest',
        attempt_no = j.attempt_no, current_attempt_id = j.current_attempt_id, available_at = j.available_at,
        max_attempts = j.max_attempts, retry_policy = j.retry_policy, started_at = j.started_at,
        error_detail = j.error_detail
    FROM runtime_job j WHERE j.job_id = b.run_id;
    ALTER TABLE engine_build ALTER COLUMN project_id SET NOT NULL;

    -- 保留 attempt 身份、租约和执行序号，通过旧 run_id 换算为公开构建身份。
    ALTER TABLE runtime_attempt ADD COLUMN build_id uuid REFERENCES engine_build(build_id);
    UPDATE runtime_attempt a SET build_id = b.build_id FROM engine_build b WHERE b.run_id = a.job_id;
    ALTER TABLE runtime_attempt ALTER COLUMN build_id SET NOT NULL, DROP COLUMN job_id,
        ADD UNIQUE (build_id, attempt_no);
    CREATE INDEX runtime_attempt_unconfirmed ON runtime_attempt(build_id)
        WHERE state = 'EXPIRED_UNCONFIRMED' AND stop_confirmed_at IS NULL;

    -- jobId 是日志 API 已公开的字段。旧日志保留原值，新日志使用 buildId。
    UPDATE runtime_execution_log l SET build_id = b.build_id FROM engine_build b
        WHERE l.job_id = b.run_id AND l.build_id IS NULL;
    ALTER TABLE runtime_execution_log ALTER COLUMN build_id SET NOT NULL,
        DROP CONSTRAINT runtime_execution_log_job_id_fkey;

    ALTER TABLE engine_build DROP COLUMN run_id, DROP COLUMN config_snapshot, DROP COLUMN requested_commit_sha;
    DROP TABLE runtime_job;
    DROP FUNCTION engine_sync_job();
    CREATE INDEX engine_build_queue ON engine_build(available_at, created_at) WHERE build_status = 'QUEUED';
END
$merge$;
COMMIT;
