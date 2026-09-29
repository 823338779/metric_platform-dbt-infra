-- 六张业务表与显式迁移版本。DDL 只在部署管理命令执行。
CREATE TABLE IF NOT EXISTS runtime_schema_version(version integer PRIMARY KEY);
COMMENT ON COLUMN runtime_schema_version.version IS '已成功安装的运行时数据库版本';
CREATE TABLE IF NOT EXISTS runtime_project (
    project_id text PRIMARY KEY,
    binding_config jsonb NOT NULL DEFAULT '{}',
    config_version text NOT NULL DEFAULT '1',
    source_set_id uuid,
    current_output_set_id uuid,
    busy_job_id uuid,
    revision bigint NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS runtime_job (
    job_id uuid PRIMARY KEY,
    kind text NOT NULL CHECK(kind IN ('BUILD_RUN','METRIC_QUERY','DBT_COMMAND','MF_COMMAND','QUERY_OPTIONS','RUN_CLEANUP')),
    project_id text NOT NULL REFERENCES runtime_project(project_id),
    parent_run_id uuid REFERENCES runtime_job(job_id),
    idempotency_scope text,
    idempotency_key text,
    request_fingerprint text NOT NULL,
    request_json jsonb NOT NULL DEFAULT '{}',
    input_mode text NOT NULL DEFAULT 'DURABLE' CHECK(input_mode IN ('DURABLE','VOLATILE')),
    pinned_instance_id uuid,
    input_lease_expires_at timestamptz,
    input_set_id uuid,
    output_set_id uuid,
    config_version text NOT NULL,
    toolchain_version text NOT NULL,
    schema_name text,
    profile_binding_id text,
    status text NOT NULL DEFAULT 'QUEUED' CHECK(status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED')),
    phase text NOT NULL DEFAULT 'PREPARING',
    run_lifecycle text CHECK(run_lifecycle IN ('ACTIVE','CLEANING','CLEANED')),
    attempt_no integer NOT NULL DEFAULT 0,
    current_attempt_id uuid,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deadline_at timestamptz NOT NULL,
    max_attempts integer NOT NULL DEFAULT 3 CHECK(max_attempts BETWEEN 1 AND 3),
    retry_policy text NOT NULL DEFAULT 'PREPARATION_ONLY' CHECK(retry_policy IN ('PREPARATION_ONLY','READ_ONLY','NEVER')),
    error_code text,
    error_detail jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    CHECK((idempotency_key IS NULL) = (idempotency_scope IS NULL)),
    CHECK(input_mode='DURABLE' OR (pinned_instance_id IS NOT NULL AND input_lease_expires_at IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS runtime_job_idempotency ON runtime_job(idempotency_scope,idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS runtime_job_queue ON runtime_job(kind,available_at,created_at) WHERE status='QUEUED';
CREATE INDEX IF NOT EXISTS runtime_job_parent ON runtime_job(parent_run_id,status);
CREATE TABLE IF NOT EXISTS runtime_attempt (
    attempt_id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES runtime_job(job_id),
    attempt_no integer NOT NULL,
    worker_id uuid NOT NULL,
    lease_token uuid NOT NULL UNIQUE,
    lease_expires_at timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    execution_stage text NOT NULL DEFAULT 'PREPARING' CHECK(execution_stage IN ('PREPARING','EXTERNAL')),
    state text NOT NULL DEFAULT 'EXECUTING' CHECK(state IN ('EXECUTING','SUCCEEDED','FAILED','EXPIRED_UNCONFIRMED','STOPPED')),
    external_execution_refs jsonb NOT NULL DEFAULT '{}',
    stop_confirmed_at timestamptz,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    UNIQUE(job_id,attempt_no)
);
CREATE INDEX IF NOT EXISTS runtime_attempt_unconfirmed ON runtime_attempt(job_id)
    WHERE state='EXPIRED_UNCONFIRMED' AND stop_confirmed_at IS NULL;
CREATE TABLE IF NOT EXISTS runtime_artifact_set (
    set_id uuid PRIMARY KEY,
    project_id text NOT NULL REFERENCES runtime_project(project_id),
    producer_attempt_id uuid REFERENCES runtime_attempt(attempt_id),
    kind text NOT NULL CHECK(kind IN ('SOURCE','EXECUTION')),
    state text NOT NULL DEFAULT 'STAGING' CHECK(state IN ('STAGING','SEALED','DELETING')),
    source_commit_sha text,
    project_digest text,
    source_set_id uuid REFERENCES runtime_artifact_set(set_id),
    config_version text,
    toolchain_version text,
    format_version text NOT NULL DEFAULT '1',
    content_digest text,
    file_count integer NOT NULL DEFAULT 0,
    raw_bytes bigint NOT NULL DEFAULT 0,
    validation_json jsonb NOT NULL DEFAULT '{}',
    catalog_json jsonb NOT NULL DEFAULT '{}',
    metadata jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    sealed_at timestamptz
);
CREATE TABLE IF NOT EXISTS runtime_artifact_file (
    set_id uuid NOT NULL REFERENCES runtime_artifact_set(set_id) ON DELETE CASCADE,
    relative_path text NOT NULL,
    content bytea NOT NULL,
    codec text NOT NULL CHECK(codec IN ('raw','gzip')),
    raw_sha256 text NOT NULL,
    raw_size bigint NOT NULL CHECK(raw_size >= 0),
    stored_size bigint NOT NULL CHECK(stored_size >= 0),
    media_type text NOT NULL DEFAULT 'application/octet-stream',
    executable boolean NOT NULL DEFAULT false,
    PRIMARY KEY(set_id,relative_path)
);
CREATE TABLE IF NOT EXISTS runtime_job_result (
    job_id uuid PRIMARY KEY REFERENCES runtime_job(job_id),
    attempt_id uuid NOT NULL REFERENCES runtime_attempt(attempt_id),
    payload_json jsonb NOT NULL DEFAULT '{}',
    format_version text NOT NULL DEFAULT '1',
    stdout_tail text NOT NULL DEFAULT '',
    stderr_tail text NOT NULL DEFAULT '',
    exit_code integer,
    output_truncated boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- 循环引用在全部表创建后声明；重复 migrate 不重复添加约束。
DO $$ BEGIN
    IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='runtime_project_source_fk') THEN
        ALTER TABLE runtime_project ADD CONSTRAINT runtime_project_source_fk FOREIGN KEY(source_set_id) REFERENCES runtime_artifact_set(set_id);
        ALTER TABLE runtime_project ADD CONSTRAINT runtime_project_output_fk FOREIGN KEY(current_output_set_id) REFERENCES runtime_artifact_set(set_id);
        ALTER TABLE runtime_project ADD CONSTRAINT runtime_project_busy_fk FOREIGN KEY(busy_job_id) REFERENCES runtime_job(job_id);
        ALTER TABLE runtime_job ADD CONSTRAINT runtime_job_input_fk FOREIGN KEY(input_set_id) REFERENCES runtime_artifact_set(set_id);
        ALTER TABLE runtime_job ADD CONSTRAINT runtime_job_output_fk FOREIGN KEY(output_set_id) REFERENCES runtime_artifact_set(set_id);
        ALTER TABLE runtime_job ADD CONSTRAINT runtime_job_attempt_fk FOREIGN KEY(current_attempt_id) REFERENCES runtime_attempt(attempt_id);
    END IF;
END $$;
-- 文件只追加；封存后禁止覆盖，删除前必须先取得集合删除权。
CREATE OR REPLACE FUNCTION runtime_guard_artifact_file() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_state text;
BEGIN
    SELECT state INTO current_state FROM runtime_artifact_set
        WHERE set_id=CASE WHEN TG_OP='DELETE' THEN OLD.set_id ELSE NEW.set_id END FOR UPDATE;
    IF TG_OP='INSERT' AND current_state='STAGING' THEN RETURN NEW; END IF;
    IF TG_OP='DELETE' AND current_state='DELETING' THEN RETURN OLD; END IF;
    RAISE EXCEPTION 'Artifact file mutation is not permitted in state %', current_state;
END $$;
DROP TRIGGER IF EXISTS runtime_artifact_file_guard ON runtime_artifact_file;
CREATE TRIGGER runtime_artifact_file_guard BEFORE INSERT OR UPDATE OR DELETE ON runtime_artifact_file
    FOR EACH ROW EXECUTE FUNCTION runtime_guard_artifact_file();
-- 封存记录也不可重写或退回暂存状态；GC 唯一允许的修改是领取删除权。
CREATE OR REPLACE FUNCTION runtime_guard_artifact_set() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.state IN ('SEALED','DELETING') AND
       ((to_jsonb(NEW)-'state') IS DISTINCT FROM (to_jsonb(OLD)-'state') OR
        NEW.state NOT IN (OLD.state,'DELETING')) THEN
        RAISE EXCEPTION 'Sealed artifact set is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS runtime_artifact_set_guard ON runtime_artifact_set;
CREATE TRIGGER runtime_artifact_set_guard BEFORE UPDATE ON runtime_artifact_set
    FOR EACH ROW EXECUTE FUNCTION runtime_guard_artifact_set();
INSERT INTO runtime_schema_version(version) VALUES(1) ON CONFLICT DO NOTHING;
COMMENT ON COLUMN runtime_project.project_id IS '请求中的逻辑项目标识';
COMMENT ON COLUMN runtime_project.binding_config IS '受控绑定的非敏感配置';
COMMENT ON COLUMN runtime_project.config_version IS '连接和项目配置版本';
COMMENT ON COLUMN runtime_project.source_set_id IS '当前项目源码快照';
COMMENT ON COLUMN runtime_project.current_output_set_id IS '当前源码版本最近成功的默认输出';
COMMENT ON COLUMN runtime_project.busy_job_id IS '尚未确认停止的通用写任务';
COMMENT ON COLUMN runtime_project.revision IS '项目指针的乐观并发版本';
COMMENT ON COLUMN runtime_job.job_id IS '公开任务标识，保留现有 run 和 query UUID';
COMMENT ON COLUMN runtime_job.kind IS '任务执行类别';
COMMENT ON COLUMN runtime_job.project_id IS '所属逻辑项目';
COMMENT ON COLUMN runtime_job.parent_run_id IS '查询或清理绑定的固定构建 run';
COMMENT ON COLUMN runtime_job.idempotency_scope IS '幂等键的接口命名空间';
COMMENT ON COLUMN runtime_job.idempotency_key IS '调用方提供的幂等键';
COMMENT ON COLUMN runtime_job.request_fingerprint IS '包含固定输入和配置版本的规范化请求摘要';
COMMENT ON COLUMN runtime_job.request_json IS '排除临时 resources 和凭据的可重放参数';
COMMENT ON COLUMN runtime_job.input_mode IS '输入为可持久恢复或仅驻接收实例内存';
COMMENT ON COLUMN runtime_job.pinned_instance_id IS '持有 VOLATILE 输入的唯一实例';
COMMENT ON COLUMN runtime_job.input_lease_expires_at IS '覆盖排队期和执行期的内存输入租约截止时间';
COMMENT ON COLUMN runtime_job.input_set_id IS '受理时固定的输入集合';
COMMENT ON COLUMN runtime_job.output_set_id IS '成功事务发布的输出集合';
COMMENT ON COLUMN runtime_job.config_version IS '执行所需配置版本';
COMMENT ON COLUMN runtime_job.toolchain_version IS '执行所需工具链与镜像版本';
COMMENT ON COLUMN runtime_job.schema_name IS '固定构建使用且不复用的物理 schema';
COMMENT ON COLUMN runtime_job.profile_binding_id IS '不含凭据的 profile 绑定引用';
COMMENT ON COLUMN runtime_job.status IS '持久队列及公开状态的内部生命周期';
COMMENT ON COLUMN runtime_job.phase IS '构建准备、执行与验证阶段';
COMMENT ON COLUMN runtime_job.run_lifecycle IS '构建 run 的活动及清理生命周期';
COMMENT ON COLUMN runtime_job.attempt_no IS '已经领取执行的次数';
COMMENT ON COLUMN runtime_job.current_attempt_id IS '唯一当前执行 attempt';
COMMENT ON COLUMN runtime_job.available_at IS '允许领取或重试的最早时间';
COMMENT ON COLUMN runtime_job.deadline_at IS '所有尝试共享的最终截止时间';
COMMENT ON COLUMN runtime_job.max_attempts IS '包含首次执行的受控最大尝试数';
COMMENT ON COLUMN runtime_job.retry_policy IS '由受控绑定决定的安全重试类别';
COMMENT ON COLUMN runtime_job.error_code IS '稳定错误诊断码';
COMMENT ON COLUMN runtime_job.error_detail IS '有界且已脱敏的错误诊断';
COMMENT ON COLUMN runtime_job.created_at IS '受理记录创建时间';
COMMENT ON COLUMN runtime_job.started_at IS '首次实际领取时间';
COMMENT ON COLUMN runtime_job.finished_at IS '任务最终结束时间';
COMMENT ON COLUMN runtime_attempt.attempt_id IS '一次实际执行的标识';
COMMENT ON COLUMN runtime_attempt.job_id IS '执行所属任务';
COMMENT ON COLUMN runtime_attempt.attempt_no IS '任务内的执行序号';
COMMENT ON COLUMN runtime_attempt.worker_id IS '领取该执行的进程实例';
COMMENT ON COLUMN runtime_attempt.lease_token IS '该次执行唯一的状态写入凭证';
COMMENT ON COLUMN runtime_attempt.lease_expires_at IS '数据库时钟确定的执行租约截止时间';
COMMENT ON COLUMN runtime_attempt.heartbeat_at IS '最近一次有效续租时间';
COMMENT ON COLUMN runtime_attempt.execution_stage IS '区分外部引擎尚未调用和已经启动';
COMMENT ON COLUMN runtime_attempt.state IS '执行成功失败或尚未确认停止的状态';
COMMENT ON COLUMN runtime_attempt.external_execution_refs IS '用于取消核对的非敏感目标库会话标识';
COMMENT ON COLUMN runtime_attempt.stop_confirmed_at IS '已确认子进程及外部执行结束的时间';
COMMENT ON COLUMN runtime_attempt.started_at IS '执行开始时间';
COMMENT ON COLUMN runtime_attempt.finished_at IS '执行终结或被判定失联的时间';
COMMENT ON COLUMN runtime_artifact_set.set_id IS '不可变源码或输出集合标识';
COMMENT ON COLUMN runtime_artifact_set.project_id IS '所属逻辑项目';
COMMENT ON COLUMN runtime_artifact_set.producer_attempt_id IS '创建该集合的 attempt，管理导入为空';
COMMENT ON COLUMN runtime_artifact_set.kind IS '源码或执行输出集合类别';
COMMENT ON COLUMN runtime_artifact_set.state IS '暂存、封存或删除中状态';
COMMENT ON COLUMN runtime_artifact_set.source_commit_sha IS '固定 Git 提交标识';
COMMENT ON COLUMN runtime_artifact_set.project_digest IS '原有项目内容摘要';
COMMENT ON COLUMN runtime_artifact_set.source_set_id IS '执行集合对应的不可变源码';
COMMENT ON COLUMN runtime_artifact_set.config_version IS '恢复所需配置版本';
COMMENT ON COLUMN runtime_artifact_set.toolchain_version IS '恢复所需工具链版本';
COMMENT ON COLUMN runtime_artifact_set.format_version IS '持久产物格式版本';
COMMENT ON COLUMN runtime_artifact_set.content_digest IS '按路径排序的原始文件摘要清单之摘要';
COMMENT ON COLUMN runtime_artifact_set.file_count IS '集合文件数量';
COMMENT ON COLUMN runtime_artifact_set.raw_bytes IS '集合原始文件总字节数';
COMMENT ON COLUMN runtime_artifact_set.validation_json IS '发布验证证据';
COMMENT ON COLUMN runtime_artifact_set.catalog_json IS '从原生产物派生的可读取目录';
COMMENT ON COLUMN runtime_artifact_set.metadata IS '不含本地路径和凭据的附加产物元数据';
COMMENT ON COLUMN runtime_artifact_set.created_at IS '集合创建时间';
COMMENT ON COLUMN runtime_artifact_set.sealed_at IS '原子封存时间';
COMMENT ON COLUMN runtime_artifact_file.set_id IS '文件所属集合';
COMMENT ON COLUMN runtime_artifact_file.relative_path IS '规范化 POSIX 相对路径';
COMMENT ON COLUMN runtime_artifact_file.content IS '原始或压缩后的文件字节';
COMMENT ON COLUMN runtime_artifact_file.codec IS '文件字节的 raw 或 gzip 编码';
COMMENT ON COLUMN runtime_artifact_file.raw_sha256 IS '原始文件字节摘要';
COMMENT ON COLUMN runtime_artifact_file.raw_size IS '原始文件字节数';
COMMENT ON COLUMN runtime_artifact_file.stored_size IS '压缩后持久化字节数';
COMMENT ON COLUMN runtime_artifact_file.media_type IS '文件媒体类型';
COMMENT ON COLUMN runtime_artifact_file.executable IS '还原时需要保留的执行位';
COMMENT ON COLUMN runtime_job_result.job_id IS '结果所属任务';
COMMENT ON COLUMN runtime_job_result.attempt_id IS '获准提交结果的实际执行';
COMMENT ON COLUMN runtime_job_result.payload_json IS '遵循公开契约的列、行、SQL 和执行结果';
COMMENT ON COLUMN runtime_job_result.format_version IS '结果格式版本';
COMMENT ON COLUMN runtime_job_result.stdout_tail IS '已脱敏且有大小上限的标准输出尾部';
COMMENT ON COLUMN runtime_job_result.stderr_tail IS '已脱敏且有大小上限的标准错误尾部';
COMMENT ON COLUMN runtime_job_result.exit_code IS '实际子进程退出码';
COMMENT ON COLUMN runtime_job_result.output_truncated IS '诊断输出是否发生截断';
COMMENT ON COLUMN runtime_job_result.created_at IS '结果原子提交时间';
