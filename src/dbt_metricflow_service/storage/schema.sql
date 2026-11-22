-- 当前服务的完整初始化结构，由 init-db 在调用方事务内安装。
-- 不接管旧库；所有对象位于调用方选择的 schema。

CREATE FUNCTION runtime_guard_artifact_file() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE current_state text;
BEGIN
    SELECT state INTO current_state FROM runtime_artifact_set
        WHERE set_id=CASE WHEN TG_OP='DELETE' THEN OLD.set_id ELSE NEW.set_id END FOR UPDATE;
    IF TG_OP='INSERT' AND current_state='STAGING' THEN RETURN NEW; END IF;
    IF TG_OP='DELETE' AND current_state='DELETING' THEN RETURN OLD; END IF;
    RAISE EXCEPTION 'Artifact file mutation is not permitted in state %', current_state;
END $$;

CREATE FUNCTION runtime_guard_artifact_set() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF OLD.state IN ('SEALED','DELETING') AND
       ((to_jsonb(NEW)-'state') IS DISTINCT FROM (to_jsonb(OLD)-'state') OR
        NEW.state NOT IN (OLD.state,'DELETING')) THEN
        RAISE EXCEPTION 'Sealed artifact set is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

CREATE TABLE engine_build (
    build_id uuid NOT NULL,
    project_id text NOT NULL,
    input_set_id uuid,
    project_digest text,
    attempt_no integer DEFAULT 0 NOT NULL,
    current_attempt_id uuid,
    available_at timestamptz DEFAULT clock_timestamp() NOT NULL,
    max_attempts integer DEFAULT 3 NOT NULL CHECK (max_attempts BETWEEN 1 AND 3),
    retry_policy text DEFAULT 'PREPARATION_ONLY' NOT NULL CHECK (retry_policy IN ('PREPARATION_ONLY', 'NEVER')),
    started_at timestamptz,
    error_detail jsonb,
    repository text NOT NULL,
    branch_name text,
    environment text NOT NULL,
    environment_id uuid NOT NULL,
    toolchain_version text NOT NULL,
    caller text NOT NULL,
    idempotency_key uuid NOT NULL,
    request_json jsonb NOT NULL,
    commit_sha text,
    build_status text DEFAULT 'QUEUED'::text NOT NULL,
    phase text DEFAULT 'RESOLVING_SOURCE'::text NOT NULL,
    output_set_id uuid,
    error_code text,
    version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    finished_at timestamp with time zone,
    CONSTRAINT engine_build_build_status_check CHECK ((build_status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'SUCCEEDED'::text, 'FAILED'::text, 'CANCELLED'::text, 'OUTCOME_UNKNOWN'::text]))),
    CONSTRAINT engine_build_environment_check CHECK (environment ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
);

COMMENT ON COLUMN engine_build.build_id IS '一次构建身份';

COMMENT ON COLUMN engine_build.repository IS '受控规范仓库地址';

COMMENT ON COLUMN engine_build.branch_name IS 'Git 短分支名';

COMMENT ON COLUMN engine_build.environment IS '物理执行环境';

COMMENT ON COLUMN engine_build.environment_id IS '调用方环境 UUID';


COMMENT ON COLUMN engine_build.toolchain_version IS '固定工具链版本';

COMMENT ON COLUMN engine_build.caller IS '可信服务调用来源';

COMMENT ON COLUMN engine_build.idempotency_key IS '调用方分配的全局唯一构建 UUID';


COMMENT ON COLUMN engine_build.request_json IS '原始规范化请求';

COMMENT ON COLUMN engine_build.commit_sha IS '实际固定源码提交';

COMMENT ON COLUMN engine_build.build_status IS '构建终态，不代表部署成功';

COMMENT ON COLUMN engine_build.phase IS '执行进度阶段';

COMMENT ON COLUMN engine_build.output_set_id IS '可靠封存产物引用';

COMMENT ON COLUMN engine_build.error_code IS '脱敏稳定错误码';

COMMENT ON COLUMN engine_build.version IS '对象递增版本';

COMMENT ON COLUMN engine_build.created_at IS '受理时间';

COMMENT ON COLUMN engine_build.updated_at IS '最近事实变更时间';

COMMENT ON COLUMN engine_build.finished_at IS '执行终态时间';

CREATE TABLE engine_deployment_attempt (
    repository text NOT NULL,
    environment text NOT NULL,
    branch_name text NOT NULL,
    generation bigint NOT NULL,
    build_id uuid NOT NULL,
    caller text NOT NULL,
    idempotency_key text NOT NULL,
    operation text DEFAULT 'BUILD'::text NOT NULL,
    deployment_status text NOT NULL,
    reason text,
    version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    deployed_at timestamp with time zone,
    last_checked_at timestamp with time zone
);

COMMENT ON COLUMN engine_deployment_attempt.repository IS '受控规范仓库地址';

COMMENT ON COLUMN engine_deployment_attempt.environment IS '物理执行环境';

COMMENT ON COLUMN engine_deployment_attempt.branch_name IS 'Git 短分支名';

COMMENT ON COLUMN engine_deployment_attempt.generation IS '按受理顺序分配的部署序号';

COMMENT ON COLUMN engine_deployment_attempt.build_id IS '一次构建身份';

COMMENT ON COLUMN engine_deployment_attempt.caller IS '可信服务调用来源';

COMMENT ON COLUMN engine_deployment_attempt.idempotency_key IS '调用方稳定请求键';


COMMENT ON COLUMN engine_deployment_attempt.operation IS '历史部署来源；新意图固定为构建自动部署 BUILD';

COMMENT ON COLUMN engine_deployment_attempt.deployment_status IS '部署意图状态';

COMMENT ON COLUMN engine_deployment_attempt.reason IS '稳定状态原因';

COMMENT ON COLUMN engine_deployment_attempt.version IS '对象递增版本';

COMMENT ON COLUMN engine_deployment_attempt.created_at IS '受理时间';

COMMENT ON COLUMN engine_deployment_attempt.deployed_at IS '实际切换时间';

COMMENT ON COLUMN engine_deployment_attempt.last_checked_at IS '内部公平调度的最近扫描时间，不生成业务变化';

CREATE TABLE engine_deployment_target (
    repository text NOT NULL,
    environment text NOT NULL,
    branch_name text NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    desired_generation bigint DEFAULT 0 NOT NULL,
    active_build_id uuid,
    observed_head_sha text,
    head_observed_at timestamp with time zone,
    source_state text DEFAULT 'UNDEPLOYED'::text NOT NULL
);

COMMENT ON COLUMN engine_deployment_target.repository IS '受控规范仓库地址';

COMMENT ON COLUMN engine_deployment_target.environment IS '物理执行环境';

COMMENT ON COLUMN engine_deployment_target.branch_name IS 'Git 短分支名';

COMMENT ON COLUMN engine_deployment_target.version IS '对象递增版本';

COMMENT ON COLUMN engine_deployment_target.desired_generation IS '最新受理的部署意图序号';

COMMENT ON COLUMN engine_deployment_target.active_build_id IS '当前有效构建指针';

COMMENT ON COLUMN engine_deployment_target.observed_head_sha IS '最近观察的远端提交';

COMMENT ON COLUMN engine_deployment_target.head_observed_at IS '远端观察时间';

COMMENT ON COLUMN engine_deployment_target.source_state IS '远端相对部署的观察状态';

CREATE TABLE runtime_artifact_file (
    set_id uuid NOT NULL,
    relative_path text NOT NULL,
    content bytea NOT NULL,
    codec text NOT NULL,
    raw_sha256 text NOT NULL,
    raw_size bigint NOT NULL,
    stored_size bigint NOT NULL,
    media_type text DEFAULT 'application/octet-stream'::text NOT NULL,
    executable boolean DEFAULT false NOT NULL,
    CONSTRAINT runtime_artifact_file_codec_check CHECK ((codec = ANY (ARRAY['raw'::text, 'gzip'::text]))),
    CONSTRAINT runtime_artifact_file_raw_size_check CHECK ((raw_size >= 0)),
    CONSTRAINT runtime_artifact_file_stored_size_check CHECK ((stored_size >= 0))
);

COMMENT ON COLUMN runtime_artifact_file.set_id IS '文件所属集合';

COMMENT ON COLUMN runtime_artifact_file.relative_path IS '规范化 POSIX 相对路径';

COMMENT ON COLUMN runtime_artifact_file.content IS '原始或压缩后的文件字节';

COMMENT ON COLUMN runtime_artifact_file.codec IS '文件字节的 raw 或 gzip 编码';

COMMENT ON COLUMN runtime_artifact_file.raw_sha256 IS '原始文件字节摘要';

COMMENT ON COLUMN runtime_artifact_file.raw_size IS '原始文件字节数';

COMMENT ON COLUMN runtime_artifact_file.stored_size IS '压缩后持久化字节数';

COMMENT ON COLUMN runtime_artifact_file.media_type IS '文件媒体类型';

COMMENT ON COLUMN runtime_artifact_file.executable IS '还原时需要保留的执行位';

CREATE TABLE runtime_artifact_set (
    set_id uuid NOT NULL,
    project_id text NOT NULL,
    producer_attempt_id uuid,
    kind text NOT NULL,
    state text DEFAULT 'STAGING'::text NOT NULL,
    source_commit_sha text,
    project_digest text,
    source_set_id uuid,
    toolchain_version text,
    format_version text DEFAULT '1'::text NOT NULL,
    content_digest text,
    file_count integer DEFAULT 0 NOT NULL,
    raw_bytes bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    sealed_at timestamp with time zone,
    CONSTRAINT runtime_artifact_set_kind_check CHECK ((kind = ANY (ARRAY['SOURCE'::text, 'EXECUTION'::text]))),
    CONSTRAINT runtime_artifact_set_state_check CHECK ((state = ANY (ARRAY['STAGING'::text, 'SEALED'::text, 'DELETING'::text])))
);

COMMENT ON COLUMN runtime_artifact_set.set_id IS '不可变源码或输出集合标识';

COMMENT ON COLUMN runtime_artifact_set.project_id IS '所属逻辑项目';

COMMENT ON COLUMN runtime_artifact_set.producer_attempt_id IS '创建该集合的 attempt，管理导入为空';

COMMENT ON COLUMN runtime_artifact_set.kind IS 'SOURCE 项目源、EXECUTION 执行产物';

COMMENT ON COLUMN runtime_artifact_set.state IS '暂存、封存或删除中状态';

COMMENT ON COLUMN runtime_artifact_set.source_commit_sha IS '固定 Git 提交标识';

COMMENT ON COLUMN runtime_artifact_set.project_digest IS '原有项目内容摘要';

COMMENT ON COLUMN runtime_artifact_set.source_set_id IS '执行集合对应的不可变源码';


COMMENT ON COLUMN runtime_artifact_set.toolchain_version IS '恢复所需工具链版本';

COMMENT ON COLUMN runtime_artifact_set.format_version IS '持久产物格式版本';

COMMENT ON COLUMN runtime_artifact_set.content_digest IS '按路径排序的原始文件摘要清单之摘要';

COMMENT ON COLUMN runtime_artifact_set.file_count IS '集合文件数量';

COMMENT ON COLUMN runtime_artifact_set.raw_bytes IS '集合原始文件总字节数';


COMMENT ON COLUMN runtime_artifact_set.created_at IS '集合创建时间';

COMMENT ON COLUMN runtime_artifact_set.sealed_at IS '原子封存时间';

CREATE TABLE runtime_attempt (
    attempt_id uuid NOT NULL,
    build_id uuid NOT NULL,
    attempt_no integer NOT NULL,
    worker_id uuid NOT NULL,
    lease_token uuid NOT NULL,
    lease_expires_at timestamp with time zone NOT NULL,
    heartbeat_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    execution_stage text DEFAULT 'PREPARING'::text NOT NULL,
    state text DEFAULT 'EXECUTING'::text NOT NULL,
    external_execution_refs jsonb DEFAULT '{}'::jsonb NOT NULL,
    stop_confirmed_at timestamp with time zone,
    started_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    finished_at timestamp with time zone,
    CONSTRAINT runtime_attempt_execution_stage_check CHECK ((execution_stage = ANY (ARRAY['PREPARING'::text, 'EXTERNAL'::text]))),
    CONSTRAINT runtime_attempt_state_check CHECK ((state = ANY (ARRAY['EXECUTING'::text, 'SUCCEEDED'::text, 'FAILED'::text, 'EXPIRED_UNCONFIRMED'::text, 'STOPPED'::text])))
);

COMMENT ON COLUMN runtime_attempt.attempt_id IS '一次实际执行的标识';

COMMENT ON COLUMN runtime_attempt.build_id IS '执行所属任务';

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

ALTER TABLE ONLY engine_build
    ADD CONSTRAINT engine_build_pkey PRIMARY KEY (build_id);

ALTER TABLE ONLY engine_build
    ADD CONSTRAINT engine_build_idempotency_key_key UNIQUE (idempotency_key);

ALTER TABLE ONLY engine_deployment_attempt
    ADD CONSTRAINT engine_deployment_attempt_pkey PRIMARY KEY (repository, environment, branch_name, generation);

ALTER TABLE ONLY engine_deployment_attempt
    ADD CONSTRAINT engine_deployment_attempt_repository_caller_operation_idemp_key UNIQUE (repository, caller, operation, idempotency_key);

ALTER TABLE ONLY engine_deployment_target
    ADD CONSTRAINT engine_deployment_target_pkey PRIMARY KEY (repository, environment, branch_name);


ALTER TABLE ONLY runtime_artifact_file
    ADD CONSTRAINT runtime_artifact_file_pkey PRIMARY KEY (set_id, relative_path);

ALTER TABLE ONLY runtime_artifact_set
    ADD CONSTRAINT runtime_artifact_set_pkey PRIMARY KEY (set_id);

ALTER TABLE ONLY runtime_attempt
    ADD CONSTRAINT runtime_attempt_build_id_attempt_no_key UNIQUE (build_id, attempt_no);

ALTER TABLE ONLY runtime_attempt
    ADD CONSTRAINT runtime_attempt_lease_token_key UNIQUE (lease_token);

ALTER TABLE ONLY runtime_attempt
    ADD CONSTRAINT runtime_attempt_pkey PRIMARY KEY (attempt_id);

CREATE INDEX engine_build_history ON engine_build USING btree (repository, created_at, build_id);

CREATE INDEX runtime_attempt_unconfirmed ON runtime_attempt USING btree (build_id) WHERE ((state = 'EXPIRED_UNCONFIRMED'::text) AND (stop_confirmed_at IS NULL));

CREATE INDEX engine_build_queue ON engine_build USING btree (available_at, created_at) WHERE (build_status = 'QUEUED'::text);


CREATE TRIGGER runtime_artifact_file_guard BEFORE INSERT OR DELETE OR UPDATE ON runtime_artifact_file FOR EACH ROW EXECUTE FUNCTION runtime_guard_artifact_file();

CREATE TRIGGER runtime_artifact_set_guard BEFORE UPDATE ON runtime_artifact_set FOR EACH ROW EXECUTE FUNCTION runtime_guard_artifact_set();

ALTER TABLE ONLY engine_build
    ADD CONSTRAINT engine_build_output_set_id_fkey FOREIGN KEY (output_set_id) REFERENCES runtime_artifact_set(set_id);

ALTER TABLE ONLY engine_deployment_attempt
    ADD CONSTRAINT engine_deployment_attempt_build_id_fkey FOREIGN KEY (build_id) REFERENCES engine_build(build_id);

ALTER TABLE ONLY engine_deployment_attempt
    ADD CONSTRAINT engine_deployment_attempt_repository_environment_branch_na_fkey FOREIGN KEY (repository, environment, branch_name) REFERENCES engine_deployment_target(repository, environment, branch_name);

ALTER TABLE ONLY engine_deployment_target
    ADD CONSTRAINT engine_deployment_target_active_build_id_fkey FOREIGN KEY (active_build_id) REFERENCES engine_build(build_id);

ALTER TABLE ONLY runtime_artifact_file
    ADD CONSTRAINT runtime_artifact_file_set_id_fkey FOREIGN KEY (set_id) REFERENCES runtime_artifact_set(set_id) ON DELETE CASCADE;

ALTER TABLE ONLY runtime_artifact_set
    ADD CONSTRAINT runtime_artifact_set_producer_attempt_id_fkey FOREIGN KEY (producer_attempt_id) REFERENCES runtime_attempt(attempt_id);

ALTER TABLE ONLY runtime_artifact_set
    ADD CONSTRAINT runtime_artifact_set_source_set_id_fkey FOREIGN KEY (source_set_id) REFERENCES runtime_artifact_set(set_id);

ALTER TABLE runtime_attempt ADD CONSTRAINT runtime_attempt_build_id_fkey
    FOREIGN KEY (build_id) REFERENCES engine_build(build_id);
ALTER TABLE engine_build ADD CONSTRAINT engine_build_current_attempt_id_fkey
    FOREIGN KEY (current_attempt_id) REFERENCES runtime_attempt(attempt_id);
ALTER TABLE engine_build ADD CONSTRAINT engine_build_input_set_id_fkey
    FOREIGN KEY (input_set_id) REFERENCES runtime_artifact_set(set_id);

CREATE INDEX IF NOT EXISTS engine_build_environment_history ON engine_build(environment_id, created_at DESC, build_id DESC);


ALTER TABLE engine_build ADD CONSTRAINT engine_build_environment_identity CHECK (environment = environment_id::text);

CREATE TABLE runtime_execution_log (
    log_id bigserial PRIMARY KEY,
    build_id uuid NOT NULL REFERENCES engine_build(build_id) ON DELETE CASCADE,
    job_id uuid NOT NULL, -- 对外日志字段，保留历史任务标识；新日志使用 build_id。
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
