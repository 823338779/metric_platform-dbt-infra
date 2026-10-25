-- 当前服务的完整初始化结构，由 init-db 在调用方事务内安装。
-- 不接管旧库；所有对象位于调用方选择的 schema。

CREATE FUNCTION engine_sync_job() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
 UPDATE engine_build SET
  build_status=CASE
   WHEN NEW.error_code='CANCELLED' THEN 'CANCELLED'
   WHEN NEW.error_code='EXECUTION_OUTCOME_UNKNOWN' OR NEW.error_detail->>'externalOutcomeUnknown'='true'
    THEN 'OUTCOME_UNKNOWN'
   ELSE NEW.status END,
  phase=CASE WHEN NEW.status='SUCCEEDED' THEN 'COMPLETE'
             WHEN NEW.phase='PREPARING' THEN 'RESOLVING_SOURCE' ELSE NEW.phase END,
  output_set_id=NEW.output_set_id,error_code=NEW.error_code,
  finished_at=NEW.finished_at,updated_at=clock_timestamp(),version=version+1
 WHERE run_id=NEW.job_id;
 RETURN NEW;
END $$;

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
    run_id uuid NOT NULL,
    repository text NOT NULL,
    branch_name text,
    environment text NOT NULL,
    environment_id uuid NOT NULL,
    physical_target_key text NOT NULL,
    config_version text NOT NULL,
    toolchain_version text NOT NULL,
    caller text NOT NULL,
    idempotency_key text NOT NULL,
    request_digest text NOT NULL,
    request_json jsonb NOT NULL,
    config_snapshot jsonb NOT NULL,
    requested_commit_sha text,
    commit_sha text,
    build_status text DEFAULT 'QUEUED'::text NOT NULL,
    phase text DEFAULT 'RESOLVING_SOURCE'::text NOT NULL,
    cancel_requested boolean DEFAULT false NOT NULL,
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

COMMENT ON COLUMN engine_build.run_id IS '内部执行记录关联，不公开为构建身份';

COMMENT ON COLUMN engine_build.repository IS '受控规范仓库地址';

COMMENT ON COLUMN engine_build.branch_name IS 'Git 短分支名';

COMMENT ON COLUMN engine_build.environment IS '物理执行环境';

COMMENT ON COLUMN engine_build.environment_id IS '调用方环境 UUID';
COMMENT ON COLUMN engine_build.physical_target_key IS '实例、catalog 和 schema 的目标摘要';

COMMENT ON COLUMN engine_build.config_version IS '不可变执行配置版本';

COMMENT ON COLUMN engine_build.toolchain_version IS '固定工具链版本';

COMMENT ON COLUMN engine_build.caller IS '可信服务调用来源';

COMMENT ON COLUMN engine_build.idempotency_key IS '调用方稳定请求键';

COMMENT ON COLUMN engine_build.request_digest IS '规范化输入摘要';

COMMENT ON COLUMN engine_build.request_json IS '原始规范化请求';

COMMENT ON COLUMN engine_build.config_snapshot IS '受理时固定的执行语义配置';

COMMENT ON COLUMN engine_build.requested_commit_sha IS '请求显式指定的提交';

COMMENT ON COLUMN engine_build.commit_sha IS '实际固定源码提交';

COMMENT ON COLUMN engine_build.build_status IS '构建终态，不代表部署成功';

COMMENT ON COLUMN engine_build.phase IS '执行进度阶段';

COMMENT ON COLUMN engine_build.cancel_requested IS '持久取消意图';

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
    request_digest text NOT NULL,
    operation text DEFAULT 'DEPLOYMENT'::text NOT NULL,
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

COMMENT ON COLUMN engine_deployment_attempt.request_digest IS '规范化输入摘要';

COMMENT ON COLUMN engine_deployment_attempt.operation IS '自动部署与独立部署的幂等操作域';

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
    config_version text,
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

COMMENT ON COLUMN runtime_artifact_set.config_version IS '恢复所需配置版本';

COMMENT ON COLUMN runtime_artifact_set.toolchain_version IS '恢复所需工具链版本';

COMMENT ON COLUMN runtime_artifact_set.format_version IS '持久产物格式版本';

COMMENT ON COLUMN runtime_artifact_set.content_digest IS '按路径排序的原始文件摘要清单之摘要';

COMMENT ON COLUMN runtime_artifact_set.file_count IS '集合文件数量';

COMMENT ON COLUMN runtime_artifact_set.raw_bytes IS '集合原始文件总字节数';


COMMENT ON COLUMN runtime_artifact_set.created_at IS '集合创建时间';

COMMENT ON COLUMN runtime_artifact_set.sealed_at IS '原子封存时间';

CREATE TABLE runtime_attempt (
    attempt_id uuid NOT NULL,
    job_id uuid NOT NULL,
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

CREATE TABLE runtime_job (
    job_id uuid NOT NULL,
    kind text NOT NULL,
    project_id text NOT NULL,
    parent_run_id uuid,
    idempotency_scope text,
    idempotency_key text,
    request_fingerprint text NOT NULL,
    request_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    input_set_id uuid,
    output_set_id uuid,
    config_version text NOT NULL,
    toolchain_version text NOT NULL,
    schema_name text,
    profile_binding_id text,
    status text DEFAULT 'QUEUED'::text NOT NULL,
    phase text DEFAULT 'PREPARING'::text NOT NULL,
    run_lifecycle text,
    attempt_no integer DEFAULT 0 NOT NULL,
    current_attempt_id uuid,
    available_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    deadline_at timestamp with time zone NOT NULL,
    max_attempts integer DEFAULT 3 NOT NULL,
    retry_policy text DEFAULT 'PREPARATION_ONLY'::text NOT NULL,
    error_code text,
    error_detail jsonb,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    CONSTRAINT runtime_job_check CHECK (((idempotency_key IS NULL) = (idempotency_scope IS NULL))),
    CONSTRAINT runtime_job_kind_check CHECK ((kind = ANY (ARRAY['BUILD_RUN'::text, 'RUN_CLEANUP'::text]))),
    CONSTRAINT runtime_job_max_attempts_check CHECK (((max_attempts >= 1) AND (max_attempts <= 3))),
    CONSTRAINT runtime_job_retry_policy_check CHECK ((retry_policy = ANY (ARRAY['PREPARATION_ONLY'::text, 'NEVER'::text]))),
    CONSTRAINT runtime_job_run_lifecycle_check CHECK ((run_lifecycle = ANY (ARRAY['ACTIVE'::text, 'CLEANING'::text, 'CLEANED'::text]))),
    CONSTRAINT runtime_job_status_check CHECK ((status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'SUCCEEDED'::text, 'FAILED'::text])))
);

COMMENT ON COLUMN runtime_job.job_id IS '内部构建或清理任务标识';

COMMENT ON COLUMN runtime_job.kind IS '构建或物理清理任务';

COMMENT ON COLUMN runtime_job.project_id IS '所属逻辑项目';

COMMENT ON COLUMN runtime_job.parent_run_id IS '清理绑定的固定构建 run';

COMMENT ON COLUMN runtime_job.idempotency_scope IS '幂等键的接口命名空间';

COMMENT ON COLUMN runtime_job.idempotency_key IS '调用方提供的幂等键';

COMMENT ON COLUMN runtime_job.request_fingerprint IS '包含固定输入和配置版本的规范化请求摘要';

COMMENT ON COLUMN runtime_job.request_json IS '排除临时 resources 和凭据的可重放参数';

COMMENT ON COLUMN runtime_job.input_set_id IS '受理时固定的输入集合';

COMMENT ON COLUMN runtime_job.output_set_id IS '成功事务发布的输出集合';

COMMENT ON COLUMN runtime_job.config_version IS '执行所需配置版本';

COMMENT ON COLUMN runtime_job.toolchain_version IS '执行所需工具链与镜像版本';

COMMENT ON COLUMN runtime_job.schema_name IS '构建请求指定的固定物理 schema，多次构建可共用';

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

CREATE TABLE runtime_project (
    project_id text NOT NULL,
    binding_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    config_version text DEFAULT '1'::text NOT NULL,
    source_set_id uuid,
    busy_job_id uuid,
    revision bigint DEFAULT 0 NOT NULL
);

COMMENT ON COLUMN runtime_project.project_id IS '请求中的逻辑项目标识';

COMMENT ON COLUMN runtime_project.binding_config IS '受控绑定的非敏感配置';

COMMENT ON COLUMN runtime_project.config_version IS '连接和项目配置版本';

COMMENT ON COLUMN runtime_project.source_set_id IS '当前项目源码快照';

COMMENT ON COLUMN runtime_project.busy_job_id IS '尚未确认停止的通用写任务';

COMMENT ON COLUMN runtime_project.revision IS '项目指针的乐观并发版本';

ALTER TABLE ONLY engine_build
    ADD CONSTRAINT engine_build_pkey PRIMARY KEY (build_id);

ALTER TABLE ONLY engine_build
    ADD CONSTRAINT engine_build_repository_caller_idempotency_key_key UNIQUE (repository, caller, idempotency_key);

ALTER TABLE ONLY engine_build
    ADD CONSTRAINT engine_build_run_id_key UNIQUE (run_id);

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
    ADD CONSTRAINT runtime_attempt_job_id_attempt_no_key UNIQUE (job_id, attempt_no);

ALTER TABLE ONLY runtime_attempt
    ADD CONSTRAINT runtime_attempt_lease_token_key UNIQUE (lease_token);

ALTER TABLE ONLY runtime_attempt
    ADD CONSTRAINT runtime_attempt_pkey PRIMARY KEY (attempt_id);

ALTER TABLE ONLY runtime_job
    ADD CONSTRAINT runtime_job_pkey PRIMARY KEY (job_id);

ALTER TABLE ONLY runtime_project
    ADD CONSTRAINT runtime_project_pkey PRIMARY KEY (project_id);

CREATE INDEX engine_build_history ON engine_build USING btree (repository, created_at, build_id);

CREATE INDEX runtime_attempt_unconfirmed ON runtime_attempt USING btree (job_id) WHERE ((state = 'EXPIRED_UNCONFIRMED'::text) AND (stop_confirmed_at IS NULL));

CREATE UNIQUE INDEX runtime_job_idempotency ON runtime_job USING btree (idempotency_scope, idempotency_key) WHERE (idempotency_key IS NOT NULL);

CREATE INDEX runtime_job_parent ON runtime_job USING btree (parent_run_id, status);

CREATE INDEX runtime_job_queue ON runtime_job USING btree (kind, available_at, created_at) WHERE (status = 'QUEUED'::text);


CREATE TRIGGER engine_job_change AFTER UPDATE OF status, phase, output_set_id, error_code, run_lifecycle ON runtime_job FOR EACH ROW EXECUTE FUNCTION engine_sync_job();

CREATE TRIGGER runtime_artifact_file_guard BEFORE INSERT OR DELETE OR UPDATE ON runtime_artifact_file FOR EACH ROW EXECUTE FUNCTION runtime_guard_artifact_file();

CREATE TRIGGER runtime_artifact_set_guard BEFORE UPDATE ON runtime_artifact_set FOR EACH ROW EXECUTE FUNCTION runtime_guard_artifact_set();

ALTER TABLE ONLY engine_build
    ADD CONSTRAINT engine_build_output_set_id_fkey FOREIGN KEY (output_set_id) REFERENCES runtime_artifact_set(set_id);

ALTER TABLE ONLY engine_build
    ADD CONSTRAINT engine_build_run_id_fkey FOREIGN KEY (run_id) REFERENCES runtime_job(job_id);

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
    ADD CONSTRAINT runtime_artifact_set_project_id_fkey FOREIGN KEY (project_id) REFERENCES runtime_project(project_id);

ALTER TABLE ONLY runtime_artifact_set
    ADD CONSTRAINT runtime_artifact_set_source_set_id_fkey FOREIGN KEY (source_set_id) REFERENCES runtime_artifact_set(set_id);

ALTER TABLE ONLY runtime_attempt
    ADD CONSTRAINT runtime_attempt_job_id_fkey FOREIGN KEY (job_id) REFERENCES runtime_job(job_id);

ALTER TABLE ONLY runtime_job
    ADD CONSTRAINT runtime_job_attempt_fk FOREIGN KEY (current_attempt_id) REFERENCES runtime_attempt(attempt_id);

ALTER TABLE ONLY runtime_job
    ADD CONSTRAINT runtime_job_input_fk FOREIGN KEY (input_set_id) REFERENCES runtime_artifact_set(set_id);

ALTER TABLE ONLY runtime_job
    ADD CONSTRAINT runtime_job_output_fk FOREIGN KEY (output_set_id) REFERENCES runtime_artifact_set(set_id);

ALTER TABLE ONLY runtime_job
    ADD CONSTRAINT runtime_job_parent_run_id_fkey FOREIGN KEY (parent_run_id) REFERENCES runtime_job(job_id);

ALTER TABLE ONLY runtime_job
    ADD CONSTRAINT runtime_job_project_id_fkey FOREIGN KEY (project_id) REFERENCES runtime_project(project_id);

ALTER TABLE ONLY runtime_project
    ADD CONSTRAINT runtime_project_busy_fk FOREIGN KEY (busy_job_id) REFERENCES runtime_job(job_id);

ALTER TABLE ONLY runtime_project
    ADD CONSTRAINT runtime_project_source_fk FOREIGN KEY (source_set_id) REFERENCES runtime_artifact_set(set_id);

CREATE INDEX IF NOT EXISTS engine_build_environment_history ON engine_build(environment_id, created_at DESC, build_id DESC);
CREATE INDEX IF NOT EXISTS engine_build_physical_target ON engine_build(physical_target_key);
ALTER TABLE runtime_job ADD COLUMN IF NOT EXISTS physical_target_key text;
ALTER TABLE runtime_job ADD COLUMN IF NOT EXISTS target_claimed boolean NOT NULL DEFAULT false;
COMMENT ON COLUMN runtime_job.physical_target_key IS '本执行的规范化物理写入目标';
COMMENT ON COLUMN runtime_job.target_claimed IS '本执行是否仍持有目标写入权，未知结果不得释放';
CREATE INDEX IF NOT EXISTS runtime_job_target_owner ON runtime_job(physical_target_key) WHERE target_claimed;


ALTER TABLE engine_build ADD CONSTRAINT engine_build_environment_identity CHECK (environment = environment_id::text);

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
