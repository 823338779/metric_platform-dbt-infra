-- 当前服务的完整初始化结构，由 init-db 在调用方事务内安装。
-- 不接管旧库；所有对象位于调用方选择的 schema。

CREATE FUNCTION engine_record_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE next_sequence bigint; identity text; record_version bigint; projection jsonb; lifecycle text;
BEGIN
 UPDATE engine_change_counter SET value=value+1 WHERE singleton RETURNING value INTO next_sequence;
 IF TG_TABLE_NAME='engine_build' THEN
  identity := NEW.build_id::text; record_version := NEW.version;
 ELSIF TG_TABLE_NAME='engine_deployment_target' THEN
  identity := jsonb_build_array(NEW.repository,NEW.environment,NEW.branch_name)::text;
  -- 观察事实有自己的变化版本，不能复用不随观察更新的目标 CAS version。
  record_version := next_sequence;
 ELSE
  identity := jsonb_build_array(NEW.repository,NEW.environment,NEW.branch_name,NEW.generation)::text;
  record_version := NEW.version;
 END IF;
 projection := to_jsonb(NEW)-'request_json'-'config_snapshot'-'caller'-'idempotency_key'
               -'request_digest'-'last_checked_at';
 IF TG_TABLE_NAME='engine_build' THEN
  SELECT run_lifecycle INTO lifecycle FROM runtime_job WHERE job_id=NEW.run_id;
  projection := projection || jsonb_build_object(
   'catalog_available',NEW.build_status='SUCCEEDED' AND NEW.output_set_id IS NOT NULL,
   'query_available',NEW.build_status='SUCCEEDED' AND NEW.output_set_id IS NOT NULL
      AND lifecycle='ACTIVE',
   'query_unavailable_reason',CASE
      WHEN NEW.build_status<>'SUCCEEDED' THEN 'BUILD_NOT_SUCCEEDED'
      WHEN NEW.output_set_id IS NULL THEN 'ARTIFACT_UNAVAILABLE'
      WHEN lifecycle IN ('CLEANING','CLEANED') THEN 'PHYSICAL_OBJECTS_REMOVED'
      ELSE NULL END);
 END IF;
 INSERT INTO engine_change VALUES(next_sequence,NEW.repository,TG_TABLE_NAME,identity,record_version,projection);
 RETURN NEW;
END $$;

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
  catalog_digest=(SELECT raw_sha256 FROM runtime_artifact_file
                  WHERE set_id=NEW.output_set_id AND relative_path='target/published_catalog.json'),
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
    execution_binding text NOT NULL,
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
    catalog_digest text,
    error_code text,
    version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    finished_at timestamp with time zone,
    CONSTRAINT engine_build_build_status_check CHECK ((build_status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'SUCCEEDED'::text, 'FAILED'::text, 'CANCELLED'::text, 'OUTCOME_UNKNOWN'::text]))),
    CONSTRAINT engine_build_environment_check CHECK ((environment = ANY (ARRAY['PREVIEW'::text, 'PRODUCTION'::text])))
);

COMMENT ON COLUMN engine_build.build_id IS '一次构建身份';

COMMENT ON COLUMN engine_build.run_id IS '内部执行记录关联，不公开为构建身份';

COMMENT ON COLUMN engine_build.repository IS '受控规范仓库地址';

COMMENT ON COLUMN engine_build.branch_name IS 'Git 短分支名';

COMMENT ON COLUMN engine_build.environment IS '物理执行环境';

COMMENT ON COLUMN engine_build.execution_binding IS '可复用执行配置名称';

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

COMMENT ON COLUMN engine_build.catalog_digest IS '封存目录字节摘要';

COMMENT ON COLUMN engine_build.error_code IS '脱敏稳定错误码';

COMMENT ON COLUMN engine_build.version IS '对象递增版本';

COMMENT ON COLUMN engine_build.created_at IS '受理时间';

COMMENT ON COLUMN engine_build.updated_at IS '最近事实变更时间';

COMMENT ON COLUMN engine_build.finished_at IS '执行终态时间';

CREATE TABLE engine_change (
    sequence bigint NOT NULL,
    repository text NOT NULL,
    object_type text NOT NULL,
    object_id text NOT NULL,
    object_version bigint NOT NULL,
    summary jsonb NOT NULL
);

COMMENT ON COLUMN engine_change.sequence IS '提交顺序可重放变化序号';

COMMENT ON COLUMN engine_change.repository IS '受控规范仓库地址';

COMMENT ON COLUMN engine_change.object_type IS '引擎事实类型';

COMMENT ON COLUMN engine_change.object_id IS '变化对象自然身份';

COMMENT ON COLUMN engine_change.object_version IS '变化对应对象版本';

COMMENT ON COLUMN engine_change.summary IS '完整有界事实摘要，不含执行秘密';

CREATE TABLE engine_change_counter (
    singleton boolean DEFAULT true NOT NULL,
    value bigint NOT NULL,
    CONSTRAINT engine_change_counter_singleton_check CHECK (singleton)
);

COMMENT ON COLUMN engine_change_counter.singleton IS '变化流计数器唯一行';

COMMENT ON COLUMN engine_change_counter.value IS '最后同事务分配的变化序号';

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

CREATE TABLE engine_execution_binding (
    repository text NOT NULL,
    execution_binding text NOT NULL,
    config_version text NOT NULL,
    config_json jsonb NOT NULL
);

COMMENT ON COLUMN engine_execution_binding.repository IS '受控规范仓库地址';

COMMENT ON COLUMN engine_execution_binding.execution_binding IS '可复用执行配置名称';

COMMENT ON COLUMN engine_execution_binding.config_version IS '不可变执行配置版本';

COMMENT ON COLUMN engine_execution_binding.config_json IS '执行配置，不含凭据正文';

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
    validation_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    catalog_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    sealed_at timestamp with time zone,
    CONSTRAINT runtime_artifact_set_kind_check CHECK ((kind = ANY (ARRAY['SOURCE'::text, 'EXECUTION'::text, 'VALIDATION_INPUT'::text]))),
    CONSTRAINT runtime_artifact_set_state_check CHECK ((state = ANY (ARRAY['STAGING'::text, 'SEALED'::text, 'DELETING'::text])))
);

COMMENT ON COLUMN runtime_artifact_set.set_id IS '不可变源码或输出集合标识';

COMMENT ON COLUMN runtime_artifact_set.project_id IS '所属逻辑项目';

COMMENT ON COLUMN runtime_artifact_set.producer_attempt_id IS '创建该集合的 attempt，管理导入为空';

COMMENT ON COLUMN runtime_artifact_set.kind IS 'SOURCE 项目源、EXECUTION 执行产物、VALIDATION_INPUT 封存的有界 YAML 操作正文';

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

CREATE TABLE runtime_branch (
    branch_id uuid NOT NULL,
    project_id text NOT NULL,
    git_ref text NOT NULL,
    mode text NOT NULL,
    status text NOT NULL,
    base_commit_sha text,
    base_release_id uuid,
    observed_head_sha text,
    active_release_id uuid,
    latest_release_id uuid,
    publication_sequence bigint DEFAULT 0 NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    binding_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    config_version text NOT NULL,
    operation_key text,
    operation_json jsonb,
    signal_version bigint DEFAULT 0 NOT NULL,
    processed_signal_version bigint DEFAULT 0 NOT NULL,
    scan_token uuid,
    scan_expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    base_input_set_id uuid,
    production_base_release_id uuid,
    CONSTRAINT runtime_branch_check CHECK (((mode <> 'PRODUCTION'::text) OR ((git_ref = 'refs/heads/main'::text) AND (status = 'ACTIVE'::text)))),
    CONSTRAINT runtime_branch_git_ref_check CHECK ((git_ref ~~ 'refs/heads/%'::text)),
    CONSTRAINT runtime_branch_mode_check CHECK ((mode = ANY (ARRAY['PRODUCTION'::text, 'PREVIEW'::text]))),
    CONSTRAINT runtime_branch_status_check CHECK ((status = ANY (ARRAY['PROVISIONING'::text, 'ACTIVE'::text, 'DELETING'::text, 'DELETED'::text, 'FAILED'::text])))
);

COMMENT ON TABLE runtime_branch IS '独立 Git 分支身份、生命周期和权威发布指针';

COMMENT ON COLUMN runtime_branch.branch_id IS '分支实例身份；删除后同名重建必须生成新值';

COMMENT ON COLUMN runtime_branch.project_id IS '所属逻辑项目，参与所有跨表归属校验';

COMMENT ON COLUMN runtime_branch.git_ref IS '受控仓库内完整分支引用';

COMMENT ON COLUMN runtime_branch.mode IS '生产或开发预览的绑定用途';

COMMENT ON COLUMN runtime_branch.status IS '分支创建、使用、删除及恢复状态';

COMMENT ON COLUMN runtime_branch.base_commit_sha IS '登记时固定的差异比较源码基线';

COMMENT ON COLUMN runtime_branch.base_release_id IS '与固定源码提交匹配的资源目录基线，没有对应发布时为空';

COMMENT ON COLUMN runtime_branch.observed_head_sha IS '最近核实的远端分支提交';

COMMENT ON COLUMN runtime_branch.active_release_id IS '本分支唯一当前发布，只由封存事务推进';

COMMENT ON COLUMN runtime_branch.latest_release_id IS '本分支最近受理的候选，不代表发布成功';

COMMENT ON COLUMN runtime_branch.publication_sequence IS '本分支最近受理序号，阻止旧候选覆盖新输入';

COMMENT ON COLUMN runtime_branch.version IS '生命周期乐观锁版本';

COMMENT ON COLUMN runtime_branch.binding_config IS '服务控制的执行绑定引用，不含凭据';

COMMENT ON COLUMN runtime_branch.config_version IS '执行绑定的配置版本';

COMMENT ON COLUMN runtime_branch.operation_key IS '创建或登记操作的项目内幂等键';

COMMENT ON COLUMN runtime_branch.operation_json IS '固定操作输入，用于重试冲突检查和中断恢复';

COMMENT ON COLUMN runtime_branch.signal_version IS '持久化待核对信号版本';

COMMENT ON COLUMN runtime_branch.processed_signal_version IS '已核对的信号版本，避免扫描期间新信号丢失';

COMMENT ON COLUMN runtime_branch.scan_token IS '当前扫描租约身份';

COMMENT ON COLUMN runtime_branch.scan_expires_at IS '扫描租约到期时间';

COMMENT ON COLUMN runtime_branch.created_at IS '分支身份登记时间';

COMMENT ON COLUMN runtime_branch.base_input_set_id IS '登记时封存的固定源码基线，删除分支后仍保留引用';

COMMENT ON COLUMN runtime_branch.production_base_release_id IS '登记时观察到的生产发布，仅用于判断生产是否推进';

CREATE TABLE runtime_branch_event (
    delivery_id text NOT NULL,
    payload_digest text NOT NULL,
    received_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL
);

COMMENT ON TABLE runtime_branch_event IS '已验证分支事件的持久幂等记录';

COMMENT ON COLUMN runtime_branch_event.delivery_id IS 'Forgejo webhook delivery 身份';

COMMENT ON COLUMN runtime_branch_event.payload_digest IS '事件正文摘要，防止同身份不同输入';

COMMENT ON COLUMN runtime_branch_event.received_at IS '首次收到事件的时间';

CREATE TABLE runtime_job (
    job_id uuid NOT NULL,
    kind text NOT NULL,
    project_id text NOT NULL,
    parent_run_id uuid,
    idempotency_scope text,
    idempotency_key text,
    request_fingerprint text NOT NULL,
    request_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    input_mode text DEFAULT 'DURABLE'::text NOT NULL,
    pinned_instance_id uuid,
    input_lease_expires_at timestamp with time zone,
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
    branch_id uuid,
    CONSTRAINT runtime_job_check CHECK (((idempotency_key IS NULL) = (idempotency_scope IS NULL))),
    CONSTRAINT runtime_job_check1 CHECK (((input_mode = 'DURABLE'::text) OR ((pinned_instance_id IS NOT NULL) AND (input_lease_expires_at IS NOT NULL)))),
    CONSTRAINT runtime_job_input_mode_check CHECK ((input_mode = ANY (ARRAY['DURABLE'::text, 'VOLATILE'::text]))),
    CONSTRAINT runtime_job_kind_check CHECK ((kind = ANY (ARRAY['BUILD_RUN'::text, 'METRIC_QUERY'::text, 'DBT_COMMAND'::text, 'MF_COMMAND'::text, 'QUERY_OPTIONS'::text, 'RUN_CLEANUP'::text, 'DRAFT_VALIDATION'::text]))),
    CONSTRAINT runtime_job_max_attempts_check CHECK (((max_attempts >= 1) AND (max_attempts <= 3))),
    CONSTRAINT runtime_job_retry_policy_check CHECK ((retry_policy = ANY (ARRAY['PREPARATION_ONLY'::text, 'READ_ONLY'::text, 'NEVER'::text]))),
    CONSTRAINT runtime_job_run_lifecycle_check CHECK ((run_lifecycle = ANY (ARRAY['ACTIVE'::text, 'CLEANING'::text, 'CLEANED'::text]))),
    CONSTRAINT runtime_job_status_check CHECK ((status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'SUCCEEDED'::text, 'FAILED'::text])))
);

COMMENT ON COLUMN runtime_job.job_id IS '公开任务标识，保留现有 run 和 query UUID';

COMMENT ON COLUMN runtime_job.kind IS '执行种类；DRAFT_VALIDATION 仅解析和语义验证，不改变发布或默认产物';

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

COMMENT ON COLUMN runtime_job.branch_id IS '发布、查询或草稿验证的固定分支；通用任务可为空';

CREATE TABLE runtime_job_result (
    job_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    payload_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    format_version text DEFAULT '1'::text NOT NULL,
    stdout_tail text DEFAULT ''::text NOT NULL,
    stderr_tail text DEFAULT ''::text NOT NULL,
    exit_code integer,
    output_truncated boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL
);

COMMENT ON COLUMN runtime_job_result.job_id IS '结果所属任务';

COMMENT ON COLUMN runtime_job_result.attempt_id IS '获准提交结果的实际执行';

COMMENT ON COLUMN runtime_job_result.payload_json IS '遵循公开契约的列、行、SQL 和执行结果';

COMMENT ON COLUMN runtime_job_result.format_version IS '结果格式版本';

COMMENT ON COLUMN runtime_job_result.stdout_tail IS '已脱敏且有大小上限的标准输出尾部';

COMMENT ON COLUMN runtime_job_result.stderr_tail IS '已脱敏且有大小上限的标准错误尾部';

COMMENT ON COLUMN runtime_job_result.exit_code IS '实际子进程退出码';

COMMENT ON COLUMN runtime_job_result.output_truncated IS '诊断输出是否发生截断';

COMMENT ON COLUMN runtime_job_result.created_at IS '结果原子提交时间';

CREATE TABLE runtime_project (
    project_id text NOT NULL,
    binding_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    config_version text DEFAULT '1'::text NOT NULL,
    source_set_id uuid,
    current_output_set_id uuid,
    busy_job_id uuid,
    revision bigint DEFAULT 0 NOT NULL
);

COMMENT ON COLUMN runtime_project.project_id IS '请求中的逻辑项目标识';

COMMENT ON COLUMN runtime_project.binding_config IS '受控绑定的非敏感配置';

COMMENT ON COLUMN runtime_project.config_version IS '连接和项目配置版本';

COMMENT ON COLUMN runtime_project.source_set_id IS '当前项目源码快照';

COMMENT ON COLUMN runtime_project.current_output_set_id IS '当前源码版本最近成功的默认输出';

COMMENT ON COLUMN runtime_project.busy_job_id IS '尚未确认停止的通用写任务';

COMMENT ON COLUMN runtime_project.revision IS '项目指针的乐观并发版本';

CREATE TABLE runtime_release (
    release_id uuid NOT NULL,
    project_id text NOT NULL,
    sequence bigint NOT NULL,
    idempotency_key text NOT NULL,
    request_json jsonb NOT NULL,
    baseline_release_id uuid,
    run_id uuid,
    artifact_set_id uuid,
    state text DEFAULT 'PREPARING'::text NOT NULL,
    build_mode text DEFAULT 'FULL_BUILD'::text NOT NULL,
    catalog_digest text,
    error_code text,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    published_at timestamp with time zone,
    branch_id uuid NOT NULL,
    CONSTRAINT runtime_release_build_mode_check CHECK ((build_mode = ANY (ARRAY['SEMANTIC_ONLY'::text, 'SELECTIVE_BUILD'::text, 'FULL_BUILD'::text]))),
    CONSTRAINT runtime_release_check CHECK (((state <> 'PUBLISHED'::text) OR ((run_id IS NOT NULL) AND (artifact_set_id IS NOT NULL) AND (published_at IS NOT NULL) AND (catalog_digest IS NOT NULL)))),
    CONSTRAINT runtime_release_state_check CHECK ((state = ANY (ARRAY['PREPARING'::text, 'BUILDING'::text, 'VALIDATING'::text, 'PUBLISHED'::text, 'FAILED'::text, 'SUPERSEDED'::text])))
);

COMMENT ON COLUMN runtime_release.release_id IS '服务生成的不可变公开发布身份';

COMMENT ON COLUMN runtime_release.project_id IS '发布所属逻辑项目';

COMMENT ON COLUMN runtime_release.sequence IS '项目内受理序号';

COMMENT ON COLUMN runtime_release.idempotency_key IS '固定候选输入的管理请求幂等键';

COMMENT ON COLUMN runtime_release.request_json IS '固定源码摘要和配置引用，不含连接凭据';

COMMENT ON COLUMN runtime_release.baseline_release_id IS '候选受理时的活动基线';

COMMENT ON COLUMN runtime_release.run_id IS '执行本候选的固定构建任务';

COMMENT ON COLUMN runtime_release.artifact_set_id IS '同事务封存的完整输出集合';

COMMENT ON COLUMN runtime_release.state IS '业务发布状态，不以执行成功替代发布成功';

COMMENT ON COLUMN runtime_release.build_mode IS '经过验证的全构建或复用模式';

COMMENT ON COLUMN runtime_release.catalog_digest IS '完整展示文件原始字节 SHA256';

COMMENT ON COLUMN runtime_release.error_code IS '发布前失败的稳定诊断码';

COMMENT ON COLUMN runtime_release.created_at IS '候选受理时间';

COMMENT ON COLUMN runtime_release.published_at IS '唯一发布事务完成时刻';

COMMENT ON COLUMN runtime_release.branch_id IS '所属分支实例，发布序号和幂等键均在此范围内';

CREATE TABLE runtime_release_relation (
    release_id uuid NOT NULL,
    native_id text NOT NULL,
    creator_run_id uuid,
    binding_json jsonb NOT NULL
);

COMMENT ON COLUMN runtime_release_relation.release_id IS '绑定所属发布快照';

COMMENT ON COLUMN runtime_release_relation.native_id IS '绑定的原生模型或 source 身份';

COMMENT ON COLUMN runtime_release_relation.creator_run_id IS '实际创建物理对象的 run，外部 source 为空';

COMMENT ON COLUMN runtime_release_relation.binding_json IS '物理名称、复用模式、摘要与验证时间';

ALTER TABLE ONLY engine_build
    ADD CONSTRAINT engine_build_pkey PRIMARY KEY (build_id);

ALTER TABLE ONLY engine_build
    ADD CONSTRAINT engine_build_repository_caller_idempotency_key_key UNIQUE (repository, caller, idempotency_key);

ALTER TABLE ONLY engine_build
    ADD CONSTRAINT engine_build_run_id_key UNIQUE (run_id);

ALTER TABLE ONLY engine_change_counter
    ADD CONSTRAINT engine_change_counter_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY engine_change
    ADD CONSTRAINT engine_change_pkey PRIMARY KEY (sequence);

ALTER TABLE ONLY engine_deployment_attempt
    ADD CONSTRAINT engine_deployment_attempt_pkey PRIMARY KEY (repository, environment, branch_name, generation);

ALTER TABLE ONLY engine_deployment_attempt
    ADD CONSTRAINT engine_deployment_attempt_repository_caller_operation_idemp_key UNIQUE (repository, caller, operation, idempotency_key);

ALTER TABLE ONLY engine_deployment_target
    ADD CONSTRAINT engine_deployment_target_pkey PRIMARY KEY (repository, environment, branch_name);

ALTER TABLE ONLY engine_execution_binding
    ADD CONSTRAINT engine_execution_binding_pkey PRIMARY KEY (repository, execution_binding, config_version);

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

ALTER TABLE ONLY runtime_branch_event
    ADD CONSTRAINT runtime_branch_event_pkey PRIMARY KEY (delivery_id);

ALTER TABLE ONLY runtime_branch
    ADD CONSTRAINT runtime_branch_pkey PRIMARY KEY (branch_id);

ALTER TABLE ONLY runtime_branch
    ADD CONSTRAINT runtime_branch_project_id_branch_id_key UNIQUE (project_id, branch_id);

ALTER TABLE ONLY runtime_branch
    ADD CONSTRAINT runtime_branch_project_id_operation_key_key UNIQUE (project_id, operation_key);

ALTER TABLE ONLY runtime_job
    ADD CONSTRAINT runtime_job_pkey PRIMARY KEY (job_id);

ALTER TABLE ONLY runtime_job_result
    ADD CONSTRAINT runtime_job_result_pkey PRIMARY KEY (job_id);

ALTER TABLE ONLY runtime_project
    ADD CONSTRAINT runtime_project_pkey PRIMARY KEY (project_id);

ALTER TABLE ONLY runtime_release
    ADD CONSTRAINT runtime_release_pkey PRIMARY KEY (release_id);

ALTER TABLE ONLY runtime_release
    ADD CONSTRAINT runtime_release_project_id_branch_id_idempotency_key_key UNIQUE (project_id, branch_id, idempotency_key);

ALTER TABLE ONLY runtime_release
    ADD CONSTRAINT runtime_release_project_id_branch_id_release_id_key UNIQUE (project_id, branch_id, release_id);

ALTER TABLE ONLY runtime_release
    ADD CONSTRAINT runtime_release_project_id_branch_id_sequence_key UNIQUE (project_id, branch_id, sequence);

ALTER TABLE ONLY runtime_release
    ADD CONSTRAINT runtime_release_project_id_release_id_key UNIQUE (project_id, release_id);

ALTER TABLE ONLY runtime_release_relation
    ADD CONSTRAINT runtime_release_relation_pkey PRIMARY KEY (release_id, native_id);

ALTER TABLE ONLY runtime_release
    ADD CONSTRAINT runtime_release_run_id_key UNIQUE (run_id);

CREATE INDEX engine_build_history ON engine_build USING btree (repository, created_at, build_id);

CREATE INDEX runtime_attempt_unconfirmed ON runtime_attempt USING btree (job_id) WHERE ((state = 'EXPIRED_UNCONFIRMED'::text) AND (stop_confirmed_at IS NULL));

CREATE UNIQUE INDEX runtime_branch_live_ref ON runtime_branch USING btree (project_id, git_ref) WHERE (status <> 'DELETED'::text);

CREATE UNIQUE INDEX runtime_branch_production ON runtime_branch USING btree (project_id) WHERE (mode = 'PRODUCTION'::text);

CREATE UNIQUE INDEX runtime_job_idempotency ON runtime_job USING btree (idempotency_scope, idempotency_key) WHERE (idempotency_key IS NOT NULL);

CREATE INDEX runtime_job_parent ON runtime_job USING btree (parent_run_id, status);

CREATE INDEX runtime_job_queue ON runtime_job USING btree (kind, available_at, created_at) WHERE (status = 'QUEUED'::text);

CREATE INDEX runtime_release_relation_creator ON runtime_release_relation USING btree (creator_run_id);

CREATE TRIGGER engine_attempt_change AFTER INSERT OR UPDATE OF deployment_status, reason, version, deployed_at ON engine_deployment_attempt FOR EACH ROW EXECUTE FUNCTION engine_record_change();

CREATE TRIGGER engine_build_change AFTER INSERT OR UPDATE ON engine_build FOR EACH ROW EXECUTE FUNCTION engine_record_change();

CREATE TRIGGER engine_job_change AFTER UPDATE OF status, phase, output_set_id, error_code, run_lifecycle ON runtime_job FOR EACH ROW EXECUTE FUNCTION engine_sync_job();

CREATE TRIGGER engine_target_change AFTER INSERT OR UPDATE ON engine_deployment_target FOR EACH ROW EXECUTE FUNCTION engine_record_change();

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

ALTER TABLE ONLY runtime_branch
    ADD CONSTRAINT runtime_branch_base_input_set_id_fkey FOREIGN KEY (base_input_set_id) REFERENCES runtime_artifact_set(set_id);

ALTER TABLE ONLY runtime_branch
    ADD CONSTRAINT runtime_branch_project_id_base_release_id_fkey FOREIGN KEY (project_id, base_release_id) REFERENCES runtime_release(project_id, release_id);

ALTER TABLE ONLY runtime_branch
    ADD CONSTRAINT runtime_branch_project_id_branch_id_active_release_id_fkey FOREIGN KEY (project_id, branch_id, active_release_id) REFERENCES runtime_release(project_id, branch_id, release_id);

ALTER TABLE ONLY runtime_branch
    ADD CONSTRAINT runtime_branch_project_id_branch_id_latest_release_id_fkey FOREIGN KEY (project_id, branch_id, latest_release_id) REFERENCES runtime_release(project_id, branch_id, release_id);

ALTER TABLE ONLY runtime_branch
    ADD CONSTRAINT runtime_branch_project_id_fkey FOREIGN KEY (project_id) REFERENCES runtime_project(project_id);

ALTER TABLE ONLY runtime_branch
    ADD CONSTRAINT runtime_branch_project_id_production_base_release_id_fkey FOREIGN KEY (project_id, production_base_release_id) REFERENCES runtime_release(project_id, release_id);

ALTER TABLE ONLY runtime_job
    ADD CONSTRAINT runtime_job_attempt_fk FOREIGN KEY (current_attempt_id) REFERENCES runtime_attempt(attempt_id);

ALTER TABLE ONLY runtime_job
    ADD CONSTRAINT runtime_job_input_fk FOREIGN KEY (input_set_id) REFERENCES runtime_artifact_set(set_id);

ALTER TABLE ONLY runtime_job
    ADD CONSTRAINT runtime_job_output_fk FOREIGN KEY (output_set_id) REFERENCES runtime_artifact_set(set_id);

ALTER TABLE ONLY runtime_job
    ADD CONSTRAINT runtime_job_parent_run_id_fkey FOREIGN KEY (parent_run_id) REFERENCES runtime_job(job_id);

ALTER TABLE ONLY runtime_job
    ADD CONSTRAINT runtime_job_project_id_branch_id_fkey FOREIGN KEY (project_id, branch_id) REFERENCES runtime_branch(project_id, branch_id);

ALTER TABLE ONLY runtime_job
    ADD CONSTRAINT runtime_job_project_id_fkey FOREIGN KEY (project_id) REFERENCES runtime_project(project_id);

ALTER TABLE ONLY runtime_job_result
    ADD CONSTRAINT runtime_job_result_attempt_id_fkey FOREIGN KEY (attempt_id) REFERENCES runtime_attempt(attempt_id);

ALTER TABLE ONLY runtime_job_result
    ADD CONSTRAINT runtime_job_result_job_id_fkey FOREIGN KEY (job_id) REFERENCES runtime_job(job_id);

ALTER TABLE ONLY runtime_project
    ADD CONSTRAINT runtime_project_busy_fk FOREIGN KEY (busy_job_id) REFERENCES runtime_job(job_id);

ALTER TABLE ONLY runtime_project
    ADD CONSTRAINT runtime_project_output_fk FOREIGN KEY (current_output_set_id) REFERENCES runtime_artifact_set(set_id);

ALTER TABLE ONLY runtime_project
    ADD CONSTRAINT runtime_project_source_fk FOREIGN KEY (source_set_id) REFERENCES runtime_artifact_set(set_id);

ALTER TABLE ONLY runtime_release
    ADD CONSTRAINT runtime_release_artifact_set_id_fkey FOREIGN KEY (artifact_set_id) REFERENCES runtime_artifact_set(set_id);

ALTER TABLE ONLY runtime_release
    ADD CONSTRAINT runtime_release_baseline_release_id_fkey FOREIGN KEY (baseline_release_id) REFERENCES runtime_release(release_id);

ALTER TABLE ONLY runtime_release
    ADD CONSTRAINT runtime_release_project_id_branch_id_baseline_release_id_fkey FOREIGN KEY (project_id, branch_id, baseline_release_id) REFERENCES runtime_release(project_id, branch_id, release_id);

ALTER TABLE ONLY runtime_release
    ADD CONSTRAINT runtime_release_project_id_branch_id_fkey FOREIGN KEY (project_id, branch_id) REFERENCES runtime_branch(project_id, branch_id);

ALTER TABLE ONLY runtime_release
    ADD CONSTRAINT runtime_release_project_id_fkey FOREIGN KEY (project_id) REFERENCES runtime_project(project_id);

ALTER TABLE ONLY runtime_release_relation
    ADD CONSTRAINT runtime_release_relation_creator_run_id_fkey FOREIGN KEY (creator_run_id) REFERENCES runtime_job(job_id);

ALTER TABLE ONLY runtime_release_relation
    ADD CONSTRAINT runtime_release_relation_release_id_fkey FOREIGN KEY (release_id) REFERENCES runtime_release(release_id);

ALTER TABLE ONLY runtime_release
    ADD CONSTRAINT runtime_release_run_id_fkey FOREIGN KEY (run_id) REFERENCES runtime_job(job_id);

-- 变化序号与业务事实在同一事务中递增。
INSERT INTO engine_change_counter(singleton, value) VALUES (true, 0);
