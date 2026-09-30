-- 独立发布身份与活动指针；普通执行输出指针不参与业务发布。
ALTER TABLE runtime_project ADD COLUMN publication_sequence bigint NOT NULL DEFAULT 0;
ALTER TABLE runtime_project ADD COLUMN active_published_release_id uuid;
COMMENT ON COLUMN runtime_project.publication_sequence IS '项目最近受理的候选序号，用于阻止旧候选覆盖新输入';
COMMENT ON COLUMN runtime_project.active_published_release_id IS '唯一当前业务发布，和通用执行输出指针独立';

CREATE TABLE runtime_release (
    release_id uuid PRIMARY KEY,
    project_id text NOT NULL REFERENCES runtime_project(project_id),
    sequence bigint NOT NULL,
    idempotency_key text NOT NULL,
    request_json jsonb NOT NULL,
    baseline_release_id uuid REFERENCES runtime_release(release_id),
    run_id uuid UNIQUE REFERENCES runtime_job(job_id),
    artifact_set_id uuid REFERENCES runtime_artifact_set(set_id),
    state text NOT NULL DEFAULT 'PREPARING'
      CHECK(state IN ('PREPARING','BUILDING','VALIDATING','PUBLISHED','FAILED','SUPERSEDED')),
    build_mode text NOT NULL DEFAULT 'FULL_BUILD'
      CHECK(build_mode IN ('SEMANTIC_ONLY','SELECTIVE_BUILD','FULL_BUILD')),
    catalog_digest text,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    UNIQUE(project_id,sequence),
    UNIQUE(project_id,idempotency_key),
    UNIQUE(project_id,release_id),
    CHECK(state<>'PUBLISHED' OR (run_id IS NOT NULL AND artifact_set_id IS NOT NULL
      AND published_at IS NOT NULL AND catalog_digest IS NOT NULL))
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
ALTER TABLE runtime_project ADD CONSTRAINT runtime_project_active_publication_fk
  FOREIGN KEY(project_id,active_published_release_id) REFERENCES runtime_release(project_id,release_id);

-- 直接引用创建对象的 run，防止来源发布过期后被 cleanup 删除。
CREATE TABLE runtime_release_relation (
    release_id uuid NOT NULL REFERENCES runtime_release(release_id),
    native_id text NOT NULL,
    creator_run_id uuid REFERENCES runtime_job(job_id),
    binding_json jsonb NOT NULL,
    PRIMARY KEY(release_id,native_id)
);
CREATE INDEX runtime_release_relation_creator ON runtime_release_relation(creator_run_id);
COMMENT ON COLUMN runtime_release_relation.release_id IS '绑定所属发布快照';
COMMENT ON COLUMN runtime_release_relation.native_id IS '绑定的原生模型或 source 身份';
COMMENT ON COLUMN runtime_release_relation.creator_run_id IS '实际创建物理对象的 run，外部 source 为空';
COMMENT ON COLUMN runtime_release_relation.binding_json IS '物理名称、复用模式、摘要与验证时间';

CREATE TABLE runtime_legacy_identity (
    project_id text NOT NULL REFERENCES runtime_project(project_id),
    kind text NOT NULL CHECK(kind IN ('RELEASE','QUERY')),
    legacy_id uuid NOT NULL,
    target_id uuid NOT NULL,
    PRIMARY KEY(project_id,kind,legacy_id)
);
COMMENT ON COLUMN runtime_legacy_identity.project_id IS '历史身份所属项目';
COMMENT ON COLUMN runtime_legacy_identity.kind IS '历史发布或查询身份类型';
COMMENT ON COLUMN runtime_legacy_identity.legacy_id IS '平台已公开的历史 UUID';
COMMENT ON COLUMN runtime_legacy_identity.target_id IS '核验过的服务发布或查询 UUID';
UPDATE runtime_schema_version SET version=2 WHERE version=1;
