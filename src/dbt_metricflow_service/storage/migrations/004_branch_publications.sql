-- 分支是发布隔离单元；历史生产身份和封存产物保持原样。
CREATE TABLE runtime_branch (
    branch_id uuid PRIMARY KEY,
    project_id text NOT NULL REFERENCES runtime_project(project_id),
    git_ref text NOT NULL CHECK(git_ref LIKE 'refs/heads/%'),
    mode text NOT NULL CHECK(mode IN ('PRODUCTION','PREVIEW')),
    status text NOT NULL CHECK(status IN ('PROVISIONING','ACTIVE','DELETING','DELETED','FAILED')),
    base_commit_sha text,
    base_release_id uuid,
    observed_head_sha text,
    active_release_id uuid,
    latest_release_id uuid,
    publication_sequence bigint NOT NULL DEFAULT 0,
    version bigint NOT NULL DEFAULT 1,
    binding_config jsonb NOT NULL DEFAULT '{}'::jsonb,
    config_version text NOT NULL,
    operation_key text,
    operation_json jsonb,
    signal_version bigint NOT NULL DEFAULT 0,
    processed_signal_version bigint NOT NULL DEFAULT 0,
    scan_token uuid,
    scan_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(project_id,branch_id),
    UNIQUE(project_id,operation_key),
    CHECK(mode<>'PRODUCTION' OR (git_ref='refs/heads/main' AND status='ACTIVE'))
);
COMMENT ON TABLE runtime_branch IS '独立 Git 分支身份、生命周期和权威发布指针';
COMMENT ON COLUMN runtime_branch.branch_id IS '分支实例身份；删除后同名重建必须生成新值';
COMMENT ON COLUMN runtime_branch.project_id IS '所属逻辑项目，参与所有跨表归属校验';
COMMENT ON COLUMN runtime_branch.git_ref IS '受控仓库内完整分支引用';
COMMENT ON COLUMN runtime_branch.mode IS '生产或开发预览的绑定用途';
COMMENT ON COLUMN runtime_branch.status IS '分支创建、使用、删除及恢复状态';
COMMENT ON COLUMN runtime_branch.base_commit_sha IS '登记时固定的差异比较源码基线';
COMMENT ON COLUMN runtime_branch.base_release_id IS '登记时固定的生产发布基线';
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
CREATE UNIQUE INDEX runtime_branch_live_ref ON runtime_branch(project_id,git_ref)
    WHERE status<>'DELETED';
CREATE UNIQUE INDEX runtime_branch_production ON runtime_branch(project_id) WHERE mode='PRODUCTION';

-- 迁移时只增补归属，不重写 release、run、query 或 artifact 身份。
INSERT INTO runtime_branch(branch_id,project_id,git_ref,mode,status,active_release_id,
    latest_release_id,publication_sequence,binding_config,config_version)
SELECT gen_random_uuid(),p.project_id,'refs/heads/main','PRODUCTION','ACTIVE',
    p.active_published_release_id,
    (SELECT r.release_id FROM runtime_release r WHERE r.project_id=p.project_id ORDER BY r.sequence DESC LIMIT 1),
    p.publication_sequence,p.binding_config,p.config_version FROM runtime_project p;
UPDATE runtime_branch b SET base_commit_sha=r.request_json->>'commitSha',
    observed_head_sha=r.request_json->>'commitSha'
    FROM runtime_release r WHERE r.release_id=b.latest_release_id;
ALTER TABLE runtime_release ADD COLUMN branch_id uuid;
ALTER TABLE runtime_job ADD COLUMN branch_id uuid;
COMMENT ON COLUMN runtime_release.branch_id IS '所属分支实例，发布序号和幂等键均在此范围内';
COMMENT ON COLUMN runtime_job.branch_id IS '发布、查询或草稿验证的固定分支；通用任务可为空';
UPDATE runtime_release r SET branch_id=b.branch_id FROM runtime_branch b WHERE b.project_id=r.project_id;
UPDATE runtime_job j SET branch_id=r.branch_id FROM runtime_release r WHERE r.run_id=j.job_id;
UPDATE runtime_job j SET branch_id=p.branch_id FROM runtime_job p WHERE j.parent_run_id=p.job_id;
ALTER TABLE runtime_release ALTER COLUMN branch_id SET NOT NULL;
ALTER TABLE runtime_release DROP CONSTRAINT runtime_release_project_id_sequence_key;
ALTER TABLE runtime_release DROP CONSTRAINT runtime_release_project_id_idempotency_key_key;
ALTER TABLE runtime_release ADD UNIQUE(project_id,branch_id,sequence);
ALTER TABLE runtime_release ADD UNIQUE(project_id,branch_id,idempotency_key);
ALTER TABLE runtime_release ADD UNIQUE(project_id,branch_id,release_id);
ALTER TABLE runtime_release ADD FOREIGN KEY(project_id,branch_id) REFERENCES runtime_branch(project_id,branch_id);
ALTER TABLE runtime_job ADD FOREIGN KEY(project_id,branch_id) REFERENCES runtime_branch(project_id,branch_id);
ALTER TABLE runtime_release ADD FOREIGN KEY(project_id,branch_id,baseline_release_id)
    REFERENCES runtime_release(project_id,branch_id,release_id);
ALTER TABLE runtime_branch ADD FOREIGN KEY(project_id,branch_id,active_release_id)
    REFERENCES runtime_release(project_id,branch_id,release_id);
ALTER TABLE runtime_branch ADD FOREIGN KEY(project_id,branch_id,latest_release_id)
    REFERENCES runtime_release(project_id,branch_id,release_id);
ALTER TABLE runtime_branch ADD FOREIGN KEY(project_id,base_release_id) REFERENCES runtime_release(project_id,release_id);
COMMENT ON COLUMN runtime_project.publication_sequence IS '迁移前生产序号，仅保留历史，运行时从生产分支读取';
COMMENT ON COLUMN runtime_project.active_published_release_id IS '迁移前生产指针，仅保留历史，运行时从生产分支读取';
UPDATE runtime_schema_version SET version=4 WHERE version=3;
