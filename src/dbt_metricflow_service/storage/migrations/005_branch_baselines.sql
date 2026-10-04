-- 固定源码比较基线和生产观察锚点分开保存，历史读取不依赖远端引用。
ALTER TABLE runtime_branch ADD COLUMN base_input_set_id uuid REFERENCES runtime_artifact_set(set_id);
ALTER TABLE runtime_branch ADD COLUMN production_base_release_id uuid;
ALTER TABLE runtime_branch ADD FOREIGN KEY(project_id,production_base_release_id)
    REFERENCES runtime_release(project_id,release_id);
COMMENT ON COLUMN runtime_branch.base_input_set_id IS '登记时封存的固定源码基线，删除分支后仍保留引用';
COMMENT ON COLUMN runtime_branch.base_release_id IS '与固定源码提交匹配的资源目录基线，没有对应发布时为空';
COMMENT ON COLUMN runtime_branch.production_base_release_id IS '登记时观察到的生产发布，仅用于判断生产是否推进';
UPDATE runtime_branch SET production_base_release_id=base_release_id;
UPDATE runtime_branch b SET base_release_id=NULL WHERE base_release_id IS NOT NULL
 AND NOT EXISTS(SELECT 1 FROM runtime_release r WHERE r.release_id=b.base_release_id
                AND r.request_json->>'commitSha'=b.base_commit_sha);

-- 重复 webhook 使用部署提供的 delivery id 去重，记录不能随分支删除清除。
CREATE TABLE runtime_branch_event (
    delivery_id text PRIMARY KEY,
    payload_digest text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
COMMENT ON TABLE runtime_branch_event IS '已验证分支事件的持久幂等记录';
COMMENT ON COLUMN runtime_branch_event.delivery_id IS 'Forgejo webhook delivery 身份';
COMMENT ON COLUMN runtime_branch_event.payload_digest IS '事件正文摘要，防止同身份不同输入';
COMMENT ON COLUMN runtime_branch_event.received_at IS '首次收到事件的时间';
UPDATE runtime_schema_version SET version=5 WHERE version=4;
