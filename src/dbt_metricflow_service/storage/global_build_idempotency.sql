-- 停止服务及 worker 后，在原服务 schema 中执行一次。
BEGIN;
LOCK TABLE engine_build, runtime_job IN ACCESS EXCLUSIVE MODE;

-- 旧协议允许任意字符串。能解析的 UUID 保留原值，其余历史构建使用自己的 build_id。
CREATE FUNCTION pg_temp.build_request_uuid(old_key text, build_id uuid) RETURNS uuid
LANGUAGE plpgsql AS $$
BEGIN
    RETURN old_key::uuid;
EXCEPTION WHEN invalid_text_representation THEN
    RETURN build_id;
END $$;

ALTER TABLE engine_build
    DROP CONSTRAINT engine_build_repository_caller_idempotency_key_key;
ALTER TABLE engine_build ALTER COLUMN idempotency_key TYPE uuid
    USING pg_temp.build_request_uuid(idempotency_key, build_id);
-- 旧数据中有多个构建映射到同一 UUID 时整笔事务回滚，保留全部记录。
ALTER TABLE engine_build
    ADD CONSTRAINT engine_build_idempotency_key_key UNIQUE (idempotency_key);
COMMENT ON COLUMN engine_build.idempotency_key IS '调用方分配的全局唯一构建 UUID';

UPDATE engine_build
SET request_json = jsonb_set(request_json, '{idempotencyKey}', to_jsonb(idempotency_key))
WHERE request_json->>'idempotencyKey' IS DISTINCT FROM idempotency_key::text;
UPDATE runtime_job j
SET request_json = jsonb_set(j.request_json, '{idempotencyKey}', to_jsonb(b.idempotency_key))
FROM engine_build b
WHERE j.job_id = b.run_id AND j.request_json->>'idempotencyKey' IS DISTINCT FROM b.idempotency_key::text;
DROP FUNCTION pg_temp.build_request_uuid(text, uuid);
COMMIT;
