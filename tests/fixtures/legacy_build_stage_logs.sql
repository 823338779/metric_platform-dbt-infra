-- 旧阶段日志结构快照，仅用于验证显式移除脚本。
CREATE FUNCTION engine_record_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE next_sequence bigint;
BEGIN
 UPDATE engine_change_counter SET value=value+1 WHERE singleton RETURNING value INTO next_sequence;
 -- 仅保留构建日志续读所需的阶段、状态与错误码。
 INSERT INTO engine_change VALUES(
  next_sequence,NEW.repository,TG_TABLE_NAME,NEW.build_id::text,NEW.version,
  jsonb_build_object('phase',NEW.phase,'build_status',NEW.build_status,'error_code',NEW.error_code));
 RETURN NEW;
END $$;

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

COMMENT ON COLUMN engine_change.summary IS '构建阶段、状态与错误码，不含执行秘密';

CREATE TABLE engine_change_counter (
    singleton boolean DEFAULT true NOT NULL,
    value bigint NOT NULL,
    CONSTRAINT engine_change_counter_singleton_check CHECK (singleton)
);

COMMENT ON COLUMN engine_change_counter.singleton IS '构建日志计数器唯一行';

COMMENT ON COLUMN engine_change_counter.value IS '最后同事务分配的变化序号';

ALTER TABLE ONLY engine_change_counter
    ADD CONSTRAINT engine_change_counter_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY engine_change
    ADD CONSTRAINT engine_change_pkey PRIMARY KEY (sequence);

CREATE TRIGGER engine_build_change AFTER INSERT OR UPDATE ON engine_build FOR EACH ROW EXECUTE FUNCTION engine_record_change();
-- 变化序号与业务事实在同一事务中递增。
INSERT INTO engine_change_counter(singleton, value) VALUES (true, 0);

