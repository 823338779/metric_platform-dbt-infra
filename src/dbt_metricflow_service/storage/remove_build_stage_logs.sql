-- 停止全部旧版本服务/worker，确认 search_path 为目标服务 schema 后，在事务中执行。
-- 此脚本会删除旧阶段变化历史；保留 runtime_job、runtime_attempt、engine_build 和 dbt 详细日志。
-- 如需留档，请在执行前备份 engine_change；不使用 CASCADE，以免删除未知依赖。
DROP TRIGGER IF EXISTS engine_build_change ON engine_build;
DROP FUNCTION IF EXISTS engine_record_change();
DROP TABLE IF EXISTS engine_change;
DROP TABLE IF EXISTS engine_change_counter;
