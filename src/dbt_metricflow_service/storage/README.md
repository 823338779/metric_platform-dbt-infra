# 持久化层

存储层使用 SQLAlchemy 2 ORM 和短 Session，Store 对外返回拒绝额外字段的 Pydantic 记录快照。业务层不持有实体或 Session。

| 模块 | 职责 |
| --- | --- |
| `entities.py` | 显式表映射与共享列类型；不负责创建或升级表 |
| `postgres.py` | 连接池、Session 事务及 schema 级事实事务锁 |
| `records.py`、`rows.py` | 对外记录类型，以及实体到记录的转换 |
| `jobs.py` | 任务受理、执行租约、完成与回收 |
| `builds.py`、`deployments.py` | 构建事实与部署意图 |
| `execution_logs.py` | 独立的 dbt 详细运行日志，后台分批追加 |
| `artifacts.py` | 产物内容、封存、读取与引用保护 |
| `schema.py`、`schema.sql` | 新库建表及启动前的表完整性检查 |

普通读取直接使用 `Session.get/scalar/scalars`，新增实体使用 `add/flush`。需要数据库原子判断的租约、版本、抢占和批量写入使用显式条件 DML；这些条件不藏进通用 Repository。

每个独立操作进入 `Database.session()`；写入构建或部署事实时使用 `fact_session()`，先取得当前数据库及 schema 范围的事务 advisory lock，保留既有事实写入的串行约束。跨 Store 的原子操作传递同一个 Session，不在内部再开启事务。`transaction()` 保留给管理命令和新库初始化的 Connection 调用。

阶段、状态和错误码直接保存在 `runtime_job`，`engine_sync_job` 继续同步 `engine_build` 供构建详情查询。已删除 `engine_change`、`engine_change_counter` 及生成阶段变化历史的触发器，不再将执行状态复制为日志。`runtime_attempt` 继续记录重试、租约及实际执行状态；dbt 事件中的 phase 仅作为该条真实日志的上下文。

Session 关闭 autoflush，涉及锁顺序或外键依赖时显式 flush。数据库触发器更新了已加载实体后，需要刷新或使用带 `populate_existing` 的返回查询，避免把旧 identity map 当成当前事实。实体转换在 Session 内完成。

字段类型、默认值、可空性及主键由真实 PostgreSQL 初始化回归校验。表约束、索引和触发器由 schema.sql 定义，不使用 `create_all()` 或运行时反射补建结构。UUID 以字符串传递，JSON 的 `None` 保持 SQL NULL 语义。

任务只包含构建，完成事务封存产物并更新状态，不保存通用结果表。查询全程在内存中执行，不持有数据库清理锁，不产生持久任务。目录以封存的 Manifest 为唯一来源，仅由 ManifestCapture 传递原文，再由 dbt SDK 提供原生节点与依赖；产物集合仅记录来源与完整性字段，不另存完整目录或重复元数据 JSON。旧分支、发布关系和默认输出复用表已移除。

构建输出的 `target/manifest.json` 来自唯一一次 `dbt build` 返回的内存 Manifest，使用 dbt 的 WritableManifest 和 JSON 编码器，持久化不读取本地 Manifest。构建成功后仅统一执行副本的 target-path，不注入 schema/alias 宏。完成事务只要求 Manifest，不再要求 catalog.json、人工验证标记或查询探测证明。ManifestCapture.persist 负责将内存内容与对应产物路径交给通用 memory_files 接口；存储层不再单独识别 manifest_json 参数。其他已有产物仍从工作目录捕获；大小、摘要、租约及事务封存规则不变。

Manifest 使用 UTF-8 紧凑 JSON（不转义中文、去掉分隔符空格），入库时采用 gzip level 6 压缩，固定 `mtime=0`。仅在压缩结果更小时保存 `codec=gzip`，否则保留 `raw`。`raw_size`、`raw_sha256` 和集合摘要按压缩前内容计算，`stored_size` 记录实际存储字节数；容量限制仍按原始内容执行。`read_file` 和 `materialize` 按 codec 自动解压并校验，旧 raw 记录继续可读，无需迁移；已有产物不自动重写。

初始化使用 `dbt-service-admin init-db`：在选定 schema 中一次创建完整当前结构。并发初始化由事务锁串行化，失败时整体回滚；已有全部业务表则直接返回，只有部分业务表时拒绝自动修补。服务启动只读检查这些表是否齐全，不维护结构版本、迁移链或历史数据兼容逻辑。

旧业务超时配置 `COMMAND_TIMEOUT_SECONDS`、执行参数和 `runtime_job.deadline_at` 已移除。升级时先从服务 YAML 中删除该配置项；停止全部旧版本服务和 worker，确认 `search_path` 指向目标服务 schema 后，在事务中执行同目录 `remove_job_deadline.sql`。该脚本仅删除停用的 deadline 列，保留任务、构建和 `runtime_attempt` 租约；新库无需执行。未升级的旧库会在服务启动检查时被拒绝，服务不会自动改表。数据库、Git 的原生超时配置和 worker 心跳租约机制不变。

固定请求、执行配置、产物元数据、诊断与日志投影均使用明确字段的模型，并通过属性访问。JSONB 列仍使用数据库 JSON 映射，在存储边界序列化/验证；dbt 工程 YAML、适配器会话标识映射及数据库驱动配置属于可扩展外部协议边界。动态路径索引和按构建身份索引的字典保留原用途。

## dbt 详细运行日志

`runtime_execution_log` 保存 service 执行入口发起的 dbt 命令事件，包括 parse、build 等命令的 DEBUG/INFO/WARN/ERROR 事件，以及当前已接管引擎命名空间的 Python 日志。单独运行 `dbtRunner()` 的脚本和第三方直接写 stdout/文件不会自动接入。

每条记录包含 `build_id`、`job_id`、`attempt_id`、`phase`、`command`、`event_time`、`level`、`event_name`、`event_code`、`invocation_id`、`node_id`、`thread_name`、`message` 和数据库写入时间 `created_at`。`log_id` 是数据库递增标识。无节点的事件、普通 Python 日志不具备的 dbt 元数据使用空字符串。原始 protobuf 载荷不整包保存；消息及文本元数据统一遮盖 `DBT_ENV_SECRET_*` 的值，再送入队列。

日志独立于 stdout 尾部缓冲区，不受 `max_output_bytes` 截断。每个 dbt 调用有最多 2048 条的队列，最多 100 条一批，约 0.5 秒提交一次；执行成功、失败或取消时等待引擎结束后排空队列。日志事务使用独立的构建级日志锁，不获取业务事实事务锁。

队列满时丢弃新增记录，写库失败时回滚当前批次并停止该命令后续写入，关闭时告警报告未保存数量；异常仅记录类型，不输出数据库连接信息或 SQL 参数。此策略优先保证 dbt 继续执行，不提供无损审计保证。进程被强制终止时尚未提交的日志可能丢失。表记录随关联构建、任务或 attempt 删除而级联清理，未设置额外的定时保留策略。

查询示例（通过数据库客户端绑定 `:build_id` 和 `:after_log_id`）：

```sql
SELECT log_id, attempt_id, phase, command, event_time, level,
       event_name, node_id, message
FROM runtime_execution_log
WHERE build_id = :build_id AND log_id > :after_log_id
ORDER BY log_id
LIMIT 100;
```

写入在分配序号前取得构建级日志事务锁，同一构建的多个 worker/attempt 按提交顺序续读不会跳过迟提交批次。不同构建仍可并发，不能将全局 `log_id` 当作跨构建提交水位。

前端使用 `GET /builds/{buildId}/execution-logs` 读取结构化记录；该接口按构建绑定游标，支持空页继续轮询，单页最多 200 条。原 `/logs` 已删除，阶段、状态和错误摘要由 `GET /builds/{buildId}` 返回。详见 [前端接入说明](../../../docs/build-execution-logs-api.md)。

新库的 `init-db` 会创建此表。已有当前结构的数据库，需要在服务停止期间显式执行同目录 `add_execution_log.sql` 一次，再启动新版服务。使用目标服务连接配置、确认 `search_path` 指向服务 schema，在一个事务中运行该脚本；不要对已有库重新执行整个 `schema.sql`。启动检查只校验结构，不自动执行升级。示例（把 `your_service_schema` 替换成实际服务 schema）：

```sql
BEGIN;
SET LOCAL search_path TO your_service_schema;
-- 在此执行 add_execution_log.sql 的内容。
COMMIT;
```

从旧阶段日志版本升级时，停止所有旧服务和 worker，在确认目标 schema 后，于事务中执行 `remove_build_stage_logs.sql`。它会删除旧阶段变化历史和计数器，不删除构建、任务、attempt 或 dbt 详细日志；如需保留旧阶段历史，应先备份 `engine_change`。升级后再统一启动新版进程，避免旧计数器行锁与新版 advisory lock 混用。新库初始化不再创建这些旧对象。

不再生成或捕获 `published_catalog.json`，构建完成也不要求该文件。Manifest 的完整性仍通过 artifact 文件摘要验证。新初始化结构不再定义 `engine_build.catalog_digest`；现有数据库可保留该旧列和旧同步触发器（新构建对应的目录摘要为空），当前 ORM 和目录读取不再使用它们，无需改写旧构建。

新初始化结构不再定义 `runtime_artifact_set.validation_json`，当前代码不读写该列。现有数据库可保留原有默认值为空对象的列，无需迁移历史产物。构建错误详情中的旧 validationSummary 字段只为历史记录读取保留，新错误直接使用 dbt 日志和执行诊断。

`remove_run_cleanup.sql` 移除旧清理生命周期字段和触发器依赖，只允许新建构建任务；保留历史清理任务，未完成的清理任务以 `CLEANUP_REMOVED` 结束。停止全部服务和 worker 后，在目标 schema 的事务内执行，再启动服务。

任务 `kind` 字段已移除，执行器直接构建；产物集合的 `SOURCE/EXECUTION` 类型保留。已有数据库停服后在目标服务 schema 的事务内执行 `remove_job_kind.sql`；若仍存在旧清理生命周期字段，先执行 `remove_run_cleanup.sql`。迁移保留任务和 attempt 历史，将未完成的旧清理任务标记失败，避免当作构建领取。
