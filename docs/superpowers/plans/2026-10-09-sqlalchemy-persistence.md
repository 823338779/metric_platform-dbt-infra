# SQLAlchemy Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用 SQLAlchemy Core 和 Alembic 替换 runtime 自制连接与迁移基础设施，保持业务原子性和外部契约。

**Architecture:** Database 提供 Engine 管理的 Connection 事务，Store 使用现有参数化 SQL 和标准 Result。跨 Store 写入显式传递同一个 Connection；Alembic 通过共享连接和事务接管旧版本 1–5。

**Tech Stack:** Python >=3.11,<3.15、SQLAlchemy 2.x、Alembic、现有 psycopg2、PostgreSQL、uv、pytest、ruff。

**Spec:** `docs/superpowers/specs/2026-10-09-sqlalchemy-persistence-design.md`（用户已确认）。

## Global Constraints

- 保持 HTTP 协议、管理命令、Store 的业务参数与返回数据语义不变。内部 cursor 参数改为 SQLAlchemy Connection，相关内部调用和测试同步调整。
- 使用框架 QueuePool；`pool_size=max_connections`、`max_overflow=0`、`pool_timeout=5`、`pool_pre_ping=True`。
- 保留连接超时 5 秒、statement_timeout 10000 毫秒和 lock_timeout 5000 毫秒。
- 保持当前 PostgreSQL DSN 输入兼容性，包括 URL 和 libpq 形式；继续正确传递 SSL、options 等驱动参数，不记录带凭据的 DSN。
- 不新增模仿 cursor 的包装器，不引入隐式线程本地事务、事务装饰器或通用 Repository 基类。
- 保留所有锁顺序、条件更新、幂等键、fencing、数据库约束和 SQL 的时间语义。
- 本次不改 legacy SQLite 平台存储，不改 dbt/MetricFlow 自身的数据仓库连接，不改 vendor，不引入异步数据库驱动，不批量重写 ORM 实体。
- 保持 `dbt-service-admin migrate` 为唯一正式升级入口；服务启动只检查结构版本，不自动修改数据库。
- 原始 SQL 文件作为不可改写的历史资源保留。旧版本表保留为历史兼容标记，接管完成后固定为 5。
- PostgreSQL 行锁、触发器和迁移必须在真实 PostgreSQL 上验证，不用 SQLite 替代。

## Review Focus

1. UUID 从字符串变成 UUID 对象，破坏比较、字典值与 JSON；任务 1 验证原类型和空值保持一致。
2. 含 `%`、空格、引号的 DSN、SQL 和 schema 名在新接口下被错误解释；任务 1 与 3 验证绑定及标识符引用。
3. SQLAlchemy 包装错误导致 503 变成 500，或清理吞掉非外键错误；任务 1 验证异常分类和响应。
4. 迁移检查误读其他 schema 的版本、同名约束或触发器；任务 3 在两个 schema 中验证独立升级与拒绝损坏历史。
5. 从安装包运行管理命令找不到 SQL/Alembic 文件，或迁移失败留下成功标记；任务 3 与 4 验证事务回滚和安装包运行。

## 执行约定与文件分工

按任务 1→2→3→4 顺序执行；连接接口变更和其所有调用点必须在任务 1 一起落地，避免提交一个调用协议不一致的版本。任务 2 专注整理事务入口；任务 3 专注升级协议；任务 4 验证部署产物。每步先写能表达行为的测试，再实现，不以静态文本断言代替数据库行为。

实施开始时检查 git 状态与适用 worktree 规则。确认独立 `SERVICE_TEST_DATABASE_URL` 可用；否则检查本地 Docker 服务，使用仅为本任务创建的 PostgreSQL 测试实例。已发现 uv 和 Docker CLI，但尚未验证 Docker daemon 可用。测试凭据不输出；不使用生产数据库。缺少可运行环境时继续可执行的工作，并如实列出数据库验证缺口。

新增文件职责：`storage/schema.py` 只负责迁移协调与版本检查；`storage/legacy_schema.py` 只负责受支持历史结构核验和接管；`storage/alembic/` 保存 env、revision 与模板。其余修改落在现有业务文件，不新增通用持久化层。

### Task 1: 标准 Connection 接口及全量调用点适配

**Files:** 修改 `pyproject.toml`、`uv.lock`；`src/dbt_metricflow_service/storage/{postgres,jobs,publications,artifacts,branches}.py`；`publications/{service,migration}.py`、`branches/{service,sync,events}.py`、`validation/service.py`、`admin.py`、`api/runtime.py`、`runtime/worker.py`（这些简写均相对 `src/dbt_metricflow_service/`）。新增 `tests/test_database.py`、`tests/test_runtime_database_errors.py`。调整下列现有测试中的 runtime 数据库调用，不改数据仓库连接：

`tests/test_branch_{catalog,migration,publication,queries,storage,sync}.py`、`test_draft_validation_execution.py`、`test_publication_{transaction,time}.py`、`test_runtime_{api,artifacts,execution,storage,migration}.py`，以及 `tests/integration/test_{branch_publication_flow,branch_starrocks_flow,publication_proxy_flow}.py`。新增异常测试复用 runtime API/worker 的现有夹具。

**Interfaces:** 保留 `Database(dsn: str, max_connections: int = 8)`、`migrate()`、`check()`、`close()`。`transaction() -> AbstractContextManager[Connection]` 产出真实 SQLAlchemy Connection；此任务暂时保留原迁移算法，但执行接口改为 Connection。Store 返回的 UUID 字段保持原字符串语义，公开字典保持可修改。

- [ ] **1. 写失败测试。** `tests/test_database.py` 包含 `test_commit_and_exception_rollback`（第二连接仅看见成功事务的行）、`test_pool_exhaustion_and_reuse`（容量 1，持有唯一连接后约 5 秒抛框架 TimeoutError；释放后查询成功）、`test_concurrent_writes_reuse_bounded_pool`（并发写入数正确且不超池容量）、`test_dsn_and_value_compatibility`（URL/libpq、特殊字符密码、SSL/options 透传和默认超时；SQL 参数不被拼接）。关键断言：

```python
assert isinstance(connection, Connection)
assert row["id"] == str(expected_uuid)
assert row["optional_id"] is None
assert row["payload"] == {"中文": "100%"}
assert bytes(row["content"]) == b"\x00\xff"
assert connection.exec_driver_sql("SHOW statement_timeout").scalar_one() == "10s"
assert connection.exec_driver_sql("SHOW lock_timeout").scalar_one() == "5s"
```

增加 `test_database_errors_return_503`：注入 DBAPIError 与框架 TimeoutError，断言 `status_code == 503`、`json() == {"detail": {"code": "runtime_unavailable"}}`，响应无凭据。`test_worker_survives_database_error` 覆盖领取与心跳失败后原处理路径；`test_gc_only_swallows_foreign_key_violation` 断言 SQLSTATE 23503 返回 False，23514/23505 抛出 IntegrityError。

- [ ] **2. 验证失败原因。** `uv run --frozen pytest tests/test_database.py tests/test_runtime_database_errors.py -q`；先证明 Connection 类型/框架异常要求与旧实现不符。尚未安装 SQLAlchemy 时先记录依赖缺失，补依赖后必须再观察行为断言失败，不能把 import 错误当作充分的红灯。
- [ ] **3. 实现 Engine 和依赖。** 用 uv 添加兼容的 SQLAlchemy 2.x 与 Alembic 并锁定版本，不升级无关依赖；配置 Global Constraints 中的池和超时。DSN 经 psycopg2 的 DSN 解析交给 dialect 参数，不手工分割；保留用户 options，并确保服务要求的超时生效。若 dialect 默认注册 UUID 对象，使用 Engine connect 事件为该连接注册 UUID/UUID-array 的字符串返回适配，禁止全局修改其他驱动连接；用实际 roundtrip 验证。`transaction()` 直接返回 `engine.begin()`，`close()` 使用 dispose。
- [ ] **4. 适配所有调用者与错误边界。** 每次 execute 持有独立 Result，行读取用 mappings；公开字典和会修改的行转换为 dict。`rowcount` 从对应更新 Result 读取。原始 SQL 用 exec_driver_sql，保留参数绑定；未绑定参数的含 `%` SQL 使用 `no_parameters=True`，不要通过全文件字符串替换转义百分号。动态标识符使用 dialect identifier_preparer.quote，不传 psycopg2 Composable。当前 `_cursor` 暂时承载 Connection，手动回滚暂时使用 Connection.rollback，任务 2 再移除这些过渡点。异常处理改为 DBAPIError/TimeoutError；IntegrityError 通过 `orig.pgcode == '23503'` 识别外键冲突。
- [ ] **5. 验证并提交。** 运行新测试和 `uv run --frozen pytest tests/test_runtime_storage.py tests/test_runtime_artifacts.py tests/test_runtime_api.py tests/test_runtime_worker.py tests/test_publication_transaction.py tests/test_branch_storage.py tests/test_branch_sync.py -q`；全部数据库用例实际执行并通过。用 `git diff --check` 和受改文件 ruff 检查，确认其他数据库路径未被改动。只提交此任务文件，提交信息 `refactor: use SQLAlchemy connections for runtime persistence`。

### Task 2: 明确共享事务入口与租约失败回滚

**Files:** 修改 `storage/jobs.py`、`storage/publications.py`、`storage/artifacts.py`、`publications/service.py`、`validation/service.py`、`branches/service.py`；修改 `tests/test_publication_transaction.py`、`tests/test_publication_service.py`、`tests/test_draft_validation_storage.py`、`tests/test_branch_draft_validation.py` 及引用 `_cursor` 的直接测试调用。

**Interfaces:** 消费任务 1 的 Connection。新增 `JobStore.reserve_in_transaction(self, connection: Connection, kind, project_id, request_json, *, ...) -> dict`；关键字参数完整复制现有 reserve，顺序、默认值不变，仅删除 `_cursor`。新增 `PublicationStore.create_candidate_in_transaction(self, connection: Connection, project_id: str, request: dict, idempotency_key: str, *, branch_id: str | None = None) -> dict`。原独立入口以自己的事务调用上述方法，两个入口均不接受 `_cursor`。`publish_in_transaction(self, connection: Connection, *, job_id: UUID, attempt_token: str, release_id: UUID, output_set_id: UUID) -> None`；`seal(self, set_id: str, connection: Connection) -> None`；`capture_validation_input(self, project_id: str, payload: bytes, connection: Connection, *, version: int = 1) -> str`。

- [ ] **1. 写失败测试。** 新增 `test_candidate_and_job_share_transaction`：调用两个新入口，在 job 插入后抛错，从另一连接断言没有候选/任务且分支序号未增加。新增 `test_validation_input_rolls_back_with_job`：受理失败后输入集合、文件和任务都不存在。新增 `test_lost_lease_after_publish_rolls_back`：在 publish 完成后让下一次 `_authorized` 返回 None，其他调用用真实数据库；沿用 prepared 夹具并断言：

```python
assert jobs.finish(job["job_id"], job["lease_token"], output_set_id=output) is False
assert jobs.result(job["job_id"]) is None
assert artifacts.metadata(output)["state"] == "STAGING"
assert store.get_release(job["project_id"], release["release_id"])["state"] == "PREPARING"
assert store.get_publication(job["project_id"])["activePublication"] is None
```

保留现有 `test_failure_after_pointer_update_rolls_back_entire_publication` 的业务断言，增加事务内方法成功后外层故意抛错仍全部回滚的用例，以证明内部方法没有自行提交。

- [ ] **2. 验证红灯。** `uv run --frozen pytest tests/test_publication_transaction.py tests/test_publication_service.py tests/test_draft_validation_storage.py tests/test_branch_draft_validation.py -q`；新事务入口应因尚未实现失败，租约回归测试可已通过，保留其行为证据。
- [ ] **3. 实现上述接口。** 把现有事务体移动到事务内方法，不复制业务 SQL；外层方法完整显式转发参数。删除 `_cursor`/nullcontext 分支，服务层在自身 transaction 内调用事务内方法。`JobStore.finish` 使用私有 `_FinishLeaseLost` 异常标记写后租约失效，仅在事务上下文外捕获它并返回 False；保持其他异常传播，初次授权失败仍可直接返回 False。seal 回调的第二参数改为 Connection。
- [ ] **4. 验证并提交。** 重跑步骤 2，加跑 `tests/test_runtime_storage.py tests/test_branch_publication.py tests/test_branch_review_regressions.py tests/test_draft_validation_execution.py`；确保幂等、锁顺序、旧候选淘汰和发布成功后响应丢失行为未变。确认运行时没有 `_cursor`、驱动 connection.rollback 或自制事务提交逻辑残留。提交 `refactor: make shared persistence transactions explicit`。

### Task 3: Alembic 历史接管、隔离与版本检查

**Files:** 新增 `src/dbt_metricflow_service/storage/schema.py`、`legacy_schema.py`、`alembic/env.py`、`alembic/script.py.mako`、`alembic/versions/0001_runtime_adoption.py`；修改 `storage/postgres.py`、`tests/test_branch_migration.py`；新增 `tests/test_schema_migrations.py`。五份已有 SQL 文件保持字节不变。

**Interfaces:** `schema.migrate(connection: Connection) -> None`、`schema.check(connection: Connection) -> None`；Database 的对应方法在自身 transaction 内调用。`schema.alembic_config(connection: Connection) -> Config` 指向包内 script_location，以 attributes 传 connection；使用 revision `0001_runtime_adoption`、down_revision=None，未来 head 从 ScriptDirectory 获取且要求单 head。`legacy_schema.adopt(connection: Connection) -> None`、`validate_legacy(connection: Connection, version: int) -> None`；接管 revision 的 upgrade 调用 adopt，downgrade 抛明确 RuntimeError。

- [ ] **1. 写失败测试。** 在真实 PostgreSQL 随机 schema 内通过原始 SQL 准备历史 1–5，每个版本放入该版本合法的项目、任务与产物记录；另测空库。`test_adopt_supported_versions` 断言：

```python
assert current_heads == ("0001_runtime_adoption",)
assert legacy_versions == [5]
assert after_job_ids == before_job_ids
assert after_file_bytes == before_file_bytes
assert second_migrate_snapshot == first_migrate_snapshot
```

保留历史 v3 发布迁移测试中 release、run、query 身份不变断言。新增 `test_reject_invalid_history` 参数化未知版本 0/6、多行、缺失版本表但存在业务表、缺列、缺约束、禁用/缺失触发器，均抛 RuntimeError，数据及版本快照不变。新增 `test_check_requires_supported_alembic_head`：纯旧版本表、未知或多 head、head 与旧标记不一致拒绝，check 从不执行 DDL。

新增 `test_concurrent_migration_is_serialized`（两独立连接同 schema，只有一份历史结果）、`test_migration_failure_is_atomic`（在 004 后注入异常，Alembic revision 和所有 DDL/DML 回滚，重试成功）、`test_two_schemas_do_not_share_history`（两个 schema 含同名表/约束，升级互不影响）、`test_quoted_schema_name`（名称含引号可正确定位）。新增 `test_downgrade_is_rejected_without_changes`。

- [ ] **2. 验证红灯。** `uv run --frozen pytest tests/test_schema_migrations.py tests/test_branch_migration.py -q`；记录当前没有 Alembic 历史和标准校验入口的失败。
- [ ] **3. 实现迁移协调。** `schema.migrate` 首先取得 advisory lock 609302026，核对已存在版本的合法性，再执行 Alembic upgrade head，最后 check。env 仅接受传入的 Connection，在同一个事务执行；不自行创建 Engine、提交或启动 offline 模式。根据 `current_schema()` 固定版本表 schema，所有历史 catalog 查询按该 schema/关系 OID 限定；不得借用 search_path 后续 schema 的版本表。正常服务启动只读核对单 head 与历史值 5，不扫描所有历史结构。
- [ ] **4. 实现版本化历史核验与接管。** 使用小型、固定的历史结构清单，按原 SQL 声明核对表、列类型/空值限制、主键、唯一键、外键及关键 check/索引/触发器；禁止运行时解析 SQL 或创建影子数据库。v1 核对六张业务表及循环外键、幂等索引、两项产物保护触发器；v2 加发布、关系、legacy identity 与发布关联约束；v3 核对 DRAFT_VALIDATION/VALIDATION_INPUT 的 check 变化；v4 加 branch、项目内分支归属外键、发布唯一键和生产/活动分支索引；v5 加两项基线列及外键、事件表。按约束语义核对，避免依赖自动生成的名称。采用 pg_constraint、pg_index、pg_trigger 和 pg_get_*def 获取定义，检查约束有效性、触发器启用状态及目标函数；只规范化 PostgreSQL 展示差异，不忽略实际条件。

合法旧库先 validate，再顺序执行尚未执行的归档 SQL，最后 validate(version=5)。完整 SQL 文件包含函数体，必须整份执行；用 `exec_driver_sql(sql, execution_options={"no_parameters": True})` 避免函数异常文本的 `%` 被视为参数。

**现有 SQL 的 schema 特例：** 001 的循环外键 DO 块按全库 conname 查重，其他 schema 有同名约束时会漏建。归档文件保持不变；仅在本次从空 schema 执行 001 后，按当前表 OID 补建该文件已明确声明但被跨 schema 查重跳过的六条循环外键，再运行后续 SQL。已有历史库缺这些约束仍按损坏历史拒绝，不隐式修补。对应测试显式检查六条外键，不能只验证版本号。

- [ ] **5. 验证并提交。** 重跑步骤 2，加跑 `tests/test_runtime_migration.py tests/test_publication_migration.py tests/test_runtime_artifacts.py tests/test_branch_storage.py`；确认失败不留下 Alembic 表/版本、并发升级一致、原 SQL 无 diff。提交 `feat: adopt runtime schema with Alembic`。

### Task 4: 安装包验收、部署说明与全量回归

**Files:** 修改 `README.md`、必要时 `pyproject.toml` 的资源打包配置；新增 `tests/test_migration_package.py`。仅对前面任务暴露的迁移相关缺陷做定点修复。

**Interfaces:** 保持 `uv run --frozen dbt-service-admin migrate`；新增测试接收打包文件路径 `SERVICE_TEST_WHEEL`（仅测试用）。安装包中的 Database、SQL 与 Alembic revision 与源码运行行为一致。

- [ ] **1. 写打包验收测试。** `test_wheel_contains_migration_resources` 用 zipfile 读取 `SERVICE_TEST_WHEEL`，断言五份 SQL、env、接管 revision、script.py.mako 均存在。`test_installed_package_migrates_without_repository_cwd` 把 wheel 安装到 tmp_path 下独立目标目录，子进程移除项目 PYTHONPATH、切换到临时 cwd 并优先从该目录导入，在独立 schema 执行 migrate/check；断言模块路径属于安装目录、head 正确、再次迁移幂等。仅使用任务测试库，不依赖源码目录查找资源。
- [ ] **2. 构建并检查。** `uv build --wheel --out-dir dist/persistence-verification`，将产物绝对路径设为 SERVICE_TEST_WHEEL，运行 `uv run --frozen pytest tests/test_migration_package.py -q`。若现有 hatch 配置已自动打包全部资源且测试通过，不为制造红灯而修改配置；否则只补所缺资源配置并重新构建验证。
- [ ] **3. 更新 README。** 说明先备份、停止旧 worker、执行原 migrate 命令、通过版本检查后启动新服务；列出支持旧版本 1–5、未知/损坏历史拒绝、升级失败整体回滚、不支持 downgrade、不得用 stamp 绕过检查。说明未来新增 Alembic revision，不再扩展旧 SQL 数组。记录真实 PostgreSQL 测试的环境变量，不写凭据。
- [ ] **4. 完整验证。** `uv run --frozen ruff check src/dbt_metricflow_service tests`、`uv run --frozen pytest -q`、`git diff --check`。数据库核心验收用例必须实际执行；外部 dbt/StarRocks/Forgejo 集成若缺环境，单独列出 skip 原因。新改动导致失败则修复并重跑相关用例；既有无关 lint/测试问题记录而不顺手重构。
- [ ] **5. 提交与最终审阅。** 提交 `test: verify packaged persistence migrations`（包含相关文档）；按用户选择的执行技能进行最终独立审阅与必要修复。仅清理本任务创建的临时实例和测试产物。报告改动、实际测试计数、跳过项、部署命令与剩余限制，不在未验证 PostgreSQL 核心场景时声称完整验收通过。

## 计划自审与执行选择

已逐项映射设计：连接/超时/DSN/类型与异常在任务 1；跨 Store 原子性及写后租约失效在任务 2；版本接管、历史核验、schema 隔离与事务回滚在任务 3；安装资源与部署、全套回归在任务 4。五项 Review Focus 都有对应行为测试。任务 1/2/3 改动共享文件，按顺序执行。

推荐 Native：由当前会话顺序实施，最后独立审阅一次。这四项依赖同一个 Connection 契约，顺序实施便于保持上下文，避免逐项交接重复适配。若选择 Subagent-driven，则每任务由独立实施者及审阅者接力，最后再做整体审阅，审阅更密集但上下文成本更高。

状态：计划已自审，等待用户审阅并选择执行方式；尚未安装依赖、修改产品代码或运行数据库迁移。
