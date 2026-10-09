# SQLAlchemy 持久化与 Alembic 迁移设计

日期：2026-10-09

状态：用户已于 2026-10-09 确认设计与计划；Native 实施及独立审阅修正已完成。验证结果见 [验收记录](../plans/2026-10-09-sqlalchemy-persistence-verification.md)。

## 目标与范围

用户希望用成熟的 Python 持久化框架减少手写连接池、事务和迁移基础设施。已同意采用 SQLAlchemy + Alembic 的渐进路线。

本次交付覆盖 PostgreSQL runtime 的连接与事务管理、所有直接使用该事务接口的调用者，以及数据库结构迁移入口。保持 HTTP 协议、管理命令、Store 的业务参数与返回数据语义不变。内部 cursor 参数改为 SQLAlchemy Connection，相关内部调用和测试同步调整。

本次不改 legacy SQLite 平台存储，不改 dbt/MetricFlow 自身的数据仓库连接，不改 vendor，不引入异步数据库驱动，不批量重写 ORM 实体。普通 CRUD 的 ORM 化作为后续独立工作。

成功标准：应用通过 SQLAlchemy 管理连接和提交/回滚；跨 Store 写入仍具有原子性；新库和受支持的旧库通过 Alembic 升级；已有队列、租约、幂等和发布行为保持一致。

## 现状与方案取舍

- `storage/postgres.py` 使用 psycopg2 ThreadedConnectionPool、BoundedSemaphore 和事务上下文，手工选择五份 SQL 迁移。
- `JobStore.reserve` 和 `PublicationStore.create_candidate` 通过可选 `_cursor` 决定加入事务还是启动事务。
- 发布结束跨越产物封存、任务结果、发布指针和任务终态；`JobStore.finish` 还有租约失效后的手动回滚。
- PostgreSQL SQL 包含行锁、SKIP LOCKED、advisory lock、JSON、触发器和历史数据转换。
- API、worker、分支同步和产物清理直接依赖 psycopg2 的异常类型。

比较三种路线：

1. 仅把原始 cursor 放进 SQLAlchemy 连接池：改动小，但仍依赖驱动事务和自制访问约定，不足以完成此次目标。
2. SQLAlchemy Core Connection + 现有参数化 SQL + Alembic：采用。替换基础设施并明确事务归属，保留已验证的 SQL 语义。
3. 全量 ORM 化：同时改变实体映射和查询方式，扩大回归范围，留到后续。

## 连接与事务

`Database` 继续作为生命周期入口，持有一个 SQLAlchemy Engine，驱动继续使用已有 psycopg2。新增 SQLAlchemy 2.x 和 Alembic 依赖，具体版本在实施时通过 Python >=3.11,<3.15 与 uv 依赖解析确定并锁定。

- 使用框架 QueuePool；`pool_size=max_connections`、`max_overflow=0`、`pool_timeout=5`、`pool_pre_ping=True`。
- 保留连接超时 5 秒、statement_timeout 10000 毫秒和 lock_timeout 5000 毫秒。
- 保持当前 PostgreSQL DSN 输入兼容性，包括 URL 和 libpq 形式；继续正确传递 SSL、options 等驱动参数，不记录带凭据的 DSN。
- `Database.transaction()` 返回 `engine.begin()` 的上下文，产出标准 `Connection`，正常退出提交，异常退出回滚；不再自行管理信号量和连接归还。
- `Database.close()` 调用 Engine.dispose()；Engine 可跨线程共享，Connection 仅在单次操作所属线程内使用。
- 保持短事务和现有默认隔离级别；Git、dbt 执行等外部长操作不纳入新的数据库事务。

保留现有参数化 SQL，通过 `connection.exec_driver_sql(sql, params)` 执行。读取使用结果对象的 `mappings()`；需要可修改字典或对外返回字典时显式转为 dict，更新条数从执行结果读取。JSON/Binary 驱动适配暂时保留。动态标识符仍须安全引用；不能把 psycopg2 Composable 对象未经转换直接传给 SQLAlchemy。

不新增模仿 cursor 的包装器，不引入隐式线程本地事务、事务装饰器或通用 Repository 基类。

## 跨 Store 协作与回滚

保持独立 Store 方法可单独调用。需要组成原子操作的方法采用显式两层入口：

- `JobStore.reserve(...)` 开启一次事务并调用 `reserve_in_transaction(connection, ...)`。
- `PublicationStore.create_candidate(...)` 开启一次事务并调用 `create_candidate_in_transaction(connection, ...)`。
- 外层业务入口使用上述事务内方法，删除可选 `_cursor` 分支和相应 nullcontext。
- `publish_in_transaction`、`ArtifactStore.seal`、`capture_validation_input` 等已有事务内方法接收 Connection，不开启事务、不提交。
- 私有 SQL 查询辅助方法接收所属事务的 Connection。

完整保留这些原子边界：候选创建与任务受理；草稿验证输入与任务受理；产物封存、结果写入、发布指针更新和任务成功。

`JobStore.finish` 提交前重新验证租约的规则不变。若在写入后失效，抛出仅用于该控制路径的内部异常，使事务上下文整体回滚；在事务外捕获该异常并继续返回 False。其他校验和数据库异常继续向上传播，不被吞掉。事务内正常提前返回仍可能提交，因此不能把这一失败路径直接写成 return False。

保留所有锁顺序、条件更新、幂等键、fencing、数据库约束和 SQL 的时间语义。框架不替代这些业务规则。

## 异常边界

SQLAlchemy 执行异常会包装驱动错误，调用者同步迁移：

- runtime API 对 SQLAlchemy DBAPIError、连接池 TimeoutError 保持当前数据库不可用响应。
- worker 与分支同步继续按照现有策略处理数据库失败；不新增自动重试，避免改变外部执行语义。
- 产物清理通过 IntegrityError 的原始错误 SQLSTATE 识别外键冲突，只处理当前预期的外键冲突，其他完整性错误继续抛出。
- 管理命令失败仍向调用者报告失败，不把迁移失败转换为成功。
- 使用独立 psycopg2 数据仓库连接的其他路径保持原异常处理；测试中与 runtime 持久化有关的异常断言改为框架异常。

## Alembic 与历史数据库接管

保持 `dbt-service-admin migrate` 为唯一正式升级入口；服务启动只检查结构版本，不自动修改数据库。Alembic 文件放在可随 Python 包安装的 storage 子目录，安装后的管理命令也必须能找到迁移资源。

采用一个明确的历史接管 revision，再以常规 Alembic revision 维护未来变化：

1. 管理命令在 Engine 事务内取得现有 PostgreSQL advisory migration lock，将同一个 Connection 传入 Alembic env。
2. 尚无 Alembic 历史时，接管 revision 检查 `runtime_schema_version`。空库运行 001–005 原始 SQL；合法历史版本 1–4 仅运行剩余历史 SQL；版本 5 不重复执行数据转换。
3. 旧版本记录必须恰有一行且在 1–5 内；不存在版本表但已存在 runtime 业务表的数据库拒绝接管。无法验证为受支持历史结构的库报错，不能通过 stamp 绕过。
4. 接管前按该历史版本核对预期表和关键列、约束、触发器；接管后核对当前结构与旧版本值 5。具体检查清单从仓库内 001–005 SQL 提取，覆盖运行时依赖的数据库保护规则。此检查只服务历史接管，不引入通用 schema diff 框架。
5. 接管成功后由 Alembic 记录 revision；失败时数据转换与版本记录在同一事务回滚。
6. 后续升级完全由 Alembic revision 链决定，不再向旧 MIGRATIONS 数组添加迁移。原始 SQL 文件作为不可改写的历史资源保留。

旧版本表保留为历史兼容标记，接管完成后固定为 5。新 `Database.check()` 要求 Alembic revision 等于当前包的预期 head，并检查历史标记一致；只有旧版本表的数据库提示运行 migrate。未知、超前或不一致的版本拒绝启动或升级。重复运行 migrate 幂等；并发 migrate 由同一个 advisory lock 串行化。

本次不提供破坏性降级；接管 revision 的 downgrade 明确拒绝执行。README 说明升级步骤、备份要求和失败后诊断方式。

## 修改范围

- `pyproject.toml`、`uv.lock`：依赖与锁定。
- `storage/postgres.py` 与新增 Alembic 环境、接管 revision：连接、事务、迁移及版本检查。
- `storage/jobs.py`、`publications.py`、`artifacts.py`、`branches.py`：标准 Connection 和结果读取，明确事务内入口。
- 使用 Database.transaction 的 publications、branches、validation、admin 等调用者：同步调整内部调用协议。
- `api/runtime.py`、`runtime/worker.py`、`branches/sync.py`：数据库异常边界。
- 相关持久化和迁移测试：保留业务断言，更新 SQL 执行接口，补充框架迁移引入的风险用例。
- README：部署迁移与版本检查说明。

## 验收与验证顺序

1. 连接和事务：成功写入提交；异常写入回滚；池满在限定时间失败；异常后连接可复用；超过池容量的并发操作不泄漏连接。
2. 跨 Store 原子性：候选创建后任务受理失败无残留；发布指针更新后注入失败，产物仍 STAGING、结果不存在、发布和指针不变；写入后租约失效同样完整回滚且返回 False。
3. 并发业务：保留现有幂等受理、SKIP LOCKED 竞争领取、过期 worker fencing 和清理引用保护测试。
4. 迁移：空库、旧版本 1–5、重复升级、并发升级、未知/多行/超前版本、缺失关键约束或触发器、迁移中途失败、独立 search_path；旧发布身份和产物字节保持一致。
5. 边界兼容：数据库不可用及池超时响应、worker 错误处理、外键清理冲突、JSON/UUID/二进制读写、安装包内的迁移资源可用。
6. 先运行受影响测试与 lint，再运行完整测试套件。PostgreSQL 行锁、触发器和迁移必须在真实 PostgreSQL 上验证，不用 SQLite 替代。

当前会话未设置 `SERVICE_TEST_DATABASE_URL`。实施时优先使用可用的独立本地测试 PostgreSQL；没有环境时明确报告哪些测试未执行，不能把 skip 当作验证成功，也不修改生产数据库来验证。

## 参考

- SQLAlchemy Connection 与事务：https://docs.sqlalchemy.org/en/20/core/connections.html
- Alembic 共享外部事务与程序化调用：https://alembic.sqlalchemy.org/en/latest/cookbook.html

## 自审记录

已核对本设计覆盖连接池、事务归属、历史升级、异常包装、打包资源和真实 PostgreSQL 验证。HTTP 与业务返回兼容不等于保留内部 cursor 接口；内部调用和测试属于本次迁移范围。保留原始 SQL 是本阶段明确选择，ORM 化不计入本次验收。
