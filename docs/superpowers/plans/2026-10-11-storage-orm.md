# 存储层 ORM 改造计划

目标：用现有 SQLAlchemy 2 ORM 实现存储层，保持 Store 返回字典、UUID 字符串、现有数据库结构和并发语义。

设计依据：用户已同意实体映射、短 Session、保留具体 Store 和 Alembic 的方案并要求实施。

## 约束

- 按用户后续要求只支持空库初始化；以 0001_initial 管理完整结构，不使用 create_all/运行时反射代替迁移。
- Database.transaction/fact_transaction 保留 Connection 供管理迁移和已有测试使用；新增 session/fact_session 供 Store 使用。
- Session 每次短事务独立，autoflush=False，expire_on_commit=False；跨 Store 操作传同一 Session。
- 保留变化计数器、parent/job/attempt、目标行的锁顺序，数据库 clock_timestamp、fencing、SKIP LOCKED 和条件更新。
- ORM 实体留在存储层，通过 entity_dict 或 SQL 投影输出现有记录形状；不增加通用 Repository。
- 不创建 worktree，不创建提交；保留当前工作区现有变更。

## 实施与验证

- [x] 基础：storage/entities.py 显式 ORM 映射；postgres.py 增加 session/fact_session；rows.py 增加 entity_dict。测试 Session 回滚、线程隔离、数据库默认值与完整字段映射。
- [x] 任务：jobs.py 用实体和 SQLAlchemy DML 重写任务认领、租约、完成、回收。以既有 runtime/storage/concurrency 测试验证。
- [x] 构建部署：builds.py、deployments.py、changes.py、migration.py 改用 Session 和实体，移除 build_tables.py 的独立 Core 映射。验证幂等、版本条件、触发器变化序号。
- [x] 产物与历史发布：artifacts.py、branches.py、publications.py、queries.py、publication_import.py 使用统一实体与 Session，保留封存和发布原子性。验证内容校验、清理引用保护、事务回滚。
- [x] 集成：更新 records.py/completion.py 的事务回调类型；完整 PostgreSQL 测试、Ruff、差异检查及独立代码审查。

共享接口：`Database.session() -> ContextManager[Session]`、`Database.fact_session() -> ContextManager[Session]`；`entity_dict(entity: Base) -> DatabaseRow`。原数据库 Connection 管理接口继续存在，业务 Store 使用新增 Session 接口。

实体：RuntimeProject、RuntimeJob、RuntimeAttempt、ArtifactSet、ArtifactFile、JobResult、Release、ReleaseRelation、LegacyIdentity、Branch、BranchEvent、ExecutionBinding、Build、DeploymentTarget、DeploymentAttempt、ChangeCounter、Change。列保持数据库名（保留名 metadata 对应 metadata_json 属性）。UUID 使用 PostgreSQL UUID(as_uuid=False)，JSONB 使用 none_as_null=True。

重点审查：触发器修改后的旧 identity map；ORM flush 改变锁顺序；JSON null 与 SQL NULL；同一事务原子封存和发布；过期租约不能再写状态。

## 按 ORM 重新整理持久化包

用户进一步要求重新设计 storage，让持久化代码尽量简洁。本轮以降低实际重复为目标，不引入通用 Repository、事务装饰器或新的依赖。

- 实体集中在 entities.py；以 SQLAlchemy Annotated 复用 UUID、JSON、BigInteger 类型，类型注解推导可空性，保留每个字段的业务注释和数据库默认值。
- postgres.py 管理连接池和短 Session；跨 Store 的写入显式共用 Session，外部调用结束后返回记录快照。
- 普通主键读取用 Session.get，单实体查询用 Session.scalar，普通新增用 add/flush；显式 flush 保证外键、触发器和锁定顺序。
- 条件更新、抢占、租约检查和批量更新继续用 SQLAlchemy DML，避免对象读改写破坏并发语义。
- Store 按任务、构建、部署、产物和发布职责组织；只提取有独立职责的输入处理代码，不拆成转发层。
- rows.py 只负责实体到既有字典契约的转换，records.py 保留调用方使用的类型。

验证先对比 ORM 映射与真实数据库的字段类型、可空性、默认值和主键，再运行现有 PostgreSQL 并发、回滚、封存、历史迁移和真实引擎测试。简化前后均保留完整断言。


## 验证结果

- 独立 PostgreSQL 16 测试库完整回归：372 passed、3 skipped、1 failed。
- 唯一失败为改造前已有的 MetricFlow 子模块审计提交号不一致：仓库 HEAD 固定为 9a07d35d6fa7cef009ea574938c1e483c29581ac，依赖测试仍期待 05551f73c22786b933969bbccafc47c2a1b51625；本次未修改 vendor。
- 单独启用真实引擎的 PostgreSQL 集成测试：1 passed，覆盖构建、部署、四种查询和历史版本隔离。
- Ruff 与 git diff --check 通过；独立审查覆盖 ORM 缓存刷新、锁顺序、外键与触发器 flush 顺序、完成前租约复查，没有未解决的审查项。
- 更新原测试的事务适配及故障注入入口，保留原有业务断言；未修改数据库迁移或新增运行依赖。


## 移除旧数据库兼容

用户明确不需要考虑历史数据库，因此取消原有接管与历史结构验证约束：

- 删除 legacy_schema.py/json、旧 SQL 001～005 及对应的两级 Alembic 接管链。
- 0001_initial 执行固定的 alembic/initial.sql，一次创建当前表、约束、索引、触发器与变化计数器初值。
- 结构版本仅使用 alembic_version；启动检查、并发初始化锁和事务回滚保持有效。
- 旧结构升级测试调整为新库初始化测试，继续校验重复执行的数据完整性、约束启用、多 schema 隔离、失败回滚和包外安装。
- 已在可丢弃数据库中逐项对比改造前后的 pg_dump 结构，除版本表外所有 17 张业务表、函数、触发器、索引、约束和注释完全一致。

初始化简化验证：包含构建 wheel 后的资源打包与包外迁移测试，完整回归为 364 passed、1 skipped、1 failed（仍为既有 vendor 提交号校验）。真实引擎集成另行启用后 1 passed；Ruff、差异检查及独立审查通过。

## 最终要求：移除迁移和历史数据兼容

用户进一步明确所有迁移及历史数据兼容都不需要，以下决定取代前文保留 Alembic 的阶段方案：

- 删除 Alembic 依赖、脚本、版本表和历史导入模块；管理命令改为 init-db。
- schema.py 在调用方事务内执行 schema.sql；仅支持当前结构初始化、重复调用和只读启动检查。
- 删除旧发布/查询 ID 映射、旧幂等摘要、缺字段查询快照及扁平查询请求的兼容回退。
- 删除历史导入专用 source_incomplete 字段及判断，构建的 run_id 改为必填。
- 保留当前业务的历史构建读取、租约与并发保护、封存约束和物理引用。
- 迁移专属测试随功能删除；保留并验证新库初始化、包外安装、当前业务及真实引擎链路。

本轮还删除 runtime_project 中迁移前的发布指针列、仅供旧协议测试使用的 history_models/validation_audit 模型和夹具，保留生产分支派生的当前发布读取。新库包含 16 张业务表，无结构版本表或旧 ID 映射表。

验证：完整回归 356 passed、1 skipped、1 failed（已有 vendor 提交号审计不一致）；之后删除旧协议模型的相关 34 项测试通过，重新构建 wheel 后的 2 项打包及包外初始化测试通过。真实引擎集成 1 passed，Ruff 和差异检查通过。未修改 vendor，未操作部署数据库。
