# 持久化层

存储层使用 SQLAlchemy 2 ORM 和短 Session，Store 对外返回普通记录字典。业务层不持有实体或 Session。

| 模块 | 职责 |
| --- | --- |
| `entities.py` | 显式表映射与共享列类型；不负责创建或升级表 |
| `postgres.py` | 连接池、Session 事务及变化计数器锁 |
| `records.py`、`rows.py` | 对外记录类型，以及实体到记录的转换 |
| `jobs.py` | 任务受理、执行租约、完成与回收 |
| `builds.py`、`deployments.py`、`changes.py` | 构建、部署意图与持久变化流 |
| `artifacts.py` | 产物内容、封存、读取与引用保护 |
| `branches.py`、`publications.py`、`queries.py` | 分支、发布与固定版本查询 |
| `schema.py`、`schema.sql` | 新库建表及启动前的表完整性检查 |

普通读取直接使用 `Session.get/scalar/scalars`，新增实体使用 `add/flush`。需要数据库原子判断的租约、版本、抢占和批量写入使用显式条件 DML；这些条件不藏进通用 Repository。

每个独立操作进入 `Database.session()`；写入构建或部署事实时使用 `fact_session()`，先取得变化计数器锁。跨 Store 的原子操作传递同一个 Session，不在内部再开启事务。`transaction()` 保留给管理命令和新库初始化的 Connection 调用。

Session 关闭 autoflush，涉及锁顺序或外键依赖时显式 flush。数据库触发器更新了已加载实体后，需要刷新或使用带 `populate_existing` 的返回查询，避免把旧 identity map 当成当前事实。实体转换在 Session 内完成。

字段类型、默认值、可空性及主键由真实 PostgreSQL 初始化回归校验。表约束、索引和触发器由 schema.sql 定义，不使用 `create_all()` 或运行时反射补建结构。UUID 以字符串传递，JSON 的 `None` 保持 SQL NULL 语义。

初始化使用 `dbt-service-admin init-db`：在选定 schema 中一次创建完整当前结构。并发初始化由事务锁串行化，失败时整体回滚；已有全部业务表则直接返回，只有部分业务表时拒绝自动修补。服务启动只读检查这些表是否齐全，不维护结构版本、迁移链或历史数据兼容逻辑。
