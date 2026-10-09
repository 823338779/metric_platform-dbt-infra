# 固定提交执行服务轻量化

## 已确认范围

用户确认：只使用 PostgreSQL；收窄发布、目录、查询职责；服务聚焦接收固定提交并验证、构建、查询；只做全量构建；API 与 worker 不分离。允许直接下线 v1 CLI/run/query、分支管理与临时 YAML resources，调用方同步升级。

## 对外契约

- 保留 `/health/live`、`/health/ready` 与版本查询；业务入口统一 `/v2/projects`。
- `POST /v2/projects/{projectId}/releases` 接收 `commitSha`、`idempotencyKey`。remote、profile、schema、配置版本仍来自项目绑定，不能由调用方覆盖。
- `POST /v2/projects/{projectId}/validations` 使用同一固定提交请求，不再接收 changes/workspace/branch。验证只检查该提交，不构建数据表或切换发布。
- Git 只读取完整 SHA，不观察 main、不创建或删除 ref、不接收 webhook、不补偿扫描。不要求提交是 main 的祖先。
- 保留发布列表/详情、活动目录、资源、查询选项任务、查询状态与结果分页。删除 compatibility 路由和分支路由。
- 发布同键同 SHA 恢复原任务（即使绑定或活动版本已变化），同键不同 SHA 冲突。
- 全量构建仍执行模板/只读 SQL 策略、必要 freshness、全部模型/测试、物理关系与查询探测验证。

## 组件边界

- `PublicationService` 只负责固定提交受理、发布信息和历史。
- `CatalogService` 负责封存目录/资源读取；`QueryService` 负责选项、查询规则、受理与结果。
- 服务通过存储方法完成查询与事务，SQL 不留在业务 service 中。
- PostgreSQL runtime 只运行构建、固定提交验证、查询/选项及管理清理。API 生命周期继续启动同进程 worker。
- `JobStore` 保留运行租约与状态保障；发布完成事务由专门 completion 协调，任务存储不直接调用发布提交。
- 固定提交源码读取发生在 Git I/O 与数据库短事务分开的边界；发布受理先封存源码，再原子创建候选及任务，执行不重新读取移动分支。

## 历史与升级

不修改历史 SQL/Alembic revisions，不删除已有数据库数据。已有 branch 外键和 production 行暂作为内部历史兼容结构保留，不再暴露分支开发能力。旧发布的复用绑定仍能读取并受 GC 保护；新发布只生成 BUILT/EXTERNAL 绑定。升级前排空旧任务；不存在自动把旧草稿任务转换为固定提交任务的逻辑。

## 验收

必须验证：无数据库配置启动失败；旧业务路由消失；非 main 提交可准确读取；幂等冲突与重试恢复；每次新发布全量构建；失败/旧候选/失效租约不覆盖活动版本；发布事务失败全部回滚；已受理查询固定原 run；验证不执行 build；同进程 worker 生命周期不变。

保留相关存储、事务、查询与安全测试，退出能力的测试同步移除，复用 fixture 抽离后再删除旧模块。执行完整 pytest 和 ruff；需要数据库的测试必须使用隔离测试库，未配置时明确记录跳过。
