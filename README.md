# dbt / MetricFlow 引擎服务

服务负责固定 Git 源码构建、产物封存、部署指针、资源目录及 MetricFlow 查询。它不创建分支、不合并代码、不理解 Agent 会话状态，也不读取指标平台数据库。`origin` 仅作调用方追溯标签。

## 包职责

| package | 职责 |
| --- | --- |
| `models` | `/v3` 请求响应与封存资源模型；无 I/O |
| `api` | 唯一 HTTP 装配、鉴权、参数和错误转换 |
| `application` | builds、deployments、catalog、queries 四个具体用例服务；不持有 Runtime |
| `storage` | PostgreSQL 短事务、任务/产物存储、变化流和历史迁移 |
| `runtime` | 同进程 worker、租约恢复、完成事务、执行工作目录 |
| `platform` | 固定 Git 读取、完整工程构建、原生目录与查询适配 |
| `execution` / `adapters` | 进程内引擎调用、通用命令控制、脱敏及底层引擎适配 |

无额外 Repository/Port/Facade 镜像层。历史 SQL、历史模型和原始封存 schema 留在 storage/models 内，仅用于历史读取、迁移和物理引用保护。

## 启动

需要 Python 3.11–3.14（推荐 3.12）、uv、Git、PostgreSQL 和目标数据仓库。依赖以 `uv.lock` 为准，vendor 不由本服务改造。

```powershell
uv sync --frozen --all-groups
$env:SERVICE_DATABASE_URL = "postgresql://user:password@localhost/dbt_service"
$env:SERVICE_TOKEN = "由部署环境提供的服务凭据"
uv run --frozen dbt-service-admin migrate
uv run --frozen dbt-service-admin register-bindings bindings.json
uv run --frozen dbt-metricflow-service
```

普通启动只检查数据库 schema，不自动执行迁移。API 和 worker 同进程运行；已受理任务在数据库中，不依赖 Agent 在线。配置读取顺序：环境变量 > `SERVICE_CONFIG_FILE` 指定文件（默认 `config/service.yaml`）> 默认值。文件内相对路径相对配置文件。

dbt 命令通过 `dbtRunner.invoke()` 直接执行，MetricFlow 验证和查询调用 Python API，均不启动引擎 CLI 子进程。
这些调用在线程中运行并共用进程级串行锁，避免 dbt 全局配置和连接相互影响；HTTP 和租约心跳保持异步。
`WORKER_CONCURRENCY` 控制任务槽位，不能使同一进程中的引擎调用并行。Git 源码操作仍使用 Git 工具。
取消或超过单次调用预算后，服务等待当前调用真正退出才释放锁和清理工作目录，不强制终止 Python 线程；
若底层驱动长时间不返回，取消完成和服务关闭也会等待。外部写结果无法确认时仍保留 `OUTCOME_UNKNOWN` 保护。

| 配置 | 用途 |
| --- | --- |
| `SERVICE_DATABASE_URL` | PostgreSQL 元数据库；必需 |
| `SERVICE_TOKEN` | 所有 POST 的 Bearer 凭据，未配置则禁用写入口 |
| `DBT_PROFILES` / `DBT_PROFILES_DIR` | 内联 profiles 或外部目录；二选一 |
| `SERVICE_TEMP_ROOT` | 可丢弃执行目录 |
| `SERVICE_CONFIG_VERSION` | 本 worker 可执行的配置版本，默认 1 |
| `SERVICE_TOOLCHAIN_VERSION` | 工具链标识；默认按依赖版本及源码计算 |
| `WORKER_CONCURRENCY` | 执行槽位，默认 2 |
| `JOB_LEASE_SECONDS` / `JOB_HEARTBEAT_SECONDS` | 默认 90 / 15 秒 |
| `COMMAND_TIMEOUT_SECONDS` | 单次命令预算，默认 1800 秒 |
| `MAX_OUTPUT_BYTES` / `MAX_RESULT_BYTES` | 默认 1 MiB 诊断 / 16 MiB 保存结果 |
| `MAX_ARTIFACT_FILE_BYTES` / `MAX_ARTIFACT_BYTES` | 默认 64 MiB 单文件 / 256 MiB 集合 |
| `SERVICE_HOST` / `SERVICE_PORT` | HTTP 地址、端口 |

绑定以仓库、executionBinding、configVersion 为键，版本不可覆盖。凭据由 Git/profile 环境提供，不写入绑定或请求。示例：

```json
[{"repository":"https://git.example.com/data/sales.git","executionBinding":"warehouse","configVersion":"1",
  "config":{"profileBindingId":"postgres","environments":["PREVIEW","PRODUCTION"],
            "businessTimezone":"Asia/Shanghai","queryRetrySafe":false}}]
```

可选 `schemaName` 指定受控 schema；每次构建仍有独立表名前缀。省略时使用独立 `run_<uuid>` schema。历史查询需要匹配原工具链与配置的执行环境，不应伪造版本标识。

## 构建与部署

```json
{"repository":"https://git.example.com/data/sales.git","branchName":"feature/orders",
 "commitSha":"0123456789012345678901234567890123456789","environment":"PREVIEW",
 "executionBinding":"warehouse","configVersion":"1","deploymentPolicy":"NONE","idempotencyKey":"build-001"}
```

`POST /v3/builds` 在 Git I/O 前原子保存构建身份和持久任务，返回 202。部署策略默认为 NONE。PREVIEW 可指定分支、完整 SHA 或两者；main 只可用于 PRODUCTION，正式环境必须显式 SHA。无分支只允许构建，不部署。分支输入在 worker 首次解析后固定 SHA，重试不会跟随最新 head。

同仓库、可信调用方、操作类型下同幂等键恢复原身份；输入不同返回 409。新键同 SHA 产生新构建。构建成功与部署成功独立，`initialDeployment` 描述随构建附带的意图。

`ON_SUCCESS` 受理时即分配 generation；较新意图即使失败，也不会恢复较旧待处理意图。独立部署使用 `POST /v3/deployments`，传入 `buildId/branchName/expectedTargetVersion/idempotencyKey`。目标自然键是 repository/environment/branchName。切换前重新核对远端 head、构建可用性和目标版本；真实 ref 消失停用指针，网络失败只标记 UNKNOWN。

完整构建运行 deps、parse、compile、build（含测试）、必要的 source freshness、物理关系核验及 MetricFlow 探测。支持 table/view/ephemeral 和不写失败表的测试；seed、snapshot、自定义物化、SQL header、执行 hook 和数据库命令宏明确拒绝。工程读取遵循实际资源目录配置。模板约束不等于任意不可信代码沙箱。

取消先持久化意图。已确认取消返回 CANCELLED；外部写执行无法确认时返回 OUTCOME_UNKNOWN，保留保护引用且不自动重跑。

管理构建使用同一应用用例：

```powershell
uv run --frozen dbt-service-admin build --file build-request.json
```

## 查询和历史读取

| 路径 | 行为 |
| --- | --- |
| `GET /v3/builds`、`GET /v3/builds/{buildId}` | repository 筛选历史、固定身份读取 |
| `GET /v3/builds/{buildId}/logs` | 可续读的持久阶段/错误码日志，不回显原始 CLI |
| `POST /v3/builds/{buildId}/cancel` | 取消构建 |
| `GET /v3/deployments/current`、`GET /v3/deployments` | 自然键当前观察、意图历史 |
| `GET /v3/builds/{buildId}/catalog` | 目录搜索、kind 筛选、游标分页 |
| `GET /v3/builds/{buildId}/resources/{resourceId}` | 资源详情；另有 `/lineage`、`/source`、`/native-details` |
| `POST /v3/builds/{buildId}/query-options` | 202 持久异步选项任务 |
| `GET /v3/query-options/{optionsTaskId}` | 选项状态与结果 |
| `POST /v3/builds/{buildId}/queries` | QUERY、EXPLAIN、PREVIEW、DIMENSION_VALUES |
| `GET /v3/queries/{queryId}/status` | 仅任务/结果存在性，不加载结果正文 |
| `GET /v3/queries/{queryId}/results` | offset/limit 分页，最多 200 行和 8 MiB |
| `GET /v3/changes` | 同事务持久变化流；省略 cursor 从起点重放 |

目录、选项、查询始终使用选定 buildId；不重新解析当前部署。新构建不使旧构建目录和查询失效。物理清理后仍保留目录，`queryAvailable=false`。QUERY/EXPLAIN 使用 metricResourceIds；分组/过滤/维度取值使用该构建及指标集合异步生成的 optionId。PREVIEW 使用 datasetResourceId。查询行数默认 1000、最大 10000；分页只切片已保存结果，不重新执行 SQL。数值精度和 null 保留，时间按固定构建时区归一化。

其他列表默认 50、最多 200，统一 `items/nextCursor`。错误统一为 `error`，包含 `code/message/retryable/phase/buildId`。完整协议以 `/openapi.json` 为准；健康及版本接口仍是 `/health/live`、`/health/ready`、`/v1/versions`。

## 升级与运维

这是整体协议替换。旧 `/v1` 业务路由、所有 `/v2/projects`、独立 validation、同步选项和分支登记入口均已移除，不提供兼容路由。调用方需另行迁移，不能混用新旧部署。

升级前停止旧受理并排空任务，备份元数据库和目标仓库。执行 migrate 后，用旧 projectId 到真实 repository 的 JSON 映射迁移历史，禁止使用统一默认仓库：

```powershell
uv run --frozen dbt-service-admin migrate-history --bindings history-bindings.json --dry-run
uv run --frozen dbt-service-admin migrate-history --bindings history-bindings.json
```

迁移给旧构建确定性分配 buildId，保持旧封存字节与关联不变。来源冲突返回报告且不写入；来源不完整只保留历史，不允许部署或查数。旧 validation 仅保留审计。`import-publication` 只用于旧版外部身份迁入旧审计表，不是构建发布接口。

对结果未知的执行，人工核实目标仓库停止后才释放保护：

```powershell
uv run --frozen dbt-service-admin reconcile-attempt <attempt-uuid> --confirm-external-stopped
uv run --frozen dbt-service-admin gc --older-than-hours 24 --limit 100
```

gc 只回收无引用产物，没有历史自动 TTL。新变化流不自动裁剪；消费端提交本地投影与游标应使用同一事务。

## 验证

自有 Python 代码的函数参数和返回值须完整标注，现有 Ruff 命令会检查缺失标注。
依赖优先使用具体类型，回调使用 `Callable`，可空值显式使用 `| None`；分页与线程辅助函数保留泛型参数。
固定的构建、部署和任务记录在 `storage/records.py` 使用 `TypedDict` 描述，序列化选项和摘要在
`models/payloads.py` 描述。`DatabaseRow` 仅用于动态 SQL 投影及历史记录，`JsonObject` 用于原生引擎扩展
和模型序列化边界；不要用它们替代已有的固定字段类型。`cast` 仅表达 SQL 形状、外键或已完成校验所保证的
不变量，不执行运行时校验；外部载荷继续交给 Pydantic 校验。`TYPE_CHECKING` 可避免执行层的类型引用形成
循环导入，HTTP 签名涉及的类型必须在运行时可解析。测试 fixture 沿用现有标注要求。

必须使用可丢弃的独立测试库。无需 Agent 或指标平台：

```powershell
$env:SERVICE_TEST_DATABASE_URL = "postgresql://user:password@localhost/dbt_service_test"
uv run --frozen pytest -q
uv run --frozen ruff check src tests
```

真实引擎验收设置 `PLATFORM_TEST_POSTGRES=1` 和 `PLATFORM_TEST_PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE`，执行：

```powershell
uv run --frozen pytest -q tests/integration/test_publication_proxy_flow.py
```

测试覆盖完整构建、部署、四种查询、版本隔离与不安全模板失败，创建独立测试 schema；该证据不代表 StarRocks 已做真实集成验证。
