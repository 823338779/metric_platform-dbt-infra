# dbt / MetricFlow 引擎服务

服务负责固定 Git 源码构建、产物封存、部署指针、资源目录及 MetricFlow 查询。它不创建分支、不合并代码、不理解 Agent 会话状态，也不读取指标平台数据库。`origin` 仅作调用方追溯标签。

## 包职责

| package | 职责 |
| --- | --- |
| `models` | 完整执行快照请求响应与封存资源模型；无 I/O |
| `api` | 唯一 HTTP 装配、鉴权、参数和错误转换 |
| `application` | builds、deployments、catalog、queries 四个具体用例服务；不持有 Runtime |
| `storage` | PostgreSQL 短事务、任务/产物存储、构建日志和新库初始化 |
| `runtime` | 同进程 worker、租约恢复、完成事务、执行工作目录 |
| `platform` | 固定 Git 读取、完整工程构建、原生目录与查询适配 |
| `execution` / `adapters` | 进程内引擎调用、串行与取消控制、脱敏及底层引擎适配 |

内部维度发现和数据查询使用独立入口：`Runtime.available_dimensions(build, metrics)` 返回
`DimensionQueryResult`，`Runtime.execute_query(build, request)` 接收 `PlatformQueryRequest` 并返回
`DataQueryResult`。两个入口直接调用各自的平台函数；共享的 `_query_context` 仅管理构建保护、
工作区、profile 和引擎环境。上下文在 `run_serialized` 的执行线程内打开，超时或取消仍须等底层
执行结束再释放锁和资源。构建验收单独调用 `probe_query_capability`。HTTP 请求和响应保持不变。

无额外 Repository/Port/Facade 镜像层。持久化通过 SQLAlchemy ORM 实现，封存产物与物理引用保护由具体 Store 和数据库约束维护。

构建响应统一读取包含物理生命周期的快照；可查询性判断和响应组装不执行 I/O。
构建、部署历史由 Store 在数据库中筛选并按游标读取，构建列表批量加载初始部署。
查询用例负责受理编排，`application/query_plan.py` 负责资源及选项到引擎请求的纯转换；
时间只归一化一次，幂等摘要与执行元数据复用同一请求。部署协调器由启动入口显式传入 worker。
构建、部署历史使用 v2 游标，按创建时间和唯一键在数据库中排序；升级前的 v1 历史游标返回
`INVALID_CURSOR`，调用方应省略游标从首页重新读取。目录游标不变。

## 维度显示值映射

在 dbt 工程的分类维度上配置正式字段 `value_mapping`，随工程构建发布：

```yaml
semantic_models:
  - name: requests
    model: ref('requests')
    entities:
      - name: request
        type: primary
        expr: id
    dimensions:
      - name: status
        type: categorical
        value_mapping:
          semantic_model: status_codes
          value_dimension: raw_value
          display_dimension: display_value
  - name: status_codes
    model: ref('dim_status_code')
    entities:
      - name: status_code
        type: primary
        expr: code
    dimensions:
      - name: raw_value
        type: categorical
        expr: code
      - name: display_value
        type: categorical
        expr: label
```

`dim_status_code` 是工程中的物理 dbt 模型，可由已有表构建；`status_codes` 是它的 semantic model。
映射模型只用于显示，不能同时定义指标或递归映射。原始键由工程维护者保证唯一。
构建探针检查原始键非 NULL、源与键类型兼容、显示列为文本，不扫描重复键。

MetricFlow 在完成原始值的筛选、分组、计算、排序和 LIMIT 后，通过显式
`ApplyDimensionValueMappingsNode` 生成最外层 LEFT JOIN；每个映射维度各自 JOIN。
404、502 都显示为“失败”时仍保留两行；列名及列顺序不变。缺失映射或显示值为 NULL
时返回原始值的文本，原始 NULL 仍为 NULL，空显示字符串有效。
指标查询和 explain 使用这条原生链路，服务不替换单元格，也不包装 SQL。

可用维度仅展示业务维度；映射表内部字段不会作为计算维度出现。维度值查询和筛选使用
原始代码及原始类型，PREVIEW 仍显示物理数据。旧配置无需迁移；该字段是本项目的协议扩展，
并非对同版本上游 dbt/MetricFlow 的兼容承诺。修改映射配置后需重新构建发布。

## 启动

需要 Python 3.11–3.14（推荐 3.12）、uv、Git、PostgreSQL 和目标数据仓库。依赖以 `uv.lock` 及 vendor 固定源码为准；维值映射使用本项目扩展的 dbt/MetricFlow 协议。

```powershell
uv sync --frozen --all-groups
$env:SERVICE_DATABASE_URL = "postgresql://user:password@localhost/dbt_service"
$env:SERVICE_TOKEN = "由部署环境提供的服务凭据"
uv run --frozen dbt-service-admin init-db
uv run --frozen dbt-metricflow-service
```

普通启动只检查数据库 schema，不自动建表。API 和 worker 同进程运行；已受理任务在数据库中，不依赖 Agent 在线。配置读取顺序：环境变量 > `SERVICE_CONFIG_FILE` 指定文件（默认 `config/service.yaml`）> 默认值。文件内相对路径相对配置文件。

dbt 命令通过 `dbtRunner.invoke()` 直接执行，MetricFlow 验证和查询调用 Python API，均不启动引擎 CLI 子进程。
这些调用在线程中运行并共用进程级串行锁，避免 dbt 全局配置和连接相互影响；HTTP 和租约心跳保持异步。
`WORKER_CONCURRENCY` 控制任务槽位，不能使同一进程中的引擎调用并行。Git 源码操作仍使用 Git 工具。
取消或超过单次调用预算后，服务等待当前调用真正退出才释放锁和清理工作目录，不强制终止 Python 线程；
若底层驱动长时间不返回，取消完成和服务关闭也会等待。外部写结果无法确认时仍保留 `OUTCOME_UNKNOWN` 保护。

| 配置 | 用途 |
| --- | --- |
| `SERVICE_DATABASE_URL` | PostgreSQL 元数据库；必需 |
| `SERVICE_TOKEN` | 所有 POST 的 Bearer 凭据，未配置则禁用写入口 |
| `EXECUTION_INSTANCES` / `EXECUTION_CREDENTIALS` | 可信物理实例清单和凭据环境变量引用 |
| `SERVICE_TEMP_ROOT` | 可丢弃执行目录 |
| `SERVICE_CONFIG_VERSION` | 本 worker 可执行的配置版本，默认 1 |
| `SERVICE_TOOLCHAIN_VERSION` | 工具链标识；默认按依赖版本及源码计算 |
| `WORKER_CONCURRENCY` | 执行槽位，默认 2 |
| `JOB_LEASE_SECONDS` / `JOB_HEARTBEAT_SECONDS` | 默认 90 / 15 秒 |
| `COMMAND_TIMEOUT_SECONDS` | 单次命令预算，默认 1800 秒 |
| `MAX_OUTPUT_BYTES` / `MAX_RESULT_BYTES` | 默认 1 MiB 诊断 / 16 MiB 保存结果 |
| `MAX_ARTIFACT_FILE_BYTES` / `MAX_ARTIFACT_BYTES` | 默认 64 MiB 单文件 / 256 MiB 集合 |
| `SERVICE_HOST` / `SERVICE_PORT` | HTTP 地址、端口 |

## 构建与部署

每次请求携带完整不可变 execution 快照，构建受理后配置变更不会改变已受理任务或查询的目标。

```json
{"repository":"https://git.example.com/data/sales.git","branchName":"feature/orders",
 "commitSha":"0123456789012345678901234567890123456789",
 "execution":{"environmentId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","configVersion":1,
   "connection":{"type":"starrocks","instanceId":"warehouse","host":"localhost","port":9030},
   "credentialRef":"warehouse-readwrite","catalog":"default_catalog","schema":"analytics",
   "threads":1,"businessTimezone":"Asia/Shanghai"},
 "deploymentPolicy":"NONE","idempotencyKey":"build-001"}
```

`POST /builds` 在 Git I/O 前原子保存完整请求和持久任务，返回 202。部署策略默认为 NONE。
环境由 environmentId 标识，不限制分支名称；branchName 必填，commitSha 可选且必须为完整 SHA。
分支输入在 worker 首次解析后固定 SHA，重试不会跟随最新 head。schema 必须显式提供；构建使用固定 schema 和原表名，后续构建可能覆盖同名表，因此固定 buildId 不表示历史物理数据快照。

同仓库、可信调用方、操作类型下同幂等键恢复原身份；输入不同返回 409。新键同 SHA 产生新构建。构建成功与部署成功独立，`initialDeployment` 描述随构建附带的意图。

`ON_SUCCESS` 受理时即分配 generation；较新意图即使失败，也不会恢复较旧待处理意图。独立部署使用 `POST /deployments`，传入 `buildId/branchName/expectedTargetVersion/idempotencyKey`。目标自然键是 repository/environmentId/branchName。切换前重新核对远端 head、构建可用性和目标版本；真实 ref 消失停用指针，网络失败只标记 UNKNOWN。

完整构建运行 deps、parse、compile、build（含测试）、必要的 source freshness、物理关系核验及 MetricFlow 探测。支持 table/view/ephemeral 和不写失败表的测试；seed、snapshot、自定义物化、SQL header、执行 hook 和数据库命令宏明确拒绝。工程读取遵循实际资源目录配置。模板约束不等于任意不可信代码沙箱。

取消先持久化意图。已确认取消返回 CANCELLED；外部写执行无法确认时返回 OUTCOME_UNKNOWN，保留保护引用且不自动重跑。

管理构建使用同一应用用例：

```powershell
uv run --frozen dbt-service-admin build --file build-request.json
```

## 查询和历史读取

| 路径 | 行为 |
| --- | --- |
| `GET /builds`、`GET /builds/{buildId}` | environmentId 必填，repository/branchName/buildStatus 可选；固定身份读取 |
| `GET /builds/{buildId}/execution-logs` | 按构建增量读取结构化 dbt 详细日志，支持 cursor、limit；[前端对接说明](docs/build-execution-logs-api.md) |
| `POST /builds/{buildId}/cancel` | 取消构建 |
| `GET /deployments/current`、`GET /deployments` | 自然键当前观察、意图历史 |
| `GET /builds/{buildId}/catalog` | 目录搜索、kind 筛选、游标分页 |
| `GET /builds/{buildId}/resources/{resourceId}` | 资源详情；另有 `/lineage`、`/source`、`/native-details` |
| `POST /query/available-dimensions?build_id={build_id}` | build_id 为独立 URL 参数，请求体指定 metricResourceIds，200 同步返回可用的 dimensions 列表，包含时间粒度和允许的过滤操作 |
| `POST /query?build_id={build_id}` | build_id 为独立 URL 参数，同步执行 QUERY、EXPLAIN、PREVIEW、DIMENSION_VALUES，直接返回本次结果，最多 8 MiB |

目录、可用维度、查询始终使用选定 buildId；不重新解析当前部署。新构建不使旧构建目录和查询失效。物理清理后仍保留目录，`queryAvailable=false`。QUERY/EXPLAIN 使用 metricResourceIds；分组/过滤/维度取值使用该构建及指标集合生成的 optionId。PREVIEW 使用 datasetResourceId。请求体 `limit` 表示本次返回行数上限，默认 1000、最大 10000；查询请求不接受 `offset` 或 `idempotencyKey`。每次请求重新调用引擎，返回 QueryResult，不提供历史结果或分页读取。数值精度和 null 保留，时间按固定构建时区归一化。

其他列表默认 50、最多 200，统一 `items/nextCursor`。错误统一为 `error`，包含 `code/message/retryable/phase/buildId`。完整协议以 `/openapi.json` 为准；健康及版本接口仍是 `/health/live`、`/health/ready`、`/versions`。

只有构建、部署及其取消流程使用异步状态。两个查询接口均通过必填的 URL 参数 `build_id` 指定构建，请求体不包含 buildId。可用维度接口请求体只接收 `metricResourceIds`，不需要幂等键或任务编号，响应列表字段为 `dimensions`。查询接口在原请求内完成并返回本次结果，相同请求也会重新执行。查询和可用维度不缓存，不写入任务、attempt 或结果表，响应不包含 `queryId`、执行状态和分页字段。原 builds 下的 query-options、available-dimensions、queries 路由及 GET results 路由已移除。失败直接返回结构化 HTTP 错误，不返回 QUEUED/RUNNING；可用维度和查询的状态轮询路由已移除。执行期间以数据库共享会话锁保护固定构建，清理同一构建时必须取得互斥锁；查询退出后释放锁，不产生查询记录。后台 worker 只执行构建和清理，不自动重试查询。同步调用方的 HTTP 超时应大于 `COMMAND_TIMEOUT_SECONDS`（默认 1800 秒），并为网络与结果传输留出余量。

## 初始化与运维

服务仅提供当前完整执行快照契约，业务接口不带版本号。

部署使用空的 PostgreSQL 数据库，执行以下命令创建完整表结构、约束和触发器：

```powershell
uv run --frozen dbt-service-admin init-db
```

空库初始化执行 storage/schema.sql。服务只支持该完整结构，不提供旧协议或旧持久化数据的升级与兼容入口。

持久化保留构建、部署、异步任务及执行尝试、构建日志和必要的产物快照。成功任务不再保存通用结果和诊断副本；目录只保存封存文件，元数据只保存独立的版本、来源和验证字段。完整构建每次独立执行，不保留旧发布基线和默认输出复用机制。执行产物仅保留恢复工程所需文件及 manifest、semantic_manifest、catalog、published_catalog；compiled/run、run_results、sources 和额外验证证据文件只在构建期间使用。

对结果未知的执行，人工核实目标仓库停止后才释放保护：

```powershell
uv run --frozen dbt-service-admin reconcile-attempt <attempt-uuid> --confirm-external-stopped
uv run --frozen dbt-service-admin gc --older-than-hours 24 --limit 100
```

gc 只回收无引用产物，没有历史自动 TTL。构建日志不自动裁剪。

## 验证

持久化使用 SQLAlchemy ORM：实体、短 Session 和具体 Store 的职责及事务规则见
[storage 说明](src/dbt_metricflow_service/storage/README.md)。schema.sql 定义新库的表结构、约束和触发器。

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

测试覆盖完整构建、部署、四种查询、固定 schema 覆盖与不安全模板失败，创建独立测试 schema；该证据不代表 StarRocks 已做真实集成验证。

## 外部执行快照与环境隔离

环境由指标平台等调用方管理。本服务不提供环境配置 CRUD 或环境注册表；每次 `POST /builds` 必须携带 repository、branchName、idempotencyKey 和完整 execution 快照。execution 包含 environmentId、configVersion、connection、credentialRef、catalog、schema、threads、businessTimezone。branchName 与环境无绑定，main 和 feature 分支使用相同规则。configVersion 是外部版本，worker 匹配仍使用 SERVICE_CONFIG_VERSION。

`GET /builds` 必须携带 environmentId；可选 repository、branchName、buildStatus、cursor、limit。详情、详细日志和取消位于 `/builds/{buildId}`、`/execution-logs`、`/cancel`。阶段、状态、错误码从构建详情读取，旧 `/logs` 接口已删除。`/deployments` 与 `/current` 以 repository/environmentId/branchName 定位部署。目录与查询使用固定 buildId，内部按该 build 的快照创建临时 profile。

部署配置示例（可在服务 YAML 中写映射，也可用同名环境变量写 JSON）：

```yaml
EXECUTION_INSTANCES:
  warehouse:
    type: starrocks
    hosts: [localhost, 127.0.0.1]
    port: 9030
EXECUTION_CREDENTIALS:
  warehouse-readwrite:
    userEnv: DBT_ENV_SECRET_WAREHOUSE_USER
    passwordEnv: DBT_ENV_SECRET_WAREHOUSE_PASSWORD
```

secret 值通过受保护的环境变量注入。profile 只写 dbt `env_var` 引用，执行后销毁且不封存。实例清单不含环境、catalog/schema；不得为同一物理实例登记多个不同 instanceId，否则无法证明跨别名的物理隔离。

StarRocks 的 profile.catalog 指定 catalog，profile.schema 指定 database；profile.database 留空，避免 source 继承后被误识别成外部 catalog。模型的 catalog/database 覆盖必须仍指向同一物理目标。PostgreSQL 的 catalog 映射为 dbname，DuckDB 的 catalog 必须等于清单中绝对文件路径的文件名（去后缀）。不存在或不可写的外部 catalog 会导致构建失败；不能把更换 catalog 字段当成已验证的适配器写入能力。

物理归属从不可变构建事实核验，已使用的目标不能交给另一环境。同一环境并发构建串行写入同一目标；租约过期或外部停止情况未知时保留写入保护，直到现有 reconcile 流程确认停止。

真实隔离验收需设置 `ENVIRONMENT_ISOLATION_E2E=1`、`SERVICE_TEST_DATABASE_URL`（专用服务测试库）、`ENVIRONMENT_TEST_REPOSITORY`、`DBT_ENV_SECRET_ISOLATION_USER`、`DBT_ENV_SECRET_ISOLATION_PASSWORD`；可选 `ENVIRONMENT_TEST_HOST/PORT/BRANCH`。使用已有真实仓库和只读原始数据，测试创建唯一 `env_e2e_*` schema，保留构建事实便于审计：

```powershell
uv run --frozen pytest -q -s tests/integration/test_execution_environment_isolation.py
```

测试核对两个 schema 的实际表内容、相同源码 SHA、固定 build 查询、环境 A 重建后环境 B 不变，以及旧配置快照不变。清理这些专用 schema 和测试数据库应在确认不再需要验收记录后单独执行。
