# dbt MetricFlow Service

接收受控项目的固定 Git commit SHA，执行定义验证、全量构建发布和指标查询。服务只使用 PostgreSQL 持久化；FastAPI 与 worker 保持同一进程，dbt/MetricFlow 在任务子进程中执行。

Git 分支的创建、提交、合并和触发时机由调用方负责。服务不读取“最新 main”决定发布输入，不管理远端 ref，不接收 webhook，也不自动扫描仓库。

## 启动与配置

需要 Python 3.11–3.14（推荐 3.12）、uv、Git、PostgreSQL，以及项目使用的数据仓库。源码依赖通过 `vendor/dbt` 和 `vendor/metricflow` 固定，安装版本以 `uv.lock` 为准，不修改上游源码。

```powershell
uv sync --frozen --all-groups
$env:SERVICE_DATABASE_URL = "postgresql://user:password@localhost/dbt_service"
$env:SERVICE_TOKEN = "由部署环境提供的服务凭据"
uv run --frozen dbt-service-admin migrate
uv run --frozen dbt-service-admin register-bindings bindings.json
uv run --frozen dbt-metricflow-service
```

先迁移、登记绑定，再启动服务。普通服务启动只检查 schema，不自动迁移。未配置 `SERVICE_DATABASE_URL` 时启动失败，不回退本地模式。API 启动同进程 worker，关闭时等待子进程清理。

服务与管理命令默认读取 `config/service.yaml`，可用 `SERVICE_CONFIG_FILE` 替换。优先级为环境变量 > 配置文件 > 默认值。文件中的相对路径相对配置文件，环境变量中的相对路径相对工作目录。未知配置项会被拒绝。

| 配置 | 用途 |
| --- | --- |
| `SERVICE_DATABASE_URL` | 必需，PostgreSQL 元数据库，支持 URL 和 libpq DSN |
| `SERVICE_TOKEN` | 发布和验证 POST 接口的 Bearer 凭据；未设置则关闭这些写入口 |
| `DBT_PROFILES` / `DBT_PROFILES_DIR` | 在配置文件内维护 dbt profiles，或指定外部目录；二选一 |
| `SERVICE_TEMP_ROOT` | 可丢弃的任务工作目录 |
| `SERVICE_CONFIG_VERSION` | 连接及编译配置版本，变化时同步更新项目绑定 |
| `SERVICE_TOOLCHAIN_VERSION` | 工具链标识；未设置时由安装包和服务源码计算 |
| `WORKER_CONCURRENCY` | 每实例执行槽位，默认 2 |
| `JOB_LEASE_SECONDS` / `JOB_HEARTBEAT_SECONDS` | 默认 90 / 15 秒，租约至少覆盖三个心跳间隔 |
| `COMMAND_TIMEOUT_SECONDS` | 命令超时，默认 1800 秒 |
| `MAX_OUTPUT_BYTES` / `MAX_RESULT_BYTES` | 默认 1 MiB 输出尾部 / 16 MiB 查询结果 |
| `MAX_ARTIFACT_FILE_BYTES` / `MAX_ARTIFACT_BYTES` | 默认 64 MiB 单文件 / 256 MiB 产物集 |
| `SYNCHRONOUS_WAIT_SECONDS` | 同步选项及管理清理的等待预算，默认 30 秒 |
| `SERVICE_HOST` / `SERVICE_PORT` | HTTP 监听地址及端口 |

`DBT_PROFILES` 生成的 profiles 保留 dbt 环境变量模板，密码通过部署 Secret 提供。不要把仓库、profile 或数据库凭据交给模型。

项目绑定示例：

```json
[
  {
    "projectId": "sales",
    "remote": "https://git.example.com/data/sales.git",
    "projectSubdir": ".",
    "profileBindingId": "postgres",
    "configVersion": "1"
  }
]
```

`remote` 不允许携带 URL 密码。Git 凭据由部署环境提供。可选 `schemaName` 为受控 schema；每次构建仍使用独立 run 表名前缀。未指定时生成独立 `run_<uuid>` schema。可选 `queryRetrySafe` 声明查询的只读重试能力。

## 固定提交验证与发布

下面路径均以 `/v2/projects/{projectId}` 为前缀。验证和发布请求使用同一输入形状：

```json
{
  "commitSha": "完整的40位或64位小写Git提交SHA",
  "idempotencyKey": "调用方分配的一次请求标识"
}
```

`POST /validations` 受理验证，`GET /validations/{validationId}` 轮询结果。验证检查 YAML、受控模板、dbt parse/compile、只读 SQL 和语义定义，不执行 build，不创建发布。`SUCCEEDED + valid=false` 表示定义未通过；`FAILED` 表示执行或基础设施失败。

`POST /releases` 受理全量构建发布，返回 `releaseId`、`runId` 等发布描述；通过 `GET /releases/{releaseId}` 轮询。两种 POST 都需要 `Authorization: Bearer <SERVICE_TOKEN>`。remote、profile、schema 和工具链来自服务绑定，请求不能覆盖。

完整 SHA 可以来自任意由受控 remote 提供的提交，不要求属于 main。Git 服务必须允许读取该 SHA。发布受理在数据库事务之外读取并封存固定源码，再原子创建候选与任务；worker 使用封存输入。验证任务在执行时读取其固定 SHA，提交必须仍可从 remote 获取。

同键同 SHA 恢复原任务，即使当前配置或活动发布已变化；同键不同 SHA 返回 409。新执行或配置变化后重新验证，使用新的幂等键。

管理命令也必须显式传 SHA：

```powershell
uv run --frozen dbt-service-admin publish --project-id sales --commit-sha <完整SHA> --idempotency-key release-001
```

每个新发布均为 `FULL_BUILD`，不复用旧版物理模型。仍执行完整模板及 SQL 检查、必要的 source freshness、全部模型和测试、物理关系核验与 MetricFlow 查询探测。所有模型使用本次 run 的独立命名，失败不覆盖上一活动版本。

支持受控 SQL 项目的 table、view、ephemeral 和不写失败表的测试。发布不支持 seed、snapshot、Python 模型、自定义物化、执行 hook、SQL header 或数据库命令宏。编译 SQL 必须是单条只读取数语句。这是受控项目执行约束，不是任意不可信 dbt 项目的沙箱。

## 目录与查询

| 接口 | 用途 |
| --- | --- |
| `GET /v2/projects` | 项目与发布概览 |
| `GET /publication` | 当前活动发布、配置/工具链上下文、`fixed-commit-v1` 能力 |
| `GET /releases`、`GET /releases/{releaseId}` | 发布历史与验证摘要 |
| `GET /releases/{releaseId}/catalog` | 活动版本目录搜索与分页 |
| `GET /releases/{releaseId}/resources/{resourceId}` | 资源详情，另有 `/lineage`、`/source`、`/native-details` |
| `POST /query-options` | 同步查询可用维度选项 |
| `POST /query-option-jobs`、`GET /query-option-jobs/{id}` | 异步查询选项 |
| `POST /queries` | 提交 QUERY、EXPLAIN、PREVIEW 或 DIMENSION_VALUES 查询 |
| `GET /queries/{id}/status` | 不加载结果正文的状态轮询 |
| `GET /queries/{id}/results?offset=0&limit=100` | 结果分页，最多 200 行、8 MiB |
| `GET /queries/{id}` | 完整有界查询结果 |

查询使用目录返回的 `resourceId` 和服务生成的 `optionId`，不接受任意 SQL。时间参数按发布的业务时区归一化。新目录/查询只接受活动版本，已替代版本返回 410；已受理查询及同键重试始终绑定原 run。

元数据保存在 PostgreSQL，源码和输出按摘要封存。产物封存、发布指针和任务成功在同一事务提交；失效租约不能发布。失联外部写执行标记 `EXECUTION_OUTCOME_UNKNOWN`，不会盲目重复写入。

健康与版本接口为 `/health/live`、`/health/ready`、`/v1/versions`。业务 OpenAPI 以实际 `/docs`、`/openapi.json` 为准。服务仍面向受控网络部署。

## 从旧版升级

本次是明确的接口收缩，调用方必须同步升级：

- 下线 `/v1/dbt/jobs`、`/v1/metricflow/jobs`、`/v1/jobs`、`/v1/project-runs`、`/v1/query-jobs`。
- 下线所有分支业务路由、`/internal/git-branch-events` 和 `compatibility/*`。
- 验证改为固定提交，不再接受 changes、baseCommitSha、workspaceId 或 draftRevision。
- 移除 SQLite、本地项目执行、临时 YAML resources 和本地旧任务导入命令。
- 移除 `PROJECTS_ROOT`、`JOB_ARTIFACTS_ROOT`、`PLATFORM_BINDINGS_FILE`、`PLATFORM_DB_PATH` 和所有 `BRANCH_*` 配置；写入口凭据改为 `SERVICE_TOKEN`。

升级前停止受理并排空旧任务，备份数据库。历史迁移不改写，已有发布、查询和封存字节不删除。数据库 branch 行及旧任务枚举暂保留为内部历史兼容结构，不再驱动 Git 分支管理。旧发布的复用绑定仍受引用/GC 保护，历史记录可通过项目级 ID 读取。

查询仍固定原 run 的配置及工具链；新 worker 只领取匹配版本的任务。已有历史 run 需要继续查询时，必须保留与其匹配的执行环境，不能把不兼容代码伪装成相同版本。元数据库备份不包含目标仓库表。

## 运维与验证

`dbt-service-admin` 保留 `migrate`、`register-bindings`、`publish`、`import-publication`、`reconcile-attempt` 和 `gc`。已发布或被引用的 run 不会被 GC 删除。对执行结果未知的任务，先人工确认目标仓库执行已结束，再执行：

```powershell
uv run --frozen dbt-service-admin reconcile-attempt <attempt-uuid> --confirm-external-stopped
uv run --frozen dbt-service-admin gc --older-than-hours 24 --limit 100
```

回归必须使用独立测试库：

```powershell
$env:SERVICE_TEST_DATABASE_URL = "postgresql://user:password@localhost/dbt_service_test"
uv run --frozen pytest -q
uv run --frozen ruff check src tests
```

真实 PostgreSQL 构建/验证/查询验收另设置 `PLATFORM_TEST_POSTGRES=1` 及 `PLATFORM_TEST_PGHOST`、`PLATFORM_TEST_PGPORT`、`PLATFORM_TEST_PGUSER`、`PLATFORM_TEST_PGPASSWORD`、`PLATFORM_TEST_PGDATABASE`，运行 `tests/integration/test_publication_proxy_flow.py`。测试创建独立 run schema，应只使用可丢弃测试仓库。该验收不代表 StarRocks 已执行真实回归。

## 包职责

- `api`：PostgreSQL 应用装配、鉴权和请求限制。
- `publications`：发布受理、全量构建、目录与查询；三种应用服务独立。
- `validation`：固定提交的只读定义验证。
- `runtime`：同进程 worker、执行器、完成事务协调和临时工作区。
- `execution`：内部子进程 runner、输出上限和凭据遮盖。
- `platform` / `adapters`：固定 Git 输入、物理命名、原生目录与 MetricFlow/仓库适配。
- `storage`：SQL、产物、任务/发布/查询事务与历史迁移。
