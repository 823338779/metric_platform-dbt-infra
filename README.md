# dbt MetricFlow Service

这是一个可在本地运行的 Python HTTP 服务。它通过只读 Git submodule 携带固定版本的 dbt 与 MetricFlow 源码，使用 `uv` 安装对应 Python 包，并通过官方公开 CLI 包装能力；服务不修改上游源码，也不跨源码目录导入内部实现。服务提供结构化命令白名单、异步任务状态、项目级写互斥、超时控制、输出截断和 dbt secret 环境变量遮盖。

## 版本基线

- Python `3.11` 至 `3.14`；本地推荐 Python `3.12`
- dbt 源码：`vendor/dbt`，使用 dbt fork 中指向官方 `v1.12.5` 提交的 `internal/dbt-core-1.12.5` 分支
- MetricFlow 与 dbt-metricflow 源码：`vendor/metricflow`，使用 MetricFlow fork 中的 `internal/metricflow-0.213.0-dbt-0.15.0` 分支
- `dbt-core==1.12.5`
- `dbt-starrocks==1.12.2`
- `dbt-duckdb==1.11.0`（提供一个可直接运行的 MetricFlow 支持 adapter）
- `dbt-postgres==1.11.0`（平台固定版本任务的 PostgreSQL 验收 adapter）
- `dbt-metricflow==0.15.0`
- `metricflow==0.213.0`
- `uv`（本地依赖管理工具）

`dbt-core`、`metricflow` 与 `dbt-metricflow` 由 `uv` 从上述本地源码构建，`dbt-starrocks` 与 `dbt-duckdb` 从包索引安装。依赖由 `uv.lock` 固定。`GET /v1/versions` 返回当前进程实际加载的服务与上游包版本。

MetricFlow 上游分别发布 core `0.213.0` 和 CLI `0.15.0`。fork 的组合提交 `05551f73` 以这两个稳定发布为基线，仅将 core 的版本元数据恢复为 `0.213.0`，使同一源码树可构建两个包；主仓固定该提交，不随 fork 分支自动移动。

## 已发布目录与统一发布

PostgreSQL 模式下，dbt-service 是发布权威。`runtime_release` 保存候选和发布历史，`runtime_project.active_published_release_id` 是唯一活动指针。生成 `target/published_catalog.json`、原生产物及完整验证证明之后，封存产物、记录发布和切换指针在同一事务提交。指标平台直接代理 `/v2/projects`，不存在发布后的平台导入步骤。平台断网会影响读取可用性，但不会改变已提交的发布状态。

升级时先备份服务数据库，再由部署管理执行迁移；普通服务启动不自动迁移。项目绑定沿用 `register-bindings`。启动服务 worker 后，用管理入口提交发布：

```powershell
uv run --frozen dbt-service-admin migrate
uv run --frozen dbt-service-admin register-bindings bindings.json
uv run --frozen dbt-service-admin publish --project-id sales --idempotency-key sales-release-001
```

幂等键代表一次发布请求，重试沿用该键；新的发布使用新键。命令返回候选身份，最终状态通过 `GET /v2/projects/sales/releases` 查看。候选固定 Git `main` 的 SHA、项目摘要、配置版本和工具链；配置或 profile 中影响编译/目标关系的参数变化时，必须同步更新绑定的 `configVersion`。

发布自动选择 `FULL_BUILD`、`SEMANTIC_ONLY` 或 `SELECTIVE_BUILD`。所有模式都发布完整目录；仅语义变更复用已验证物理对象，SQL 变更重建受影响下游，宏、source、配置变化或不能证明兼容时全量构建。模板使用 `env_var` 时保守全构建，不持久化环境变量值。首版不执行行级 incremental，不自动清理发布对象。

构建只支持受控 SQL 项目：table、view、ephemeral 和不写失败表的测试；不支持 seed、snapshot、Python 模型、自定义物化、执行钩子、SQL header 或宏中的数据库命令。项目/依赖宏仅支持表达式模板，宏名称限定为字母数字且不能覆盖内置函数；动态调用、赋值、导入和不受支持的 Jinja 扩展会在 dbt 解析前被拒绝。编译 SQL 必须是单条只读取数语句，不能包含写入 CTE 或多语句。数据库函数仍属于受信任的部署能力，不能给项目使用具有写入副作用的 UDF。这是受控项目执行约束，不是运行任意不可信 dbt 项目的沙箱。

配置了 source freshness 门槛的源每次发布都必须通过 freshness 检查。所有发布模型/测试均需完整执行证明，MetricFlow 查询探测和最终物理绑定验证通过才能发布。复用绑定直接引用创建该物理对象的 run；已发布或被引用的 run 受到 cleanup/GC 保护，首版保留全部发布历史，需为持续增长的存储预留容量。

v2 目录和新查询只接受活动版本；历史版本返回 410，发布记录与已受理查询仍可读取。查询请求使用资源 ID、服务返回的选项 ID 和幂等键。服务在项目锁下固定查询 run，切换版本后同键同输入仍返回原查询身份，新键不能查询已替代版本。`/v2/projects/{projectId}/compatibility/*` 仅供平台 v1 兼容转接，名称转换仍在服务内完成。

迁移旧身份只允许映射到已经完成本服务完整发布验证的同一 run：

```powershell
uv run --frozen dbt-service-admin import-publication --file publication-identities.json --dry-run
uv run --frozen dbt-service-admin import-publication --file publication-identities.json
```

文件包含 `projectId`、`legacyReleaseId`、`runId` 和 `queries: [{legacyQueryId, queryId}]`。dry-run 不写库，重复实际导入幂等，冲突和跨项目映射拒绝。未具备服务发布证明的旧 READY run 不会被伪装为已发布；此时先完成服务新发布，旧平台发布/查询记录保留只读历史，不把旧 ID 映射到内容不同的新版本。

真实发布验收同时需要独立 `SERVICE_TEST_DATABASE_URL`、`PLATFORM_TEST_POSTGRES=1` 和本文的 `PLATFORM_TEST_PG*` 参数：

```powershell
uv run --frozen pytest -q tests/integration/test_publication_proxy_flow.py
uv run --frozen pytest -q
uv run --frozen ruff check src tests
```

该集成场景验证 PostgreSQL adapter；不能将它视为 StarRocks 物理隔离验收。生产切换前应在目标 adapter、权限和受控项目模板上执行同等验收。

## 目录和配置

服务和 `dbt-service-admin` 默认读取 `config/service.yaml`，其中集中维护全部服务参数，包括监听地址、端口、存储连接、目录、并发、租约和产物大小限制。配置项名称与环境变量一致，**环境变量 > 配置文件 > 代码默认值**；可通过 `SERVICE_CONFIG_FILE` 指定其他配置文件。指定文件不存在、YAML 格式错误或包含未知配置项时，启动会报错。

配置文件中的相对路径以配置文件所在目录为基准，环境变量中的相对路径仍以启动工作目录为基准。数据仓库连接统一在 `service.yaml` 的 `DBT_PROFILES` 中维护，其结构与标准 dbt profile 相同。服务和管理命令加载配置时，会在 `SERVICE_TEMP_ROOT/profiles/<内容摘要>/profiles.yml` 自动生成 dbt CLI 所需的文件；保留 Jinja 模板，由 dbt 在执行时读取环境变量。该文件是可重建的运行产物，无需手动维护。配置变化会生成新快照，不覆盖正在执行任务使用的旧文件。兼容模式目录沿用 `../../tmp/metric-debug/`，独立部署时应按实际目录调整。

随仓库提供的 profile 名称为 `ecommerce_metrics`，target 为 `starrocks`，与当前项目和受控绑定一致。默认连接根工作区 compose 的本机 StarRocks；可通过 `DBT_STARROCKS_HOST`、`DBT_STARROCKS_PORT`、`DBT_STARROCKS_USER`、`DBT_ENV_SECRET_STARROCKS_PASSWORD` 覆盖连接参数。平台任务通过 `DBT_PLATFORM_SCHEMA` 指定 schema，手工检查默认使用 `dbt_ecom`。这些参数是 dbt 目标数据仓库连接，与 `SERVICE_DATABASE_URL` 的服务存储连接用途不同。修改配置后需重启服务，IDE 和中控均读取同一份 `service.yaml`。

已有部署可继续只配置 `DBT_PROFILES_DIR`，使用自行维护的外部 `profiles.yml`。文件内的 `DBT_PROFILES` 和 `DBT_PROFILES_DIR` 不能同时非空；环境变量 `DBT_PROFILES_DIR` 可以覆盖内联模式，且服务不会写入该外部目录。

当前 `config/service.yaml` 已直接配置本机 PostgreSQL 的 `dbt_service` 业务库，服务任务、项目绑定、构建产物和查询结果均保存到该库。数据库和业务表已初始化；后续普通启动仅检查结构，不自动建库或执行迁移。`dbt_service_test` 仅供独立测试。将 `SERVICE_DATABASE_URL` 显式改为 `null` 才会使用兼容开发模式的 SQLite 和内存任务；两种模式不共享任务历史，不应同时接收同一业务项目的请求。

- `PROJECTS_ROOT`：每个一级子目录是一个 dbt 项目；dbt 会在项目内写入 `target/` 和 `logs/`。
- `DBT_PROFILES`：内联的数据仓库 profile；使用外部文件的部署可改用 `DBT_PROFILES_DIR`。
- `JOB_ARTIFACTS_ROOT`：请求级 resources 的派生产物，由服务用户管理和清理；不要由多个服务实例共享。

项目通过安全的单段标识访问，例如 `projects/sales` 对应请求字段 `"project": "sales"`。服务拒绝路径、绝对路径和逃逸项目根目录的符号链接。

StarRocks profile 可以只引用环境变量，避免把凭据写进仓库：

```yaml
sales:
  target: prod
  outputs:
    prod:
      type: starrocks
      host: "{{ env_var('DBT_STARROCKS_HOST') }}"
      port: "{{ env_var('DBT_STARROCKS_PORT') | int }}"
      schema: "{{ env_var('DBT_STARROCKS_SCHEMA') }}"
      username: "{{ env_var('DBT_STARROCKS_USER') }}"
      password: "{{ env_var('DBT_ENV_SECRET_STARROCKS_PASSWORD') }}"
```

以 `DBT_ENV_SECRET_` 开头的非空环境变量值如果被子进程写入 stdout 或 stderr，服务会在保存任务结果前替换为 `***`。不要把真实凭据放在请求参数、项目文件或日志中。

## 获取完整源码

新检出必须递归初始化 submodule：

```bash
git clone --recurse-submodules https://github.com/823338779/metric_platform-dbt-infra.git
cd metric_platform-dbt-infra
```

已有检出执行：

```bash
git submodule update --init --recursive
git submodule status
```

`git submodule status` 行首为 `-` 表示源码尚未初始化，行首为 `+` 表示工作树 commit 与服务锁定的 gitlink 不一致；正常状态以一个空格开头。Windows 检出前应启用开发者模式和 Git 的符号链接支持，否则 MetricFlow 测试用 YAML 链接会变成普通文本文件。

## 启动

在仓库根目录使用已安装的 Git 和 `uv`。Windows PowerShell：

```powershell
git submodule update --init --recursive
uv sync --frozen --all-groups --python 3.12
# 编辑 config/service.yaml 中的 DBT_PROFILES 和目录，并准备 dbt 项目
uv run --frozen dbt-metricflow-service
```

macOS/Linux：

```bash
git submodule update --init --recursive
uv sync --frozen --all-groups --python 3.12
# 编辑 config/service.yaml 中的 DBT_PROFILES 和目录，并准备 dbt 项目
uv run --frozen dbt-metricflow-service
```

服务默认监听 `http://localhost:8000`，可在配置文件中修改 `SERVICE_HOST` 和 `SERVICE_PORT`。根工作区与子仓库的 IDEA 启动配置均已指定同一 `config/service.yaml`。运行 `curl http://localhost:8000/health/ready` 可检查 CLI 与目录是否就绪；首次调用前应配置实际的数据仓库连接和 dbt 项目。

指定部署配置文件（PowerShell）：

```powershell
$env:SERVICE_CONFIG_FILE = "E:/deployment/dbt-service/service.yaml"
uv run --frozen dbt-service-admin migrate
uv run --frozen dbt-metricflow-service
```

可选运行参数：

- `COMMAND_TIMEOUT_SECONDS`：单个 CLI 子进程的最长运行时间，默认 `1800` 秒。
- `MAX_OUTPUT_BYTES`：每个 stdout/stderr 流保留的尾部字节数，默认 `1048576`。
- `JOB_ARTIFACTS_ROOT`：请求级任务派生产物根目录，默认由 `config/service.yaml` 指定。

`GET /health/live` 只检查 HTTP 进程；`GET /health/ready` 检查 `dbt`、`mf`、项目/profile 目录以及任务产物目录。

## 平台固定版本任务

完整且明确为空的定义允许发布为空目录：模型、source、语义模型、指标、物理目录和构建记录均为空时，跳过 MetricFlow 查询探针，READY 返回的 `queryCapability` 与 `representativeQueryPassed` 均为 false，其余产物版本、摘要和配置校验照常执行。产物缺失或非空项目查询失败仍拒绝发布。指标平台保留旧版本物理表，不再自动调用清理接口；需要清理时由运维显式使用 dbt 工具处理。

设置 `PLATFORM_BINDINGS_FILE` 为服务拥有的 JSON 文件路径，并设置 `PLATFORM_DB_PATH` 为服务独占的 SQLite 索引路径。绑定文件是数组，例如：

```json
[
  {
    "projectId": "ecommerce_metrics",
    "remote": "https://example.invalid/data-model.git",
    "projectSubdir": ".",
    "profileBindingId": "starrocks",
    "schemaName": "dbt_ecom"
  }
]
```

`profileBindingId` 对应 `profiles.yml` 中的 target。该 target 的 `schema` 必须引用 `{{ env_var('DBT_PLATFORM_SCHEMA') }}`。设置受控绑定的 `schemaName` 后，服务在固定 schema 内按 run ID 为模型表加 `rv_<runId>_` 前缀；不设置时沿用旧的独立 `run_...` schema。`dbt_ecom` 需预建或由受控运行账号创建，运行账号需要建表、删表权限；`ecommerce_raw` 只需读取权限。连接凭据只放在服务环境变量中。平台请求不能提供 remote、profile 内容、schema 或 CLI 参数。
固定 schema 构建会拒绝模型的 `pre-hook`、`post-hook` 和项目级 `on-run-start`、`on-run-end`，避免钩子误写旧活动表。受控 Git 项目中的自定义宏和 materialization 仍按可信项目代码执行；生产部署须限制谁能修改该项目的 `main`。

`POST /v1/project-runs` 接收 `projectId`、`commitSha`、`projectDigest`、`profileBindingId`、`configVersion` 和 `idempotencyKey`；按 `GET /v1/project-runs/{runId}` 或 `/v1/project-runs/by-key/{key}` 轮询。任务只从受控 main 的固定 SHA 读取允许的 dbt 输入，核对与平台一致的摘要，对全部定义执行 `dbt build`，验证产物和真实 MetricFlow 查询后才返回 `READY`。`GET /v1/project-runs/{runId}/catalog` 返回版本化原生目录及依赖。

`GET /v1/project-runs/{runId}/query-options?metrics=revenue` 返回该指标组合可用的维度与 `metric_time` 粒度 token。`POST /v1/query-jobs` 接收固定 `runId`、幂等键、`QUERY`、`EXPLAIN`、`PREVIEW` 或 `DIMENSION_VALUES` 模式，以及指标、维度、筛选、时间范围和行数上限；按 `/v1/query-jobs/{queryId}` 或 `/v1/query-jobs/by-key/{key}` 轮询。查询结果有列类型、行、截断标记；Decimal 值作为字符串返回以保留精度。`POST /v1/project-runs/{runId}:cleanup` 仅在无活动查询时回收该 run 的对象和目录；固定 schema 模式只删除本 run 前缀对象，不删除 schema。

独立 PostgreSQL 测试库配置 `PLATFORM_TEST_POSTGRES=1`、`PLATFORM_TEST_PGHOST`、`PLATFORM_TEST_PGPORT`、`PLATFORM_TEST_PGUSER`、`PLATFORM_TEST_PGPASSWORD`、`PLATFORM_TEST_PGDATABASE` 后运行：

```bash
uv run --frozen pytest -q tests/integration/test_postgres_platform_flow.py
```

这组测试覆盖两次提交的全量构建、独立 schema、目录、`metric_time__month`、维度值与清理。平台固定版本路径也可使用 StarRocks：服务复用 MetricFlow 的 DuckDB SQL 渲染器，并通过 `dbt-starrocks` 执行生成的聚合 SQL；只有真实查询探针与构建产物校验通过才标为 `READY`。当前端到端测试覆盖简单聚合与月份分组，其他 SQL 表达式需结合实际指标验证。

## dbt 任务

允许的 dbt 命令为 `parse`、`compile`、`seed`、`run`、`test`、`build` 和 `debug`。服务只接受结构化选项，不接受任意 shell 命令。

提交一次 StarRocks build：

```bash
curl -sS -X POST http://127.0.0.1:8000/v1/dbt/jobs \
  -H 'Content-Type: application/json' \
  -d '{"project":"sales","command":"build","target":"prod","select":["tag:daily"]}'
```

服务返回 HTTP 202 和任务 ID。使用统一端点轮询：

```bash
curl -sS http://127.0.0.1:8000/v1/jobs/00000000-0000-0000-0000-000000000000
```

状态依次为 `queued`、`running`，最终进入 `succeeded`、`failed` 或 `timed_out`。任务保存在当前进程内存中，进程重启后旧 ID 返回 `job_not_found`。同一项目同时只能运行一个 dbt 写任务；冲突返回 HTTP 409 `project_busy`。

## 请求级 YAML resources

dbt 和 MetricFlow 的任务请求都可以携带可选的 `resources`。key 是单个 `.yml` 或 `.yaml` 文件名，value 是本次请求使用的 YAML 原文：

```json
{
  "project": "sales",
  "command": "parse",
  "resources": {
    "orders.yml": "version: 2\nmodels:\n  - name: orders\n"
  }
}
```

存在同名项目 YAML 时，本次解析优先读取内存原文；不存在同名文件时，资源会作为第一个 model-path 下的虚拟 schema 文件参与解析。外部原文不会创建、覆盖或修改项目 YAML。

缺失、空字符串和纯空白条目沿用项目默认定义。非空白内容一律交给 dbt 解析，因此语法、引用或校验错误会使任务失败，不会回退到磁盘版本；注释和 `{}` 也属于非空白内容。资源名不接受路径，多处存在同名 YAML 时任务失败。`debug` 不消费 schema，因此只接受缺失或全部为空白的 resources。

资源原文通过子进程 stdin 传输，不进入 argv、环境变量、任务响应或磁盘请求文件。`partial_parse.msgpack` 在资源模式下禁用。`manifest.json`、`semantic_manifest.json`、编译 SQL 等派生产物允许写入任务独立目录，并在任务结束后清理；无法确认已结束的残留目录会保留供维护处理。

MetricFlow 资源请求会先用本次 YAML 执行 dbt parse，再直接读取本次任务的 semantic manifest，不要求项目已有 `target/manifest.json`。例如：

```json
{
  "project": "sales",
  "command": "list_metrics",
  "resources": {
    "orders.yml": "version: 2\nmodels:\n  - name: orders\n    semantic_model:\n      enabled: true\n"
  }
}
```

未提供有效 resources 的请求保持原有 CLI 行为及 MetricFlow manifest 前置检查。MetricFlow 仍不支持 StarRocks；资源模式会在异步任务结果中返回该限制。

## MetricFlow 任务

MetricFlow 使用同一个异步任务和轮询接口。四类请求示例：

```bash
# 发现指标
curl -sS -X POST http://127.0.0.1:8000/v1/metricflow/jobs \
  -H 'Content-Type: application/json' \
  -d '{"project":"sales","command":"list_metrics"}'

# 发现指标可用维度
curl -sS -X POST http://127.0.0.1:8000/v1/metricflow/jobs \
  -H 'Content-Type: application/json' \
  -d '{"project":"sales","command":"list_dimensions","metrics":["revenue"]}'

# 生成 SQL 说明
curl -sS -X POST http://127.0.0.1:8000/v1/metricflow/jobs \
  -H 'Content-Type: application/json' \
  -d '{"project":"sales","command":"explain","metrics":["revenue"],"group_by":["metric_time__month"]}'

# 执行指标查询
curl -sS -X POST http://127.0.0.1:8000/v1/metricflow/jobs \
  -H 'Content-Type: application/json' \
  -d '{"project":"sales","command":"query","metrics":["revenue"],"limit":100}'
```

未携带有效 resources 时，提交 MetricFlow 任务前必须先运行 dbt parse，让项目生成 `target/manifest.json`。通用 `/v1/metricflow/jobs` 仍使用上游 CLI，MetricFlow `0.213.0` 不支持其 StarRocks adapter：普通请求同步返回 HTTP 422 `metricflow_adapter_not_supported`，资源请求在异步任务中失败并返回同一诊断代码。平台固定版本的 `/v1/project-runs`、`/v1/query-jobs` 通过已有渲染器和 `dbt-starrocks` 执行已验证的聚合与时间分组查询。dbt 对 StarRocks 的 parse、compile、seed、run、test、build 和 debug 不受此限制。

## PostgreSQL 无状态部署

该模式覆盖通用 dbt/MetricFlow 任务和平台固定版本任务。PostgreSQL 保存六张 `runtime_*` 业务表及迁移版本；产物按原始字节保存到 `bytea`，目录、请求和结果使用 JSONB。实例仅持有可丢弃的临时目录。`vendor/` 无需修改。

### 初始化与启动

首次部署时，先创建 PostgreSQL 数据库和服务账号，在 `config/service.yaml` 中直接填写 `SERVICE_DATABASE_URL`（libpq DSN 或 PostgreSQL URL）。服务与管理命令必须使用同一配置文件；环境变量仍可覆盖文件配置。当前本地业务库已完成初始化，只需正常启动。安装和首次初始化命令：

```powershell
uv sync --frozen --all-groups
# 先在 config/service.yaml 中填写数据库连接和 profiles 目录
uv run --frozen dbt-service-admin migrate
uv run --frozen dbt-service-admin register-bindings bindings.json
uv run --frozen dbt-service-admin import-project sales projects/sales
uv run --frozen dbt-metricflow-service
```

`register-bindings` 使用前文的平台绑定数组，可增加 `configVersion`（默认 `1`）和 `queryRetrySafe`（默认关闭）。仅在目标库账号权限保证只读时启用 `queryRetrySafe`。绑定信息保存到 PostgreSQL，HTTP 请求不能覆盖 remote 或 profile。通用 CLI 项目通过 `import-project` 导入；项目包含有效 `target/manifest.json` 时同时导入现有输出，之后 MetricFlow 请求无需本地项目挂载。输入文件会排除 profile、Git 元数据、日志及 partial parse 缓存。

启动多个副本时，使用同一服务数据库、同一版本镜像和相同 profile/Secret 绑定，每个副本配置**不同的临时目录**。已发布版本查询不访问 Git，不要求共享磁盘。`dbt deps` 在构建阶段解析的依赖随执行产物一起保存。

Windows 建议使用较短的临时根目录（如 `E:\dbt-tmp\node1`），避免项目包和编译产物的深层路径超过系统路径长度限制。运行中的 Windows 服务会锁住入口 exe；更新依赖时先停止该实例，或采用新目录部署后切换。仅运行测试时可使用 `uv run --no-sync pytest` 复用已安装依赖。

当前本地 `SERVICE_TEMP_ROOT` 使用工作区 `tmp/dbt`，构建配置版本为 `local-debug-v8`，与指标平台及已登记绑定一致。切换 PostgreSQL 时采用空业务库重新发布项目，旧 SQLite 保留在 `tmp/metric-debug/postgres-switch-backup/`，旧任务历史未导入新库。

| 配置 | 默认值与用途 |
| --- | --- |
| `SERVICE_DATABASE_URL` | 当前配置指向 `dbt_service`；全部公开任务走 PostgreSQL |
| `SERVICE_TEMP_ROOT` | 当前为工作区 `tmp/dbt`；实例独占的临时目录 |
| `SERVICE_CONFIG_VERSION` | 当前为 `local-debug-v8`；该实例支持的连接配置版本 |
| `SERVICE_TOOLCHAIN_VERSION` | 当前与指标平台固定版本一致；设为 `null` 时由服务代码和安装包版本计算 |
| `WORKER_CONCURRENCY` | `2`；每实例执行槽位 |
| `JOB_LEASE_SECONDS` / `JOB_HEARTBEAT_SECONDS` | `90` / `15` 秒；租约至少覆盖三个心跳间隔 |
| `MAX_ARTIFACT_FILE_BYTES` / `MAX_ARTIFACT_BYTES` | 单文件 `64 MiB` / 单集合 `256 MiB` |
| `MAX_RESULT_BYTES` | 单结果 `16 MiB` |
| `MAX_OUTPUT_BYTES` | stdout/stderr 各保留 `1 MiB` 尾部 |
| `SYNCHRONOUS_WAIT_SECONDS` | `30`；选项和清理接口等待任务完成的预算 |

数据库迁移只由管理命令执行，服务启动仅检查 schema 版本。健康检查验证数据库、CLI、profile 文件和临时目录；数据库不可用时受理返回 503，不返回未持久化的成功。数据库需要常规备份、容量/WAL 告警和独立的恢复演练；服务数据库备份不包含目标仓库的数据表。

### 故障恢复与清理

- 持久任务通过 `FOR UPDATE SKIP LOCKED` 领取，状态更新和产物发布均检查当前 attempt 与未过期的租约。构建发布、文件封存和结果成功在同一事务完成。
- 未开始外部执行的持久任务可恢复；确认只读的查询失联后最多尝试三次。已经开始 dbt 外部执行的失联任务标为 `EXECUTION_OUTCOME_UNKNOWN`，不自动重复写入。
- 临时 YAML `resources` 仅由受理节点在内存持有，经 stdin 传输；不进入任务参数或持久产物。输入租约也覆盖排队阶段。节点丢失后状态为失败、诊断为 `INPUT_LOST`，调用方重新提交；资源解析失败时不持久化可能回显 YAML 的子进程日志。
- 任何活动查询、选项任务或未确认停止的外部 attempt 都会阻止 run 清理。清理任务真正完成后才返回 `200/CLEANED`；等待超时返回 503，重复调用继续等待原任务。不能因收到 503 就认定 schema 已删除。
- 对未知外部执行，先在目标仓库核实会话、写入和子进程已结束，再执行下列管理命令解除保护。该命令是人工确认，不会自动取消目标库会话。

```powershell
uv run --frozen dbt-service-admin reconcile-attempt <attempt-uuid> --confirm-external-stopped
uv run --frozen dbt-service-admin gc --older-than-hours 24 --limit 100
```

GC 只回收满足引用和执行保护条件的孤立集合。已发布 run 和默认项目输出不会按时间自动淘汰；任务状态与有界结果默认保留。配置版本或工具链变更后，旧 run 查询仍绑定原版本，应保留相应 worker，不能将不同配置或代码伪装成同一个版本标识。

### 旧数据切换

1. 暂停旧实例受理并排空所有任务，备份 SQLite、run/query 目录及通用项目目录。
2. 初始化服务 PostgreSQL 数据库，登记绑定并导入通用项目。
3. 执行下面的只读旧库导入；保留原 run/query UUID、schemaName、原文件字节和查询结果，不重新构建目标表。
4. 从空临时目录的新实例验证原 run 的目录和查询，再切换指标平台请求。不要同时向两套存储写入。

```powershell
uv run --frozen dbt-service-admin import-legacy path/to/platform-jobs.sqlite
```

导入验证 READY 的完整产物和摘要，缺失或发生改变时拒绝；重复导入相同记录不会产生新的任务 ID。旧通用任务只存在旧进程内存中，已丢失历史无法从 SQLite 恢复。新系统受理新任务后回退必须先排空并核对新增状态，不能直接恢复旧 SQLite 覆盖新历史。

旧失败任务缺少可靠的外部停止证据，导入后保留待核实 attempt；管理员确认目标库执行结束后再解除清理保护。历史请求文件仍需存在，已被旧清理流程删除的请求无法仅凭 SQLite 索引重建。

### 无状态验收测试

为避免影响业务库，单独配置 `SERVICE_TEST_DATABASE_URL` 指向可写测试库：

```powershell
uv run --frozen pytest -q tests/test_runtime_storage.py tests/test_runtime_artifacts.py tests/test_runtime_worker.py tests/test_runtime_execution.py tests/test_runtime_api.py tests/test_runtime_migration.py
```

真实双进程验收还需设置 `SERVICE_RUNTIME_E2E=1` 和前文 `PLATFORM_TEST_PG*` 目标库参数，然后运行：

```powershell
uv run --frozen pytest -q tests/test_runtime_e2e.py
```

该测试启动两个独立 HTTP 进程和独立临时目录，执行真实 dbt build、跨节点读取、停止受理进程后的 MetricFlow 查询及清理。未配置测试库时显式跳过。

## 测试

常规验证不需要数据库：

```bash
uv sync --frozen --all-groups
uv run pytest -v
uv run ruff check src tests
uv run dbt --version
uv run mf --version
```

Windows 必须启用“开发者模式”并用 `core.symlinks=true` 检出 MetricFlow submodule；否则 Git symlink 会被保存为只含目标路径的普通文本文件，读取上游测试 YAML 时会失败。如果已有检出是这种状态，在干净的 `vendor/metricflow` 工作树中运行 `git config core.symlinks true` 和 `git checkout-index --force --all` 可重新生成链接。

以下命令同时验证三个源码包的安装来源；输出必须是仓库内 `vendor/` 路径：

```bash
uv run pytest tests/test_dependencies.py -v
uv run python -c "import importlib.metadata as m; print(m.distribution('dbt-core').read_text('direct_url.json')); print(m.distribution('metricflow').read_text('direct_url.json')); print(m.distribution('dbt-metricflow').read_text('direct_url.json'))"
```

真实 StarRocks E2E 默认跳过。提供临时测试 schema 的连接参数后显式启用；测试会依次通过 HTTP 运行 `debug`、`seed`、`build`、`test`，并在该 schema 中创建测试对象：

```bash
export RUN_STARROCKS_E2E=1
export DBT_STARROCKS_HOST=127.0.0.1
export DBT_STARROCKS_PORT=9030
export DBT_STARROCKS_USER=fixture
export DBT_ENV_SECRET_STARROCKS_PASSWORD=replace-me
export DBT_STARROCKS_SCHEMA=dbt_wrapper_e2e
uv run pytest tests/test_starrocks_e2e.py -v
```

请只使用可安全清理的专用 schema。没有显式开关或缺少连接参数时，测试报告为 `SKIPPED`，不会伪造成功结果。

## 升级上游版本

先验证目标稳定 tag 与当前 adapter 兼容。dbt fork 保留官方仓库为 upstream，从目标官方 tag 建内部维护分支。MetricFlow fork 在分别发布的 core 与 CLI 版本上创建可同时构建两个包的组合提交，并验证两个 wheel 的版本及依赖；主仓只移动对应的两个 gitlink。

更新 gitlink 后，同步 `pyproject.toml` 中的精确版本，重新生成 `uv.lock`，然后运行完整测试、CLI 冒烟测试和本地 HTTP 健康检查。不得只移动 submodule 指针而保留旧锁文件，也不得直接修改 `vendor/` 内的上游源码。

## Agent 协议接入（agent-dbt-v1）

在既有固定项目 Git/profile 绑定上，`GET /v2/projects/{projectId}/publication` 增加协议能力、业务时区和项目子目录。agent 可直接访问本服务；现有平台同步 options 和完整 query 接口保留。

- `POST /validations` / `GET /validations/{validationId}`：固定基线与 YAML 增改删的耐久定义验证。正文按 UTF-8 计算旧文件摘要，单文件 512 KiB、总计 5 MiB、最多 100 个文件。路径相对绑定 dbt 项目，只能位于其 model-paths。
- `POST /query-option-jobs` / `GET /query-option-jobs/{optionsJobId}`：异步准备合法选项，复用原 QUERY_OPTIONS 任务与 optionId。
- `GET /queries/{queryId}/status`：轻量轮询，不读取结果正文。
- `GET /queries/{queryId}/results?offset=0&limit=100`：读取固定查询结果，每页最多 200 行、8 MiB，超限按整行缩小；`nextOffset` 为实际游标。

上述相对路由均位于 `/v2/projects/{projectId}` 下。验证只执行 YAML、模板、deps/parse、锁定版本语义校验，不执行 build/run，也不更改活动发布指针。`SUCCEEDED + valid=false` 是定义错误，`FAILED` 是任务/基础设施失败；配置在受理后变化会拒绝该尝试，重试应新建尝试键。同键同输入恢复原任务，不同输入返回冲突。旧 main 祖先基线可验证，不要求等于最新 main。

服务数据迁移增加 schema version 3；先升级服务并完成迁移，再开启 agent 的 dbt 配置。agent 与服务必须绑定相同 Git 仓库和子目录；发布继续走原管理命令。不要把 profile、仓库凭据或数据库地址交给模型。

带 offset 时间按发布记录的业务时区转换，无 offset 的历史平台输入不二次转换。响应给出 `normalizedTimeRange` 和 MetricFlow 粒度对齐策略。`availableRows` 是保留行数，`resultTruncated` 表示执行截断；读完分页不意味着获取了数据库全量。

协议样例和 SHA-256 清单位于 `tests/fixtures/agent_contract/`，消费端独立保存相同版本，不需要跨仓库导入。新增验证可运行 `uv run --frozen pytest -q tests/test_draft_validation_execution.py tests/test_agent_response_contract.py`；数据库测试需设置既有 `SERVICE_TEST_DATABASE_URL`，必须指向隔离测试库。
