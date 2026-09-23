# dbt MetricFlow Service

这是一个可在本地运行的 Python HTTP 服务。它通过只读 Git submodule 携带固定版本的 dbt 与 MetricFlow 源码，使用 `uv` 安装对应 Python 包，并通过官方公开 CLI 包装能力；服务不修改上游源码，也不跨源码目录导入内部实现。服务提供结构化命令白名单、异步任务状态、项目级写互斥、超时控制、输出截断和 dbt secret 环境变量遮盖。

## 版本基线

- Python `3.11` 至 `3.14`；本地推荐 Python `3.12`
- dbt 源码：`vendor/dbt`，使用 dbt fork 中指向官方 `v1.12.5` 提交的 `internal/dbt-core-1.12.5` 分支
- MetricFlow 与 dbt-metricflow 源码：`vendor/metricflow`，使用 MetricFlow fork 中的 `internal/metricflow-0.213.0-dbt-0.15.0` 分支
- `dbt-core==1.12.5`
- `dbt-starrocks==1.12.2`
- `dbt-duckdb==1.11.0`（提供一个可直接运行的 MetricFlow 支持 adapter）
- `dbt-metricflow==0.15.0`
- `metricflow==0.213.0`
- `uv`（本地依赖管理工具）

`dbt-core`、`metricflow` 与 `dbt-metricflow` 由 `uv` 从上述本地源码构建，`dbt-starrocks` 与 `dbt-duckdb` 从包索引安装。依赖由 `uv.lock` 固定。`GET /v1/versions` 返回当前进程实际加载的服务与上游包版本。

MetricFlow 上游分别发布 core `0.213.0` 和 CLI `0.15.0`。fork 的组合提交 `05551f73` 以这两个稳定发布为基线，仅将 core 的版本元数据恢复为 `0.213.0`，使同一源码树可构建两个包；主仓固定该提交，不随 fork 分支自动移动。

## 目录和配置

从仓库根目录启动时，服务默认使用三个本地目录，也可以用环境变量指定其他绝对路径：

- `projects/`（`PROJECTS_ROOT`）：每个一级子目录是一个 dbt 项目；dbt 会在项目内写入 `target/` 和 `logs/`。
- `profiles/`（`DBT_PROFILES_DIR`）：包含 `profiles.yml`。
- `job-artifacts/`（`JOB_ARTIFACTS_ROOT`）：请求级 resources 的派生产物，由服务用户管理和清理；不要由多个服务实例共享。

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
New-Item -ItemType Directory -Force projects, profiles, job-artifacts
# 将自己的 profiles.yml 放在 profiles/ 下，将 dbt 项目放在 projects/<项目名>/ 下
uv run --frozen dbt-metricflow-service
```

macOS/Linux：

```bash
git submodule update --init --recursive
uv sync --frozen --all-groups --python 3.12
mkdir -p projects profiles job-artifacts
# 将自己的 profiles.yml 放在 profiles/ 下，将 dbt 项目放在 projects/<项目名>/ 下
uv run --frozen dbt-metricflow-service
```

服务监听 `http://localhost:8000`。运行 `curl http://localhost:8000/health/ready` 可检查 CLI 与目录是否就绪；首次调用前应配置实际的 `profiles.yml` 和 dbt 项目。

可选运行参数：

- `COMMAND_TIMEOUT_SECONDS`：单个 CLI 子进程的最长运行时间，默认 `1800` 秒。
- `MAX_OUTPUT_BYTES`：每个 stdout/stderr 流保留的尾部字节数，默认 `1048576`。
- `JOB_ARTIFACTS_ROOT`：请求级任务派生产物根目录，默认当前工作目录下的 `job-artifacts/`。

`GET /health/live` 只检查 HTTP 进程；`GET /health/ready` 检查 `dbt`、`mf`、项目/profile 目录以及任务产物目录。

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

未携带有效 resources 时，提交 MetricFlow 任务前必须先运行 dbt parse，让项目生成 `target/manifest.json`。MetricFlow `0.213.0` 不支持 StarRocks adapter：普通请求同步返回 HTTP 422 `metricflow_adapter_not_supported`，资源请求在异步任务中失败并返回同一诊断代码。dbt 对 StarRocks 的 parse、compile、seed、run、test、build 和 debug 不受此限制。

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
