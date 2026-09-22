# dbt MetricFlow Service

这是一个可独立部署的 Python HTTP 服务。它通过只读 Git submodule 携带固定版本的 dbt 与 MetricFlow 源码，在构建阶段安装对应 Python 包，并通过官方公开 CLI 包装能力；服务不修改上游源码，也不跨源码目录导入内部实现。服务提供结构化命令白名单、异步任务状态、项目级写互斥、超时控制、输出截断和 dbt secret 环境变量遮盖。

## 版本基线

- Python `3.11` 至 `3.14`，容器使用 Python `3.12`
- dbt 源码：`vendor/dbt`，固定到 tag `v1.12.5`
- MetricFlow core 源码：`vendor/metricflow`，固定到 tag `v0.213.0`
- dbt-metricflow CLI 源码：`vendor/dbt-metricflow`，固定到 tag `dbt-metricflow/v0.15.0`
- `dbt-core==1.12.5`
- `dbt-starrocks==1.12.2`
- `dbt-duckdb==1.11.0`（提供一个可直接运行的 MetricFlow 支持 adapter）
- `dbt-metricflow==0.15.0`
- `metricflow==0.213.0`
- `uv==0.12.17`（容器构建工具）

`dbt-core`、`metricflow` 与 `dbt-metricflow` 由 `uv` 从上述本地源码构建，`dbt-starrocks` 与 `dbt-duckdb` 从包索引安装。依赖由 `uv.lock` 固定。`GET /v1/versions` 返回当前进程实际加载的服务与上游包版本。

MetricFlow 上游在不同 commit 发布 core 和 CLI，因此服务使用两个只读 gitlink 分别固定这两个稳定 tag；两个目录仍来自同一个 `dbt-labs/metricflow` 仓库。

## 目录和配置

服务使用两个固定容器目录：

- `/workspace/projects`：每个一级子目录是一个 dbt 项目。dbt 需要在项目内写入 `target/` 和 `logs/`，因此该挂载必须可写。
- `/workspace/profiles`：包含 `profiles.yml`，只需只读挂载。

项目通过安全的单段标识访问，例如 `/workspace/projects/sales` 对应请求字段 `"project": "sales"`。服务拒绝路径、绝对路径和逃逸项目根目录的符号链接。

StarRocks profile 可以只引用环境变量，避免把凭据写进镜像或仓库：

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

`git submodule status` 行首为 `-` 表示源码尚未初始化，行首为 `+` 表示工作树 commit 与服务锁定的 gitlink 不一致。两种状态都不能用于发布构建；正常状态以一个空格开头。

## 启动

本地开发：

```bash
uv sync --frozen --all-groups
PROJECTS_ROOT=/absolute/path/projects \
DBT_PROFILES_DIR=/absolute/path/profiles \
uv run dbt-metricflow-service
```

容器部署：

```bash
docker build -t dbt-metricflow-service:0.1.0 .
docker run --rm -p 8000:8000 \
  -v /absolute/path/projects:/workspace/projects \
  -v /absolute/path/profiles:/workspace/profiles:ro \
  --env-file /absolute/path/starrocks.env \
  dbt-metricflow-service:0.1.0
```

Compose 需要设置宿主机路径并在当前环境提供 profile 引用的变量：

```bash
export PROJECTS_PATH=/absolute/path/projects
export PROFILES_PATH=/absolute/path/profiles
docker compose up -d --build
```

可选运行参数：

- `COMMAND_TIMEOUT_SECONDS`：单个 CLI 子进程的最长运行时间，默认 `1800` 秒。
- `MAX_OUTPUT_BYTES`：每个 stdout/stderr 流保留的尾部字节数，默认 `1048576`。

`GET /health/live` 只检查 HTTP 进程；`GET /health/ready` 检查 `dbt`、`mf` 以及两个挂载目录；容器健康检查使用 liveness 端点。

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

提交 MetricFlow 任务前必须先运行 dbt parse，让项目生成 `target/manifest.json`。MetricFlow `0.213.0` 不支持 StarRocks adapter，因此 StarRocks 项目的 MetricFlow 请求固定返回 HTTP 422 `metricflow_adapter_not_supported`，不会启动 `mf` 子进程。dbt 对 StarRocks 的 parse、compile、seed、run、test、build 和 debug 不受此限制。

## 测试

常规验证不需要数据库：

```bash
uv sync --frozen --all-groups
uv run pytest -v
uv run ruff check src tests
uv run dbt --version
uv run mf --version
```

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

先验证目标 tag 与当前 adapter 兼容，再移动对应 gitlink。MetricFlow core 与 dbt-metricflow CLI 分别发布，升级时可能需要同时调整 `vendor/metricflow` 和 `vendor/dbt-metricflow`。

更新 gitlink 后，同步 `pyproject.toml` 中的精确版本，重新生成 `uv.lock`，然后运行完整测试、CLI 冒烟测试和 Docker 镜像验收。不得只移动 submodule 指针而保留旧锁文件，也不得直接修改 `vendor/` 内的上游源码。
