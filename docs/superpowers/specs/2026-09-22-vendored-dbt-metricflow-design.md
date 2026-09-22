# dbt 与 MetricFlow 源码自包含设计

## 背景

`dbt-metricflow-service` 已通过 HTTP API 包装 `dbt` 与 `mf` CLI，并支持异步任务、项目白名单、超时、日志脱敏、健康检查及容器部署。当前运行环境从 PyPI 安装 `dbt-core`、`metricflow` 和 `dbt-metricflow`，服务仓库本身没有固定对应的上游源码。

本次调整把 dbt 与 MetricFlow 的固定版本源码纳入服务仓库的版本边界，使服务能够从自身携带的源码构建这三个 Python 包，同时保持两个上游仓库的历史和源码独立。

## 目标

- 使用 Git submodule 把 dbt 与 MetricFlow 源码固定在服务仓库中。
- 从 submodule 的本地路径构建并安装 `dbt-core`、`metricflow` 和 `dbt-metricflow`。
- 保持现有 HTTP API、任务执行模型和安全边界不变。
- 继续以最小依赖方式从 PyPI 安装 `dbt-starrocks` 和 `dbt-duckdb`。
- 支持通过一次递归克隆取得构建所需的服务源码与两份上游源码。
- 构建过程中禁止静默回退到 PyPI 中的同名 dbt 或 MetricFlow 包。

## 非目标

- 不修改 dbt 或 MetricFlow 的上游源码。
- 不合并或重写两个上游仓库的 Git 历史。
- 不在本次调整中新增 API、任务类型或 StarRocks 专用 MetricFlow 方言。
- 不把所有传递依赖源码一并纳入服务仓库。
- 不要求运行镜像保留上游源码；运行镜像只需包含由这些源码构建的可执行 Python 环境。

## 固定版本与目录

服务仓库增加以下 submodule：

| 目录 | 上游仓库 | tag | 固定 commit |
| --- | --- | --- | --- |
| `vendor/dbt` | `https://github.com/dbt-labs/dbt.git` | `v1.12.5` | `7f78d7b6aa3a88e5efa6dd92753e4983d92aeba4` |
| `vendor/metricflow` | `https://github.com/dbt-labs/metricflow.git` | `dbt-metricflow/v0.15.0` | `bd05dd9a145f290a1bf3090cbdbb3b11d7322701` |

Gitlink commit 是实际构建依据；tag 用于说明该 commit 对应的上游发布版本。服务仓库不依赖父工作区中并列存在的 `dbt/` 与 `metricflow/` 目录。

## 依赖解析

`pyproject.toml` 保留明确的版本约束，并通过 `tool.uv.sources` 把三个包映射到 submodule 内的本地构建目录：

| Python 包 | 本地源码路径 |
| --- | --- |
| `dbt-core` | `vendor/dbt/core` |
| `metricflow` | `vendor/metricflow` |
| `dbt-metricflow` | `vendor/metricflow/dbt-metricflow` |

本地路径依赖采用非 editable 安装。`uv.lock` 记录本地 source，使本地开发、测试和 Docker 构建使用同一套依赖图。`dbt-starrocks==1.12.2`、`dbt-duckdb==1.11.0` 及其他传递依赖继续从包索引安装。

若 submodule 未初始化、源码目录不完整、版本约束不兼容或本地包无法构建，`uv sync --frozen` 必须直接失败。配置中不提供同名包的备用索引来源。

## 构建与部署流程

完整检出使用：

```shell
git clone --recurse-submodules https://github.com/823338779/metric_platform-dbt-infra.git
```

已有检出使用：

```shell
git submodule update --init --recursive
uv sync --frozen
```

Docker builder 先复制依赖清单和两个 submodule 源码，再执行 `uv sync --frozen --no-dev --no-install-project`。之后复制服务源码并安装服务包。这样，上游源码变化会使依赖构建层失效，而单纯的服务源码变化仍可复用依赖层。

运行镜像继续使用非 root 用户，只复制已构建的 `.venv`、服务源码和启动配置。容器内的 `dbt` 与 `mf` 命令来自 builder 根据 submodule 源码构建的包，不要求运行镜像存在 `vendor/` 目录。

Compose 的启动方式、挂载目录、环境变量和健康检查保持现有契约。

## 运行时行为

本次调整只改变 Python 包的构建来源。请求进入 FastAPI 后的项目解析、命令参数生成、子进程执行、并发控制、超时终止、输出截断与脱敏、任务状态保存均保持不变。

版本接口继续报告已安装包版本。安装元数据中的 `direct_url.json` 用于验证三个本地包的构建来源，不把本机绝对路径暴露到服务 API。

## 错误处理

- submodule 未初始化：依赖同步或 Docker `COPY` 阶段失败，并提示缺少本地源码。
- gitlink 不符合声明版本：仓库验证失败，不继续执行依赖同步。
- 本地包构建失败：构建直接退出，不回退到 PyPI。
- 第三方依赖解析冲突：`uv lock` 或 `uv sync --frozen` 失败，必须显式调整版本后重新生成锁文件。
- 运行时 CLI 失败：沿用现有任务失败状态、退出码、超时和脱敏输出行为。

## 验证与验收

实现完成后执行以下验证：

1. 检查 `.gitmodules`、两个 gitlink commit 和上游 tag 对应关系。
2. 检查 `uv.lock` 的三个本地 source，并读取已安装 distribution 的 `direct_url.json`，确认没有使用 PyPI 回退。
3. 确认安装版本为 `dbt-core 1.12.5`、`metricflow 0.213.0`、`dbt-metricflow 0.15.0`、`dbt-starrocks 1.12.2`。
4. 运行完整 `pytest` 与 `ruff check`。
5. 运行 `dbt --version`、`mf --version` 和现有 DuckDB 集成测试。
6. 构建并启动容器，验证非 root 用户、存活与就绪检查、版本接口以及容器内两个 CLI。
7. 验证 `dbt-starrocks` adapter 能加载。真实 StarRocks 端到端任务仍以可访问实例和凭据为前提。
8. 检查两个 submodule、父工作区原有 `dbt/` 与 `metricflow/` 仓库没有源码改动。

## 升级流程

升级 dbt 或 MetricFlow 时，先在独立验证中确认目标 tag 与现有 adapter 兼容，再更新对应 submodule 的 gitlink、`pyproject.toml` 版本约束、`uv.lock` 和版本说明。每次升级都重新执行完整测试及容器验收；不得只移动 submodule 指针而保留旧锁文件。

## 文档与仓库规则调整

README 增加递归克隆、初始化 submodule、本地构建和版本升级说明。仓库级 `AGENTS.md` 改为禁止修改 `vendor/` 内上游源码，并明确三个核心包必须从固定 submodule 构建；原有“不得读取、复制或构建父工作区源码”的约束保留其意图，但改为区分父工作区并列源码与服务仓库自己的 submodule。
