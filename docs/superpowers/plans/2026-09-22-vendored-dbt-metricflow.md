# dbt 与 MetricFlow 源码自包含实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `dbt-metricflow-service` 通过固定 Git submodule 自带 dbt 与 MetricFlow 源码，并从这些源码构建可独立部署的 Python 服务。

**Architecture:** 在 `vendor/` 下固定两个上游仓库的三个 gitlink，通过 `tool.uv.sources` 将三个核心 Python 包解析到对应的本地源码路径。Docker builder 从这些路径构建非 editable 包，runtime 只携带已安装环境和服务代码，现有 HTTP API 与任务行为不变。

**Tech Stack:** Git submodule、Python 3.12、uv 0.12.17、Hatchling、FastAPI、pytest、Docker、dbt Core、MetricFlow

**Spec:** `docs/superpowers/specs/2026-09-22-vendored-dbt-metricflow-design.md`

## Global Constraints

- `vendor/dbt` 必须固定到 dbt `v1.12.5` 的 commit `7f78d7b6aa3a88e5efa6dd92753e4983d92aeba4`。
- `vendor/metricflow` 必须固定到 `v0.213.0` 的 commit `4200f85c59b2bb334f0b0dea851b38d0b8198134`。
- `vendor/dbt-metricflow` 必须固定到 `dbt-metricflow/v0.15.0` 的 commit `bd05dd9a145f290a1bf3090cbdbb3b11d7322701`。
- `dbt-core`、`metricflow` 和 `dbt-metricflow` 必须从本地 submodule 构建，禁止回退到 PyPI 同名包。
- `dbt-starrocks==1.12.2` 与 `dbt-duckdb==1.11.0` 继续从包索引安装。
- 不得修改三个 submodule 或父工作区并列 `dbt/`、`metricflow/` 仓库的源码。
- 保持现有 HTTP API、任务模型、安全边界和 Compose 运行契约不变。
- 每项任务完成验证后创建一个独立 commit。

## Review Focus

- submodule 未初始化时，本地依赖检查必须失败，不能静默安装索引包；Task 1 的目录与 source 配置测试负责覆盖。
- submodule 被移动到其他 commit 时，固定版本测试必须给出失败；Task 1 的 git HEAD 断言负责覆盖。
- `uv` 错误地从 PyPI 安装同名包时，安装来源测试必须失败；Task 1 的 `direct_url.json` 断言负责覆盖。
- 源码声明版本与运行环境不一致时，版本测试必须失败；Task 1 扩展现有依赖版本测试负责覆盖。
- runtime 镜像没有 `vendor/` 源码时，已构建的 `dbt`、`mf` 和服务仍必须运行；Task 2 的容器冒烟测试负责覆盖。

---

### Task 1: 固定上游源码并切换本地依赖来源

**Files:**
- Create: `.gitmodules`
- Create: `vendor/dbt`（Git submodule）
- Create: `vendor/metricflow`（Git submodule）
- Create: `vendor/dbt-metricflow`（Git submodule）
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `AGENTS.md`
- Modify: `tests/test_dependencies.py`

**Interfaces:**
- Consumes: dbt tag `v1.12.5`、MetricFlow tag `dbt-metricflow/v0.15.0`、现有精确依赖版本。
- Produces: `tool.uv.sources` 中的三个固定本地包来源，以及可供 Docker builder 构建的 `vendor/dbt/core`、`vendor/metricflow`、`vendor/dbt-metricflow/dbt-metricflow` 路径。

- [ ] **Step 1: 编写依赖来源和 submodule 固定测试**

在 `tests/test_dependencies.py` 中保留现有版本测试，并加入以下导入、常量、辅助函数和测试：

```python
import json
import subprocess
import tomllib
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

PROJECT_ROOT = Path(__file__).parents[1]
EXPECTED_LOCAL_SOURCES = {
    "dbt-core": "vendor/dbt/core",
    "dbt-metricflow": "vendor/dbt-metricflow/dbt-metricflow",
    "metricflow": "vendor/metricflow",
}
EXPECTED_SUBMODULE_COMMITS = {
    "vendor/dbt": "7f78d7b6aa3a88e5efa6dd92753e4983d92aeba4",
    "vendor/dbt-metricflow": "bd05dd9a145f290a1bf3090cbdbb3b11d7322701",
    "vendor/metricflow": "4200f85c59b2bb334f0b0dea851b38d0b8198134",
}


def installed_source_path(distribution_name: str) -> Path:
    """Return the local directory recorded by PEP 610 installation metadata."""
    direct_url_text = importlib.metadata.distribution(distribution_name).read_text("direct_url.json")
    assert direct_url_text is not None
    direct_url = json.loads(direct_url_text)
    assert direct_url.get("dir_info", {}).get("editable") is not True
    parsed_url = urlparse(direct_url["url"])
    assert parsed_url.scheme == "file"
    return Path(url2pathname(parsed_url.path)).resolve()


def test_local_source_configuration_is_complete() -> None:
    """Every source-built distribution must point at an initialized vendored path."""
    configuration = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    sources = configuration["tool"]["uv"]["sources"]

    assert {name: sources[name]["path"] for name in EXPECTED_LOCAL_SOURCES} == EXPECTED_LOCAL_SOURCES
    for relative_path in EXPECTED_LOCAL_SOURCES.values():
        assert (PROJECT_ROOT / relative_path).is_dir()


def test_vendored_sources_are_at_audited_commits() -> None:
    """The checked-out submodules must match the release commits accepted by the service."""
    for relative_path, expected_commit in EXPECTED_SUBMODULE_COMMITS.items():
        actual_commit = subprocess.run(
            ["git", "-C", str(PROJECT_ROOT / relative_path), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        ).stdout.strip()
        assert actual_commit == expected_commit


def test_core_packages_are_installed_from_vendored_sources() -> None:
    """The runtime must build dbt and MetricFlow packages from the checked-out sources."""
    for distribution_name, relative_path in EXPECTED_LOCAL_SOURCES.items():
        assert installed_source_path(distribution_name) == (PROJECT_ROOT / relative_path).resolve()
```

- [ ] **Step 2: 运行新增测试并确认当前索引安装不能满足要求**

Run: `uv run pytest tests/test_dependencies.py -v`

Expected: FAIL，因为 `tool.uv.sources` 和三个 `vendor/` 源码路径尚不存在，已安装包也没有指向这些本地路径的 `direct_url.json`。

- [ ] **Step 3: 增加并固定三个 Git submodule 工作树**

在服务仓库根目录执行：

```powershell
git submodule add https://github.com/dbt-labs/dbt.git vendor/dbt
git -C vendor/dbt checkout 7f78d7b6aa3a88e5efa6dd92753e4983d92aeba4
git submodule add https://github.com/dbt-labs/metricflow.git vendor/metricflow
git -C vendor/metricflow checkout 4200f85c59b2bb334f0b0dea851b38d0b8198134
git submodule add https://github.com/dbt-labs/metricflow.git vendor/dbt-metricflow
git -C vendor/dbt-metricflow checkout bd05dd9a145f290a1bf3090cbdbb3b11d7322701
```

运行 `git submodule status`，确认输出中的三个 commit 与 Global Constraints 完全一致，并且行首没有 `-`、`+` 或 `U`。

- [ ] **Step 4: 配置 uv 本地非 editable source**

在 `pyproject.toml` 的依赖声明后加入：

```toml
[tool.uv.sources]
dbt-core = { path = "vendor/dbt/core" }
dbt-metricflow = { path = "vendor/dbt-metricflow/dbt-metricflow" }
metricflow = { path = "vendor/metricflow" }
```

保留 `[project].dependencies` 中的精确版本声明。执行：

```powershell
uv lock
uv sync --frozen --all-groups
```

检查 `uv.lock` 中三个包的 `source` 均为相对目录，不存在对应包的 registry source。

- [ ] **Step 5: 更新仓库约束以保护 submodule 边界**

将 `AGENTS.md` 开头的上游规则改为以下含义明确的条目：

```markdown
- 上游版本固定为 `dbt-core==1.12.5`、`dbt-starrocks==1.12.2`、`dbt-duckdb==1.11.0`、`dbt-metricflow==0.15.0` 和 `metricflow==0.213.0`。
- `vendor/dbt`、`vendor/metricflow` 与 `vendor/dbt-metricflow` 是只读 Git submodule；不得在其中修改或提交源码，只能通过更新 gitlink 升级。
- `dbt-core`、`metricflow` 与 `dbt-metricflow` 必须从固定 submodule 构建；父工作区并列的 `dbt/` 与 `metricflow/` 不得作为依赖来源。
- 服务只能通过已安装包提供的公开 CLI 或公开 API 调用 dbt 与 MetricFlow，不得跨目录导入其内部实现。
```

- [ ] **Step 6: 运行来源、版本和 CLI 集成测试**

Run: `uv run pytest tests/test_dependencies.py tests/test_cli_integration.py -v`

Expected: PASS；输出包含 `dbt-core 1.12.5`、`dbt-metricflow 0.15.0`、`metricflow 0.213.0`、StarRocks adapter `1.12.2`，并且 DuckDB MetricFlow 冒烟流程成功。

Run: `uv run ruff check tests/test_dependencies.py`

Expected: PASS。

- [ ] **Step 7: 提交源码边界与依赖解析变更**

```powershell
git add .gitmodules vendor/dbt vendor/metricflow vendor/dbt-metricflow pyproject.toml uv.lock AGENTS.md tests/test_dependencies.py docs/superpowers/specs/2026-09-22-vendored-dbt-metricflow-design.md docs/superpowers/plans/2026-09-22-vendored-dbt-metricflow.md
git commit -m "build: source dbt metricflow from submodules"
```

### Task 2: 从 submodule 构建独立运行镜像

**Files:**
- Modify: `Dockerfile`

**Interfaces:**
- Consumes: Task 1 产生的三个 `tool.uv.sources` 路径和初始化后的 submodule 工作树。
- Produces: `dbt-metricflow-service:source-bundle` 镜像；其中 `.venv` 包含源码构建的 `dbt` 与 `mf`，runtime 文件系统不依赖 `/app/vendor`。

- [ ] **Step 1: 证明当前 Dockerfile 未携带本地 source**

Run: `docker build -t dbt-metricflow-service:source-bundle .`

Expected: FAIL 于首次 `uv sync`，错误指出三个本地 source 路径至少有一个不存在。

- [ ] **Step 2: 在 builder 中复制构建所需源码**

把 Dockerfile builder 的依赖安装段改为：

```dockerfile
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY vendor/dbt/core ./vendor/dbt/core
COPY vendor/metricflow ./vendor/metricflow
COPY vendor/dbt-metricflow/requirements-files ./vendor/dbt-metricflow/requirements-files
COPY vendor/dbt-metricflow/dbt-metricflow ./vendor/dbt-metricflow/dbt-metricflow
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev
```

runtime 阶段保持只复制 `.venv`、`src` 和 `pyproject.toml`，不要复制 `vendor/`。

- [ ] **Step 3: 构建镜像并验证安装来源与源码独立性**

Run: `docker build -t dbt-metricflow-service:source-bundle .`

Expected: PASS。

Run:

```powershell
docker run --rm dbt-metricflow-service:source-bundle python -c "import importlib.metadata as m, json, pathlib; expected={'dbt-core':'/app/vendor/dbt/core','metricflow':'/app/vendor/metricflow','dbt-metricflow':'/app/vendor/dbt-metricflow/dbt-metricflow'}; actual={name:json.loads(m.distribution(name).read_text('direct_url.json'))['url'] for name in expected}; assert all(path in actual[name] for name,path in expected.items()), actual; assert not pathlib.Path('/app/vendor').exists()"
```

Expected: exit code `0`；安装元数据指向 builder 内的固定本地源码，同时 runtime 不存在 `/app/vendor`。

- [ ] **Step 4: 验证容器内 CLI、用户和 HTTP 健康状态**

Run:

```powershell
docker run --rm dbt-metricflow-service:source-bundle dbt --version
docker run --rm dbt-metricflow-service:source-bundle mf --version
docker run --rm dbt-metricflow-service:source-bundle id -u
```

Expected: `dbt` 报告 core `1.12.5` 和 StarRocks `1.12.2`；`mf` 报告 `0.15.0`；UID 为 `10001`。

使用现有挂载约定启动镜像并请求服务接口：

```powershell
$repositoryPath = (Get-Location).Path
docker run -d --rm --name dbt-metricflow-source-test -p 18000:8000 `
  --mount "type=bind,source=$repositoryPath\tests\fixtures,target=/workspace/projects" `
  --mount "type=bind,source=$repositoryPath\tests\fixtures\profiles,target=/workspace/profiles,readonly" `
  dbt-metricflow-service:source-bundle
do {
  $healthStatus = docker inspect --format '{{.State.Health.Status}}' dbt-metricflow-source-test
  if ($healthStatus -eq 'unhealthy') { throw 'container health check failed' }
  if ($healthStatus -ne 'healthy') { Start-Sleep -Seconds 2 }
} until ($healthStatus -eq 'healthy')
curl.exe --fail http://127.0.0.1:18000/health/live
curl.exe --fail http://127.0.0.1:18000/health/ready
curl.exe --fail http://127.0.0.1:18000/v1/versions
docker stop dbt-metricflow-source-test
```

Expected: 两个健康接口返回 HTTP 200，版本接口返回 Task 1 的固定版本集合，容器随后正常停止。

- [ ] **Step 5: 提交容器构建变更**

```powershell
git add Dockerfile
git commit -m "build: package vendored dbt tools in image"
```

### Task 3: 记录自包含检出、验证与升级流程

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: Task 1 的 submodule 目录与依赖命令、Task 2 的容器构建行为。
- Produces: 面向开发和部署人员的唯一操作说明，不改变任何运行时接口。

- [ ] **Step 1: 更新项目定位与版本来源说明**

将 README 开头改为说明服务通过只读 Git submodule 携带 dbt 与 MetricFlow 源码，并通过其公开 CLI 包装能力。版本基线中注明：

```markdown
- dbt 源码：`vendor/dbt`，固定到 tag `v1.12.5`
- MetricFlow core 源码：`vendor/metricflow`，固定到 tag `v0.213.0`
- dbt-metricflow CLI 源码：`vendor/dbt-metricflow`，固定到 tag `dbt-metricflow/v0.15.0`
- `dbt-core`、`metricflow`、`dbt-metricflow` 由 `uv` 从上述本地源码构建
- `dbt-starrocks==1.12.2` 与 `dbt-duckdb==1.11.0` 从包索引安装
```

- [ ] **Step 2: 增加完整检出与已有检出初始化命令**

在启动章节之前加入：

```bash
git clone --recurse-submodules https://github.com/823338779/metric_platform-dbt-infra.git
cd metric_platform-dbt-infra
```

并说明已有仓库必须先执行：

```bash
git submodule update --init --recursive
git submodule status
```

明确写出行首出现 `-` 表示尚未初始化，出现 `+` 表示工作树 commit 与服务锁定的 gitlink 不一致；这两种状态都不能用于发布构建。

- [ ] **Step 3: 增加升级和来源验证说明**

记录升级顺序：验证目标 tag、移动一个 submodule gitlink、同步版本声明、重新生成 `uv.lock`、运行完整测试和容器验收。增加以下来源验证命令：

```bash
uv run pytest tests/test_dependencies.py -v
uv run python -c "import importlib.metadata as m; print(m.distribution('dbt-core').read_text('direct_url.json')); print(m.distribution('metricflow').read_text('direct_url.json')); print(m.distribution('dbt-metricflow').read_text('direct_url.json'))"
```

- [ ] **Step 4: 执行完整服务回归**

Run:

```powershell
uv lock --check
uv sync --frozen --all-groups
uv run pytest -v
uv run ruff check src tests
uv run dbt --version
uv run mf --version
docker build -t dbt-metricflow-service:source-bundle .
```

Expected: 锁文件检查、同步、完整测试、lint、两个 CLI 和镜像构建全部通过；未提供 StarRocks E2E 凭据时仅对应测试按现有规则显示 `SKIPPED`。

- [ ] **Step 5: 检查仓库边界**

Run:

```powershell
git status --short --branch
git submodule status
git -C vendor/dbt status --short
git -C vendor/metricflow status --short
git -C ..\dbt status --short
git -C ..\metricflow status --short
```

Expected: 服务仓库只包含本计划的 README 变更；三个 submodule 与父工作区两个上游仓库的内部状态均为空。

- [ ] **Step 6: 提交部署文档**

```powershell
git add README.md
git commit -m "docs: explain self-contained source deployment"
```
