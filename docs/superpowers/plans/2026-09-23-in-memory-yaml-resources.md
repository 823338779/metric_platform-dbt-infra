# In-memory YAML Resources Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给现有任务接口增加请求级 resources，优先消费内存 YAML，缺失或空白时沿用默认行为，非空白解析错误明确失败。

**Architecture:** 有效 resources 为空时保留原有 CLI；否则通过 stdin 将请求交给一次性 Python worker。worker 仅在固定版本 dbt 的 schema 读取层使用内存适配，禁用原文缓存，派生产物写入独立任务目录，mf 配置绑定本次语义 manifest。生产代码和测试均从已安装的上游包导入。

**Tech Stack:** Python 3.11–3.14（开发/容器验证 3.12）、FastAPI、Pydantic、asyncio subprocess、dbt-core 1.12.5、dbt-metricflow 0.15.0、metricflow 0.213.0、pytest、ruff、uv。

**Spec:** `docs/superpowers/specs/2026-09-23-in-memory-yaml-resources-design.md`，已获用户确认；执行者必须同时阅读设计和本计划。

状态：实施计划待用户评审和选择执行方式；本文件中的代码块是实施指导，尚未写入运行代码。

## Global Constraints

- 版本固定：`dbt-core==1.12.5`、`dbt-starrocks==1.12.2`、`dbt-duckdb==1.11.0`、`dbt-metricflow==0.15.0`、`metricflow==0.213.0`。
- 不修改 vendor 源码、gitlink、依赖版本或 uv.lock；不读取父工作区同名源码作为依赖。
- key 是单段 `.yml`/`.yaml` 文件名，最多 128 字符；最多 100 项，单项 UTF-8 原文不超过 1 MiB，总原文不超过 5 MiB；HTTP 请求体上限 8 MiB。
- 先验证原始输入，再过滤 `not value.strip()` 的条目；保留剩余原文，不 trim 后覆盖它。不存在“语法错误自动回退”。
- resources 原文、请求转储、携带原文的缓存不落盘；允许独立 JSON/编译 SQL 派生产物；配置和凭据由服务端提供。
- `JOB_ARTIFACTS_ROOT` 默认 `/workspace/job-artifacts`，每任务独立目录；不写回基础项目 target/logs。
- 每个请求一个进程；保留按基础项目识别的 dbt 写互斥、进程树终止、脱敏和输出上限。
- 非空白 resources + debug 返回 422；全空白 resources + debug 保留原行为。全空白 MetricFlow 请求保留原 manifest 前置检查。
- 所有新 Python 模块具有 future annotations、模块 logger、完整类型注解；新增字段用 Field 的 description 参数或相邻注释说明用途；复用字符串用模块常量。
- 先失败测试再实现。受影响测试通过后运行完整 pytest 和 `ruff check src tests scripts`；不修复只读 vendor 的已有 lint 问题。
- 当前仅单服务进程部署；不新增发布体系、认证体系、Git 同步、通用文件系统、持久化任务队列或资源会话缓存。

## Review Focus

1. 磁盘同名 YAML 本身无效时，内存有效定义必须成功：在 YAML 解码前覆盖，不能先完整读取基础 schema（Task 2）。
2. 全空白资源、混合空白资源、被忽略且同名的文件不能误触发 worker 或创建虚拟定义；Unicode 空白、中文原文按 UTF-8 计数（Tasks 1、2、7）。
3. `.env`、profiles 和父环境可能重设解析器、target/log 路径；最终生效配置必须仍指向任务目录，mf 不能重新读取基础旧 manifest（Tasks 2、3）。
4. worker 不读取 stdin、提前退出、等待输入时被取消，不能使父进程死锁、遗留原文或锁；共享根目录的未知残留不得被递归删除（Tasks 4、5）。
5. 现有 dbt run/build 的清理与数据库副作用不能被重复执行；mf 只 parse 一次，错误处理不能把无效定义重试成基础定义（Tasks 3、6、8）。

## 文件布局与接口依赖

现有模块只做必要扩展，不移动无关代码：

| 文件 | 职责 |
| --- | --- |
| 新增 `src/dbt_metricflow_service/resources.py` | 名称/大小校验、空白过滤、资源输入常量 |
| 修改 `models.py` | 两种请求 resources 字段、内部 WorkerRequest、CommandSpec stdin/产物标记 |
| 新增 `resource_adapter.py` | 唯一允许导入上游内部 API 的模块；读取适配、缓存/日志控制、mf 配置桥接 |
| 新增 `resource_worker.py` | 受限 stdin 协议、分发、退出码；只调用服务适配层 |
| 新增 `job_artifacts.py` | 服务所有的任务目录创建、退出清理、保守启动恢复 |
| 新增 `request_limits.py` | 两个 POST 任务接口的 ASGI 请求体大小限制 |
| 修改 `commands.py`、`jobs.py` | 固定 worker argv、管道输入、产物生命周期及现有命令白名单 |
| 修改 `api.py`、`settings.py`、`main.py` | 路由分流、配置、启动检查与清理 |
| 修改 `Dockerfile`、`README.md`、`AGENTS.md` | 非 root 产物目录、接口文档、限定内部 API 例外 |

测试文件：新增 `tests/test_resources.py`、`tests/resource_helpers.py`、`tests/test_resource_adapter.py`、`tests/test_resource_metricflow.py`、`tests/test_job_artifacts.py`、`tests/test_request_limits.py`、`tests/test_resources_e2e.py`；扩展现有 commands/jobs/API/settings 测试和 `tests/fixtures/fake_cli.py`。

按 Task 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 顺序实施。Task 2 和 3 是首批技术验证门槛；不得先接入生产 API，再发现内存语义无法兑现。中间提交用于审查，不单独部署。

执行开始时先使用 Superpowers using-git-worktrees 检查并准备隔离工作区，再遵循 test-driven-development；本计划编写阶段不创建实施工作区。文件表中的生产模块全部位于 `src/dbt_metricflow_service/`，各任务的简写文件名均指该表中的同一绝对仓库位置。

## Task 1: resources 契约与内部输入协议

**Files:** Create `resources.py`, `tests/test_resources.py`; Modify `models.py`。

**Interfaces:**

```text
resources.normalize_resources(value: object) -> dict[str, str]
models.DbtJobRequest.resources: dict[str, str] = {}
models.MetricFlowJobRequest.resources: dict[str, str] = {}
models.WorkerRequest.kind: Literal["dbt", "metricflow"]
models.WorkerRequest.request: DbtJobRequest | MetricFlowJobRequest
```

WorkerRequest 是服务私有传输结构，`extra="forbid"`，after-validator 强制 kind 与请求类型一致，且必须有有效 resources。路径只来自父进程环境，不能出现在该结构中。

- [ ] **1.1 写失败测试：回退和原文保持。** 以下放入带标准导入、logger 的 `test_resources.py`：

```python
@pytest.mark.parametrize("raw", [{}, {"orders.yml": ""}, {"orders.yml": " \n\t\u3000"}])
def test_blank_resources_keep_default_behavior(raw: dict[str, str]) -> None:
    request = DbtJobRequest(project="sales", command="parse", resources=raw)
    assert request.resources == {}

def test_nonblank_content_is_not_trimmed_or_parsed_at_boundary() -> None:
    raw = " \nversion: [broken\n"
    request = DbtJobRequest(project="sales", command="parse", resources={"orders.yml": raw})
    assert request.resources == {"orders.yml": raw}

def test_mixed_resources_filter_only_blank_entries() -> None:
    request = MetricFlowJobRequest(
        project="sales", command="list_metrics",
        resources={"orders.yml": " \n", "metrics.yml": "metrics: []\n"},
    )
    assert request.resources == {"metrics.yml": "metrics: []\n"}
```

- [ ] **1.2 验证失败：** `uv run --frozen pytest tests/test_resources.py -q`，应因字段未支持而失败。
- [ ] **1.3 实现严格验证和过滤。** 两种请求均用 before-field-validator 调用共享函数，不复制规则：

```python
resources: dict[str, str] = Field(
    default_factory=dict,
    repr=False,
    description="本次任务优先读取的 YAML 原文；缺失或空白时沿用默认资源。",
)

@field_validator("resources", mode="before")
@classmethod
def validate_resources(cls, value: object) -> dict[str, str]:
    return normalize_resources(value)
```

`normalize_resources` 顺序固定为：检查实际 dict → 项数 → key/value 均为 str → 文件名长度/合法性/大小写冲突 → 各 value 的 UTF-8 字节数和总数 → 返回非空白条目的新 dict。禁止斜杠、反斜杠、控制字符、Windows 保留名和 `<>:"|?*`；只允许小写扩展名 `.yml`/`.yaml`，名称主体可含普通 Unicode。拒绝不可编码的孤立 surrogate。不在 ValueError 消息中引用 value。大小写冲突只在原始 key 集合中检查，匹配根项目由 Task 2 完成。

- [ ] **1.4 增加失败测试后实现 debug 与 WorkerRequest 检查。** `debug + {"a.yml":" "}` 成功，`debug + {"a.yml":"{}"}` 验证失败；错误 kind、未知字段和空 resources 的 WorkerRequest 失败。
- [ ] **1.5 增加边界参数化测试。** 覆盖 `None`、列表、非字符串 value、101 项、空白超限、128/129 字符 key、路径/设备名、`A.yml` 与 `a.yml`、中文 UTF-8 大小。用 `normalize_resources({"a.yml": "x" * limit})` 检验等于上限成功、加一失败；总量用六个合法资源验证。`# comment` 和 `{}` 不被过滤。
- [ ] **1.6 运行与提交：** `uv run --frozen pytest tests/test_resources.py tests/test_commands.py -q`，再 `uv run --frozen ruff check src tests`；提交 `feat: define request-scoped YAML resources contract`。

## Task 2: 冷启动内存读取、禁止缓存落盘与 dbt worker

**Files:** Create `resource_adapter.py`, `resource_worker.py`, `tests/resource_helpers.py`, `tests/test_resource_adapter.py`; Modify `AGENTS.md`。

**Interfaces:**

```text
resource_adapter.execute_dbt(request: DbtJobRequest, project_dir: Path,
    profiles_dir: Path, artifact_dir: Path) -> int
resource_adapter.IncompatibleRuntimeError(RuntimeError)
resource_worker.main() -> int
tests.resource_helpers.ResourceProject = tuple[Path, Path, Path]
tests.resource_helpers.make_resource_project(tmp_path: Path) -> ResourceProject
tests.resource_helpers.call_worker(project: ResourceProject,
    request: dict[str, object], kind: str = "dbt") -> subprocess.CompletedProcess[str]
```

路径 tuple 顺序为 project_dir、profiles_dir、artifact_dir。helper 复制现有 dbt_project fixture 到 tmp_path，写测试专用 DuckDB profile（库文件在 tmp_path，schema 固定 main），创建 artifact_dir。fixture 和磁盘副本不属于接口传入资源；外部原文只构建为内存字符串。

helper 的基础实现如下，测试模块使用 `from resource_helpers import make_resource_project, call_worker` 导入（pytest 会将测试目录加入导入路径）：

```python
from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PROJECT_FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "dbt_project"
WORKER_MODULE = "dbt_metricflow_service.resource_worker"
ResourceProject = tuple[Path, Path, Path]

def make_resource_project(tmp_path: Path) -> ResourceProject:
    project = tmp_path / "project"
    profiles = tmp_path / "profiles"
    artifacts = tmp_path / "artifacts"
    shutil.copytree(PROJECT_FIXTURE, project)
    profiles.mkdir()
    artifacts.mkdir()
    database_path = json.dumps(str(tmp_path / "warehouse.duckdb"))
    profile = (
        "wrapper_fixture:\n  target: test\n  outputs:\n    test:\n"
        f"      type: duckdb\n      path: {database_path}\n      schema: main\n"
    )
    (profiles / "profiles.yml").write_text(profile, encoding="utf-8")
    return project, profiles, artifacts

def call_worker(
    project: ResourceProject, request: dict[str, object], kind: str = "dbt",
) -> subprocess.CompletedProcess[str]:
    project_dir, profiles_dir, artifact_dir = project
    environment = dict(os.environ)
    environment.update({
        "DBT_PROJECT_DIR": str(project_dir),
        "DBT_PROFILES_DIR": str(profiles_dir),
        "JOB_ARTIFACT_DIR": str(artifact_dir),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "PYTHONUTF8": "1",
        "PYTHONPATH": str(REPOSITORY_ROOT / "src"),
    })
    return subprocess.run(
        [sys.executable, "-m", WORKER_MODULE],
        input=json.dumps({"kind": kind, "request": request}),
        text=True, encoding="utf-8", capture_output=True,
        cwd=project_dir, env=environment, timeout=90, check=False,
    )
```

- [ ] **2.1 先实现测试 helper 和失败测试。** `call_worker` 使用 `sys.executable -m dbt_metricflow_service.resource_worker`，`subprocess.run(input=json.dumps({"kind": kind,"request":request}), text=True, encoding="utf-8", capture_output=True, timeout=90, cwd=project_dir)`。环境设置 `DBT_PROJECT_DIR`、`DBT_PROFILES_DIR`、`JOB_ARTIFACT_DIR`，关闭匿名遥测；测试时只把本仓 `src` 加入 PYTHONPATH，不加入 vendor。

```python
def test_memory_yaml_precedes_invalid_disk_yaml(tmp_path: Path) -> None:
    project = make_resource_project(tmp_path)
    source = project[0] / "models" / "orders.yml"
    original = source.read_text(encoding="utf-8")
    source.write_text("version: [broken\n", encoding="utf-8")
    marker = "MEMORY_RESOURCE_COMMENT_57bd"
    raw = original + f"\n# {marker}\n"
    result = call_worker(project, {
        "project": "sales", "command": "parse", "resources": {"orders.yml": raw},
    })
    assert result.returncode == 0, result.stderr
    semantic = project[2] / "semantic_manifest.json"
    assert semantic.is_file()
    assert source.read_text(encoding="utf-8") == "version: [broken\n"
    assert not list(project[2].rglob("*.msgpack"))
    assert not list(project[2].rglob("*.yml"))
    for path in project[2].rglob("*"):
        if path.is_file():
            assert marker.encode() not in path.read_bytes()
```

- [ ] **2.2 验证失败：** `uv run --frozen pytest tests/test_resource_adapter.py::test_memory_yaml_precedes_invalid_disk_yaml -q`，应因 worker 模块不存在失败。
- [ ] **2.3 增加已批准的仓库规则例外。** 仅 `resource_adapter.py` 允许从已安装固定包调用内部解析/配置 API；其他模块继续调用服务自己的接口。禁止 vendor 修改和源码路径导入保持不变。
- [ ] **2.4 实现 worker 输入边界。** `sys.stdin.buffer.read(MAX_WORKER_INPUT_BYTES + 1)`，上限设为 8 MiB；超限、JSON/模型验证失败只输出固定错误类别，返回 2。采用 WorkerRequest.model_validate_json，再从固定环境变量读取路径。dbt 分支调用 execute_dbt；此任务尚不开放 MetricFlow 分支，明确返回不支持，不回退 CLI。直接入口以 `raise SystemExit(main())` 退出。
- [ ] **2.5 实现资源感知 schema 读取。** 在 contextmanager 中临时替换 `dbt.parser.read_files.get_source_files`，保存原函数并在 finally 恢复。包装器使用原签名 `(project, paths, extension, parse_file_type, saved_files, ignore_spec)`：非根项目或非 Schema 调用原函数；根 Schema 使用下列算法。

```text
首次访问根 Schema：
  用上游 filesystem_search 对 all_source_paths 的 .yml/.yaml 建索引；
  按 original_file_path 去重，再按 basename.casefold() 聚合；
  将所有有效资源解析为唯一现有 FilePath 或首个 model-path 下的虚拟 FilePath；
  对参与覆盖的名称检查歧义、大小写冲突、.dbtignore、路径包含关系；
  在未忽略索引之外另查忽略匹配，禁止把已忽略的同名实体当成新增覆盖。
每次 extension 访问：
  枚举对应文件，命中资源则从原文构建 SchemaSourceFile，不打开实体文件；
  其余文件调用上游 load_source_file；
  附加该 extension 的虚拟文件，只附加一次；
  返回给原 read_files_for_parser 建立 manifest.files 和 parser 索引。
```

内存 SourceFile 用 `FileHash.from_contents(normalize_file_contents(content))`；`contents` 保留原文，`parse_file_type=ParseFileType.Schema`，`project_name` 来自根 Project；复用 `yaml_from_file(source_file, validate=True)` 和 `validate_yaml` 填充 dfy。对注释/空映射沿用上游返回行为，不引入自动回退。匹配存在时保留原 FilePath；虚拟 FilePath 的 modification_time 为 0，绝不能访问虚拟文件的 stat/file_size。

- [ ] **2.6 控制缓存、配置和错误输出。** 适配 contextmanager 将 `ManifestLoader.write_manifest_for_partial_parse` 暂时替换为不写入的函数，同时显式使用 `--no-partial-parse --no-use-v2-parser`。execute_dbt 调用现有命令构造器的普通路径（request.model_copy(update={"resources": {}})），去掉首个 `dbt` 后交给 dbtRunner，追加服务生成的 `--target-path`、`--log-path`、`--log-level-file none`。console 解析原文错误不能经默认日志直接回显：资源解析异常转为服务异常，只携带 name/line/column/固定类别；未识别的上游异常输出类型和固定说明，不调用包含原文的 repr。

同一 runtime context 还在已安装 `dbt.cli.main` 的 `load_dotenv` 绑定上安装窄包装：调用原加载函数后，重新设置父进程已固定的 project/profiles/target/log 路径、禁用文件日志和禁用替代 parser/partial parse 的环境值；finally 恢复绑定。该 context 必须包住整个 parse → mf 阶段，包括 mf setup 内的 debug；不 patch dotenv 包或通用文件 API。这样后续 dbtRunner 调用不能被项目 `.env` 再次改写产物路径。为这些允许的 hook 设置版本/签名检查。

- [ ] **2.7 版本门槛失败测试。** monkeypatch 已安装 distribution 版本或必要入口，assert execute_dbt 抛出 IncompatibleRuntimeError；不得因为缺少内部入口执行普通磁盘解析。检查 dbt/mf 版本及本任务使用入口的可调用性和参数名，不维护第二套兼容分支。
- [ ] **2.8 逐项补测试再实现。** 参数化文件名歧义、忽略文件、新增虚拟文件、不匹配资源、依赖包中同名 YAML、有效内存替换错误磁盘、无效内存不回退、未知 ref、已有过期缓存、纯注释/{}/中文、不同名称但重复模型定义。对基础项目文件树运行前后做内容哈希比较（只读项目测试用 parse）；查任务目录及日志中的注释标记。
- [ ] **2.9 运行与提交：** `uv run --frozen pytest tests/test_resources.py tests/test_resource_adapter.py -q`，`uv run --frozen ruff check src tests`；提交 `feat: parse request YAML from memory in an isolated worker`。冷启动或禁止缓存写入不能成立时停止后续任务，返回设计问题，不创建临时 YAML 作为替代。

## Task 3: MetricFlow 使用本次语义产物

**Files:** Modify `resource_adapter.py`, `resource_worker.py`, `commands.py`, `api.py`; Create `tests/test_resource_metricflow.py`。

**Interfaces:**

```text
resource_adapter.execute_metricflow(request: MetricFlowJobRequest, project_dir: Path,
    profiles_dir: Path, artifact_dir: Path) -> int
resource_adapter.TaskCLIConfiguration(CLIConfiguration)
resource_adapter.TaskCLIConfiguration.dbt_artifacts -> dbtArtifacts
```

将 adapter 白名单和固定版本常量从 api.py 移到 `commands.py` 的模块常量，api 和适配器统一引用，避免 worker 导入整个 FastAPI 模块。

- [ ] **3.1 写失败测试：默认旧 manifest 不能获胜。** 用 helper 创建项目，在基础 target 放入无效旧 manifest 哨兵；请求将 fixture 的 `"    metrics:\n      - name: revenue"` 替换为 `"    metrics:\n      - name: request_revenue"`（只替换 metrics 段，不能改列名），走 worker list_metrics。

```python
def test_metricflow_uses_request_manifest(tmp_path: Path) -> None:
    project = make_resource_project(tmp_path)
    raw = (project[0] / "models" / "orders.yml").read_text(encoding="utf-8")
    changed = raw.replace(
        "    metrics:\n      - name: revenue",
        "    metrics:\n      - name: request_revenue",
    )
    assert changed != raw
    stale = project[0] / "target"
    stale.mkdir()
    (stale / "semantic_manifest.json").write_text("invalid old artifact", encoding="utf-8")
    result = call_worker(project, {
        "project": "sales", "command": "list_metrics",
        "resources": {"orders.yml": changed},
    }, kind="metricflow")
    assert result.returncode == 0, result.stderr
    assert "request_revenue" in result.stdout
    assert (stale / "semantic_manifest.json").read_text() == "invalid old artifact"
```

- [ ] **3.2 验证失败：** `uv run --frozen pytest tests/test_resource_metricflow.py -q`，应因未支持 metricflow 分支失败。
- [ ] **3.3 实现 parse → mf。** 用 DbtJobRequest(project=request.project, command="parse", resources=request.resources) 调用 execute_dbt。只有退出码 0 才读取本任务 manifest 的 adapter_type；不支持时输出现有诊断代码并返回 2；绝不尝试 mf 或执行 dbt build。
- [ ] **3.4 实现配置桥接。** TaskCLIConfiguration 保存 artifact_dir；setup 指定 project/profiles 且 configure_file_logging=False。覆盖 dbt_artifacts 属性：复用 dbt_project_metadata 的 profile/project，通过固定上游 adapter 工厂取已注册 adapter，从 artifact_dir 的 semantic_manifest.json 调用上游 manifest parser，再构造 dbtArtifacts；不调用其读取项目默认产物的 load_from_project_metadata。

```python
configuration = TaskCLIConfiguration(artifact_dir)
configuration.setup(
    dbt_profiles_path=profiles_dir,
    dbt_project_path=project_dir,
    configure_file_logging=False,
)
arguments = build_metricflow_command(
    request.model_copy(update={"resources": {}}), project_dir, profiles_dir,
).argv[1:]
try:
    mf_cli.main(args=list(arguments), obj=configuration, standalone_mode=False)
except SystemExit as error:
    return error.code if isinstance(error.code, int) else 1
return 0
```

该代码位于适配器内部并使用已安装 CLI。setup 里的 dbt debug 也必须绑定本任务 target/logs 并禁用文件日志；让错误诊断中的 target_path 指向真实任务产物。`mf_cli` 即 `dbt_metricflow.cli.main.cli`，不得重写指标 SQL 或表格格式化。配置使用完成后在 finally 关闭已创建的 SQL client。

- [ ] **3.5 真实覆盖四种命令。** list_dimensions 用 request_revenue；explain 断言 SQL 中引用对应指标表达式；query 先用普通 dbt build 在该临时 DuckDB 建 orders/time_spine，断言查询数值符合 fixture。增加两个同时运行的 call_worker 使用同名文件和不同指标名、不同 artifact_dir，分别检查输出。
- [ ] **3.6 补充边界。** 基础 target 完全不存在时成功；服务显式路径覆盖继承的陈旧路径；项目 `.env` 把 DBT_TARGET_PATH/DBT_LOG_PATH/DBT_LOG_LEVEL_FILE/DBT_ENGINE_USE_V2_PARSER 指向冲突值时，parse 和 mf setup 仍受任务配置约束，基础 target/logs 均不被新建或更改；StarRocks parse 能成功但 mf 明确失败；无效 YAML 即使基础产物有效也不回退。断言没有执行 run/build，可用包装 execute_dbt 的 mock 检查只提交 parse，真实查询另行验证。
- [ ] **3.7 运行与提交：** `uv run --frozen pytest tests/test_resource_adapter.py tests/test_resource_metricflow.py -q`，再 ruff；提交 `feat: bind MetricFlow to request semantic artifacts`。若 Click 配置不能可靠注入，不扩大为全局文件系统替换，返回设计评审。

## Task 4: JobRunner 内存 stdin 传输与进程生命周期

**Files:** Modify `models.py`, `jobs.py`, `tests/test_jobs.py`, `tests/fixtures/fake_cli.py`。

**Interfaces:**

```text
CommandSpec.stdin_data: bytes | None = None  # repr=False
CommandSpec.use_job_artifacts: bool = False
JobRunner._feed_stdin(process: asyncio.subprocess.Process, payload: bytes) -> None
```

- [ ] **4.1 写失败测试。** 新 fake_cli 模式 `stdin-length` 读取 sys.stdin.buffer 并输出字节数；`stdout-before-stdin` 先输出 256 KiB 再读输入；`exit-before-stdin` 立即退出 7；`never-read-stdin` 睡眠。

```python
async def test_stdin_backpressure_does_not_deadlock(tmp_path: Path) -> None:
    runner = JobRunner(timeout_seconds=3.0, max_output_bytes=128)
    payload = b"x" * (512 * 1024)
    spec = dataclasses.replace(
        command_spec(tmp_path, "stdout-before-stdin"), stdin_data=payload,
    )
    submitted = await runner.submit("sales", spec)
    completed = await runner.wait(submitted.id)
    assert completed.status is JobStatus.SUCCEEDED
    assert str(len(payload)) in completed.stdout
```

- [ ] **4.2 验证失败：** `uv run --frozen pytest tests/test_jobs.py -k stdin -q`，应因 CommandSpec 没有 stdin_data 失败。
- [ ] **4.3 扩展既有 _run。** 只有 stdin_data 非 None 时开启 subprocess.PIPE；开始 stdin writer 和两个既有 readers，再等待子进程。writer 必须进入同一 timeout/取消范围；BrokenPipe/ConnectionReset 只表示子进程已停止接收，保留其真实退出码。其他 writer 错误使任务失败并终止进程。finally 关闭 stdin、取消/收尾 writer 和 readers，不遗留持有 payload 的 Task。

```python
async def _feed_stdin(process: asyncio.subprocess.Process, payload: bytes) -> None:
    if process.stdin is None:
        raise RuntimeError("stdin pipe was not created")
    try:
        process.stdin.write(payload)
        await process.stdin.drain()
    except (BrokenPipeError, ConnectionResetError):
        pass
    finally:
        process.stdin.close()
```

收尾可以补充 wait_closed，但必须有界且允许早退异常；不能把无限等 writer 放到 timeout 外。无 stdin 的原流程不增加无意义的 writer。

- [ ] **4.4 补失败测试再实现收尾。** 512 KiB + never-read-stdin 必须 timed_out；exit-before-stdin 保留 exit_code=7；发送期间 runner.close 后任务失败且进程树结束；完成 JobRecord.model_dump 不包含原文、argv 不包含原文；既有写锁、流式脱敏/截断测试通过。
- [ ] **4.5 运行与提交：** `uv run --frozen pytest tests/test_jobs.py -q`，再 ruff；提交 `feat: stream resource requests to worker stdin`。

## Task 5: 独立任务目录与安全清理

**Files:** Create `job_artifacts.py`, `tests/test_job_artifacts.py`; Modify `settings.py`, `main.py`, `api.py`, `jobs.py`, `tests/test_settings.py`, `Dockerfile`。

**Interfaces:**

```text
Settings.job_artifacts_root: Path  # default /workspace/job-artifacts
JobRunner.__init__(self, timeout_seconds: float, max_output_bytes: int,
    max_completed_jobs: int = 100, job_artifacts_root: Path | None = None) -> None
job_artifacts.create_job_directory(root: Path, job_id: UUID) -> Path
job_artifacts.remove_job_directory(root: Path, directory: Path) -> None
job_artifacts.recover_finished_directories(root: Path) -> None
```

job_artifacts_root 放在构造参数末尾，旧 runner 单元测试无需创建目录；资源任务必须有配置。根目录与项目根、profiles 根互不嵌套，防止恢复操作触及基础项目。Settings.from_environment 解析绝对路径；新字段使用默认值保证旧测试夹具兼容。

- [ ] **5.1 写失败测试。** UUID 命名目录创建/删除，拒绝越界及 symlink/reparse point 目标；清理未知名称目录不执行；资源任务结束成功/失败/超时都清理，无资源任务不创建目录。测试不要真的在工作区执行递归删除，全部使用 tmp_path。

```python
def test_cleanup_rejects_outside_directory(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    outside = tmp_path / "keep"
    root.mkdir()
    outside.mkdir()
    (outside / "keep.txt").write_text("keep", encoding="utf-8")
    with pytest.raises(ValueError):
        remove_job_directory(root, outside)
    assert (outside / "keep.txt").read_text() == "keep"
```

- [ ] **5.2 验证失败：** `uv run --frozen pytest tests/test_job_artifacts.py -q`，应因模块缺失失败。
- [ ] **5.3 实现创建与结束清理。** 父进程用 UUID 子目录，验证 resolve 后为 root 的直接子目录且不是链接；worker 环境中固定 JOB_ARTIFACT_DIR 和 DBT_TARGET_PATH，cwd 仍是基础项目。父进程在确认 worker 与后代已结束、reader 已收尾后清理。Windows 用 pathlib/shutil 同一 Python 实现，不跨 shell 拼接删除命令。删除失败仅记录目录 ID 和错误类型，不改变已确定的任务结果。
- [ ] **5.4 保守恢复。** 父进程在确认 worker 终止后创建固定的 `.finished` 标记，再尝试删除；启动恢复只删除合法 UUID、非链接且具有本服务完成标记的目录。缺少标记的目录无法证明没有孤儿 worker，保留并给出固定诊断，不在重启时盲目删除。单实例限制写入 README；完整自动清理未知孤儿不在本计划引入进程注册表。
- [ ] **5.5 配置与容器。** main 将 settings.job_artifacts_root 传给 runner；lifespan 启动时创建并验证目录、调用 recover_finished_directories。Dockerfile 在 USER app 前创建 `/workspace/job-artifacts` 并授予 UID 10001 写权限；不增加依赖和额外宿主挂载要求。测试环境使用 tmp_path 注入配置。
- [ ] **5.6 验证清理后的状态。** 确认 _tasks 不再持有 stdin bytes；queued 阶段取消也不创建多余目录；清理异常不掩盖退出码；同一项目不同 artifact_dir 仍然触发原写锁。运行 `uv run --frozen pytest tests/test_job_artifacts.py tests/test_jobs.py tests/test_settings.py -q`，再 ruff；提交 `feat: isolate and clean resource job artifacts`。

## Task 6: 命令构造与 worker 路由

**Files:** Modify `commands.py`, `resource_worker.py`, `tests/test_commands.py`。

**Interfaces:**

```text
commands.build_dbt_command(request: DbtJobRequest, project_dir: Path,
    profiles_dir: Path) -> CommandSpec
commands.build_metricflow_command(request: MetricFlowJobRequest, project_dir: Path,
    profiles_dir: Path) -> CommandSpec
commands._resource_command(kind: Literal["dbt", "metricflow"],
    request: DbtJobRequest | MetricFlowJobRequest,
    project_dir: Path, profiles_dir: Path) -> CommandSpec
```

公开函数签名保持不变；新 helper 只用于两条资源分支。

- [ ] **6.1 写失败测试：** 两个 builder 的空/全空白输入仍生成原 CLI argv，非空白资源生成固定 worker 模块，YAML 不在 argv/env 中。以下断言对 dbt 与 mf 参数化：

```python
def test_resource_command_keeps_yaml_only_in_stdin(
    project_dir: Path, profiles_dir: Path,
) -> None:
    raw = "version: 2\n# resource-only-marker\n"
    request = DbtJobRequest(project="sales", command="parse", resources={"a.yml": raw})
    spec = build_dbt_command(request, project_dir, profiles_dir)
    assert spec.argv == (sys.executable, "-m", "dbt_metricflow_service.resource_worker")
    assert spec.stdin_data is not None
    envelope = json.loads(spec.stdin_data)
    assert envelope["request"]["resources"] == {"a.yml": raw}
    assert raw not in " ".join(spec.argv)
    assert raw not in " ".join(spec.environment.values())
    assert spec.cwd == project_dir
    assert spec.write_operation is True
    assert spec.use_job_artifacts is True
```

- [ ] **6.2 验证失败：** `uv run --frozen pytest tests/test_commands.py -k resource -q`，应仍生成 dbt/mf argv 而失败。
- [ ] **6.3 实现最小分支。** dbt builder 起始位置增加以下分支；mf builder 将 kind 改为 `"metricflow"`。其余原有白名单构造不改。helper 用 WorkerRequest.model_dump_json().encode("utf-8") 构造 stdin，设置 kind 对应的 write_operation（dbt=True，mf=False），固定 project/profiles 环境，不从请求接受 paths。必须保持资源适配器中清空 resources 后复用 builder 的路径，避免 worker 递归启动 worker。

```python
if request.resources:
    return _resource_command("dbt", request, project_dir, profiles_dir)
```
- [ ] **6.4 worker 统一结果。** main 根据 kind 调用两个执行函数；退出码 int；未知内部错误输出固定类别、非零退出。不在整个 worker 外加“失败则原 CLI 重试”。parse 失败不得进入 mf；run/build 必须只执行一次。
- [ ] **6.5 运行与提交：** `uv run --frozen pytest tests/test_commands.py tests/test_resources.py tests/test_resource_adapter.py tests/test_resource_metricflow.py -q`，再 ruff；提交 `feat: route nonblank YAML resources through workers`。

## Task 7: HTTP 分流与有界请求体

**Files:** Create `request_limits.py`, `tests/test_request_limits.py`; Modify `api.py`, `tests/test_api_dbt.py`, `tests/test_api_metricflow.py`, `tests/test_api_health.py`。

**Interfaces:**

```text
request_limits.RequestBodyLimitMiddleware(app: ASGIApp, max_bytes: int = 8 * 1024 * 1024)
RequestBodyLimitMiddleware.__call__(scope: Scope, receive: Receive, send: Send) -> None
```

只对现有两个 POST 任务路径启用，不给 GET 或其他路由增加 body 缓冲。ASGIApp、Scope、Receive、Send 从 starlette.types 导入。

- [ ] **7.1 写失败 API 测试。** 没有基础 manifest 的 resources list_metrics 返回 202 且提交 worker；同请求 resources 全空白返回原来的 manifest 未生成错误。无 resources 的 StarRocks 仍 HTTP 422；有资源时先返回 202，执行失败由集成测试检查。debug 空白成功、非空白 422；响应任何字段不含 resources 原文。
- [ ] **7.2 验证失败：** `uv run --frozen pytest tests/test_api_dbt.py tests/test_api_metricflow.py -q`，新 tests 应暴露 adapter 预检查未分流。
- [ ] **7.3 修改路由：** `if not payload.resources:` 内保留原 metricflow adapter 检查，之后照常 build_metricflow_command；dbt 路由沿用 builder。不要在 HTTP 线程调用 parser。ready 增加任务目录可写状态，测试注入 tmp_path。
- [ ] **7.4 写 ASGI 层失败测试。** 用一个仅记录是否被调用的下游 app，receive 依次返回多个 `http.request` frame，累计超过 max_bytes 必须 413、下游没有被调用；精确等于上限成功；Content-Length 缺省或谎报较小仍按实际字节数拒绝；disconnect 不启动任务；非目标 GET 原样转发。

```python
async def test_chunked_limit_precedes_application() -> None:
    called = False
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal called
        called = True
    frames = iter([
        {"type": "http.request", "body": b"abcd", "more_body": True},
        {"type": "http.request", "body": b"efghi", "more_body": False},
    ])
    async def receive() -> Message:
        return next(frames)
    sent: list[Message] = []
    async def send(message: Message) -> None:
        sent.append(message)
    middleware = RequestBodyLimitMiddleware(app, max_bytes=8)
    scope: Scope = {"type": "http", "method": "POST", "path": "/v1/dbt/jobs", "headers": []}
    await middleware(scope, receive, send)
    assert not called
    assert sent[0]["status"] == 413
```

- [ ] **7.5 实现有界预读取。** 在调用下游前累计不超过上限的 bytearray；Content-Length 超限可直接拒绝，实际 frame 超限也拒绝；仅在完整合法 body 到达后重放一次给下游。使用 `JSONResponse(status_code=413, content={"detail":{"code":"request_too_large","message":"request body exceeds limit"}})`，不返回内容片段。重放后继续委托原 receive 处理断连，防止重复 replay。不使用无限制 request.body()。
- [ ] **7.6 运行与提交：** `uv run --frozen pytest tests/test_request_limits.py tests/test_api_dbt.py tests/test_api_metricflow.py tests/test_api_health.py -q`，再 ruff；提交 `feat: expose resource-aware jobs with bounded request bodies`。

## Task 8: 端到端验收、部署说明与全量检查

**Files:** Create `tests/test_resources_e2e.py`; Modify `README.md`, necessary tests from previous tasks。

**Interfaces:** 仅使用现有 HTTP 提交/轮询和 JobRunner.wait；不引入新生产 API。

- [ ] **8.1 写并运行真实 HTTP + DuckDB 测试。** 用 tmp_path 项目、profiles、artifacts 配置真实 app/runner；每例资源仅在内存构建。覆盖 parse/compile/seed/run/test/build 和 mf 四种命令；建表只在测试专用库中进行。对于 seed，在 tmp_path 创建常规 seed CSV，用传入 YAML 的 seed 配置验证其生效；对于 test，用传入的 data_tests 制造实际失败，证明没有忽略 resources。运行 `uv run --frozen pytest tests/test_resources_e2e.py -q`，新增用例未满足时先记录失败再改实现。
- [ ] **8.2 将设计验收矩阵映射到测试。** 同名覆盖、空白回退、混合输入、无效不回退、无基础 target、陈旧默认 manifest、并发两版本、随后普通请求恢复基底、生命周期清理、原文注释标记、磁盘 YAML 不变均必须有真实测试。对未被 mock 覆盖的集成路径补测试，禁止只检查 helper 是否被调用。
- [ ] **8.3 更新 README，写入以下语义。**

```text
resources 为可选的文件名到 YAML 原文映射，仅本次请求有效。
缺失、空字符串或纯空白条目使用默认定义；非空白语法/引用/校验错误直接失败。
注释和 {} 是非空白内容，按 dbt 原有语义解析。
名称唯一时优先内存，多处同名时报错；不接受文件路径或项目配置上传。
非空白资源的 MetricFlow 请求自动 parse；不要求基底已有 manifest。
无有效资源的请求保持既有行为；debug 拒绝非空白资源。
资源原文/缓存不落盘，派生产物位于 JOB_ARTIFACTS_ROOT 并在任务结束后清理。
该目录必须由单服务实例专用；无法确认已结束的残留目录保留供维护处理。
StarRocks 仍不支持 MetricFlow；资源模式在异步任务中报告该错误。
```

示例给出 dbt parse 和 mf list_metrics 的 JSON，必须是真实可解析的 YAML；可用 fixture 内容通过 Python json.dumps 生成文档示例，不把真实 profile 凭据写进文档。

- [ ] **8.4 完整验证。** 执行以下命令并记录真实结果，不因工具不可用而虚构通过：

```powershell
uv run --frozen pytest
uv run --frozen ruff check src tests scripts
git diff --check
git diff --submodule=short -- vendor
```

- [ ] **8.5 容器验证。** 若 Docker 可用，`docker build -t dbt-metricflow-service:resources .`，确认 UID 10001 可写默认任务目录；启动映射临时测试项目/profile，执行一次 resources parse 和 mf list_metrics，并检查不落原文。Windows 路径测试在当前宿主运行；Docker 不可用则明确记录此验证缺口，不安装或改动上游来绕过。
- [ ] **8.6 提交：** `test: verify in-memory resource execution end to end`。检查工作区和 gitlink，最终报告范围、通过测试、未运行的外部验证，以及针对固定版本内部接口的维护限制。

## 自审映射

| 设计要求 | 计划任务 |
| --- | --- |
| resources 契约、空白回退、原文保留、上限 | 1、7 |
| 根项目身份、默认回退、忽略与歧义 | 2 |
| 冷启动、无原文缓存、内部版本门槛 | 2 |
| MetricFlow 当前语义来源、adapter 限制 | 3、7 |
| 管道传输、超时、取消、原写锁 | 4、6 |
| 独立产物、恢复、非 root 权限 | 5 |
| 两个现有 HTTP 入口及默认路径兼容 | 6、7 |
| 真实命令效果、并发、无串扰 | 3、8 |
| 文档、仓库规则、完整测试 | 2、8 |

实现前仅需评审本计划并选择执行方式。建议同一执行者按顺序实施：适配器、worker、管道与路由紧密依赖；先验证 Task 2/3，再继续集成。独立最终代码评审仍需覆盖内部 hook 和资源生命周期。
