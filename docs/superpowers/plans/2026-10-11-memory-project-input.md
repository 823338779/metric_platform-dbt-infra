# dbt 与 MetricFlow 内存项目输入实施计划

> **执行要求：** 使用 `superpowers:executing-plans` 或用户选择的 `superpowers:subagent-driven-development` 逐项执行，使用复选框记录进度。执行方式待用户选择；不得提前启动子代理。

**目标：** Git 对象、pack、项目及依赖源码不写入本地文件，dbt 与 MetricFlow 直接消费内存输入，同时保留数据库封存和历史查询。

**架构：** dbt vendor 提供内存项目、配置加载、资源解析和依赖获取入口；MetricFlow vendor 提供从配置与语义产物建立引擎的入口。应用层只负责固定 Git 版本、数据库快照、发布约束和任务编排，删除源码目录往返与临时 JSON 传参。

**技术栈：** 当前锁定的 Python、dbt、MetricFlow、PostgreSQL、pytest；新增 Git 依赖优先采用 Dulwich，在任务 2 中完成版本与兼容性核验后锁定。

**设计依据：** [已确认设计](../specs/2026-10-11-memory-project-input-design.md)。本计划中的路径均相对于 `dbt-metricflow-service/`。

## 全局约束

- Git 对象库、下载 pack 和项目源码均不得写入本地文件，不以临时 bare 仓库或 tmpfs 代替。
- PostgreSQL 可以继续封存源码和构建产物，保留任务恢复、历史源码查看和固定 buildId 查询。
- 保留现有公开 HTTP 契约、固定提交身份、构建与部署规则，以及现有资源类型限制。
- 改动位于本服务及其两个 vendor 子仓库，在当前工作区直接开发。
- 不生成内存输入对应的 partial_parse.msgpack；允许当前派生产物与日志，禁止日志输出原文和凭据。
- 不全局替换 open/os/pathlib；不增加任意虚拟文件系统、HTTP 原文上传入口或产品外的抽象层。
- 内存项目保留原始 bytes 和文件可执行属性；缺失、空文件、无效文件是三个不同状态。
- 新字段与关键逻辑块编写注释，应用层新增函数使用完整类型注解；遵守各 vendor 最近的 AGENTS.md。
- 不删除、弱化或改写已有测试以适配新实现；新行为用新增测试覆盖，保留独立使用的旧入口。
- 构建与验证命令统一维护在各 README；下文只给出测试选择器及通过标准，不复制维护另一套命令。

## 优先审查的五类边界

1. 与磁盘同名的缺失、空白或无效内存文件：不得发生内容回退。归属任务 1、3。
2. Unicode、Windows 大小写碰撞、路径穿越、Git 链接和包内链接：不能越界或被静默规范化成另一份文件。归属任务 1、2、4。
3. Git pack、delta 解码及压缩包膨胀超过预算：在读取与解码过程中终止，不能下载完才检查。归属任务 2、4。
4. 依赖同名、传递依赖与本地依赖循环：沿用原生冲突语义并稳定失败，不能漏加载或读磁盘缓存。归属任务 4。
5. A 构建失败后执行 B，或取消与租约过期时输入仍被线程使用：不泄漏快照、不提前释放锁、不串用 manifest。归属任务 3、7、8。

## 文件与接口边界

- `vendor/dbt/core/dbt/contracts/project_input.py`：不可变 `ProjectFile` 与 `ProjectInput`，无 Git/数据库业务。
- `vendor/dbt/core/dbt/clients/memory_git.py`：受限内存 Git 传输与对象读取，供 Git 依赖和服务共同使用。
- `vendor/dbt/core/dbt/config/memory.py`：从内存项目与 profile 原文构造原生配置。
- `vendor/dbt/core/dbt/parser/read_files.py`、`manifest.py`：内存资源发现与原生解析接入。
- `vendor/dbt/core/dbt/deps/memory.py`：锁定依赖的内存解析、获取和展开。
- `vendor/metricflow/dbt-metricflow/dbt_metricflow/api.py`：显式程序化引擎入口。
- `src/dbt_metricflow_service/storage/artifacts.py`：文件集合与既有数据库封存结构之间转换。
- `src/dbt_metricflow_service/platform/` 与 `execution/embedded.py`、`runtime/executor.py`：消费上述入口，移除本链路源码目录操作。

新增模块只服务列出的明确边界，不再增加 Repository/Port/Facade 包装层。

## 任务 1：dbt 内存输入模型与配置加载

**文件：** 新增 `vendor/dbt/core/dbt/contracts/project_input.py`、`config/memory.py`；修改 `config/project.py`、`profile.py`、`runtime.py`；新增 `vendor/dbt/tests/unit/config/test_memory_project.py`。

**产出接口：**

- `ProjectFile(content: bytes, executable: bool = False)`：不可变文件数据。
- `ProjectInput(files: Mapping[str, ProjectFile])`：复制并只读持有规范相对路径映射；禁止外部字典后续修改影响输入。
- `ProjectInput.read_bytes(path: str) -> bytes`、`read_text(path: str) -> str`：UTF-8 解码，缺失明确失败。
- `ProjectInput.subproject(path: str) -> ProjectInput`：受控目录切片；`with_files(updates: Mapping[str, ProjectFile]) -> ProjectInput`：返回新输入，不修改原件。
- `load_memory_config(project: ProjectInput, profiles: str, args: Any) -> RuntimeConfig`：复用 PartialProject/Profile 原生渲染和校验；`args` 是 dbt 自有运行参数对象。

- [ ] 新增失败测试：`test_raw_bytes_and_modes_are_preserved`、`test_input_does_not_follow_mutated_mapping`、`test_missing_empty_invalid_input_never_falls_back_to_disk`、`test_memory_project_paths_and_selectors`、`test_profile_env_vars_and_target`。断言读取原文逐字节一致、空内容不回退、非法 YAML 返回原生错误、目录/selector/target 与磁盘配置结果一致。
- [ ] 使用 dbt README 支持的单测入口运行 `tests/unit/config/test_memory_project.py`，确认失败来自未实现接口。
- [ ] 实现模型和配置入口；复用现有项目/包配置转换，不重新定义 YAML 语义。逻辑输入路径与绝对产物输出路径分离，保留错误中的相对路径。禁止 credential 进入可封存 ProjectInput。
- [ ] 重跑新测试和已有 `test_project.py`、`test_profile.py`、`test_runtime.py`；全部通过才进入下一任务。

## 任务 2：无磁盘 Git 获取和服务来源解析

**文件：** 新增 `vendor/dbt/core/dbt/clients/memory_git.py`、`vendor/dbt/tests/unit/clients/test_memory_git.py`；修改 `vendor/dbt/core/pyproject.toml`、服务 `uv.lock`、`platform/bindings.py`、`platform/source.py`；新增 `tests/test_memory_git_source.py`。

**接口：**

- `GitLimits(max_pack_bytes: int, max_object_bytes: int, max_total_object_bytes: int, max_objects: int, timeout_seconds: float)`：传输与解码上限，不扩散到产品 HTTP。
- `MemoryGitRepository.fetch(repository: str, revisions: Sequence[str], limits: GitLimits) -> MemoryGitRepository`；`read_tree(commit_sha: str) -> ProjectInput`；`is_ancestor(ancestor: str, descendant: str) -> bool`；对象通过上下文管理关闭。
- `remote_head(repository: str, branch_name: str, limits: GitLimits) -> str | None`：只读取精确 ref，网络失败抛异常。
- 服务新增 `resolve_commit_input(binding: ProjectBinding, commit_sha: str, limits: GitLimits) -> tuple[ProjectInput, str]`，返回筛选后的工程与现有算法的 projectDigest。

- [ ] 核对 Dulwich 固定版本的 Python 支持、许可证、安全公告、MemoryRepo/MemoryObjectStore 和 fetch_pack 实际实现；验证 pack 与 delta 解码不借用 TemporaryFile，确认认证配置与 SHA 格式支持。发现不兼容时修复明确缺口，不改回磁盘获取。
- [ ] 新增失败测试：精确旧 SHA、缺失提交、分支不存在与网络失败区分、祖先关系、文件 mode、非默认资源目录、链接/子模块拒绝、中文路径、大小写碰撞。
- [ ] 新增传输和解包超限测试，断言错误发生时未创建任何 pack/对象/源码文件。服务采用 64 MiB pack、64 MiB 单对象、256 MiB 解码对象总量、100000 个对象、60 秒获取预算；项目筛选后保留原 _git 的 8 MiB 单次输出等价限制（包括单个 blob），并执行 ArtifactStore 单文件与集合限额，不误将 8 MiB 改成整个工程总量。
- [ ] 实现受限内存获取；共享基础传输用于服务和 dbt Git 依赖，不在 service 和 vendor 内各复制一套。保留已有路径和摘要检查，先校验再暴露输入。
- [ ] 运行新 Git 测试、既有 `tests/test_platform_bindings.py` 和分支来源相关测试；本地、HTTP(S)、SSH 使用受控测试服务验证，认证凭据仅置于测试进程环境。测试源仓库本身可在系统临时目录，接收端禁止写出对象。

## 任务 3：dbt 程序化解析和执行消费内存项目

**文件：** 修改 `vendor/dbt/core/dbt/cli/main.py`、`requires.py`、`flags.py`、`config/runtime.py`、`parser/read_files.py`、`parser/manifest.py`；新增 `vendor/dbt/tests/unit/parser/test_memory_read_files.py`、`vendor/dbt/tests/functional/memory_input/test_memory_project.py`。

**接口：** `dbtRunner` 保留现有参数，增加仅关键字参数 `project_input: ProjectInput | None = None`、`profiles: str | None = None`；继续通过 `invoke(args)` 返回原生 `dbtRunnerResult`。`RuntimeConfig` 携带本次 project_input，依赖配置各自携带对应输入；不得使用模块全局快照。

- [ ] 新增失败测试：`test_memory_parse_matches_filesystem_manifest`、`test_memory_compile_uses_macros_and_refs`、`test_no_project_read_or_partial_parse_write`、`test_failed_invocation_does_not_leak_input`。忽略 invocation 时间等易变字段后比较资源与边；检查原文、诊断路径及编译 SQL。
- [ ] 运行新增测试，确认缺失内存入口的预期失败。
- [ ] 接通命令 flags/preflight/config/manifest 全链路，避免前置存在性检查仍要求真实 dbt_project.yml。内存资源读取生成 SourceFile，复用原生 file type 规则与 YAML 校验，不直接滥用 ReadFilesFromDiff。
- [ ] 原生 adapter 宏只从安装目录读取；用户项目与依赖不回退。内存模式不读写 partial_parse.msgpack，产物只能写明确输出目录。源码输入不用于 cwd 切换；不为满足路径检查生成空项目文件。
- [ ] 运行新单测、原有 read_files/manifest 单测，以及真实数据库下的内存 parse/compile/build 对照测试。覆盖 `.dbtignore`、docs、fixture、selector、嵌套非默认目录和空文件。

## 任务 4：依赖获取与加载全程内存化

**文件：** 新增 `vendor/dbt/core/dbt/deps/memory.py`；修改 `deps/base.py`、`git.py`、`local.py`、`registry.py`、`tarball.py`、`private_package.py`、`resolver.py`、`task/deps.py`、`config/runtime.py` 中实际需要接入的边界；新增 `vendor/dbt/tests/unit/deps/test_memory_deps.py`。

**接口：** `resolve_memory_dependencies(project: ProjectInput, renderer: PackageRenderer, limits: GitLimits) -> Mapping[str, ProjectInput]`，键为解析后的 dbt package 名称。内存 `deps` 将结果保存在 runner 当前输入对应的依赖集合中，后续调用复用；配置改变时失效。磁盘模式沿用既有 install 行为。

- [ ] 新增失败测试：本地、锁定 registry、Git 固定 revision、tarball 及传递依赖；相同包名冲突、本地循环、包路径穿越/链接、压缩膨胀超限、锁文件不一致。断言依赖宏实际参与编译，所有输入目录和包缓存没有写操作。
- [ ] 运行新增测试确认失败，再实现锁文件解析与内存安装分支；复用原生包解析、版本/名称校验和认证约定，不重写另一套解析器。
- [ ] 包内容使用有界流读入和受限逐项解包，不调用 extractall 或磁盘下载缓存。复用任务 2 的 Git 传输限制；HTTP 包复用同等传输、单文件、解码总量和文件数预算。
- [ ] 本地依赖从同一快照切片，外部依赖拥有独立 ProjectInput；同一个依赖不可因不同父节点重复下载。服务既有锁文件要求保持，不接受浮动分支替代锁定版本。
- [ ] 运行新增测试、原生 `tests/unit/deps/` 和带依赖的内存真实引擎测试，确认磁盘 CLI 路径无回归。

## 任务 5：MetricFlow 显式内存初始化入口

**文件：** 新增 `vendor/metricflow/dbt-metricflow/dbt_metricflow/api.py`；修改 `cli/dbt_connectors/dbt_config_accessor.py` 中可复用的语义转换边界；新增 `vendor/metricflow/dbt-metricflow/tests_dbt_metricflow/test_memory_api.py`。

**接口：** `create_engine(runtime_config: RuntimeConfig, semantic_manifest: str | SemanticManifest, *, sql_client: SqlClient | None = None) -> MetricFlowEngine`。配置和 adapter 的建立遵循 dbt 当前调用生命周期；可传入应用现有 StarRocksSqlClient。此入口不设置文件日志、不搜索项目目录、不读取语义 manifest 文件。

- [ ] 新增失败测试：`test_create_engine_from_manifest_text_without_files`、`test_create_engine_from_manifest_object`、`test_supplied_sql_client_is_used`、`test_invalid_semantic_manifest_is_rejected`；断言 list_metrics、维度枚举和 explain 使用传入定义。
- [ ] 使用 MetricFlow 规定的 hatch 测试入口运行该文件，确认失败后实现接口，复用 SemanticManifestLookup 和原生语义转换。
- [ ] 重跑新增测试及现有 CLI 配置/查询测试，保持 CLI 文件入口；该入口不引入 dbt-service 依赖。

## 任务 6：数据库直接封存与恢复内存文件集合

**文件：** 修改 `src/dbt_metricflow_service/storage/artifacts.py`；新增 `tests/test_memory_artifacts.py`。

**接口：**

- `ArtifactStore.capture_files(project_id: str, files: Mapping[str, ProjectFile], *, producer_attempt_id: str | None = None, kind: str = SOURCE, metadata: JsonObject | None = None) -> str`。
- `ArtifactStore.read_files(set_id: str) -> Mapping[str, ProjectFile]`：仅返回校验通过的 SEALED 集合。
- 原目录接口与内存接口共用 SQL 写入、摘要、大小和封存校验，不增加数据库迁移。

- [ ] 新增失败测试：同字节/路径/mode 的目录输入与内存输入摘要相等；旧快照读回原文；损坏、越界、超限、未封存集合拒绝；事务失败不留下半份可读快照。
- [ ] 通过隔离测试库运行新增测试确认失败，实现接口并复用已有校验；避免读取后再验证造成无界数据库字节加载。
- [ ] 重跑新测试及 `test_runtime_artifacts.py`、`test_job_artifacts.py`，确认封存、历史迁移与 GC 行为保持。

## 任务 7：应用层删除源码目录与临时 JSON 桥接

**文件：** 修改 `platform/bindings.py`、`source.py`、`template.py`、`namespace.py`、`build.py`、`metricflow.py`、`catalog.py`、`sealed_catalog.py`，`execution/models.py`、`embedded.py`，`runtime/executor.py`；按引用检查清理已失去调用者的辅助函数。新增 `tests/test_memory_execution.py`。

**接口与迁移：**

- 模板策略新增 `validate_input_templates(project: ProjectInput) -> None`；命名注入新增 `prepare_publication_input(project: ProjectInput, run_id: UUID, schema: str) -> tuple[ProjectInput, str]`，返回执行副本与 prefix。
- `execute_programmatic_input(runtime_config: RuntimeConfig, semantic_manifest: str, input_data: JsonObject, *, catalog: JsonObject | None = None) -> JsonObject` 使用任务 5 入口，保留现有四种模式和清理行为。
- 新增 `run_embedded_call(project: str, invoke: Callable[[], JsonObject], timeout: float, max_output_bytes: int, authorize: Callable[[], Awaitable[None]]) -> tuple[JobRecord, JsonObject | None]`；与原 run_embedded 共用锁、日志和清理机制，消除输入/输出 JSON 文件。
- 构建阶段 dbtRunner 使用同一个内存执行输入，发生命名注入后重新建立解析上下文。源码快照用 capture_files，查询用 read_files；派生产物可以继续落盘，但封存时与内存执行输入合并传入 capture_files。

- [ ] 新增失败测试：Git→数据库→dbt 无源码写出；历史查询不调用 materialize；MetricFlow 不生成 input.json/output.json；命名副本不修改封存原文；模板规则在非默认目录和依赖中继续生效。
- [ ] 新增 `test_cancel_waits_for_engine_before_releasing_input`、`test_lease_loss_prevents_engine_write`、`test_build_b_does_not_reuse_failed_build_a_manifest`，固定租约、锁及取消语义。
- [ ] 实施上述接口并切换新任务链路；PREVIEW 使用固定目录数据，cleanup 只加载连接所需配置，不能要求失败构建已经有语义产物。
- [ ] 历史构建从旧数据库文件集合直接恢复输入与语义产物，工具链不匹配仍按原规则处理；不重新解析最新 Git 或当前部署。
- [ ] 对源码、Git、依赖缓存路径的写入设置测试拦截，对派生产物目录设允许范围；断言发生过任何禁止写入即失败，而非只检查运行后目录为空。
- [ ] 运行新增测试及 embedded/runtime execution、publication build、template、namespace、queries、options、v3 build execution 测试；保持已有断言原样。

## 任务 8：真实引擎验收、文档与分仓提交

**文件：** 新增 `tests/integration/test_memory_project_flow.py`；更新服务 `README.md`、dbt `README.md`、MetricFlow `dbt-metricflow/README.md`；按 dbt 规则新增 changie 条目；检查并按需更新实际工具链指纹实现及 `tests/test_runtime_toolchain.py`。

- [ ] 执行前确认运行环境加载修改后的 vendor，而不是旧 wheel；以模块路径和新入口测试为证据。更新锁文件后核对仅包含本次必要依赖变化。
- [ ] 在 README 维护新增定向测试命令，再按命令运行真实 Git 输入、dbt 构建、MetricFlow 四种查询、命名隔离、取消/失败及数据库恢复测试。SSH/HTTPS 获取测试不能由普通本地路径测试代替；未验证的认证形式明确记录。
- [ ] 运行服务完整 pytest/Ruff、dbt 相关单测/功能测试与代码检查、MetricFlow 的规定 lint/test。使用隔离 PostgreSQL 测试库；将引擎能力测试与 StarRocks 真实验证的证据分开报告。
- [ ] 文档说明新的程序化入口、禁落盘范围、允许的派生产物、内存预算和受支持的认证方式；删除“vendor 不由本服务改造”等失效描述。更新工具链指纹覆盖新增 vendor 能力，不伪造历史版本兼容性。
- [ ] 对根仓库、服务、两个 vendor 分别检查 diff/status，包括隐藏子模块变更。已有服务未提交工作保留；只暂存本任务新增文件与对应改动块。
- [ ] 按独立仓库依赖顺序提交：dbt vendor、MetricFlow vendor、服务中的本任务改动及采用的 vendor Git 链接。建议消息分别为 `feat: support in-memory dbt project execution`、`feat: expose in-memory MetricFlow initialization`、`feat: execute fixed Git builds without local source files`。不更新工作区根仓库的 Git 链接，不推送远端。
- [ ] dbt vendor 遵守已有签名规则，不能擅自生成密钥或关闭签名；如签名条件不足，报告具体阻碍，不把未提交报告为已交付提交。
- [ ] 清理仅本任务创建且不再需要的系统临时目录，核对绝对路径及归属后再删除。最终报告实现范围、验证证据、提交与尚存限制。

## 自审与执行交接

设计覆盖映射：Git 无磁盘获取→任务 2；dbt 原文入口→任务 1、3；依赖与原生校验→任务 4；MetricFlow→任务 5；数据库恢复→任务 6；应用简化与生命周期→任务 7；历史与实际引擎验收→任务 8。五类审查边界均已分配测试。

推荐当前会话顺序执行：这些任务共享 ProjectInput、RuntimeConfig 与已有未提交的 embedded 调用实现，依次接入能减少交叉修改。若用户选择子代理方式，也按依赖顺序实施并逐项评审，不让多个代理同时编辑上述共享文件。

状态：计划已自审，待用户评审并选择执行方式。尚未实施产品代码或运行产品测试。
