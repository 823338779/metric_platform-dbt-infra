# dbt 与 MetricFlow 内存项目输入设计

状态：用户已于 2026-10-11 确认设计，进入实施计划阶段，尚未实施。

## 目标与已确认边界

用户要求适当修改 vendor 内的 dbt、MetricFlow，为两者提供内存原文输入能力，并简化应用层。已进一步确认：

- Git 对象库、下载 pack 和项目源码均不得写入本地文件，不以临时 bare 仓库或 tmpfs 代替。
- PostgreSQL 可以继续封存源码和构建产物，保留任务恢复、历史源码查看和固定 buildId 查询。
- 保留现有公开 HTTP 契约、固定提交身份、构建与部署规则，以及现有资源类型限制。
- 改动位于本服务及其两个 vendor 子仓库，在当前工作区直接开发。

本地派生产物与服务日志不属于源码输入禁写范围。允许生成现有 manifest、编译 SQL 和执行结果文件；不得将原始项目文件或 Git 对象伪装成缓存、日志或临时产物落盘。为避免内存模式缓存还原源码，不生成 partial_parse.msgpack。

## 当前问题

Git 获取流程创建 bare 仓库，读取目标 tree/blob，再写出 run 目录。RuntimeExecutor 随后让 ArtifactStore 从目录读取源码写入数据库，删除目录，又从数据库还原 project 目录执行。

dbtRunner 虽已在进程内运行，但项目配置、资源发现和依赖加载仍基于路径。MetricFlow 的核心引擎可消费 SemanticManifestLookup，而 dbt-metricflow 的配置入口仍从项目目录和 semantic_manifest.json 加载。应用层还通过临时 input.json/output.json 传递进程内调用的参数和结果。

因此不能只修改 YAML loader，也不能通过应用层全局 monkey patch open/os/pathlib 完成。

## 方案比较与选择

1. 内存文件系统：兼容现有路径行为，但不满足直接提供原文与取消本地 Git 对象库的确认范围，排除。
2. vendor 提供显式内存项目入口，应用层传递源码快照：推荐。复用解析、校验、编译与执行逻辑，集中处理输入边界。
3. 重写全部引擎 I/O：包含产物、日志与适配器底层操作，超出本次必要范围，排除。

## 数据流与职责

```text
受控 repository + 固定 commitSha
  → 内存 Git 对象库
  → 经路径、模式、大小与摘要校验的项目快照
  → PostgreSQL 封存 / 恢复为内存快照
  → dbt 内存项目入口 → 原生 Manifest 与执行产物
  → MetricFlow 程序化入口 → 校验、选项、查询结果
```

### Git 获取

采用支持内存对象库的 Git 实现，优先评估 Dulwich MemoryRepo；不自行实现 Git 协议。它是新增运行时依赖，锁定版本前需核实 Python 兼容性、许可证、安全公告与传输实现。官方说明见 [MemoryRepo](https://dulwich.io/api/dulwich.repo.MemoryRepo.html) 和 [Dulwich](https://github.com/jelmer/dulwich)。

获取指定提交所需对象，禁止先写 pack 再读取。保留精确 SHA 核对、非默认资源目录、本地依赖目录、文件 mode、摘要算法和拒绝链接的现有行为。对传输、解包和快照分别设置有界资源使用；不能只在完整下载后检查总大小。

分支 head 读取与祖先检查使用同一内存 Git 能力，避免 source.py 的祖先核对继续创建 bare 仓库。保持“不存在”和“网络失败”的区别。验证受支持的本地、HTTP(S)、SSH 输入以及现有认证配置的兼容性；不静默降级到磁盘 Git。

### dbt vendor

增加显式的内存项目输入类型，保存规范化相对路径、原始 bytes 与必要文件属性；项目名称、资源目录与依赖关系仍由原生项目配置决定。输入类型不承载 Git、数据库或服务业务规则。

程序化入口接受内存项目及 profile 配置，文件系统 CLI 入口保持原行为。内存模式覆盖项目配置、selectors、YAML、SQL、宏、文档、测试 fixture 和现有受支持的依赖项目加载；资源解析仍进入原生 SourceFile/SchemaSourceFile 和 ManifestLoader。

缺失文件视为缺失，空文件视为空文件，无效内容仍报错；不回退读取磁盘上同名项目。保留原始路径用于诊断和资源来源。内置 adapter 宏可读取安装目录，但用户项目与依赖源码不可因此回退落盘。

已锁定的远端依赖在内存中获取与解包，本地依赖来自同一快照；不能以取消 deps 支持来缩小范围。依赖下载、Git 依赖和包缓存不得偷偷写出项目源码。受控发布配置与命名宏作为独立执行快照上的内存变更，不修改原始封存快照。

产物输出目录与输入项目逻辑路径分开。内存模式禁用磁盘 partial parse 缓存；后续命令可复用当前执行内的原生对象，复用前需明确配置与快照身份，避免串用版本。

### MetricFlow vendor

在 dbt-metricflow 的程序化连接入口支持已加载的项目/profile、adapter 和语义 manifest 原文或对象。复用现有语义转换与 MetricFlowEngine，不再要求调用方制造项目目录或 semantic_manifest.json 文件。

查询恢复时从数据库读取固定构建的语义产物和必要配置，在内存中完成初始化。保留现有 StarRocks 客户端选择、连接清理、四种查询模式及校验行为，不重写 MetricFlow 规划器。

### 应用层与存储

ArtifactStore 增加接收和返回内存文件集合的接口，与原有目录接口复用路径、大小、内容哈希、可执行属性和封存校验；数据库 schema 与历史记录保持兼容。

新构建直接将内存 Git 快照封存，执行与重试直接恢复内存快照。构建模板约束、命名注入、来源提取改为消费相同快照。历史目录接口只有在仍有独立调用方时保留，不增加新的双轨业务模式。

引擎调用直接传递类型化参数、返回对象，删除本链路为进程内 MetricFlow 调用创建 input.json/output.json 的桥接逻辑。保留现有串行锁、租约检查、取消等待、超时与 OUTCOME_UNKNOWN 语义。

## 验收标准

1. 从固定旧提交获取正确原文；过程中没有 Git 对象、pack、原始项目或依赖源码文件落盘。
2. 对同一工程，文件模式和内存模式的资源、依赖、编译结果及诊断语义一致；覆盖非默认目录、宏、引用、空文件和无效 YAML。
3. 覆盖本地及锁定远端依赖；缺失文件不得读取同名磁盘文件补足；不同构建和异常退出后不泄漏输入状态。
4. 数据库快照可以恢复内存执行，原文与摘要一致；旧封存构建继续支持目录、源码、选项和查询。
5. 真实引擎构建及 QUERY、EXPLAIN、PREVIEW、DIMENSION_VALUES 通过；构建失败、取消、租约失效和版本隔离行为保持。
6. 为禁止的源码路径操作设置可失败的测试边界，并检查执行临时目录；仅检查目录最终已删除不构成未落盘证据。
7. 先运行新增最小回归测试，再运行相关 vendor 测试、服务 pytest/Ruff 和隔离数据库真实引擎验收。按 README 与适用规则执行更广泛检查，记录未能执行的项目。

## 工作区与交付约束

服务当前已有未提交的进程内引擎改动，README、execution/models.py、platform/metricflow.py、runtime/executor.py、runtime/worker.py、集成测试，以及未跟踪的 embedded.py/测试均须保留并作为现状核对。两个 vendor 子仓库检查时没有未提交改动。

不修改 Agent 或指标平台，不创建 worktree，不弱化现有测试。实施计划执行完成后按工作区规则提交本任务代码，提交前分别审查服务与 vendor 的变更归属；不把已有未提交工作整体打包。本设计阶段不创建提交。
