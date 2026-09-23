# 请求级内存 YAML resources 设计

状态：待用户评审；本文件不代表已完成可运行实现或获准开始实现。

## 1. 目标与已确认边界

在现有 dbt 和 MetricFlow 任务请求中增加可选字段 `resources: dict[str, str]`。key 是外部 YAML 文件名，value 是 UTF-8 YAML 原文。解析项目时，优先读取本次请求中同名资源的非空白原文；资源缺失或原文为空白时，回退到原有默认读取和解析行为。非空白 YAML 的语法、引用或 dbt 校验错误仍然报错，不回退。

用户已确认：

- 外部 YAML 不创建、替换或修改磁盘上的 YAML 文件。
- 外部原文及包含原文的解析缓存不落盘；允许任务独立的 JSON、编译 SQL 等派生产物。
- 仅资源缺失、空字符串或纯空白原文触发默认行为；非空白但无效的 YAML 不触发回退。
- 保留现有项目、命令、任务提交和轮询接口。
- 接受评估固定版本的内部解析适配层；不修改 `vendor/` 源码，不从父工作区或源码目录跨目录导入。
- 引擎将为指标平台和 AGENT 提供能力；AGENT 第一期只使用已发布指标。

本设计将“优先”定义为同一逻辑 YAML 文件的内容选择，发生于解析之前。它不是按指标名称逐字段合并，也不是查询时动态寻找 YAML。该语义和下文命名规则随本文件一起评审。

## 2. 范围

包含：

- 两类任务请求的 resources 校验及内存传输。
- 根项目 schema YAML 的内存覆盖和新增；保留 dbt 自身的 YAML、Jinja、引用和语义验证。
- 请求级 worker、解析状态、产物及生命周期隔离。
- MetricFlow 使用本次 resources 生成的语义定义。
- 内部依赖范围、兼容性与回归测试。

不包含 Git 同步、定义保存与发布、跨请求资源缓存、鉴权系统、分布式队列、结构化查询结果改造、StarRocks 的 MetricFlow 方言，以及模型 SQL/宏文件的上传。

平台可以从自己的 Git 或数据库取得定义后提交 resources。引擎不承担内容的来源管理。此前讨论的 Git 发布体系不作为本功能的前置条件。

## 3. 接口契约

```json
{
  "project": "sales",
  "command": "parse",
  "resources": {
    "orders.yml": "version: 2\nmodels:\n  ..."
  }
}
```

MetricFlow 请求使用相同字段：

```json
{
  "project": "sales",
  "command": "query",
  "metrics": ["revenue"],
  "resources": {
    "orders.yml": "version: 2\nmodels:\n  ..."
  }
}
```

- 默认值为 `{}`；省略或空字典走现有 CLI 路径。`null` 不接受。
- value 必须严格为字符串，不将字典或其他值隐式序列化成 YAML。
- 完成字段、名称及大小校验后，过滤满足 `not value.strip()` 的条目，得到本次请求的有效资源集合。此处“有效”仅指非空白候选原文，不代表已通过 dbt 校验；保存原文，不用 strip 后的文本替代它。下文“有效 resources”均指这个集合。
- 有效集合为空时，包括传入全部为空白的 resources，走现有 CLI 路径；既有 MetricFlow manifest 前置检查也保持不变。
- key 第一阶段只接受单段文件名，以 `.yml` 或 `.yaml` 结尾；禁止路径分隔符、绝对路径、控制字符、`.`、`..` 及平台特殊路径语法。
- key 不带目录。匹配冲突时明确失败，不增加递归猜测或任意路径访问能力。
- 存在有效 resources 时适用于 `parse`、`compile`、`seed`、`run`、`test`、`build` 和现有四种 MetricFlow 命令。`debug` 不消费 schema 定义，携带有效 resources 返回 422，防止静默忽略；全部为空白时按普通 debug 执行。
- 空字符串或纯空白 value 视为未提供该资源，不代表删除文件。非空白原文进入 dbt 正常解析；语法、引用或校验失败时任务失败，不以默认定义重试。注释、`{}` 等非空白 YAML 也交由 dbt 正常处理，不额外推断它是否包含“足够”的业务定义。
- 第一阶段采用模块常量限制：最多 100 个资源，单项原文 UTF-8 大小不超过 1 MiB，总原文大小不超过 5 MiB，文件名不超过 128 个字符。上限按过滤前的请求计算，空白条目不能绕过限制；父进程与 worker 使用一致的验证规则。
- HTTP 接入层按总请求体上限 8 MiB 提前拒绝超大请求，并支持分块请求累计计数，不能等 JSON 完整载入后才限制大小。超过 HTTP 上限返回 413；已载入请求的字段格式和资源数量/大小错误返回现有 422 validation_error。
- 现有 `JobRecord` 不保存或返回 resources；仅排队/执行期间的私有任务输入持有原文。任务终止后释放引用，不增加资源回显接口。

## 4. 文件身份及内存优先规则

仅对根项目中由 dbt 识别的 schema YAML 生效，不能覆盖依赖包、项目配置、profiles、packages、selectors 或服务配置。

1. 使用当前项目配置的 schema 搜索路径及 `.dbtignore` 规则建立根项目 YAML 文件索引；依赖包独立处理。
2. 对有效 resources 中的每个 key，以文件名匹配根项目被纳入解析的文件。缺失或空白条目不参加覆盖、新增、名称匹配或虚拟文件创建，相关文件继续由默认流程处理。
3. 唯一匹配：保留 dbt 的逻辑路径和文件身份，内容来自 resources，原文件不打开、不解析。
4. 无匹配：在第一个 `model-paths` 下分配该文件名作为虚拟路径，仅创建内存文件对象。必须存在有效的 model-path 配置；虚拟位置若被忽略或与被忽略的实体文件冲突则报错，不绕过忽略规则。
5. 多个匹配：任务失败并报告资源名歧义，要求调用方或项目维护方消除同名文件。第一期不支持通过 key 指定目录。
6. 为保证 Windows/Linux 一致性，匹配及冲突检查使用大小写折叠；仅大小写不同的 key 或基础文件视为冲突，不依赖操作系统选取其中之一。
7. 未命中的其他文件继续由 dbt 正常读取。不同文件内出现同名模型或指标，继续按 dbt 的重复定义规则报错，不做对象级合并。

示例：项目存在 `models/orders.yml` 和 `models/customers.yml`，resources 只有 `orders.yml`。本次解析采用内存中的 orders 定义和磁盘中的 customers 定义。磁盘 orders 原文即使语法有误，也不应影响本次解析；这要求覆盖发生在 YAML 解码之前。

回退示例：如果 `resources["orders.yml"]` 是 `""` 或 `" \n\t"`，则读取磁盘上的 orders 定义。如果资源名未对应任何基础文件且 value 为空白，不创建虚拟文件。若 value 是非空白但语法错误的 YAML，则任务失败，不改用磁盘上的 orders 定义。所有条目都被过滤后，整个请求回到现有 CLI 流程。

外部 YAML 仍需引用基础项目已有的 SQL 模型、宏和其他依赖。传入 YAML 不会自动创建 SQL 模型，也不会自动执行建表。

## 5. 方案比较与选择

| 方案 | 优点 | 代价与限制 | 决策 |
| --- | --- | --- | --- |
| 独立 worker + 限定范围的内存 schema 读取适配 | 可复用现有 dbt 执行和 mf 输出；能在读取前覆盖；不依赖已有解析缓存 | 少量固定版本内部接入点必须维护和测试 | 推荐 |
| 用 FileDiff 驱动已有 manifest 的增量解析 | 上游已有 InputFile/content 和差异解析代码 | 冷启动缺少完整基础文件集合，涉及缓存有效性及依赖包身份；不能直接等价于磁盘回退 | 第一阶段不采用 |
| 全局虚拟文件系统或拦截通用 open | 表面上能模拟文件存在 | 枚举、stat、路径和第三方库行为均需模拟，影响范围过大 | 不采用 |

不是把 FileDiff 当作稳定 SDK，也不复制整套 dbt parser。只在服务自己的适配模块中使用已安装的固定版本包。所有内部接入仅存在于一次性 worker 进程内。

## 6. 执行架构

```text
HTTP 请求
  → 校验原始 resources，过滤空白条目
  ├─ 有效 resources 为空 → 原有 dbt/mf CLI 与行为
  └─ 存在有效 resources
       → 排队
       → 独立 Python worker（stdin 接收 JSON）
       → 内存 YAML 读取适配 + 禁用原文缓存
       → dbt 解析/执行
       → 可选：把本次语义 manifest 接入 mf 命令
       → stdout/stderr + 退出码
       → 现有任务状态和轮询接口
```

### 6.1 worker 与生命周期

- 使用当前服务解释器运行固定的 worker 模块；不接受客户端指定可执行程序、模块或命令字符串。
- resources 和结构化命令通过 stdin 管道发送，不放进命令行参数、环境变量或临时请求文件。
- 父进程并行处理 stdin 写入和 stdout/stderr 排空，避免管道反压死锁。超时从进程启动覆盖输入传输、解析、执行到退出；处理 worker 提前退出导致的 BrokenPipe。
- 沿用现有进程组、超时终止、输出上限和凭据遮盖。取消或超时必须停止 worker 及其后代进程，并关闭输入管道。
- 每个请求独立进程，不在 FastAPI 进程修改 dbt 模块状态，不建立跨请求 parser 单例。
- 带 resources 的任务从提交到终态持有其输入副本，终态释放；保留的任务历史只包含现有结果字段。

### 6.2 dbt 读取适配

- 继续使用 `dbtRunner.invoke` 的命令执行流程，在 worker 中为固定版本安装最小范围的 schema 读取适配。
- 在 schema 文件枚举/加载层构造“原有文件集合 + 虚拟资源”的有效文件集合。覆盖资源直接构建 `SchemaSourceFile`，使用 dbt 自身的 checksum、YAML 解析和校验逻辑。
- 保留原始逻辑路径、项目归属和 parser 文件索引。SQL、宏、种子、依赖包、OSI 等非目标读取沿用上游实现。
- 不能先调用原始 schema 文件读取再替换结果，否则磁盘上的无效 YAML 会提前报错。
- 第一阶段存在有效 resources 时始终完整解析，禁止复用和写入 `partial_parse.msgpack`。必须显式隔离其写入点，不能只依赖 `--no-partial-parse` 或 `--no-write-json`。
- 强制使用已验证的固定 Python parser 路径，拒绝或覆盖会绕过适配的解析器选择。内部接入点或包版本不符合预期时直接失败，不静默退回磁盘定义。
- 同一请求内的 target、vars、环境和 profiles 传递保持一致；外部 resources 不得覆盖这些配置。

### 6.3 MetricFlow 桥接

- 存在有效 resources 时，在同一 worker 内先执行 dbt parse，再运行请求指定的 mf 操作。调用方无需提前生成基础项目 manifest；有效集合为空时保持原有前置要求。
- 解析产物写入当前任务目录；从本次 manifest 获取 adapter 类型，继续应用现有 MetricFlow adapter 白名单。
- 原来基于项目 `target/manifest.json` 的同步预检查用于有效 resources 为空的路径；存在有效 resources 时，适配器检查转移到 worker。此时不支持的 adapter 体现为已接受任务的失败，而非提交时的 HTTP 422。
- 为现有 mf CLI 提供任务级 `CLIConfiguration` 适配：复用根项目/profile 配置和上游命令格式化，但将语义 manifest 来源绑定到当前任务生成的 `semantic_manifest.json`，关闭文件日志。
- 初始化后的配置通过 Click 的对象上下文传给原有命令，防止命令重新从根项目的 `target/` 加载旧 manifest。该具体接入须由兼容性测试验证，不能只检查配置对象的字段值。
- 不直接调用 MetricFlow YAML parser 来解释 dbt YAML，不自行实现指标规则和 ref 解析。
- 不改造查询结果协议；四种命令的 stdout、stderr 和退出状态与现有契约保持一致。只允许服务现有白名单命令及参数。
- dbt parse 与 mf 必须使用相同 profile/target 及相关环境，固定 `DBT_PROJECT_DIR` 与 `DBT_PROFILES_DIR`，不能让继承的环境指向其他项目。

## 7. 产物、日志与并发

### 7.1 允许和禁止写入的内容

允许在任务目录写入正常的 `manifest.json`、`semantic_manifest.json`、编译 SQL、run results 等派生产物。解析后的指标名、表达式和模型配置属于允许的派生语义内容；不允许以 raw YAML、SourceFile.contents 或完整请求副本的形式把原文嵌入产物。

禁止：外部 YAML 文件、stdin 请求落地、包含原文的 partial-parse 缓存、原文调试转储，以及携带 YAML 源码片段的磁盘日志。

存在有效 resources 的模式关闭 dbt/mf 文件日志，诊断经 stdout/stderr 返回并沿用服务脱敏。对于可能包含原文的解析异常，只输出资源名、行列和整理后的错误类别/消息，不直接转储请求或异常中的 YAML 源码片段。HTTP 请求体不写日志。

这是一项应用主动写入约束，不声称控制操作系统 swap、崩溃转储或外部基础设施的日志策略。

### 7.2 路径与清理

- 服务增加 `JOB_ARTIFACTS_ROOT`，默认 `/workspace/job-artifacts`，需服务用户可写；客户端不能传产物路径。
- 父进程按内部 job ID 创建独立子目录，worker 的 target 路径绑定到该目录。使用绝对路径并检查包含关系，不写回项目 `target/` 或 `logs/`。
- 产物在任务结束后用于退出收尾，完成后由父进程清理；当前 API 没有产物下载能力，因此不增加保留策略或下载接口。
- 清理失败不得覆盖原任务结果；记录不包含原文的诊断。异常退出残留目录在服务启动、且确认当前没有活跃 worker 后清理。第一阶段按现有单服务进程部署，不宣称支持多实例共享此目录。
- 测试应覆盖取消、超时、进程提前退出和正常成功后的目录清理。

### 7.3 并发边界

- 不变更现有 dbt 写任务的项目级互斥；锁按基础项目识别，不能因产物目录不同失效。
- 不同 resources 查询拥有独立进程、原文、manifest、target 和 mf 配置；同一逻辑文件名不造成共享缓存。
- 资源隔离不等于数据库快照隔离。dbt build 改动底层表时，MetricFlow 查询仍遵循数据库自身的一致性，不在本功能中新增数据库锁。
- 文件读取期间若外部系统修改基础项目，本功能不保证整个项目的版本快照。部署方应保持作为基底的项目稳定；Git 版本快照是后续独立能力。

## 8. AGENT 与平台的关系

本引擎提供的是请求级能力，不根据可伪造的 `caller` 字段决定权限，也不在本功能增加认证系统。

指标平台可传入定义执行验证或查询；AGENT 第一期由上层调用约定使用已发布定义，不直接获得任意改写定义的权限。resources 不自动写回、保存或发布，连续调用若需要同一外部定义，必须重新携带相同 resources 和稳定的基础项目；本功能不新增 session/token 缓存。

## 9. 错误与兼容性

| 场景 | 行为 |
| --- | --- |
| resources 缺省、为空或全部条目为空白 | 保持当前 HTTP/CLI 行为，包括现有 manifest 前置要求 |
| 部分资源缺失或为空白 | 这些文件按默认行为读取，其他有效资源仍从内存读取 |
| 请求体超限 | HTTP 413，不启动任务 |
| 字段类型、资源名、数量/大小非法，debug 携带有效 resources | HTTP 422，不启动任务 |
| 基础项目不存在或不安全 | 沿用现有 HTTP 错误 |
| 同名文件歧义、虚拟位置非法、YAML/引用错误 | 任务 failed，提供资源定位信息，不回退旧定义 |
| resources 路径下 adapter 不支持 | 任务 failed，保留可识别的 metricflow_adapter_not_supported 诊断 |
| 版本或内部接入点不兼容 | 任务 failed，明确 incompatible_runtime，不运行磁盘备用路径 |
| worker 超时 | 沿用 timed_out，终止进程树并清理资源 |
| mf 执行失败 | 沿用 failed 与非零退出码 |

有效 resources 为空的任务不受内存适配 hook、目标路径或缓存策略影响。现有 StarRocks 的 dbt 能力保留，MetricFlow 限制不变。

## 10. 代码边界与仓库规则

预期变更集中于：

- `models.py`：两个请求模型增加字段及验证，私有命令描述支持内存输入。
- `api.py`：请求体上限及按有效 resources 是否为空选择提交分支。
- `commands.py`：构建固定 worker 调用，复用现有白名单参数生成。
- `jobs.py`：stdin 传输、输入生命周期和任务产物目录清理，保留现有进程控制。
- `settings.py`：任务产物根目录。
- 新增 worker 入口及一个集中管理 dbt/mf 内部依赖的适配模块；实现时按职责拆分，不创建通用插件体系。
- 相关测试、README 和 AGENTS.md。

实现阶段在 AGENTS.md 中增加狭窄例外：仅内存资源适配模块可以调用已安装、固定版本的 dbt/mf 内部解析和配置接口；其他代码仍通过原有公开入口调用。禁止修改 vendor、跨目录 import、伪装包来源及全局拦截通用文件 API 的规则继续成立。

本设计阶段只添加设计文档，不提前修改上述运行代码或仓库规则。

## 11. 验收与实现前验证重点

验收测试必须覆盖：

1. 不传 resources、`{}` 和全部 value 为空字符串/纯空白时，原有 dbt/mf 行为一致，包括原有 manifest 前置检查和 debug；不启动资源 worker。
2. 内存同名 YAML 改变解析后的模型/指标语义，磁盘源文件逐字节不变。
3. 磁盘同名 YAML 无效、内存 YAML 有效时成功，证明覆盖发生在解析之前。
4. 新增虚拟 YAML 可被发现；缺失及空白资源从基础项目读取；不存在的空白资源不创建虚拟文件；混合空白和非空白条目只应用非空白条目。
5. 非空白 YAML 语法错误、未知 ref、dbt 校验错误时明确失败，即使默认文件有效也不回退；注释和 `{}` 等非空白内容按 dbt 自身语义处理。名称歧义、大小写冲突、路径输入、忽略规则、重复定义、非字符串值及上限边界按契约处理，空白条目计入原始请求上限。
6. 没有既存 target 或 partial_parse 缓存也能冷启动，依赖包和宏仍可用；反之存在过期缓存也不污染结果。
7. 以只存在于原文注释中的唯一标记检查所有任务产物/日志，标记不得落盘；包含标记的 msgpack、YAML、请求转储均不得出现。检查基础项目文件树的写入范围。
8. 两个并发 MetricFlow 任务传入不同的同名 YAML，各自生成不同、正确的结果，互不串扰。
9. resources 请求结束后，再提交无 resources 请求，仍使用原项目定义。
10. `parse`、`compile`、`seed`、`run`、`test`、`build` 的代表性 DuckDB 测试消费内存定义，debug 仅在存在有效 resources 时明确拒绝；四种 mf 命令验证当前语义来源和现有输出契约。
11. MetricFlow 不能读取基础项目旧 semantic_manifest；实际 adapter 校验保持有效，StarRocks 的限制按两条路径分别测试。
12. stdin 大输入/早退、超时、取消、输出截断和脱敏均不死锁、不泄漏原文、不残留活跃 worker。
13. 内部 API/版本校验失败时直接报错；vendor 工作树无改动。

最先验证冷启动读取适配、禁止 msgpack 写入、mf 配置注入三条技术路径。当前方案有源码依据，但尚未通过可运行原型验证；若其中任一需要复制大段上游实现或超出限定接入范围，应返回设计评审，不能悄悄改成文件落地。

实现遵循先失败测试后最小实现，最终运行受影响测试、完整 `uv run pytest`、本仓代码的 `uv run ruff check src tests scripts` 及 Windows/容器路径相关验证。裸 `ruff check` 会递归检查只读 vendor 源码，其已有检查问题不在本功能修复范围内。

## 12. 源码依据

- `vendor/dbt/core/dbt/parser/read_files.py`：`load_source_file`、schema 搜索、`ReadFilesFromFileSystem`、`InputFile` 与 `ReadFilesFromDiff`。
- `vendor/dbt/core/dbt/parser/manifest.py`：文件读取入口、partial-parse 缓存写入、manifest 与 semantic_manifest 输出。
- `vendor/dbt/core/dbt/cli/main.py`：`dbtRunner` 和现有命令入口。
- `vendor/dbt/core/dbt/contracts/graph/semantic_manifest.py`：dbt manifest 到语义 manifest 的转换。
- `vendor/dbt-metricflow/dbt-metricflow/dbt_metricflow/cli/cli_configuration.py`：任务配置、adapter 和语义 manifest 来源。
- `vendor/dbt-metricflow/dbt-metricflow/dbt_metricflow/cli/main.py`：Click 配置对象及既有命令输出。
- `src/dbt_metricflow_service/jobs.py`：当前项目互斥、子进程、超时和输出处理。

## 13. 评审与下一阶段

本次评审确认：文件名匹配规则、缺失/空白时回退而非空白解析错误不回退、仅请求内生效、原文/缓存不落盘与派生产物可落盘的边界、限定内部 API 例外、debug 的拒绝语义，以及 resources 路径中适配器错误发生于异步任务阶段。

用户确认本设计后，再使用 Superpowers writing-plans 形成实施计划；设计评审不等于直接开始修改实现。
