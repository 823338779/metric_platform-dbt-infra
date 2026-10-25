# dbt-service 全接口审查与精简设计

状态：按 mattpocock-skills 完成第二轮审查并修订，尚未实施。日期：2026-10-25。

## 第二轮审查修正

本轮采用 mattpocock-skills 1.3.1 的 codebase-design 和 improve-codebase-architecture。第一轮的独立检查只检查了方案是否有重大实施歧义，不代表方案中的每项删减都通过正确性验证。

优先处理有行为证据的问题，再收敛 module 的 interface。文件移动、减少字段数本身不作为成功标准。

1. **P1：部署观察与提交存在竞态。** `storage/deployments.py:135` 更新 observed_head_sha 时，普通观察不递增 version；`:171` 提交仅比较旧 version 和传入的旧 head。观察 B → 另一请求观察 C → 按旧 B 提交，可以得到 active=B、observedHead=C、sourceState=MATCHED。已用真实 Store 方法和 Session 替身按此交错顺序复现；没有连接 PostgreSQL。必须把“提交所依据的观察仍有效”纳入同一个事实提交检查。仅保留现有 version 判断不够，也不能宣称消除 Git 远端的全部竞态。
2. **P2：请求错误被报成 503。** `_where_constraints` 对 IN 标量、空列表、非法标量抛 ValueError，发生在 MetricFlowClient 的 InformativeUserError 捕获范围外，再被 `_engine_context` 的宽泛异常处理转成 COMMAND_FAILED/503。输入语义检查应在初始化外部引擎前完成，明确转换为 422；不能把所有 ValueError 都视作用户错误。
3. **P2：EXPLAIN 的 limit 被加一。** `metric_flow_client.py:100` 对所有模式使用 limit+1。替身引擎确认请求 limit=10 时 explain 实收 11。仅实际取数需要探测多一行，编译 SQL 应忠实保留请求 limit。
4. **P2：构建可读性检查缺失。** QueryService 只 load 构建，随后运行引擎；替身验证 RUNNING 且 output_set_id=None 仍到达引擎。真实执行可能失败于产物恢复，而不是稳定的构建未就绪错误。目录与查询应共享这一不变量。
5. **架构：一次带维度查询准备两套执行上下文。** `application/queries.py:64-68` 先 discover 再 execute，两者各自还原工程、读取 Manifest、生成 profile 并初始化 client。只移动 py 文件不会改善这个成本。把一次查询的选项解析、编译与执行收敛在同一个请求级生命周期内，复用同一上下文；不建立跨请求全局引擎缓存，不延长到 HTTP 响应发送阶段。
6. **架构：选项信息被拆成并行容器。** compile_query 同时接收公开 dimensions 和内部 mapping，再为前者重建 option_index。两者必须由调用方保证一致。由查询 module 内部持有一份已解析的选项集合，并在响应时投影公开字段，减少跨 seam 传递的约束。
7. **P2：幂等恢复受当前配置变化影响。** `api/builds.py:30-34` 在进入幂等查询前校验当前执行实例清单，而 `storage/builds.py:65-78` 才查询已有请求。原构建受理后，若清单改变或实例被移除，同键同输入重试会被拒绝。将校验搬进 BuildService 还不够：必须先恢复或拒绝已有幂等请求，只对新受理校验当前配置，并保持事务内防重。当前认证仍是调用前提，不绕过鉴权。

撤回或降级第一轮的部分建议：

- **保留历史日志 phase/command。** `docs/build-execution-logs-api.md:28-29` 明确记录 PARSING/parse；当前生产端只写 BUILDING/build 不能证明历史恒定。本仓库不包含生产数据，不能认定存量实际有哪些值。jobId 删除也降级为契约评估项，不与消除内部重复模型绑定。
- **保留查询响应 mode。** 若响应分为 explain、预览和数据结果，mode 是真实的 discriminator。删除它会把判断成本转给调用方，不符合 deletion test。boundaryPolicy 仍可移到静态文档。
- **不把目录树作为实施规格。** 模型应按一起变化的业务概念归属；仅几十行的类型不必独立开文件。先把一个查询概念跨多个目录的跳转收敛，再决定文件长度是否需要拆分。
- **内部重复与 HTTP 回显分开判断。** AvailableDimensions 的两次对象转换可以确定消除；buildId/metrics/resourceId 等公共字段是否移除，需要逐项说明调用方迁移收益，不能仅以服务端没有再次读取它们为依据。
- **不把所有 Store 都 mock 掉作为完成证据。** HTTP 形状检查可使用替身；部署并发、封存、租约及幂等仍需独立 PostgreSQL 测试。未取得真实数据库证据时，这些验证保持未完成。

## 目标与已确认边界

用户要求逐个 review 当前接口，精简无意义字段、包装和恒定值判断，重新整理 Python 包与文件职责。用户已明确允许调整对外接口，优先简化整体设计。以下具体改动为审查后的实施建议。

保持现有 14 个接口及构建、部署、原生目录、同步查询能力。保留 dbt/MetricFlow SDK 对业务语义的解释权。保持 PostgreSQL 持久化、历史 Manifest、租约、幂等、物理目标隔离和日志续读语义。此次不改变数据库 schema，不自动修改已有数据，不改动当前已有修改的 vendor/dbt。

## 方案取舍

1. 仅内部兼容重构：风险小，但无用的 HTTP 字段和混合模式请求继续存在。
2. 接口与实现一起精简：消除重复模型和无意义协议，调用方同步适配。用户已选择此方向。
3. 重写队列、数据库和全部引擎封装：影响远超接口精简，历史迁移及并发风险高，不采用。

实现采用方案 2。不引入通用 Repository、插件式查询分派器或新的服务框架。

## 逐接口结论

| 方法与路径 | 审查结论 | 拟实施变化 |
| --- | --- | --- |
| POST /builds | 完整执行快照、幂等键及部署策略有实际用途；当前实例校验先于幂等恢复，破坏配置变化后的重试 | 将完整受理规则收敛到构建 module：先恢复旧幂等请求，仅新请求校验当前实例；构建响应可删顶层 configVersion，保留 execution.configVersion；worker 运行配置版本仍保持独立 |
| GET /builds | environmentId 已是必填，但 Store 仍先判空、后条件过滤；列表复用大响应类型可以保留，避免无依据再造摘要模型 | Store 明确必填环境并直接过滤；响应同构建详情 |
| GET /builds/{build_id} | BuildView 只有一个 SnapshotBuildView 子类；名字来自旧协议，完整快照才是当前协议 | 合并为 BuildView；构建请求统一命名 BuildRequest；保留 buildStatus、phase、catalogAvailable、错误和部署意图 |
| GET /builds/{build_id}/execution-logs | 续读游标和 hasMore 有独立含义；当前写入固定，但历史协议包含 PARSING/parse | 保留 phase/command 与 attemptId，保持历史排障上下文；jobId 删减降级为契约评估项 |
| GET /builds/{build_id}/catalog | 已直接返回 dbt 原生 artifact，没有必要重包统一资源模型 | 保留原生结构；统一构建可读状态检查 |
| GET /builds/{build_id}/resources/{resource_id} | buildId/resourceId 属于请求身份回显；resource/source 的区分有意义 | 优先保持原生节点及完整源码读取的集中实现；删除身份回显降级为契约评估项，损坏快照仍报错 |
| GET /builds/{build_id}/lineage | 路由先读取 Manifest 又丢弃，执行器再次读取；路由直接协调执行 | 移入 CatalogService；只读一次 Manifest；保留 selector 和原生数组 |
| GET /deployments | 路由复制 DeploymentService.list 的错误处理和分页转换，并直接访问 store | 路由仅调用应用层；持久层不生成 HTTP 响应模型 |
| GET /deployments/current | GET 的 Git 观察会写入事实并被 reconcile 复用；普通观察不增加 version，结算可能使用过期 head | 先修复观察与结算的一致性；公共响应不再充当内部状态载体；version/desiredGeneration 的对外删减延后到一致性测试通过后评估 |
| POST /query/available-dimensions | AvailableDimensions 和 AvailableDimensionsView 重复；native.metrics 无消费者；valueType 没有可靠类型推导 | 删除内部重复结构及未消费的 metrics；valueType 可删除；保留有意义的 optionId、名称、显示名、dimensionType、granularities、operators；是否删除身份回显与内部去重分开评估 |
| POST /query | 四种模式共享大而全模型；DIMENSION_VALUES 的 groupBy 被覆盖；两份 QueryMode；内部 run_id 无消费者；错误当返回值再转回异常；响应上限同时存在硬编码 8MiB 和配置值 | 按 mode 定义严格请求；精简响应（见下）；统一枚举；删 run_id 和未用 caller 传参；直接传播异常；最终公开响应统一按 Settings.max_result_bytes 检查 |
| GET /health/live | 存活状态是标准健康探测信息，固定值不是必须删除的理由 | 保留 status；明确响应模型 |
| GET /health/ready | 只使用 SDK 执行却要求 dbt/mf 命令存在，PATH 状态会造成无关的未就绪判断 | 移除 CLI 可执行文件检查，保留数据库、临时目录、实例配置检查；明确响应模型 |
| GET /versions | 引擎/适配器版本有排障用途，无需删除包装 | 保留响应并明确模型；与运维路由一起移出 app 装配文件 |

## 查询协议与实现

### 请求

保留 POST /query?build_id=... 和 mode，不增加新路径或 API 版本层。

- QUERY、EXPLAIN：metrics 必填，支持 groupBy、filters、startTime、endTime、orderBy、limit；禁止 selector 和 dimensionOptionId。
- PREVIEW：只有 mode、selector、limit；selector 必须非空；其他字段即使传空值也拒绝。
- DIMENSION_VALUES：metrics、dimensionOptionId 必填，允许 filters、startTime、endTime、limit；禁止 groupBy、orderBy、selector。现有 orderBy 校验基于指标/分组，而维度值结果不返回指标，不继续保留这套含混行为。
- 使用 Pydantic 按 mode 区分的请求联合，避免用 if 模拟字段存在性；内部经过编译的请求仍可以是单个明确的类型，包含引擎实际需要的值。

只保留一份 QueryMode。HTTP 入口负责外部类型校验；compile_query 负责选项归属、粒度和排序选择验证。时间归一化只做一次，指标集合去重排序只做一次。所有查询及维度发现执行前检查构建成功及 output_set_id，未完成返回 409；成功构建缺产物返回 503。

### 响应

- 公共上下文：buildId、commitSha。保留固定构建身份便于调用方关联结果。
- QUERY、DIMENSION_VALUES：另含 businessTimezone、normalizedTimeRange、columns、rows、resultTruncated。
- EXPLAIN：另含 businessTimezone、normalizedTimeRange、sql；不带空 columns/rows 或恒为 false 的截断标志。
- PREVIEW：另含 columns、rows、resultTruncated；不带没有消费的时间参数回显。
- 保留 mode 作为结果类型的 discriminator，删除恒定 boundaryPolicy。时间边界由 MetricFlow 解释这一事实写在接口文档中。
- JSON 单元格继续保留 Decimal 字符串和日期序列化；仅取数模式使用 limit+1 判断截断，EXPLAIN 使用请求原始 limit。

响应根据实际模式明确构造，避免为了减少几个 if 引入注册表/策略类。mode 的四种值是真实行为差异，必要分支保留。

### 执行边界

查询 module 对外保留可读的业务操作，内部将同一次请求的维度解析和执行放在同一个生命周期中。用正常异常传播替代 `Result | ServiceError`。共享串行执行器的取消屏障、全局环境恢复继续保留。允许 before_start 为 None，使同步查询无需空回调。只删由类型契约已经保证的结果 isinstance 检查，保留外部产物损坏和引擎错误处理。

CatalogService 负责构建定位、状态验证、原生目录/节点/血缘用例。QueryService 负责查询编译及响应转换。查询工作目录恢复的错误也应转换为稳定公开错误，避免裸 ValueError 被误报为用户输入错误。

统一最终公开响应的大小检查；SQL、数据、维度和血缘使用同一配置上限。执行器内可保留必要的结果保护，但不得存在不同的硬编码业务上限。目录 Manifest 继续由产物大小上限约束。

## 模块组织

鉴权使用无返回值的路由依赖；只有构建幂等需要固定服务来源，该值在构建受理边界明确提供。查询不再透传未消费的 caller。认证失败直接生成 ServiceError，保留 mutations_disabled/service_credential_required 错误码，不再经过会丢弃 detail 的通用 HTTPException 转换。未知 HTTP 错误继续返回脱敏通用响应。

以下是第一轮的职责归属示意，不是强制的新增文件列表。第二轮要求以 module 的 locality 为准：一起变化的类型和逻辑就近放置，优先合并 shallow module，而不是只替换目录名称。没有实际独立职责的 errors.py/logs.py/results.py 可以合并到所属业务文件：

```text
api/                 HTTP 路由、鉴权、错误转换、请求大小限制
  app.py             依赖组装与生命周期
  system.py          live / ready / versions
  builds.py          构建及日志路由
  catalog.py         目录、详情、血缘路由
  deployments.py     部署读取路由
  queries.py         查询路由
application/         构建、部署、目录、查询用例和响应组装
models/
  contracts.py       唯一 Contract、JsonObject、通用 Page
  errors.py          公共错误类型
  builds.py          BuildRequest / BuildView / 构建状态
  execution.py       执行快照、连接目标和物理目标身份
  logs.py            执行日志响应与续读分页
  queries.py         公开查询请求/响应和维度选项
  deployments.py     部署协议及内部自然键
  catalog.py         resource/source 协议
execution/           引擎上下文、运行、取消、日志、工作目录
  query_models.py    编译后的查询、原生维度、引擎结果
engines/             dbt / MetricFlow SDK 客户端及必要转换
  dbt.py
  metricflow.py
  dimensions.py      原生可用维度与粒度验证
  results.py         共用单元格/结果转换，dbt 不再从 MetricFlow 私有函数导入
platform/            现有源码获取、profile、Manifest 捕获工具；查询映射/时间归一化移入 application
storage/             ORM、记录、事务、分页、具体 Store
workers/             任务领取、心跳与恢复
```

`models/artifacts.py` 中重复的 Contract 合并到 contracts.py；通用类型不再从 builds/artifacts 转出口。`payloads.py` 中按职责迁移类型后移除空壳。`platform/models.py` 的引擎查询类型移到 execution/query_models.py。`delegates/clients` 和 `delegates/metricflow` 收敛到 engines，无兼容转发壳。

常量处理以阅读价值为标准：路由字面量、UTF-8、Pydantic mode、普通字段名就地书写；真正跨模块的业务状态、错误码和资源限制保留明确归属。不会把 constants.py 机械拆成更多常量文件，也不会为了整理常量重写无关 SQL。

## 本次审查发现但不应直接删掉的部分

- execution_stage 有 PREPARING 和 EXTERNAL 两种真实状态，决定外部写入是否可能仍在运行。
- retry_policy 当前生产入口默认 PREPARATION_ONLY，但持久约束允许 NEVER；历史任务不能按默认值臆断为同一种。
- DeploymentAttempt.operation 当前创建值为 BUILD，但代码明确标注历史部署来源，删除条件需先核对存量数据并设计迁移。
- buildId、run_id、attempt_id 分别代表公开构建、内部任务和一次执行；只删同步查询中无消费者的 run_id，不合并持久身份。
- 幂等摘要、封存摘要、租约令牌和版本不是“重复包装”，保留验证与事务边界。
- `execution/metricflow.py`、`models/native.py` 中的旧节点投影和部分 Git 帮助函数没有当前生产引用，先在审查结论中标注；除本次迁移产生的孤立代码外，不顺带删除旧实现。
- `JobRequest` 同时保存 execution/binding、任务存储保留通用父任务和项目能力，属于下一步数据库收敛候选；本次不把接口精简扩成历史数据迁移。

## 验收

当前基线：`uv run --frozen ruff check src` 通过；`uv run --frozen pytest -q` 没有收集到测试，tests/ 只有 __pycache__。README 所称测试覆盖不能视为本 checkout 的验证证据。

额外只读复现：使用替代 Runtime 创建实际 FastAPI 应用，OpenAPI 枚举确认共 14 个操作；未认证查询返回 401，但 code 为 HTTP_ERROR；DIMENSION_VALUES 确实接受 groupBy；两个 Contract 和两个 QueryMode 均为不同的 Python 类。复现没有启动 worker 或连接数据库。

实施验收必须包括：

1. 14 个接口的 HTTP 回归测试：有效输入、主要错误和精简后的响应；使用应用依赖替身避免要求业务数据库。
2. 四种查询模式的输入边界、无效混合字段、选项/粒度/排序、时间归一化、Decimal 序列化、截断和响应上限。
3. 未完成构建不进入引擎；成功构建缺产物明确失败；血缘不重复读取 Manifest；鉴权仍执行且认证错误码可读。
4. 串行锁下异常传播及取消时等待线程退出，临时上下文退出后才能释放资源。
5. OpenAPI 可生成，模式联合及精简响应可见，全部自有模块可导入，Ruff 通过。
6. 部署观察与结算竞态、租约、封存和幂等需要可丢弃的独立 PostgreSQL 测试库验证。替身只作为复现和 HTTP 形状测试证据；没有独立库时明确记录这部分验收未完成，不连接或清理业务库冒充测试。
7. EXPLAIN limit 不加一；非法过滤值在外部引擎初始化之前返回 422；一次带维度的查询只建立一次请求级执行上下文。
8. 同键同输入在实例清单改变后仍恢复原构建；同键不同输入仍冲突；新请求使用当前配置校验，不绕过认证。

完成后更新 README 的接口说明和迁移清单，并交付各项实际验证结果。实施代码应保持未提交状态，便于用户审阅。
