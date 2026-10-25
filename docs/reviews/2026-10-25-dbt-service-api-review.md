# dbt service 全接口审查与重构建议

日期：2026-10-25。范围：当前 `8d9480dc3240eac9b7a92a4c46e38e0889ba6dcb` 的 14 个 HTTP 操作及其应用、执行和持久化调用链。按用户要求审查当前整体实现，不使用历史 diff 代替全量接口审查。

结论：确有重复类型、空传参、异常包装和不必要的可选分支。最有实际收益的调整是让一次查询只准备一次执行上下文，并让 HTTP 路由只调用完整业务用例。另有三项行为问题需要修复；不能把部署一致性、日志续读和历史记录分支一起当成过度设计删掉。

本文记录实施前的审查证据及已确认的设计。用户已批准按方案 B 实施、调整接口且不保留旧协议兼容；当前变更及验收进度见 `docs/superpowers/plans/2026-10-25-api-review-implementation.md`，调用方变更见 `docs/api-simplification-migration.md`。工作区原有 `docs/superpowers/specs/2026-10-25-api-simplification-design.md` 与 `vendor/dbt` 改动保留；本次结论以独立核验为依据。

## Standards

规范依据：用户提供的 AGENTS.md、README.md、docs/typed-data-access.md、src/dbt_metricflow_service/storage/README.md。以下八组是有代码证据的设计判断，不把启发式 smell 当成硬性规范违规，也不重复列举 Ruff 能检查的问题。

1. **重复执行生命周期。** `application/queries.py:65–68` 先发现维度再执行查询；`execution/query.py:56,78` 分别进入 `_engine_context`。工程恢复、profile 生成、Manifest 读取和引擎初始化做两遍。让同一查询的维度发现、选项编译和执行共用一个请求级上下文。
2. **重复 DTO 与枚举。** `models/contracts.py:20` 与 `models/artifacts.py:9` 各有 Contract；`models/queries.py:22` 与 `platform/models.py:15` 各有 QueryMode。`application/queries.py:47` 又把 AvailableDimensions 转成同形 View。合并定义，避免一个协议变化维护多个位置。
3. **没有业务消费者的传参。** `application/queries.py:42,57` 的 caller、`platform/models.py:38` 的同步查询 run_id、`delegates/metricflow/queries.py:75` 生成的原生 metrics 没有后续业务消费。删除这些传递，鉴权仍执行；持久构建 run_id 另有真实用途。
4. **返回异常后重新抛出。** `execution/query.py:54–113` 三次出现 `catch → return ServiceError → isinstance → raise`。`execution/embedded.py` 的 run_serialized 已自然传播异常，直接抛出即可。不能删除线程退出屏障、串行锁或环境恢复。
5. **路由重复应用用例。** `api/deployments.py:32–35` 复制 DeploymentService.list；`api/catalog.py:29` 读取 Manifest 后丢弃，执行器又读取一次。列表和血缘编排归应用用例。`storage/deployments.py` 的 attempt_view 也应移到应用层，存储层返回记录。
6. **并行容器必须人为同步。** `platform/dimensions.py:23–57` 同时生成公开维度列表和内部映射，`application/query_plan.py:28` 又从公开列表建立索引。内部保留一份包含公开选项与 token/grain 映射的解析结果，响应时才投影公开字段。
7. **诊断包装成另一份任务。** `execution/dbt.py:54–61,87` 临时生成随机 JobRecord.id、重复的提交/开始时间，再转换成 JobErrorDetail。直接构造错误诊断，保留 stdout、异常类型、截断等有用信息。旧记录的 optional 字段仍可读取。
8. **内部协议过度可选。** `storage/builds.py:156,163` 先拒绝空 environment_id 又判非空过滤；`execution/build.py:72,140–143` 唯一调用始终提供 profile，却保留 fallback；`execution/models.py:64–67` 成功结果唯一字段 output_set_id 可空，但生产端成功时始终有值。必填参数和必填产物身份足够表达真实状态。

Standards 共 8 组，影响最大的是重复创建完整查询上下文。

## Spec

### P1 部署结算使用了过期观察

README.md:134 承诺“切换前重新核对远端 head、构建可用性和目标版本”。`storage/deployments.py:135–141` 更新普通观察却不改变 version，`:171–175` 结算只比较旧 version 和调用方传入的 head。

可复现顺序：协调器取得 B 的观察和 version=3；另一请求把观察更新成 C，version 仍为 3；协调器按旧 B 结算。实际 Store 方法配合 Session 替身得到 `active=B、observedHead=C、sourceState=MATCHED、DEPLOYED`。

修复方向：在结算事实事务内同时验证观察仍有效。可以让观察变更参与版本失效，或对当前观察字段做明确校验；需覆盖 UNKNOWN、MISSING 和观察被覆盖的交错，避免只补一个 head 相等判断。保留 generation、构建状态、产物和目标并发检查。这只能保证已知观察与提交一致，不能消除远端 Git 在最后一次观察后继续变化的事实。

### P2 幂等恢复被当前配置拦截

README.md:132 承诺“同幂等键恢复原身份”。但 `api/builds.py:30–34` 先校验当前实例清单，`storage/builds.py:68–78` 才恢复已有请求。构建受理后清单移除或变化，同键同输入重试会返回 `422 EXECUTION_TARGET_UNVERIFIED`。

实际路由函数与应用层替身复现：第一次进入 service；清空实例清单后第二次在入口失败，未再进入 service。管理命令 `admin.py` 也在调用 BuildService 前校验，有同样的顺序问题。

修复方向：鉴权后先恢复同键同输入或拒绝同键不同输入；仅新请求验证当前配置。最终防重和新构建受理必须保持事务原子性。不能只把校验函数从路由搬到 service 开头。

### P2 非法筛选值返回引擎故障

`models/queries.py` 接受 JsonValue，`IN value=3`、空列表、对象等可进入内部链路。`delegates/clients/metric_flow_client.py:51–55` 明确拒绝这些输入，却抛普通 ValueError；该异常被 `execution/query.py` 的宽泛异常转换捕获，最终成为 `503 COMMAND_FAILED`。

这与服务已有的请求错误 422 分类不一致，调用方难以区分输入错误与服务故障。既有查询计划 `docs/superpowers/plans/2026-10-25-native-manifest-sdk.md:19` 也将无效查询输入归为 422。实际转换链使用 I/O 替身复现了 503。

修复方向：服务自有的操作符/值形状检查在引擎初始化前完成；使用明确输入错误类型。SDK 负责指标、维度及查询语义。不能把所有 ValueError 都无差别转换成用户错误，因为产物损坏也可能抛 ValueError。

Spec 共 3 项，最严重的是部署观察与结算不一致。以上没有连接真实 PostgreSQL，不能代替数据库并发回归。

## 逐接口结论

| 接口 | 当前判断 | 建议 |
| --- | --- | --- |
| POST /builds | 实例校验在幂等恢复之前；受理规则分散于 HTTP、CLI 和 Store | 收敛受理顺序；完整执行快照、幂等和部署意图保留；BuildRequest 归 models/builds.py |
| GET /builds | environmentId 必填，下层却声明可选；大响应没有证据必须另建摘要模型 | 内部改必填并直接过滤；继续复用构建响应，避免再增一套列表 DTO |
| GET /builds/{build_id} | BuildView 只有一个 SnapshotBuildView 子类；两处公开 configVersion 同源 | 合并 BuildView；若允许改接口，只保留 execution.configVersion；worker 配置版本仍独立 |
| GET /builds/{build_id}/execution-logs | nextCursor 与 hasMore 语义不同；phase/command 有历史用途 | 保留日志协议和末页游标；日志模型先留在 builds.py，无需为几十行机械开包 |
| GET /builds/{build_id}/catalog | 已返回原生 Manifest，整体结构符合用途 | 保留 dbt 原生结构及完整性检查，不增加统一资源 DTO |
| GET /builds/{build_id}/resources/{resource_id} | resource 与 source 分别是原生节点和源文件，包装有实际意义 | 保留两部分；buildId/resourceId 回显暂保留，没有调用方消费证据支持直接删除 |
| GET /builds/{build_id}/lineage | 路由预读 Manifest 后丢弃，再由执行器读取 | CatalogService 暴露完整 lineage 用例，Manifest 只读一次；保留 dbt 原生 selector 和数组响应 |
| GET /deployments | 路由绕过已有 Service.list，复制错误处理与分页转换 | 直接调用 Service.list，响应投影归应用层 |
| GET /deployments/current | GET 进行 Git 观察并写入事实；公共 View 又被 reconcile 当内部状态使用 | 修复观察一致性；内部使用存储记录/观察结果，HTTP 最后投影；保留版本和意图事实 |
| POST /query/available-dimensions | 重复 DTO、无消费者 native.metrics、类型信息推导不充分 | 合并 DTO；删除原生 metrics；公开 valueType 在接口调整方案中删除；optionId、粒度和操作符保留 |
| POST /query | 四模式共用大请求和大响应；无效混用字段可被接受；重复上下文及异常包装 | 以 mode 区分请求与响应；合并上下文；提前检查筛选值；统一响应限制配置及其文档 |
| GET /health/live | 固定 ok 是健康探测协议，并非应删的无用字段 | 保留；使用显式 response_model，移到 api/system.py |
| GET /health/ready | 实际使用 SDK，却额外依赖 dbt/mf 可执行文件在 PATH；OpenAPI 无响应结构 | 去掉 CLI 的 PATH 检查，保留真实运行前提；这属于去除额外依赖的设计调整 |
| GET /versions | 版本有排障价值；OpenAPI 无响应结构 | 保留字段；显式 response_model，放在 api/system.py |

## 字段与分支的删减边界

| 分类 | 对象 | 处理 |
| --- | --- | --- |
| 已证实内部无消费者 | 查询 caller、同步查询 run_id、DimensionQueryResult.metrics | 删除；鉴权依赖和持久任务身份保留 |
| 重复类型 | Contract、QueryMode、AvailableDimensions/View、BuildView 唯一子类 | 合并；不建兼容转出口壳 |
| 重复公开字段 | BuildView.configVersion 与 execution.configVersion | 对当前受理数据来源相同；在允许协议调整时删除顶层字段 |
| 恒定说明 | boundaryPolicy | 改为文档说明，不在每次结果回传；这是公共协议变化 |
| 不完整的信息 | DimensionOption.valueType | 不是严格单值：原生维度常为 unknown，合成 metric_time 可为 datetime；建议删除误导性类型承诺，不伪造类型推导 |
| 模式无关字段 | PREVIEW 的时间回显、EXPLAIN 的空 rows/columns、取数结果的 sql=null | 按结果模式分别定义；mode 有真实区分作用，应保留 |
| 不必要的内部防御 | 已验证必填环境再次判空、唯一调用必传 profile 的 fallback | 用实际类型约束替代 |
| 有实际多值含义 | execution_stage、retry_policy、历史 operation、日志 phase/command | 保留；当前生产默认值不足以证明历史恒定 |
| 有一致性含义 | generation、version、lease token、摘要、产物保护和取消屏障 | 保留，不以减少 if 为理由删除 |

`DbtExecution.record.status` 也不能简单宣称成功分支永远不可达：dbt 返回成功之后捕获 Manifest 失败，exit_code 可能仍为 0。直接围绕失败构造诊断，比模拟第二个任务生命周期更准确。

## 需要修正的既有草案判断

1. **8 MiB 不是单纯配置失效。** README.md:163 明确规定公开查询最多 8 MiB，Settings.max_result_bytes 默认 16 MiB 则用于内部结果。两套限制不易理解，应统一设计，但不能当成现行实现必然违反规格。建议最终只保留一个公开响应上限，初始默认保持 8 MiB；若改成 16 MiB，要作为显式协议变化记录。
2. **EXPLAIN 的 limit+1 不是已证明的错误。** 维度显示值映射设计规定保留额外一行取数策略，EXPLAIN 返回相同路径的最终 SQL。它可以合理表示实际执行 SQL。建议本轮保持这一含义并写清文档，不顺带改为逻辑请求 limit。
3. **查询前置状态检查是策略变化。** README.md:25 明确要求查询不按状态、产物引用或工具链版本预判可用性。建议本轮保持现行策略，只明确产物恢复错误；若改为未完成统一 409，应单独确认并更新契约。
4. **常量集中化目前是仓库规范。** docs/typed-data-access.md 明确要求集中共享字符串。可以建议把普通局部字面量留在原处，但需要同步修订规范；不能把按现行标准写的常量判为违规。
5. **当前代码少一种值不代表历史少一种值。** 日志文档明确包含 PARSING/parse，不能因当前只调用 build 而删掉历史语义。

## 重构方案

方案 A 是保持 HTTP 完全兼容，只清理内部重复和修复行为问题；调用方无需改动，但混合模式请求和无关响应字段继续存在。

方案 B 是在 A 的基础上一起精简请求响应，调用方同步迁移。它更符合本次减少无意义字段的目标；用户已选择“允许调整接口，调用方同步适配”，因此采用方案 B。具体设计已通过独立可实施性检查，未发现其他重大未决问题。

不建议重写全部队列或数据库。任务、attempt、产物、部署各自承担不同事务职责，改成通用 Repository 或策略注册表不会解决这里的实际问题。

### 分包与文件职责

保留现有层次，将真正一起变化的代码放近。目录名称变更必须服务于依赖收敛，不以文件数变少或变多为验收标准。

```text
api/
  app.py              依赖组装、生命周期
  system.py           live、ready、versions
  builds.py           构建、历史、日志 HTTP
  catalog.py          目录、节点、血缘 HTTP
  deployments.py      部署 HTTP
  queries.py          查询 HTTP
  auth.py/errors.py/limits.py
application/
  builds.py           HTTP 与 CLI 共用的构建受理、读取和响应转换
  deployments.py      观察、协调和响应转换
  catalog.py          原生目录、节点、血缘用例
  queries.py          查询用例、最终响应与限制
  query_plan.py       选项解析、时间归一化、请求编译
  errors.py           应用错误
models/
  contracts.py        唯一 Contract、JsonObject、Page
  builds.py           BuildRequest、BuildView、构建与日志契约
  execution.py        执行快照、连接目标、物理目标身份
  queries.py          唯一 QueryMode、公开请求响应、维度选项
  deployments.py      部署公开协议与自然键
  catalog.py          resource/source
  service.py          健康、版本和错误响应
execution/
  build.py/query.py   执行上下文与编排
  embedded.py         串行、取消屏障、引擎全局状态恢复
  dbt.py              运行诊断与日志上下文
  ...                 保留有独立职责的日志、工作目录及产物工具
engines/
  dbt.py              DbtClient，dbt SDK 适配
  metricflow.py       MetricFlowClient 与维度能力读取
  models.py           编译后的查询、引擎维度和结果类型
  results.py          dbt/MetricFlow 共享的单元格序列化
platform/             Git 源码、profile、Manifest 捕获
storage/              现有具体 Store、ORM、记录、事务
workers/              任务领取、心跳、恢复
```

具体迁移：

- `platform/dimensions.py`、`platform/query_time.py` 的服务查询规则合入 `application/query_plan.py`；维度选项只维护一份内部集合。
- `delegates/clients` 与 `delegates/metricflow/queries.py` 收敛进 engines；DbtClient 不再导入 MetricFlow 模块私有 `_json_cell`。
- `platform/models.py` 的引擎请求/结果迁移到 engines/models.py；QueryMode 只引用同一份定义。
- `models/payloads.py` 的查询类型并入 models/queries.py；仍用于旧诊断的 ValidationSummary 归其诊断模型附近。删除的是迁移后空壳，不顺带删除 unrelated 历史模型。
- 存储记录与 HTTP DTO 保持区分：它们是不同边界，不能把所有同形类型都合并。
- 已有无生产引用的旧节点投影和 `execution/metricflow.py` 只记录为候选；除非本轮迁移使其成为孤立代码，否则另行清理。

### 一次查询的执行流程

```text
HTTP 模式校验与鉴权
  → 加载固定构建，校验服务自有的过滤值形状
  → 在串行执行线程中打开一次工作目录/profile/引擎上下文
  → 必要时发现维度并生成选项
  → 在同一上下文中编译并执行
  → 清理上下文、释放串行锁
  → 组装对应模式响应，检查最终响应大小
```

纯编译函数允许执行层在上下文内调用；应用服务不接收已打开的 SDK 连接或工作目录。用一个明确的查询入口保证生命周期，无需全局引擎缓存、通用 dispatcher 或插件策略。

### 方案 B 的查询协议

保留现有路径和 mode，用 Pydantic discriminated union 表达不同请求，禁止模式无关字段，即使传空数组或 null 也不接受。

| 模式 | 合法请求字段 | 结果 |
| --- | --- | --- |
| QUERY / EXPLAIN | metrics、groupBy、filters、startTime、endTime、orderBy、limit | QUERY 为数据；EXPLAIN 为最终执行 SQL |
| PREVIEW | selector、limit | 数据；没有时间条件回显 |
| DIMENSION_VALUES | metrics、dimensionOptionId、filters、startTime、endTime、limit | 维度值数据；不接受会被覆盖的 groupBy 和与结果字段不一致的 orderBy |

结果保留 buildId、commitSha、mode。QUERY 与 DIMENSION_VALUES 返回 columns、rows、resultTruncated、businessTimezone、normalizedTimeRange；EXPLAIN 返回 sql 和时间上下文；PREVIEW 返回数据部分。删除 boundaryPolicy，不删具有区分作用的 mode。Decimal 和日期序列化保持现有语义。

## 实施顺序与验收

1. **建立回归基线。** 当前 tests 没有 Python 测试源码；先为三个行为问题写能失败的用例，再覆盖 14 个入口的契约与错误。验证：修复前稳定复现，修复后通过。
2. **修复行为问题。** 幂等受理顺序、过滤值分类、部署观察一致性分别处理，避免与文件移动混成一个不可读 diff。验证：同键重试/冲突、新请求目标校验、错误 422、部署交错。
3. **删除内部冗余。** 合并类型，删空传参和异常返回包装，将不可能为空的内部参数改必填。验证：所有调用点和模块导入通过，线程异常与取消语义不变。
4. **收敛生命周期及职责。** 一次查询只恢复一次工程/初始化一次引擎，血缘 Manifest 只读一次，路由不碰 Store。验证：有行为意义的计数与完整查询回归，而非只测试函数转发。
5. **按已选兼容策略整理公共契约。** 方案 B 同步更新调用示例、OpenAPI 和迁移清单；保持 14 个入口。验证：四模式有效输入、混用字段、过滤/粒度/排序、时间归一化、精度、截断与上限。
6. **完成数据库与 SDK 验收。** 独立可丢弃 PostgreSQL 验证事务幂等、观察结算、租约与封存；真实 SDK 验证构建和四种查询模式。不使用业务库代替测试库。

本轮实际证据：

- `uv run --frozen --no-sync ruff check src`：通过。
- `uv run --frozen --no-sync pytest -q`：输出 `no tests ran`，不能报告回归通过。文档里的历史测试数量不是本 checkout 的验证结果。
- 使用 Runtime 替身创建实际 FastAPI 应用，OpenAPI 枚举得到 14 个操作；三个系统接口响应 schema 均为空。
- 实际 HTTP 认证失败返回 401，但专用认证错误码被转换为 HTTP_ERROR。建议让鉴权直接抛公开 ServiceError，未知框架异常仍脱敏。
- 实际模型接受 DIMENSION_VALUES 的 groupBy，以及 PREVIEW 的显式 metrics=[]、startTime=null；后者说明当前校验检查值真假而非字段是否允许出现。
- 实际 QueryService 的带维度请求依次调用 discover 和 execute；结合执行器实现确认两个独立上下文。
- 部署交错、幂等入口与非法过滤错误链已用实际代码加替身复现；没有连接真实 PostgreSQL，也没有运行真实仓库构建或仓库查询。

Standards：8 组，最大设计成本是重复查询上下文。Spec：3 项，最高优先级是部署观察与结算不一致。两轴分别评价，不用代码简洁程度掩盖行为问题。
