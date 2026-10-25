# dbt service 接口精简迁移

本次保留 14 个 HTTP 操作，直接更新请求与响应，不提供旧协议适配层。调用方应同步更新 DTO 和请求构造。
数据库 schema、构建身份、任务租约、产物封存及历史日志结构不变；无需为本次变更清空或改写存量数据。

## 公共字段变化

| 对象 | 删除或变化 | 调用方调整 |
| --- | --- | --- |
| 构建响应 | 删除顶层 configVersion | 从 execution.configVersion 读取外部配置版本；worker 的 SERVICE_CONFIG_VERSION 含义不变 |
| 维度选项 | 删除 valueType | 不依赖未可靠推导的值类型；dimensionType、granularities、operators 保留 |
| 查询响应 | 删除 boundaryPolicy | 时间边界继续由 MetricFlow 根据粒度解释，normalizedTimeRange 只表示业务时区归一化的输入 |
| 查询响应 | 按 mode 区分字段 | 不再假设四种模式都有 sql、rows、columns 和时间字段 |
| 认证错误 | 返回 service_credential_required 或 mutations_disabled | 不再把这两种情况当通用 HTTP_ERROR；状态码仍为 401/503 |
| 结果上限 | MAX_RESULT_BYTES 统一限制最终公开 JSON，默认 8 MiB | 查询、可用维度和血缘使用同一配置；UTF-8 字节计数，超限返回 RESULT_TOO_LARGE/422 |

buildId、commitSha、mode 仍保留在查询结果中。资源详情仍是 buildId/resourceId/resource/source；
日志仍保留 jobId、attemptId、phase、command、续读游标及 hasMore。

## 四种查询请求

所有模式仍调用 `POST /query?build_id=...`，必须带服务凭据。模式无关字段即使为 `null` 或 `[]` 也返回 422。

| mode | 必填字段 | 可选字段 |
| --- | --- | --- |
| QUERY、EXPLAIN | metrics | groupBy、filters、startTime、endTime、orderBy、limit |
| PREVIEW | selector | limit |
| DIMENSION_VALUES | metrics、dimensionOptionId | filters、startTime、endTime、limit |

metrics 至少一个指标；limit 默认 1000，范围 1–10000；selector 不能是空白文本。
过滤操作仍为 `= != > >= < <= IN`，最终还要满足该维度公开的 operators。
IN 要求 1–100 个有限 JSON 标量，其他操作只接受单个字符串、数字或布尔值；null、对象和嵌套数组不接受。
零、false 和空字符串是合法标量，不按真假忽略。指标、维度及粒度语义继续由 SDK 决定。

```json
{"mode":"QUERY","metrics":["revenue"],"groupBy":[{"optionId":"选项ID"}],"limit":100}
```

```json
{"mode":"EXPLAIN","metrics":["revenue"],"limit":100}
```

```json
{"mode":"PREVIEW","selector":"orders","limit":100}
```

```json
{"mode":"DIMENSION_VALUES","metrics":["revenue"],"dimensionOptionId":"选项ID","limit":100}
```

## 四种查询响应

| mode | 除 buildId、commitSha、mode 外的字段 |
| --- | --- |
| QUERY、DIMENSION_VALUES | businessTimezone、normalizedTimeRange、columns、rows、resultTruncated |
| EXPLAIN | businessTimezone、normalizedTimeRange、sql |
| PREVIEW | columns、rows、resultTruncated |

EXPLAIN 展示实际查询路径生成的 SQL，所以 limit=100 时仍可看到 LIMIT 101：额外一行用于实际取数时判断截断。
EXPLAIN 不执行指标取数。QUERY/DIMENSION_VALUES/PREVIEW 最多返回请求 limit 行，只有发现额外一行才标记截断。
Decimal 继续转成字符串以保留精度，日期/时间继续使用 ISO 字符串。

未增加按构建状态预判查询可用性的规则。产物恢复、SDK 初始化及运行失败保持服务错误；
输入校验与已识别的 SDK 查询选择错误为 422。

## 构建和部署行为修复

同键同输入重试先恢复原构建，即使当前实例清单已变化。不同输入仍为 409；只有新请求检查当前目标配置。
HTTP 和 CLI 使用同一受理规则，幂等查询、新请求校验和创建位于同一受理事务。

每次被接受的 Git 观察都会递增部署目标 version。结算必须匹配当前观察版本、head 和有效状态。
被更新观察覆盖的结算不会激活旧构建，也不会仅因观察竞争终结部署意图，后续协调会重试。
分支真实消失仍停用指针并将相关意图标记过期；网络失败仍记录 UNKNOWN。

## 模块迁移

- `engines/dbt.py`、`engines/metricflow.py`：SDK 客户端及 MetricFlow 维度能力读取。
- `engines/results.py`：两种引擎共用的单元格与结果转换。
- `engines/models.py`：编译后的查询和引擎结果；移除 run_id、重复 QueryMode 和原生 metrics 回显。
- `application/query_plan.py`：选项身份、时间归一化和编译；内部只维护一份已解析选项集合。
- `models/contracts.py`：唯一 Contract、Page 和 JsonObject；BuildRequest/BuildView 归 models/builds.py。
- `api/system.py`：健康与版本接口，显式公开 OpenAPI 模型；ready 不再检查未使用的 CLI 可执行文件。

迁移后的原 delegates、platform 查询辅助文件及重复模型文件已移除，不保留导入兼容壳。
完整调用结构和测试方法见 README。
