# 用 MCP 查询上月各渠道销售额

下面是通用 Agent 的调用示例。假设提问日期为 2024-02-15，测试构建的业务时区为 UTC，测试数据为 2024 年 1 月。用实际工具返回的构建 UUID 和维度选项替换示例中的占位符。

1. 用户问“上月各渠道销售额是多少”。调用 `get_current_deployment(repository, environment_id, branch_name)`，从 `activeBuildId` 取得构建 A。若返回 `undeployed`，告诉用户该环境和分支尚无生效部署。若用户一开始指定 A，则直接使用 A。
2. 调用 `get_build(build_id=A)` 读取 `execution.businessTimezone`。调用 `search_catalog(build_id=A, keyword="销售额", resource_type="metric")`。如有 `nextCursor`，用相同筛选条件续页。结果有“销售额”和“双倍销售额”，分别读取候选的 `get_resource` 定义，再询问“你要看实际销售额，还是两倍销售额？”。
3. 用户选择实际销售额。使用已选的完整 ID `metric.api_review.revenue` 调用 `get_available_dimensions`，从返回的渠道维度取得 `optionId`。发现多个渠道口径时，先澄清。维度选项绑定到构建和指标组合。
4. 调用 `query_metrics`。请求示例如下。若分析期间部署切换到 B，仍传 A；服务若拒绝查询，向用户报告该构建的错误，并由用户决定是否改用 B。

```json
{
  "build_id": "<A>",
  "metric_resource_ids": ["metric.api_review.revenue"],
  "group_by": [{"optionId": "<渠道 optionId>"}],
  "order_by": [{"fieldId": "<渠道 optionId>", "direction": "ASC"}],
  "start_time": "2024-01-01T00:00:00Z",
  "end_time": "2024-01-31T23:59:59Z"
}
```

5. 读取返回的列名和类型解释每行。此测试集的渠道编码 `new`、`paid` 对应销售额为 2.00、12.34。向用户说明指标定义、响应中的 `businessTimezone`、`normalizedTimeRange`、`buildId` 及 `commitSha`。若 `resultTruncated` 为真，说明仅展示部分结果；成功且行集为空时报告该范围没有匹配数据。保持服务返回的空值和单位，不自行补零或添加币种。

## 提交构建与恢复

使用 `submit_build` 传入仓库、分支、完整提交 SHA、执行快照和显式部署策略。为每次新构建生成一个全局唯一的 UUID，填入 `idempotency_key`。快照中的 `credentialRef` 是已配置的引用。选 `ON_SUCCESS` 会触发部署尝试。返回构建 ID 后用 `get_build` 读取 `buildStatus`、`error` 和 `initialDeployment.deploymentStatus`。若收到 `BUILD_ACCEPTANCE_UNKNOWN`，用原 UUID 重试即可找回已受理的构建。构建成功、部署失败时同时报告两个事实。
