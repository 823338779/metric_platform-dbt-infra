# 多通路查询：可执行用例与预期结果

以下例子对应真实 DuckDB 测试，不是只检查 SQL 文本。原生测试用 `sales`，服务测试的同义指标叫 `revenue`。订单与退款位于不同事实表，测试数据如下：

| 买家国家 | 卖家国家 | 日期 | sales | refunds |
| --- | --- | --- | ---: | ---: |
| CN | US | 2024-01-01 | 10 | 1 |
| CN | JP | 2024-01-02 | 20 | 2 |
| NULL | NULL | 2024-02-01 | 30 | 3 |
| US | DE | 2024-02-02 | 40 | 4 |

表中每行只是为了展示对应关系，两种金额分别来自订单表和退款表。

## 1. 两个公共维度，各指标使用相反通路

```python
from metricflow.engine.metricflow_engine import (
    MetricFlowQueryRequest, OutputColumnOrderMode,
)
from metricflow_semantics.specs.bound_group_by_parameter import (
    BoundGroupByParameter, MetricGroupByBinding,
)
from metricflow_semantics.specs.query_param_implementations import (
    DimensionOrEntityParameter, TimeDimensionParameter,
)

request = MetricFlowQueryRequest.create(
    metric_names=["sales", "refunds"],
    bound_group_by=(
        BoundGroupByParameter("country", (
            MetricGroupByBinding("sales", DimensionOrEntityParameter("buyer__country")),
            MetricGroupByBinding("refunds", DimensionOrEntityParameter("seller__country")),
        )),
        TimeDimensionParameter("metric_time", grain="month"),
        BoundGroupByParameter("counterparty", (
            MetricGroupByBinding("sales", DimensionOrEntityParameter("seller__country")),
            MetricGroupByBinding("refunds", DimensionOrEntityParameter("buyer__country")),
        )),
    ),
    output_column_order_mode=OutputColumnOrderMode.INPUT_ORDER,
)
result = engine.query(request)  # engine 使用包含上述实体关系的语义模型
```

输出列序是 `country, metric_time__month, counterparty, sales, refunds`。完整对齐键有三列，不能只按 country 合并。

| country | 月份 | counterparty | sales | refunds |
| --- | --- | --- | ---: | ---: |
| CN | 2024-01 | US | 10 | NULL |
| CN | 2024-01 | JP | 20 | NULL |
| US | 2024-01 | CN | NULL | 1 |
| JP | 2024-01 | CN | NULL | 2 |
| US | 2024-02 | DE | 40 | NULL |
| DE | 2024-02 | US | NULL | 4 |
| NULL | 2024-02 | NULL | 30 | 3 |

测试断言恰好 7 行，复合 NULL 键只产生一行，没有重复放大金额。

服务请求的 `groupBy` 对应结构如下。占位符必须替换为当前构建、对应指标集合的 available-dimensions 响应中的真实 optionId，不应人为约定或拼接 ID。

```json
{
  "mode": "QUERY",
  "metrics": ["revenue", "refunds"],
  "groupBy": [
    {"name": "country", "bindings": {
      "revenue": {"optionId": "<revenue 单指标的 buyer__country 选项>"},
      "refunds": {"optionId": "<refunds 单指标的 seller__country 选项>"}
    }},
    {"optionId": "<revenue+refunds 公共 metric_time 选项>", "grain": "month"},
    {"name": "counterparty", "bindings": {
      "revenue": {"optionId": "<revenue 单指标的 seller__country 选项>"},
      "refunds": {"optionId": "<refunds 单指标的 buyer__country 选项>"}
    }}
  ]
}
```

## 2. 同一个源维度映射到两个公共输出列

把例 1 的 counterparty 绑定改成与 country 完全相同，即 sales 都走 buyer、refunds 都走 seller。两个公共列仍须保留，不能因 source_spec 相同而去掉一列。此时 CN 的两笔销售按月聚合为 30，共 6 行；`country == counterparty`（包括同时为 NULL）。

这同时验证“计算时去重 grouping”和“输出时保留槽位”是两件不同的事。

## 3. 不同事实表的日期映射到一个公共月份

沿用 country 绑定，把时间输出改为：

```python
period = BoundGroupByParameter("period", (
    MetricGroupByBinding("sales", TimeDimensionParameter("order__ds", grain="month")),
    MetricGroupByBinding("refunds", TimeDimensionParameter("refund__ds", grain="month")),
))
```

再指定 `order_by_names=["period", "country"]`、`limit=2`。原生结果严格为两行：

```text
CN, 2024-01-01, 30,   NULL
JP, 2024-01-01, NULL, 2
```

排序和 limit 在各分支对齐后应用；服务仍额外取一行判断 resultTruncated。

## 4. 嵌套派生指标与顶层输入各走自己的通路

定义 `net_sales = sales - refunds`，`double_net_sales = net_sales * 2`。一次请求三个指标：

| 请求指标 | country 通路 |
| --- | --- |
| sales | buyer__country |
| net_sales | seller__country |
| double_net_sales | buyer__country |

预期结果：

| country | sales | net_sales | double_net_sales |
| --- | ---: | ---: | ---: |
| CN | 30 | NULL | 54 |
| US | 40 | 9 | 72 |
| JP | NULL | 18 | NULL |
| DE | NULL | 36 | NULL |
| NULL | 30 | 27 | 54 |

double_net_sales 的内部 net_sales 继承 buyer 路径，不能复用顶层 seller 路径的 net_sales 结果。没有给公式内部输入单独配置通路。

## 5. 共享筛选针对源路径，不针对公共输出别名

sales/revenue 走 buyer，refunds 走 seller。共享筛选选择 `buyer__country IN ('CN', 'US')` 后，refunds 仍能出现卖家国家 JP 和 DE；它们的买家符合条件。

服务测试使用真实公共 buyer 选项及 `IN` 数组验证，最终保留 CN/US 的销售与 US/JP/DE 的退款，去掉买家为 NULL 的源行。

## 6. 空数据和连续调用

- 退款表为空：保留 CN=30、US=40、NULL=30 的销售行，refunds 全为 NULL。
- 两张事实表都为空：返回零行，仍保留完整结果列；不制造一行零值。
- 同一 engine 连续执行 buyer 路径、seller 路径、`IS NULL` 筛选、不匹配筛选、原 buyer 请求：分别验证结果，并确认最后一次与第一次一致，路径与筛选没有串到后续请求。

## 测试入口

- `vendor/metricflow/tests_metricflow/engine/test_bound_query.py`：本轮新增 28 项参数化真实执行测试，覆盖 DFS/PassThrough × O4 子查询/O5 CTE。
- `tests/test_bound_query_integration.py`：本轮新增 3 项服务适配与真实执行测试，覆盖双维度、重复槽位、共享 IN 筛选。
- 固定金额、完整键、列名、位置值及空结果均有断言。没有增加生产逻辑或放宽校验来让这些例子通过。

本轮验证：原生场景文件 63 项通过；使用同一夹具的求值、执行和 Dataflow 回归 20 项通过，最终合并运行共 **83 项通过**。服务绑定查询集成文件 **27 项通过**；最后增强 IN 筛选的行数断言后，该用例再次通过。原生修改文件的 Ruff/Black/mypy、服务 Ruff 均通过。服务仍有 2 项既有依赖弃用警告。本轮未重跑与此次测试扩展无关的全仓库测试，不将上次原生全量的失败记录改写为全绿。
