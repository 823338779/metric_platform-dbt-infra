# 使用 dbt model 承载业务本体的可行性

调研日期：2026-10-26。范围：官方能力与当前仓库静态检查；未修改或运行业务代码。

## 结论

可以用 **dbt 数据模型 + 结构化元数据约定** 实现面向分析的轻量领域本体：模型产出对象、关系、事件和历史状态的数据，YAML 描述含义，`meta` 保存补充业务语义，Manifest 提供机器读取入口。但 dbt 不会自动把自定义元数据变成对象查询、关系遍历、动作执行或形式逻辑推理能力。

## 能力边界

| 需求 | dbt 原生能力 | 需要补充的内容 |
| --- | --- | --- |
| 用户、订单、商品等对象及属性 | SQL model、列定义、description | 约定对象类型、稳定标识、粒度、业务含义；一张模型不一定对应一种对象 |
| 用户下单、订单包含明细等关系 | 外键列、关系表、relationships 数据测试 | 关系名称、方向、基数、连接字段、时间有效性等结构化约定与消费者 |
| 业务规则 | contracts、数据测试 | contracts 主要约束结果结构；业务断言需测试 SQL，执行效果取决于配置和平台 |
| 历史状态 | snapshots 的 SCD2 | 区分采样历史和完整事件流；完整事件需要上游日志或 CDC 等数据源 |
| 业务词汇与模型映射 | `meta` 任意键值及 Manifest | 自定义 schema、校验与读取逻辑；dbt 不理解 `ontology` 的业务语义 |
| 指标查询 | semantic models、MetricFlow | 关联业务对象与指标的约定，避免重复定义指标公式 |
| 按对象 ID 查询、沿关系遍历 | 表和视图提供底层数据 | 对象查询工具/API，或受控 SQL 查询层 |
| 下单、退款、补货等动作 | 可保存动作描述 | 业务服务、权限、事务及动作执行运行时 |

依据：[meta](https://docs.getdbt.com/reference/resource-configs/meta)、[Manifest](https://docs.getdbt.com/reference/artifacts/manifest-json)、[contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts)、[数据测试](https://docs.getdbt.com/docs/build/data-tests)、[snapshots](https://docs.getdbt.com/docs/build/snapshots)、[MetricFlow](https://docs.getdbt.com/docs/build/about-metricflow)。表中补充项是基于能力边界的工程判断，并非 dbt 原生功能声明。

## 两个关键区别

`ref()` 建立模型构建依赖并解析数据库关系名。它表达“这张结果由哪些上游数据计算”，不会自动表达“用户购买商品”或“一张订单包含多条明细”。同理，`relationships` 测试可检查引用值的存在性，但不能完整定义关系的业务含义与全部基数规则。[ref 官方说明](https://docs.getdbt.com/reference/dbt-jinja-functions/ref)、[数据测试](https://docs.getdbt.com/docs/build/data-tests)

Snapshot 记录批量执行时观察到的状态变化，提供 SCD2 历史。由其批处理机制可推知，两次采集之间已经被覆盖的中间状态不能凭空恢复；所以 snapshot 不等于订单已提交、已支付、已退款的完整事件流。Contracts 也不等于所有数据约束都由数据库强制执行，官方明确区分不同适配器的约束支持程度。[snapshots](https://docs.getdbt.com/docs/build/snapshots)、[contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts)

## 当前仓库已经具备的基础

以下路径和行号基于本次静态检查：

- `src/dbt_metricflow_service/platform/manifest_capture.py:34`：序列化完整 WritableManifest，仅删除节点顶层运行路径；第 40 行明确保留 `meta/config` 内的业务字段。
- `src/dbt_metricflow_service/engines/dbt.py:81`：`resource()` 返回完整原生节点，因此可读取解析进节点的自定义 metadata；`application/catalog.py:68` 将该节点与源码一起返回。
- `src/dbt_metricflow_service/engines/dbt.py:102`：从保存的 Manifest 转换原生语义模型，支持现有 MetricFlow 查询链路。
- `src/dbt_metricflow_service/engines/dbt.py:84`：目录与血缘委托 `dbt ls`，返回的是 dbt 资源关系，不是业务对象图。
- `src/dbt_metricflow_service/models/queries.py:81`：已有 PREVIEW，但请求只有 `selector` 与 `limit`；`engines/dbt.py:111` 通过 `dbt show` 预览模型行。它不是按 `User.id` 检索并遍历订单、评价的对象查询 API。
- `README.md:185`、`README.md:204`、`README.md:218`：说明 Manifest、源码快照和语义转换的保留与读取方式。

因此，当前服务不只是“只有指标”，它已提供模型目录、原生资源详情、血缘与数据预览，并具备保存和暴露业务本体元数据的基础；但现有接口没有解释自定义 ontology 约定，也没有通用对象检索和关系遍历运行时。

## 对该项目的适用方式

可将稳定业务对象投影为 dbt models，例如 `users`、`orders`、`order_items`、`reviews`，将库存明确建模为 SKU × 仓库/店铺 × 时间粒度；用 `config.meta` 保存一个自定义命名空间，描述对象标识、关系、状态或事件时间。该命名空间是项目协议，不是 dbt 官方 ontology 标准。

对于只读 LLM 分析，首先需要可发现的业务定义和可获取的事实证据，未必需要图数据库或完整动作平台。当前仓库能承载定义的传递；如需 LLM 查询具体用户的订单、评价或历史库存，还要补充受控的数据访问能力。若仅把这些定义写入 YAML 而没有消费者，得到的是结构化文档，不是可运行的本体系统。

以上为可行性评估，不是已批准的实现方案。在线 dbt 文档有版本差异；具体配置语法、约束和适配器行为仍需以项目依赖版本验证。
