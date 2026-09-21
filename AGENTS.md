# dbt MetricFlow Service 规则

本仓库是 dbt 与 MetricFlow 的独立 HTTP 包装服务。

- 上游版本固定为 `dbt-core==1.12.5`、`dbt-starrocks==1.12.2`、`dbt-metricflow==0.15.0` 和 `metricflow==0.213.0`。
- 不得读取、修改、复制或构建父工作区的 `dbt/` 与 `metricflow/` 源码。
- dbt 与 MetricFlow 只能通过已安装包提供的公开 CLI 或公开 API 调用。
- 所有 Python 文件使用 `from __future__ import annotations`、模块 logger 和完整类型注解。
- 增量字段必须用 `Field(description=...)` 或相邻注释说明用途。
- 按逻辑块添加说明用途和关键约束的注释，避免解释不言自明的语句。
- 可复用字符串定义为模块常量；配置键、默认值和错误消息可以直接使用。
- 不接受任意 shell 字符串、任意文件路径或未经白名单约束的命令。
- 凭据只能通过环境变量或只读 profile 挂载提供，不得进入源码、测试固件、日志或响应。
- 使用 `uv` 管理依赖；修改行为时先写失败测试，再做最小实现。
- 完成任务前运行受影响测试、完整 `pytest` 和 `ruff check`。
