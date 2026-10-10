"""内部序列化载荷的类型；仅描述形状，不增加转换或运行时校验。"""

from typing import Any, TypeAlias, TypedDict

# 原生 dbt/MetricFlow 扩展字段及 Pydantic 序列化结果的开放对象边界。
JsonObject: TypeAlias = dict[str, Any]
# 仅包含 JSON 支持的值，用于递归处理外部载荷。
JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]


class QueryOption(TypedDict):
    """绑定构建和指标集合的公开查询选项。"""

    optionId: str  # 构建、指标集合和原生路径共同确定的选项身份。
    resourceId: str | None  # 可唯一对应的目录维度，无法唯一对应时为空。
    displayName: str  # 面向调用方的维度或路径名称。
    dimensionType: str  # 时间或分类等维度语义。
    valueType: str  # 过滤值的公开数据类型。
    granularities: list[str]  # 当前选项支持的时间粒度。
    operators: list[str]  # 当前选项允许的过滤操作。


class QueryOptions(TypedDict):
    """异步选项计算完成后的固定版本结果。"""

    buildId: str  # 选项所属构建。
    metricResourceIds: list[str]  # 排序去重后的指标资源集合。
    options: list[QueryOption]  # 该指标集合可用的选项。


class ValidationCheck(TypedDict):
    """单项构建检查的脱敏结果。"""

    name: str  # 原生检查名称或失败阶段。
    status: str  # 通过、失败或跳过状态。
    message: str | None  # 失败时的安全提示。


class ValidationSummary(TypedDict):
    """构建失败摘要，不携带原始 CLI 输出。"""

    phase: str  # 发生失败的执行阶段。
    checks: list[ValidationCheck]  # 有界的检查结果集合。
    truncated: bool  # 原始证明或检查列表是否超出限制。


class HistoryMigrationReport(TypedDict):
    """历史发布映射到构建身份的结果。"""

    mapping: dict[str, str]  # 旧发布身份到新构建身份的映射。
    sourceIncomplete: list[str]  # 来源证据不完整的旧发布身份。
    conflicts: list[str]  # 来源或执行关联冲突的旧发布身份。
    dryRun: bool  # 是否只预览迁移结果。
