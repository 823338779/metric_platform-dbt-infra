"""dbt 和 MetricFlow 的调用边界与可公开请求错误。"""


class InvalidDimensionsRequest(ValueError):
    """引擎确认的查询选择错误，不包括连接或初始化故障。"""
