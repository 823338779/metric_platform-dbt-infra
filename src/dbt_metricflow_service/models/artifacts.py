"""服务拥有的接口契约基础配置。"""

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from dbt_metricflow_service import constants as strings


class Contract(BaseModel):
    """拒绝未约定字段，Python 内部名称映射为公开 camelCase。"""

    model_config = ConfigDict(extra=strings.EXTRA_FORBID, populate_by_name=True, alias_generator=to_camel)
