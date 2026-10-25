"""引擎日志的明确字段；不依赖存储层，不复制完整事件载荷。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from dbt_metricflow_service import constants as strings

if TYPE_CHECKING:
    from dbt_common.events.base_types import EventMsg

TIMESTAMP_ATTRIBUTE = "ts"
DATA_ATTRIBUTE = "data"
NODE_INFO_ATTRIBUTE = "node_info"
UNIQUE_ID_ATTRIBUTE = "unique_id"
LEVEL_ATTRIBUTE = "level"
NAME_ATTRIBUTE = strings.NAME_FIELD
CODE_ATTRIBUTE = "code"
INVOCATION_ATTRIBUTE = "invocation_id"
THREAD_ATTRIBUTE = "thread"
INFO_LEVEL = "info"


@dataclass(frozen=True, slots=True)
class EngineLog:
    """一次事件的内容，时间保留 UTC，文本在统一入口脱敏后才能外发。"""

    event_time: datetime
    level: str
    message: str
    event_name: str = ""
    event_code: str = ""
    invocation_id: str = ""
    node_id: str = ""
    thread_name: str = ""

    @classmethod
    def from_dbt(cls, event: EventMsg) -> EngineLog:
        """从 protobuf 事件提取字段，兼容不携带节点的命令级事件。"""
        info = event.info
        timestamp = getattr(info, TIMESTAMP_ATTRIBUTE, None)
        node = getattr(getattr(event, DATA_ATTRIBUTE, None), NODE_INFO_ATTRIBUTE, None)
        return cls(
            event_time=timestamp.ToDatetime(tzinfo=UTC) if timestamp is not None else datetime.now(UTC),
            level=getattr(info, LEVEL_ATTRIBUTE, INFO_LEVEL), message=str(info.msg),
            event_name=getattr(info, NAME_ATTRIBUTE, ""), event_code=getattr(info, CODE_ATTRIBUTE, ""),
            invocation_id=getattr(info, INVOCATION_ATTRIBUTE, ""),
            node_id=getattr(node, UNIQUE_ID_ATTRIBUTE, ""), thread_name=getattr(info, THREAD_ATTRIBUTE, ""),
        )
