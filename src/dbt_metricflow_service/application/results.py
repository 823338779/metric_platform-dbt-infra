"""按最终公开 JSON 约束查询、维度和血缘的响应大小。"""

from pydantic import TypeAdapter

from .. import constants as strings
from ..models.contracts import Contract, JsonObject
from .errors import ServiceError

_JSON_OBJECTS = TypeAdapter(list[JsonObject])


def check_result_size(value: Contract | list[JsonObject], maximum: int, build_id: str) -> None:
    if isinstance(value, Contract):
        size = len(value.model_dump_json(by_alias=True).encode())
    else:
        size = len(_JSON_OBJECTS.dump_json(value, by_alias=True))
    if size > maximum:
        raise ServiceError(strings.RESULT_TOO_LARGE, "query response exceeds the size limit", 422,
                           build_id=build_id)
