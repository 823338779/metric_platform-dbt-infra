"""Convert a result row to the mutable dictionaries exposed by runtime stores."""

from typing import Any, overload

from sqlalchemy import Result, inspect

from dbt_metricflow_service.storage.entities import Base
from dbt_metricflow_service.storage.records import DatabaseRow


def row_dict(result: Result[Any]) -> DatabaseRow | None:
    row = result.mappings().fetchone()
    return dict(row) if row is not None else None


@overload
def entity_dict(entity: Base) -> DatabaseRow: ...


@overload
def entity_dict(entity: None) -> None: ...


def entity_dict(entity: Base | None) -> DatabaseRow | None:
    """在 Session 内读取实体列，返回不依赖懒加载的现有字典快照。"""
    if entity is None:
        return None
    return {field.columns[0].name: getattr(entity, field.key) for field in inspect(type(entity)).column_attrs}
