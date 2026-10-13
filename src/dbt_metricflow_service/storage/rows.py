"""Validate SQL boundary rows into declared, session-independent records."""

from datetime import date, datetime
from decimal import Decimal
from typing import TypeAlias, TypeVar, overload
from uuid import UUID

from pydantic import BaseModel, JsonValue
from sqlalchemy import Result

from dbt_metricflow_service.storage.entities import Base

Record = TypeVar("Record", bound=BaseModel)
DatabaseValue: TypeAlias = JsonValue | bytes | date | datetime | Decimal | UUID
RowTuple = TypeVar("RowTuple", bound=tuple[DatabaseValue, ...])


def row_record(result: Result[RowTuple], model: type[Record]) -> Record | None:
    row = result.mappings().fetchone()
    return model.model_validate(row) if row is not None else None


@overload
def entity_record(entity: Base, model: type[Record]) -> Record: ...


@overload
def entity_record(entity: None, model: type[Record]) -> None: ...


def entity_record(entity: Base | None, model: type[Record]) -> Record | None:
    """Read declared columns while the session is open; never expose ORM state."""
    return model.model_validate(entity, from_attributes=True) if entity is not None else None
