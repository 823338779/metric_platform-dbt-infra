"""Convert a result row to the mutable dictionaries exposed by runtime stores."""

from sqlalchemy import CursorResult

from dbt_metricflow_service.storage.records import DatabaseRow


def row_dict(result: CursorResult) -> DatabaseRow | None:
    row = result.mappings().fetchone()
    return dict(row) if row is not None else None
