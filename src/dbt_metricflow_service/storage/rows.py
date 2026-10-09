"""Convert a result row to the mutable dictionaries exposed by runtime stores."""

from sqlalchemy import CursorResult


def row_dict(result: CursorResult) -> dict | None:
    row = result.mappings().fetchone()
    return dict(row) if row is not None else None
