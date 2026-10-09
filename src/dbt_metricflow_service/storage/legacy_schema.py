"""One-time adoption of the five immutable runtime SQL migrations."""

import json
from pathlib import Path

from sqlalchemy import Connection

from .postgres import MIGRATIONS

UNSUPPORTED = "Unsupported runtime database schema"
CYCLIC_FOREIGN_KEYS = (
    ("runtime_project", "source_set_id", "runtime_artifact_set", "set_id", "runtime_project_source_fk"),
    ("runtime_project", "current_output_set_id", "runtime_artifact_set", "set_id", "runtime_project_output_fk"),
    ("runtime_project", "busy_job_id", "runtime_job", "job_id", "runtime_project_busy_fk"),
    ("runtime_job", "input_set_id", "runtime_artifact_set", "set_id", "runtime_job_input_fk"),
    ("runtime_job", "output_set_id", "runtime_artifact_set", "set_id", "runtime_job_output_fk"),
    ("runtime_job", "current_attempt_id", "runtime_attempt", "attempt_id", "runtime_job_attempt_fk"),
)


def schema_shape(connection: Connection) -> dict:
    """Read the fixed historical contract, scoped to the current schema's OIDs."""
    tables = connection.exec_driver_sql(
        "SELECT c.oid,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relkind IN ('r','p') AND left(c.relname,8)='runtime_'",
    ).all()
    shape = {}
    schema = connection.exec_driver_sql("SELECT current_schema()").scalar_one()
    prefix = connection.dialect.identifier_preparer.quote(schema) + "."

    def normalized(value):
        return " ".join(value.replace(prefix, "").split())

    for oid, name in tables:
        columns = connection.exec_driver_sql(
            "SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,pg_get_expr(d.adbin,d.adrelid) "
            "FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
            "WHERE a.attrelid=%s AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum", (oid,),
        ).all()
        constraints = connection.exec_driver_sql(
            "SELECT pg_get_constraintdef(oid,true),convalidated FROM pg_constraint WHERE conrelid=%s", (oid,),
        ).all()
        indexes = connection.exec_driver_sql(
            "SELECT indisunique,indisvalid,pg_get_indexdef(indexrelid,0,true) FROM pg_index WHERE indrelid=%s", (oid,),
        ).all()
        triggers = connection.exec_driver_sql(
            "SELECT t.tgname,t.tgenabled,pg_get_triggerdef(t.oid,true),p.prosrc "
            "FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid "
            "WHERE t.tgrelid=%s AND NOT t.tgisinternal", (oid,),
        ).all()
        shape[name] = {
            "columns": {col: [kind, required, default] for col, kind, required, default in columns},
            "constraints": sorted([[normalized(definition), valid] for definition, valid in constraints]),
            "indexes": sorted([[unique, valid, normalized(definition.split(" USING ", 1)[1])]
                               for unique, valid, definition in indexes]),
            "triggers": sorted([[name, enabled, normalized(definition), normalized(body)]
                                for name, enabled, definition, body in triggers]),
        }
    return shape


def validate_legacy(connection: Connection, version: int) -> None:
    if version not in range(1, 6):
        raise RuntimeError(UNSUPPORTED)
    # Each entry replaces only tables changed by that archived migration.
    history = json.loads(Path(__file__).with_name("legacy_schema.json").read_text(encoding="utf-8"))
    expected = {}
    for step in history[:version]:
        expected.update(step)
    actual = schema_shape(connection)
    for table, required in expected.items():
        present = actual.get(table, {})
        if any(present.get("columns", {}).get(column) != definition
               for column, definition in required["columns"].items()):
            raise RuntimeError(f"{UNSUPPORTED}: columns of {table}")
        for category in ("constraints", "indexes", "triggers"):
            if any(item not in present.get(category, []) for item in required[category]):
                raise RuntimeError(f"{UNSUPPORTED}: {category} of {table}")


def legacy_version(connection: Connection) -> int:
    exists = connection.exec_driver_sql(
        "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relname='runtime_schema_version' AND c.relkind='r'",
    ).first()
    if not exists:
        if schema_shape(connection):
            raise RuntimeError(UNSUPPORTED)
        return 0
    versions = connection.exec_driver_sql("SELECT version FROM runtime_schema_version").scalars().all()
    if len(versions) != 1 or versions[0] not in range(1, 6):
        raise RuntimeError(UNSUPPORTED)
    return versions[0]


def _complete_initial_foreign_keys(connection: Connection) -> None:
    # 001's global constraint-name guard may skip these in a second schema.
    # Only called just after installing 001, never to repair an existing database.
    for table, column, target, target_column, name in CYCLIC_FOREIGN_KEYS:
        exists = connection.exec_driver_sql(
            "SELECT 1 FROM pg_constraint WHERE conrelid=%s::regclass AND conname=%s", (table, name),
        ).first()
        if not exists:
            connection.exec_driver_sql(
                f"ALTER TABLE {table} ADD CONSTRAINT {name} FOREIGN KEY({column}) REFERENCES {target}({target_column})",
            )


def adopt(connection: Connection) -> None:
    version = legacy_version(connection)
    if version:
        validate_legacy(connection, version)
    for number, migration in enumerate(MIGRATIONS[version:], start=version + 1):
        connection.exec_driver_sql(migration.read_text(encoding="utf-8"), execution_options={"no_parameters": True})
        if number == 1:
            _complete_initial_foreign_keys(connection)
    validate_legacy(connection, 5)
