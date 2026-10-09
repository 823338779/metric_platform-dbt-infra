"""Alembic coordinator; callers own the connection and its transaction."""

from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Connection

from .legacy_schema import UNSUPPORTED, legacy_version


def alembic_config(connection: Connection) -> Config:
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).with_name("alembic")).replace("%", "%%"))
    config.attributes["connection"] = connection
    config.attributes["schema"] = connection.exec_driver_sql("SELECT current_schema()").scalar_one()
    return config


def _versions(connection: Connection, config: Config) -> tuple[tuple[str, ...], str]:
    scripts = ScriptDirectory.from_config(config)
    heads = scripts.get_heads()
    if len(heads) != 1:
        raise RuntimeError(UNSUPPORTED)
    context = MigrationContext.configure(connection, opts={"version_table_schema": config.attributes["schema"]})
    current = context.get_current_heads()
    known = {revision.revision for revision in scripts.walk_revisions()}
    if len(current) > 1 or any(revision not in known for revision in current):
        raise RuntimeError(UNSUPPORTED)
    return current, heads[0]


def check(connection: Connection) -> None:
    config = alembic_config(connection)
    current, head = _versions(connection, config)
    if current != (head,) or legacy_version(connection) != 5:
        raise RuntimeError(f"{UNSUPPORTED}; run dbt-service-admin migrate")


def migrate(connection: Connection) -> None:
    connection.exec_driver_sql("SELECT pg_advisory_xact_lock(609302026)")
    config = alembic_config(connection)
    current, _ = _versions(connection, config)
    if current and legacy_version(connection) != 5:
        raise RuntimeError(UNSUPPORTED)
    command.upgrade(config, "head")
    check(connection)
