"""Migrations run exclusively inside the admin command's locked transaction."""

from alembic import context

config = context.config
connection = config.attributes["connection"]
context.configure(connection=connection, version_table_schema=config.attributes["schema"])
with context.begin_transaction():
    context.run_migrations()
