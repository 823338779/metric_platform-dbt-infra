from __future__ import annotations

import logging
from importlib.metadata import version

logger = logging.getLogger(__name__)
METRICFLOW_PACKAGE_VERSION = version("metricflow")
METRICFLOW_SUPPORTED_ADAPTERS = frozenset(
    {
        "athena",
        "bigquery",
        "databricks",
        "duckdb",
        "postgres",
        "redshift",
        "snowflake",
        "trino",
        "vertica",
    }
)
