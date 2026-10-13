from __future__ import annotations

import logging
from importlib.metadata import version

from dbt_metricflow_service import constants as strings

logger = logging.getLogger(__name__)
METRICFLOW_PACKAGE_VERSION = version(strings.METRICFLOW_PACKAGE)
METRICFLOW_SUPPORTED_ADAPTERS = frozenset(
    {
        strings.ATHENA_ADAPTER,
        strings.BIGQUERY_ADAPTER,
        strings.DATABRICKS_ADAPTER,
        strings.DUCKDB_ADAPTER,
        strings.POSTGRES_ADAPTER,
        strings.REDSHIFT_ADAPTER,
        strings.SNOWFLAKE_ADAPTER,
        strings.TRINO_ADAPTER,
        strings.VERTICA_ADAPTER,
    }
)
