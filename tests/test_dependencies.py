from __future__ import annotations

import importlib.metadata
import logging

logger = logging.getLogger(__name__)

EXPECTED_VERSIONS = {
    "dbt-core": "1.12.5",
    "dbt-starrocks": "1.12.2",
    "dbt-metricflow": "0.15.0",
    "metricflow": "0.213.0",
}


def test_upstream_versions_are_pinned() -> None:
    """The runtime must use the audited upstream release set."""
    installed = {name: importlib.metadata.version(name) for name in EXPECTED_VERSIONS}

    assert installed == EXPECTED_VERSIONS
