from __future__ import annotations

import importlib.metadata
import json
import logging
import subprocess
import tomllib
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parents[1]
EXPECTED_VERSIONS = {
    "dbt-core": "1.12.5",
    "dbt-starrocks": "1.12.2",
    "dbt-duckdb": "1.11.0",
    "dbt-metricflow": "0.15.0",
    "metricflow": "0.213.0",
}
EXPECTED_LOCAL_SOURCES = {
    "dbt-core": "vendor/dbt/core",
    "dbt-metricflow": "vendor/dbt-metricflow/dbt-metricflow",
    "metricflow": "vendor/metricflow",
}
EXPECTED_SUBMODULE_COMMITS = {
    "vendor/dbt": "7f78d7b6aa3a88e5efa6dd92753e4983d92aeba4",
    "vendor/dbt-metricflow": "c10daa3b2eb275bd7a84fb1d84a76e6cdcaff3d2",
    "vendor/metricflow": "4200f85c59b2bb334f0b0dea851b38d0b8198134",
}


def installed_source_path(distribution_name: str) -> Path:
    """Return the local directory recorded by PEP 610 installation metadata."""
    direct_url_text = importlib.metadata.distribution(distribution_name).read_text(
        "direct_url.json"
    )
    assert direct_url_text is not None
    direct_url = json.loads(direct_url_text)
    assert direct_url.get("dir_info", {}).get("editable") is not True
    parsed_url = urlparse(direct_url["url"])
    assert parsed_url.scheme == "file"
    return Path(url2pathname(parsed_url.path)).resolve()


def test_upstream_versions_are_pinned() -> None:
    """The runtime must use the audited upstream release set."""
    installed = {name: importlib.metadata.version(name) for name in EXPECTED_VERSIONS}

    assert installed == EXPECTED_VERSIONS


def test_local_source_configuration_is_complete() -> None:
    """Every source-built distribution must point at an initialized vendored path."""
    configuration = tomllib.loads(
        (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )
    sources = configuration["tool"]["uv"]["sources"]

    assert {
        name: sources[name]["path"] for name in EXPECTED_LOCAL_SOURCES
    } == EXPECTED_LOCAL_SOURCES
    for relative_path in EXPECTED_LOCAL_SOURCES.values():
        assert (PROJECT_ROOT / relative_path).is_dir()


def test_vendored_sources_are_at_audited_commits() -> None:
    """The checked-out submodules must match the release commits accepted by the service."""
    for relative_path, expected_commit in EXPECTED_SUBMODULE_COMMITS.items():
        actual_commit = subprocess.run(
            ["git", "-C", str(PROJECT_ROOT / relative_path), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        ).stdout.strip()
        assert actual_commit == expected_commit


def test_core_packages_are_installed_from_vendored_sources() -> None:
    """The runtime must build dbt and MetricFlow packages from checked-out sources."""
    for distribution_name, relative_path in EXPECTED_LOCAL_SOURCES.items():
        assert installed_source_path(distribution_name) == (
            PROJECT_ROOT / relative_path
        ).resolve()
