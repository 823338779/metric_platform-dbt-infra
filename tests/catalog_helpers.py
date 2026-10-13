"""Complete typed catalog fixtures for tests that isolate query mapping."""
from uuid import UUID

from pydantic import TypeAdapter

from dbt_metricflow_service.models.artifacts import PublishedCatalog, PublishedResource

FIXTURE_ID = UUID("00000000-0000-0000-0000-000000000001")


def resource(resource_id, **changes):
    kind = changes.pop("kind", "METRIC" if resource_id.startswith("metric.") else "TABLE")
    attributes = {
        "METRIC": {"metricType": "simple", "definitionSummary": "fixture", "inputMetricIds": [],
                   "datasetIds": [], "timeConfig": {}},
        "TABLE": {"origin": "BUILT", "relation": {"schema": "fixture", "identifier": "fixture"}, "columns": {}},
    }[kind]
    return TypeAdapter(PublishedResource).validate_python({
        "resourceId": resource_id, "nativeId": resource_id, "kind": kind, "name": resource_id.split(".")[-1],
        "displayName": resource_id, "capabilities": [], "nativeDetails": {}, "attributes": attributes,
    } | changes)


def catalog(resources=()):
    return PublishedCatalog(project_id="fixture", release_id=FIXTURE_ID,
                            resources=list(resources), relations=[], relation_bindings=[])
