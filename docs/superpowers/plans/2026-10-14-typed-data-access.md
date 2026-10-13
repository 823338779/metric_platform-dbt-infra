# Typed data access implementation plan

**Goal:** Replace fixed-field dictionary and JSON manipulation in service-owned code with explicit data models and attribute access.

**Architecture:** Validate external JSON/YAML and database snapshots at their boundaries using Pydantic models. Carry those objects through application, runtime and platform code; serialize only at HTTP, database and engine boundaries. Preserve public wire names and existing behavior.

**Scope:** `src/dbt_metricflow_service` and its tests. Vendored upstream implementations are dependency boundaries. Dynamic indexes, environment variables, arbitrary query values, native extension fields and ASGI/SQLAlchemy protocol mappings remain mappings where their keys are not a fixed service schema.

1. [x] Convert persistence records and nested request, artifact and diagnostic payloads; verify storage contracts and lifecycle tests.
2. [x] Convert catalog, manifest projections, dimensions and engine requests/results; verify publication and query tests.
3. [x] Convert service settings and execution profiles; verify configuration validation and secret handling tests.
4. [x] Integrate runtime/application/API consumers and update tests to use data objects internally while retaining JSON assertions at wire boundaries.
5. [x] Audit remaining accesses, document justified exceptions, run Ruff and the full test suite.

**Baseline:** 254 passed, 183 skipped, 1 existing failure in `test_vendored_sources_are_at_audited_commits` (dbt submodule HEAD differs from expected audited commit). PostgreSQL-dependent tests require a configured test database.

**Review focus:** missing/unknown fields; camelCase and snake_case aliases; null versus omitted fields; source freshness and hooks; credential redaction; leases and immutable snapshots.

**Final verification:** Ruff and git diff --check pass. Full pytest with independent PostgreSQL and real SDK integration: 464 passed, 3 skipped, 1 unchanged vendor commit audit failure. Exceptions documented in docs/typed-data-access.md.
