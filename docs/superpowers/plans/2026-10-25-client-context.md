# Client Context Implementation Plan

**Goal:** Keep thin SDK clients in dbt-service and use supplied manifest for all engine operations.
**Spec:** docs/superpowers/specs/2026-10-25-client-context-design.md

- [x] Replace abandoned downstream-package tests with service Client tests. Run and confirm query still writes a semantic manifest (red).
- [x] In engines/metricflow.py cache DbtClient and native SemanticManifestLookup; create execution adapter using native dbt project configuration, without loading disk semantic artifacts.
- [x] Remove now-unused write_semantic_manifest from engines/dbt.py and update dimension test hooks.
- [x] Verify tests/test_client_manifest_context.py, existing dimension/catalog/API tests, full suite, lint, and diff. Document actual test coverage and limitations.

Constraints: preserve current work, HTTP/storage contracts, global execution locking and cleanup; no new vendor SDK. Continue in current checkout because current changes are the approved migration baseline.

Verification: initial regression 1 failed / 4 passed (old query wrote semantic_manifest.json); targeted suite 38 passed; complete suite 101 passed / 7 environment-gated skips; those 7 PostgreSQL and real SDK integration tests subsequently passed against isolated random schemas. Ruff src/tests and git diff --check passed. Independent read-only review found no actionable issues.
