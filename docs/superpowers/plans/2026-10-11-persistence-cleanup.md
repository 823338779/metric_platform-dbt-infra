# Persistence cleanup implementation plan

**Goal:** Remove all five categories of redundant persistence approved in the review, while retaining fixed-build realtime queries and asynchronous build/deployment state.

**Architecture:** Keep the v3 build/deployment model and immutable execution inputs. Remove the obsolete release/branch model, generic job result storage, duplicated artifact metadata, and unused intermediate artifacts. The database remains explicitly initialized from schema.sql; do not mutate an existing user database.

**Tech stack:** Python, SQLAlchemy, PostgreSQL, dbt, MetricFlow, pytest.

**Spec:** User approval of all five review recommendations in this conversation.

## Constraints

- Preserve the existing uncommitted realtime query and naming changes.
- No query result cache, idempotency or query records.
- Keep build/deployment idempotency, lease fencing, recovery and cleanup protection.
- Keep source, required manifests, sealed catalog, provenance and integrity checks.
- Do not commit, deploy or delete existing database data.

## Tasks

- [x] Storage: remove JobResult, legacy releases/branches and default-output reuse; update entities, DDL, fixtures and transaction regressions.
- [x] Artifacts: remove duplicate catalog/metadata and unconsumed evidence/plan persistence; reduce execution artifacts; retain in-memory validation and required provenance.
- [x] Runtime: complete jobs with output identity only; remove publication completion wrapper and legacy executor paths; adapt consumers/tests.
- [x] Integration: verify cold restoration, all query modes, independent builds, deployment and cleanup with real PostgreSQL/dbt/MetricFlow.
- [x] Finish: run the full suite, lint and diff checks; review cross-layer removals and document schema transition constraints.

## Review focus

- Completion still atomically seals artifacts and updates build state; stale leases roll back.
- Cleanup cannot remove active deployments or builds serving realtime queries.
- No legacy table remains referenced by foreign keys, triggers, ORM or garbage collection.
- A fresh execution workspace from reduced artifacts supports all MetricFlow query modes.
- Success flags and provenance remain validated even without full evidence/result payloads.

## Progress

- Storage and artifact implementation dispatched with disjoint file ownership; runtime/integration handled in the parent.
- Removed orphan volatile input protocol, draft artifact kind and query retry configuration after their consumers were removed.
- Verification: full pytest with PostgreSQL, real engine integration and installed-wheel checks: 384 passed, 1 known baseline failure (`test_vendored_sources_are_at_audited_commits`, MetricFlow checkout 9a07d35 versus expected 05551f7). Vendor checkout unchanged.
- Additional real cleanup integration: 1 passed, including physical schema removal, retained history and active-build queries.
- Ruff and git diff --check passed. Independent read-only review found no actionable cleanup regressions.
- Test execution uses disposable databases; concurrent integration and generic worker tests must use separate databases because the worker reconciles all pending deployment intents.
- Existing user databases were not modified. Current schema.sql is for fresh initialization, as documented in README.
