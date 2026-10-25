# Memory Manifest Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Persist the final dbt Manifest from memory without rereading its JSON file during capture.

**Architecture:** Add an optional Manifest field to dbtRunnerResult. Serialize only when the final docs invocation requests a callback; feed those bytes into ArtifactStore.capture, retaining existing sealing and lease checks.

**Tech Stack:** Python, vendored dbt, SQLAlchemy, PostgreSQL, pytest.

**Spec:** docs/superpowers/specs/2026-10-25-memory-manifest-design.md

## Constraints and review focus

- Preserve command reparsing and original result types; never substitute an early parse snapshot.
- Failed/missing-manifest calls must not provide success payloads.
- Keep raw manifest content out of JobRecord diagnostics and logs.
- Memory payload must obey file/set limits and checksum validation.
- Preserve local files for existing validation and keep other artifacts unchanged.

## Tasks

- [x] Add failing tests in tests/test_memory_manifest.py for runner return, in-memory capture and failure/limit behavior. Extend tests/test_publication_build.py for final docs snapshot forwarding.
- [x] Add dbtRunnerResult.manifest in vendor/dbt/core/dbt/cli/main.py without changing result semantics. Add changelog entry.
- [x] Add optional manifest_sink to execution/dbt.py and runtime/executor.py; serialize the successful manifest and forward bytes from platform/build.py to storage/artifacts.py.
- [x] Run the new tests, publication and embedded regressions, isolated PostgreSQL artifact tests, and Ruff; review the final diff.

## Execution notes

Work in the existing checkout to retain the user's active vendored sources and configured editable environment. Preserve the existing staged deletion/addition in vendor/dbt/custom_tests. No commit, branch changes or business-data mutations are required.

Verified: 123 tests passed (2 pre-existing library deprecation warnings) across memory manifest, publication build, embedded engine, execution logs/storage, runtime artifacts, catalog queries/API and fixed-schema build. PostgreSQL tests ran in temporary isolated schemas. Ruff and both repository diff whitespace checks passed.

Independent review identified custom target-path handling; added two failing regression cases, derived the override path from project configuration, and reran the combined suite. Reviewer confirmed no remaining blocker. Serialization uses WritableManifest.to_dict(omit_none=False, context={"artifact": True}) with dbt's JSONEncoder; the vendored artifact model does not expose to_json().

Follow-up requested by user: manifest_sink now receives the Manifest object unchanged. Serialization and size checking belong to capture_manifest in platform/build.py, still invoked before the engine environment exits. Object-identity and publication tests failed before the move, then 69 focused tests passed (memory manifest, publication, embedded engine and logs); Ruff and whitespace checks passed.

Compression follow-up: compact UTF-8 JSON is serialized in capture_manifest; ArtifactStore stores gzip level 6 (mtime=0) for the memory manifest when smaller, retaining raw for tiny/incompressible payloads. Original byte limits, SHA-256 and collection digest remain based on uncompressed content; stored_size reflects stored bytes. Existing read_file/materialize decoding handles both codecs, with no rewrite of old records. Observed failing compression/compactness tests before implementation; final 73 tests passed (2 library warnings) covering memory manifests, publication, artifact corruption/limits/roundtrips and historical catalogs. Ruff/whitespace checks passed; independent review found no blockers.
