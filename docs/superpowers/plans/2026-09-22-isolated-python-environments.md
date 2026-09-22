# Isolated Python Environments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Configure one main-service environment and two isolated upstream development environments, with all required packages importable from the intended local sources.

**Architecture:** The repository root keeps the uv-managed `.venv` used by the HTTP service. MetricFlow core and dbt-metricflow use their existing Hatch `dev-env` definitions so incompatible upstream development constraints do not leak into the service environment. IDE metadata points each module at its matching interpreter.

**Tech Stack:** Python 3.14, uv 0.12.17, Hatch, Pytest, PyCharm module SDKs

**Spec:** User-approved environment split in this task.

## Global Constraints

- Do not edit source files inside `vendor/dbt`, `vendor/metricflow`, or `vendor/dbt-metricflow`.
- Build the service's dbt-core, MetricFlow, and dbt-metricflow packages from the pinned submodules.
- Use uv for the root project and the upstream-provided Hatch environments for upstream tests.
- Preserve existing user changes and untracked files.

## Review Focus

- A root import must resolve `dbt`, `metricflow`, and `dbt_metricflow` from this checkout or its installed local-path distributions.
- MetricFlow's test environment must not reuse the root pytest/httpx versions.
- dbt-metricflow's test environment must install the matching local MetricFlow checkout.
- PyCharm module interpreters must resolve to existing Python executables.
- Windows symlink limitations must be reported separately from Python dependency failures.

---

### Task 1: Root service environment

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock`

**Interfaces:**
- Consumes: pinned path sources under `vendor/`
- Produces: `.venv` with service and root-test packages

- [ ] Restore the root development group to service-owned dependencies only.
- [ ] Run `uv lock` and `uv sync --frozen --all-groups`.
- [ ] Verify package versions, import paths, and `direct_url.json` local sources.

### Task 2: MetricFlow core development environment

**Files:**
- No tracked files; use `vendor/metricflow/pyproject.toml` as read-only Hatch configuration.

**Interfaces:**
- Consumes: MetricFlow's `dev-env-requirements` extra
- Produces: an isolated Hatch `dev-env` and its Python executable path

- [ ] Create the environment through Hatch.
- [ ] Verify imports for MetricFlow, SQLAlchemy, DuckDB, and pytest.
- [ ] Run the requested test and distinguish dependency failures from Windows symlink checkout failures.

### Task 3: dbt-metricflow development environment

**Files:**
- No tracked files; use `vendor/dbt-metricflow/dbt-metricflow/pyproject.toml` as read-only Hatch configuration.

**Interfaces:**
- Consumes: the nested package's `dev-env-requirements` and local parent MetricFlow source
- Produces: an isolated Hatch `dev-env` and its Python executable path

- [ ] Create the environment through Hatch.
- [ ] Verify `dbt_metricflow`, `metricflow`, dbt, and pytest imports and sources.

### Task 4: IDE indexing and final verification

**Files:**
- Modify locally ignored PyCharm `.iml` files only if interpreter mappings are missing or stale.
- Modify: `README.md` only if repeatable commands are not already documented.

**Interfaces:**
- Consumes: interpreter paths from Tasks 1–3
- Produces: locally resolvable IDE modules and documented commands

- [ ] Point each module at its intended interpreter without indexing `.venv` contents as source.
- [ ] Run root affected tests, full `pytest`, and `ruff check`.
- [ ] Confirm all submodules are source-clean and summarize any OS-level blocker.
