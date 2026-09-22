# Portable Development Bootstrap Design

## Purpose

Provide a one-command development bootstrap for Windows and macOS so a fresh checkout can install its required tools, create reproducible Python environments, initialize pinned submodules, and become usable from PyCharm without committing machine-specific interpreter paths.

The bootstrap prepares the project for PyCharm but does not install, launch, or modify PyCharm itself.

## Supported Systems

- Windows with `winget` available.
- macOS with Homebrew, or a system capable of installing Homebrew through its official installer.
- CPython is fixed to the latest uv-managed `3.12` patch release.
- Linux is outside the initial scope.

## User Interface

The repository exposes two platform launchers:

```powershell
.\bootstrap.ps1
.\bootstrap.ps1 --check
```

```bash
./bootstrap.sh
./bootstrap.sh --check
```

The default mode installs missing prerequisites, initializes source checkouts, creates or synchronizes environments, and verifies the result. `--check` is read-only and reports whether the same postconditions already hold.

## Architecture

### Platform launchers

`bootstrap.ps1` owns only Windows prerequisites and elevation behavior:

- Verify `winget` is available.
- Install Git and uv when missing.
- Detect Windows Developer Mode and request elevation when it must be enabled.
- Re-enter the unelevated bootstrap flow after the operating-system setting is ready.
- Invoke the shared orchestrator through uv-managed Python 3.12.

`bootstrap.sh` owns only macOS prerequisites:

- Install Homebrew through its official installer when missing.
- Install Git and uv when missing.
- Invoke the shared orchestrator through uv-managed Python 3.12.

Neither launcher contains dependency synchronization or project verification logic.

### Shared orchestrator

`scripts/bootstrap.py` is a standard-library Python program that implements all shared behavior. It executes commands as argument arrays without accepting arbitrary shell strings. It resolves the repository root from its own location and uses only repository-relative, allow-listed paths.

Command execution, platform inspection, and filesystem inspection are separated behind small typed functions so tests can substitute them without invoking package managers, modifying the registry, or contacting the network.

### Version declaration

`.python-version` contains `3.12`. The bootstrap explicitly requests Python `3.12` from uv rather than relying on a system interpreter. Tool versions or minimum supported versions used by the launchers are declared once in each launcher's constants and documented in the README.

## Environment Layout

The development environments use stable repository-relative paths:

```text
dbt-metricflow-service/
├── .venv/
└── vendor/
    ├── metricflow/.venv/
    └── dbt-metricflow/.venv/
```

- The root `.venv` is owned by uv and contains the service plus the root development dependency group.
- `vendor/metricflow/.venv` is the upstream MetricFlow `dev-env` created by Hatch.
- `vendor/dbt-metricflow/.venv` is created from `vendor/dbt-metricflow/dbt-metricflow/pyproject.toml`. The existing upstream Hatch post-install command installs the parent MetricFlow checkout in editable mode.

The orchestrator selects each Hatch environment location with the process-local `HATCH_ENV_TYPE_VIRTUAL_PATH` variable. It also selects the uv-managed Python 3.12 interpreter explicitly. No Hatch cache hash, user name, drive letter, or absolute interpreter path is stored in tracked files.

The three environments remain isolated because the service and upstream projects have incompatible development constraints, including different pytest and HTTPX ranges.

## Bootstrap Flow

The default flow performs these stages in order:

1. Validate the repository root and required tracked configuration files.
2. Install or locate Git and uv through the platform launcher.
3. Install uv-managed CPython 3.12.
4. Install the supported Hatch version as a uv tool.
5. On Windows, enable Developer Mode if necessary and set repository-local `core.symlinks=true`.
6. Initialize all submodules recursively.
7. Verify every submodule is at the gitlink commit recorded by the parent repository.
8. Synchronize the root environment with `uv sync --frozen --all-groups --python 3.12`.
9. Create or synchronize the MetricFlow Hatch `dev-env` at `vendor/metricflow/.venv`.
10. Create or synchronize the dbt-metricflow Hatch `dev-env` at `vendor/dbt-metricflow/.venv` from the nested dbt-metricflow project.
11. Run environment, import-source, symlink, lint, and representative-test verification.
12. Print a concise summary containing repository-relative interpreter paths.

Every successful stage is naturally repeatable. A subsequent invocation synchronizes existing environments instead of deleting them.

## Windows Symlink Handling

MetricFlow fixtures contain Git symlinks. A Windows checkout without Developer Mode can represent those links as plain files whose contents are paths, which causes YAML parsing failures unrelated to Python dependencies.

The Windows launcher may request administrator approval to enable Developer Mode. The orchestrator then configures symlink support only for the current Git repository.

If submodules were initialized before symlink support was enabled, repair is allowed only when all affected submodule worktrees are clean. If any submodule contains changes or untracked files, the bootstrap stops and lists them. It never resets, deinitializes, or overwrites a dirty submodule.

After initialization or repair, the bootstrap inspects known fixture links and fails if they are still regular text files.

## Safety and Recovery

- The orchestrator never accepts arbitrary commands or arbitrary target paths.
- It never deletes an unknown virtual environment.
- An existing environment that does not use Python 3.12 produces an actionable error and a narrowly scoped manual rebuild command.
- Tool-install and child-process failures retain the executable, arguments, exit code, and captured diagnostic output without exposing environment variables or credentials.
- Database credentials are neither requested nor logged.
- Submodule modifications stop any operation that could replace files.
- Failure stops the current run immediately. Rerunning resumes safely because completed stages are idempotent.
- `--check` performs no package installation, environment synchronization, registry write, Git checkout, or submodule update.

## Verification Contract

A default bootstrap is successful only when all of the following hold:

- The root and both upstream interpreters exist and report CPython 3.12.
- The root environment imports `dbt`, `metricflow`, `dbt_metricflow`, `fastapi`, and `pytest`.
- Installed metadata for dbt-core, MetricFlow, and dbt-metricflow identifies the pinned checkout under this repository's `vendor` directory.
- Each upstream environment imports its project and test dependencies.
- Dependency integrity checks have no unexplained missing requirement.
- Windows fixture symlinks are real links.
- `tests/test_dependencies.py` passes in the root environment.
- Ruff passes for repository-owned Python code.
- `tests_metricflow/integration/test_rendered_query.py::test_render_query` passes in the relevant upstream environment.

Database-backed StarRocks end-to-end tests remain opt-in because bootstrap does not collect credentials or provision an external database.

## PyCharm Experience

The bootstrap does not create or modify `.idea` metadata. PyCharm opens the repository as ordinary existing source and discovers the root `.venv`. Nested MetricFlow projects have stable `.venv` paths adjacent to their project definitions, so they can be selected without looking up a Hatch cache path.

The README lists these relative interpreter locations for both platforms:

- Root: `.venv/Scripts/python.exe` on Windows or `.venv/bin/python` on macOS.
- MetricFlow: `vendor/metricflow/.venv/Scripts/python.exe` on Windows or `vendor/metricflow/.venv/bin/python` on macOS.
- dbt-metricflow: `vendor/dbt-metricflow/.venv/Scripts/python.exe` on Windows or `vendor/dbt-metricflow/.venv/bin/python` on macOS.

No committed file refers to a specific user's home directory or absolute checkout location.

## Files

- Create `.python-version` for the Python 3.12 selection.
- Create `bootstrap.ps1` for Windows prerequisite installation and elevation.
- Create `bootstrap.sh` for macOS prerequisite installation.
- Create `scripts/bootstrap.py` for cross-platform orchestration.
- Create `scripts/tests/test_bootstrap.py` for isolated bootstrap behavior tests.
- Modify `pyproject.toml` so the root pytest suite includes `scripts/tests`.
- Modify `README.md` with one-command setup, interpreter locations, diagnostics, and recovery instructions.

The implementation does not modify source files inside `vendor/dbt`, `vendor/metricflow`, or `vendor/dbt-metricflow`.

## Test Strategy

Unit tests substitute the command runner and platform probes to cover:

- Windows and macOS prerequisite branches with tools present or absent.
- Administrator and Developer Mode decisions.
- Repository-root and allow-listed-path validation.
- Clean and dirty submodule handling.
- Correct generation of uv and Hatch command arguments.
- Existing Python 3.12 environments and incompatible environments.
- Immediate propagation of child-process failures.
- Read-only behavior of `--check`.

Repository-level verification runs the bootstrap in `--check` mode after setup, the root test suite, the requested MetricFlow test, and Ruff. Operating-system mutation and package-manager installation are not performed by automated unit tests.

## Documentation

The README begins its development setup with the two one-command entry points. Manual uv and Hatch commands remain available as diagnostic and recovery references rather than the primary onboarding path.
