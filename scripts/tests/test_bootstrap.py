from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

from scripts.bootstrap import (
    DBT_METRICFLOW_EDITABLE_MISMATCH,
    BootstrapContext,
    BootstrapError,
    create_upstream_environment,
    environment_python,
    find_uv_python,
    parse_args,
    prepare_submodules,
    require_environment_version,
    resolve_repository_root,
    run_checked,
    submodule_is_clean,
    sync_environments,
    validate_uv_version,
    verification_environment,
    verify_environments,
    verify_pip_check,
    verify_submodule_commit,
    verify_windows_symlinks,
)


class RecordingRunner:
    def __init__(self, results: list[subprocess.CompletedProcess[str]] | None = None) -> None:
        self.calls: list[tuple[tuple[str, ...], Path, dict[str, str]]] = []
        self._results = list(results or [])

    def __call__(
        self,
        args: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append((tuple(args), cwd, dict(env or {})))
        if self._results:
            return self._results.pop(0)
        return subprocess.CompletedProcess(args, 0, "", "")


def test_parse_args_defaults_to_mutating_mode() -> None:
    assert parse_args([]).check is False


def test_parse_args_accepts_check_mode() -> None:
    assert parse_args(["--check"]).check is True


def test_environment_python_is_platform_specific(tmp_path: Path) -> None:
    assert environment_python(tmp_path, "windows") == tmp_path / "Scripts" / "python.exe"
    assert environment_python(tmp_path, "macos") == tmp_path / "bin" / "python"


def test_repository_root_requires_expected_configuration(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "bootstrap.py"
    script.parent.mkdir()
    script.touch()

    with pytest.raises(BootstrapError, match="pyproject.toml"):
        resolve_repository_root(script)


def test_context_uses_only_fixed_repository_paths(tmp_path: Path) -> None:
    context = BootstrapContext(root=tmp_path, platform="macos", check_only=True)

    assert context.root_environment == tmp_path / ".venv"
    assert context.metricflow_environment == tmp_path / "vendor" / "metricflow" / ".venv"
    assert context.dbt_metricflow_environment == tmp_path / "vendor" / "dbt-metricflow" / ".venv"


def completed(
    stdout: str = "",
    returncode: int = 0,
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(("git",), returncode, stdout, stderr)


def bash_path(path: Path) -> str:
    """Return a path accepted by native Bash or Git Bash."""
    resolved = path.resolve()
    if resolved.drive:
        return f"/{resolved.drive[0].lower()}{resolved.as_posix()[2:]}"
    return resolved.as_posix()


def test_dirty_submodule_stops_before_update(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "windows", False)
    (tmp_path / "vendor" / "dbt" / ".git").mkdir(parents=True)
    runner = RecordingRunner([completed(" M metricflow/file.py\n")])

    with pytest.raises(BootstrapError, match="vendor/dbt"):
        prepare_submodules(context, runner)

    assert all("update" not in args for args, _, _ in runner.calls)


def test_check_mode_never_configures_or_updates_git(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "windows", True)
    runner = RecordingRunner()

    with pytest.raises(BootstrapError, match="not initialized"):
        prepare_submodules(context, runner)

    forbidden = {"config", "update", "restore", "deinit", "reset", "checkout"}
    assert not any(forbidden.intersection(args) for args, _, _ in runner.calls)


def test_unexpected_submodule_commit_fails(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "macos", True)
    runner = RecordingRunner(
        [
            completed("160000 commit expected\tvendor/metricflow\n"),
            completed("actual\n"),
        ]
    )

    with pytest.raises(BootstrapError, match="gitlink"):
        verify_submodule_commit(context, runner, Path("vendor/metricflow"))


def test_windows_symlink_check_rejects_regular_file(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "windows", True)
    link_path = Path("fixtures/project_configuration.yaml")
    absolute_link = tmp_path / "vendor" / "metricflow" / link_path
    absolute_link.parent.mkdir(parents=True)
    absolute_link.write_text("../shared/project_configuration.yaml", encoding="utf-8")
    runner = RecordingRunner(
        [
            completed(f"120000 hash 0\t{link_path.as_posix()}\0"),
            completed(),
        ]
    )

    with pytest.raises(BootstrapError, match="regular file"):
        verify_windows_symlinks(context, runner)


def test_windows_symlink_check_accepts_real_link(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "windows", True)
    link_path = Path("fixtures/project_configuration.yaml")
    link_parent = tmp_path / "vendor" / "metricflow" / link_path.parent
    link_parent.mkdir(parents=True)
    target = link_parent / "shared.yaml"
    target.touch()
    try:
        (tmp_path / "vendor" / "metricflow" / link_path).symlink_to(target)
    except OSError as error:
        pytest.skip(f"Current process cannot create Windows symlinks: {error}")
    runner = RecordingRunner(
        [
            completed(f"120000 hash 0\t{link_path.as_posix()}\0"),
            completed(),
        ]
    )

    verify_windows_symlinks(context, runner)


def test_wrong_existing_python_is_not_replaced(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "macos", False)
    python = environment_python(context.root_environment, context.platform)
    python.parent.mkdir(parents=True)
    python.touch()
    runner = RecordingRunner([completed("cpython\n3.11\n/uv/python\n")])

    with pytest.raises(BootstrapError, match="Python 3.12"):
        require_environment_version(context, runner, context.root_environment)

    assert not any("sync" in args or "env" in args for args, _, _ in runner.calls)


def test_existing_environment_requires_an_interpreter(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "macos", False)
    context.root_environment.mkdir()

    with pytest.raises(BootstrapError, match="no Python interpreter"):
        require_environment_version(context, RecordingRunner(), context.root_environment)


def test_existing_environment_requires_cpython(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "macos", False)
    python = environment_python(context.root_environment, context.platform)
    python.parent.mkdir(parents=True)
    python.touch()
    runner = RecordingRunner([completed("pypy\n3.12\n/uv/python\n")])

    with pytest.raises(BootstrapError, match="CPython 3.12"):
        require_environment_version(context, runner, context.root_environment)


def test_find_uv_python_requires_managed_existing_interpreter(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "macos", True)
    python = tmp_path / "managed" / "python"
    python.parent.mkdir()
    python.touch()
    runner = RecordingRunner([completed(f"{python}\n")])

    assert find_uv_python(context, runner) == python
    assert runner.calls[0][0] == (
        "uv",
        "--no-python-downloads",
        "python",
        "find",
        "--system",
        "--managed-python",
        "3.12",
    )


def test_metricflow_environment_uses_local_path(tmp_path: Path) -> None:
    root = tmp_path / "checkout with spaces"
    context = BootstrapContext(root, "windows", False)
    runner = RecordingRunner()

    create_upstream_environment(
        context,
        runner,
        project=root / "vendor" / "metricflow",
        environment=root / "vendor" / "metricflow" / ".venv",
        python=Path("C:/uv/python.exe"),
    )

    create_args, create_cwd, create_env = runner.calls[0]
    assert create_args == (
        "uv",
        "venv",
        "--python",
        "C:\\uv\\python.exe",
        str(root / "vendor" / "metricflow" / ".venv"),
    )
    assert create_cwd == root / "vendor" / "metricflow"
    assert create_env == {}

    install_args, install_cwd, install_env = runner.calls[1]
    assert install_args == (
        "uv",
        "pip",
        "install",
        "--exact",
        "--python",
        str(root / "vendor" / "metricflow" / ".venv" / "Scripts" / "python.exe"),
        "--editable",
        f"{root / 'vendor' / 'metricflow'}[dev-env-requirements]",
    )
    assert install_cwd == root / "vendor" / "metricflow"
    assert install_env == {}


def test_dbt_metricflow_environment_installs_parent_editable(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "macos", False)
    project = tmp_path / "vendor" / "dbt-metricflow" / "dbt-metricflow"
    runner = RecordingRunner()

    create_upstream_environment(
        context,
        runner,
        project=project,
        environment=context.dbt_metricflow_environment,
        python=Path("/uv/python"),
        editable_overrides=(project.parent,),
    )

    assert runner.calls[2][0] == (
        "uv",
        "pip",
        "install",
        "--no-deps",
        "--python",
        str(context.dbt_metricflow_environment / "bin" / "python"),
        "--editable",
        str(project.parent),
    )


def test_check_mode_does_not_install_or_sync(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "macos", True)
    runner = RecordingRunner()

    sync_environments(context, runner)

    forbidden = {"install", "sync", "create"}
    assert not any(forbidden.intersection(args) for args, _, _ in runner.calls)


def test_pip_check_accepts_clean_environment(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "windows", True)
    runner = RecordingRunner([completed("No broken requirements found.\n")])
    python = Path("python.exe")

    verify_pip_check(context, runner, python)

    assert runner.calls[0][0] == (
        "uv",
        "pip",
        "check",
        "--python",
        str(python),
    )


def test_pip_check_accepts_only_the_known_editable_mismatch(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "windows", True)
    runner = RecordingRunner(
        [
            completed(
                "Using Python 3.12 environment at: .venv\n"
                "Checked 115 packages in 5ms\n"
                "Found 1 incompatibility\n"
                f"{DBT_METRICFLOW_EDITABLE_MISMATCH}\n",
                returncode=1,
            )
        ]
    )

    verify_pip_check(
        context,
        runner,
        Path("python.exe"),
        allowed_lines=frozenset({DBT_METRICFLOW_EDITABLE_MISMATCH}),
    )


def test_pip_check_rejects_an_additional_problem(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "windows", True)
    output = (
        "Checked 116 packages in 5ms\n"
        "Found 2 incompatibilities\n"
        f"{DBT_METRICFLOW_EDITABLE_MISMATCH}\n"
        "missing-package 1.0 requires absent-package\n"
    )
    runner = RecordingRunner([completed(output, returncode=1)])

    with pytest.raises(BootstrapError, match="missing-package"):
        verify_pip_check(
            context,
            runner,
            Path("python.exe"),
            allowed_lines=frozenset({DBT_METRICFLOW_EDITABLE_MISMATCH}),
        )


def test_old_uv_version_is_rejected(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "macos", True)

    with pytest.raises(BootstrapError, match="uv 0.12"):
        validate_uv_version(context, RecordingRunner([completed("uv 0.11.9\n")]))


def test_missing_executable_becomes_bootstrap_error(tmp_path: Path) -> None:
    def missing_runner(
        args: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del args, cwd, env
        raise FileNotFoundError("missing-tool")

    with pytest.raises(BootstrapError, match="Unable to start command"):
        run_checked(missing_runner, ("missing-tool",), cwd=tmp_path)


def test_check_verification_disables_repository_caches(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "macos", True)
    managed_python = tmp_path / "managed" / "python"
    managed_python.parent.mkdir()
    managed_python.touch()
    for environment in (
        context.root_environment,
        context.metricflow_environment,
        context.dbt_metricflow_environment,
    ):
        python = environment_python(environment, context.platform)
        python.parent.mkdir(parents=True)
        python.touch()
    identity = f"cpython\n3.12\n{managed_python.parent}\n"
    runner = RecordingRunner(
        [completed(f"{managed_python}\n"), *(completed(identity) for _ in range(3))]
    )

    verify_environments(context, runner)

    check_env = verification_environment(context)
    assert check_env["PYTHONDONTWRITEBYTECODE"] == "1"
    assert check_env["GIT_OPTIONAL_LOCKS"] == "0"
    pytest_calls = [args for args, _, _ in runner.calls if "pytest" in args]
    assert pytest_calls
    assert all(("-p", "no:cacheprovider") == args[-2:] for args in pytest_calls)
    ruff_call = next(args for args, _, _ in runner.calls if "ruff" in args)
    assert "--no-cache" in ruff_call
    assert all(
        env.get("PYTHONDONTWRITEBYTECODE") == "1"
        for args, _, env in runner.calls
        if "-c" in args or "pytest" in args or "ruff" in args
    )


def test_check_git_inspection_disables_optional_locks(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "macos", True)
    runner = RecordingRunner([completed()])

    assert submodule_is_clean(context, runner, Path("vendor/metricflow"))
    assert runner.calls[0][2]["GIT_OPTIONAL_LOCKS"] == "0"


def test_windows_launcher_parses_and_check_mode_skips_winget(tmp_path: Path) -> None:
    if sys.platform != "win32":
        pytest.skip("PowerShell launcher is exercised on Windows")
    powershells = tuple(
        dict.fromkeys(
            path
            for name in ("powershell", "pwsh")
            if (path := shutil.which(name)) is not None
        )
    )
    if not powershells:
        pytest.skip("PowerShell is not available")
    project_root = Path(__file__).parents[2]
    launcher = project_root / "bootstrap.ps1"
    marker = tmp_path / "winget-invoked"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "winget.cmd").write_text(
        f'@echo invoked>"{marker}"\r\n@exit /b 0\r\n',
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment["PATH"] = str(fake_bin) + os.pathsep + environment["PATH"]
    parse_command = (
        "$tokens = $null; $errors = $null; "
        "[System.Management.Automation.Language.Parser]::ParseFile("
        f"'{launcher}', [ref]$tokens, [ref]$errors) | Out-Null; "
        "if ($errors.Count -ne 0) { $errors; exit 1 }"
    )

    parsed_results = [
        subprocess.run(
            [powershell, "-NoProfile", "-Command", parse_command],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        for powershell in powershells
    ]
    checked = subprocess.run(
        [
            powershells[-1],
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(launcher),
            "--check",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        encoding="utf-8",
        errors="replace",
    )

    assert all(result.returncode == 0 for result in parsed_results)
    assert "positional parameter" not in checked.stderr
    assert not marker.exists()


def test_macos_launcher_has_valid_bash_and_rejects_unknown_argument() -> None:
    git_bash = Path("C:/Program Files/Git/bin/bash.exe")
    bash = str(git_bash) if git_bash.is_file() else shutil.which("bash")
    if bash is None:
        pytest.skip("Bash is not available")
    launcher = Path(__file__).parents[2] / "bootstrap.sh"

    syntax = subprocess.run(
        [bash, "-n", str(launcher)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    invalid = subprocess.run(
        [bash, str(launcher), "--unsupported"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert syntax.returncode == 0, syntax.stderr
    assert invalid.returncode != 0


def test_macos_launcher_rejects_linux_before_homebrew(tmp_path: Path) -> None:
    git_bash = Path("C:/Program Files/Git/bin/bash.exe")
    bash = str(git_bash) if git_bash.is_file() else shutil.which("bash")
    if bash is None:
        pytest.skip("Bash is not available")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    marker = tmp_path / "brew-invoked"
    uname = fake_bin / "uname"
    brew = fake_bin / "brew"
    uname.write_text("#!/usr/bin/env bash\necho Linux\n", encoding="utf-8")
    brew.write_text(
        f"#!/usr/bin/env bash\nprintf invoked > '{bash_path(marker)}'\n",
        encoding="utf-8",
    )
    uname.chmod(0o755)
    brew.chmod(0o755)
    result = subprocess.run(
        [
            bash,
            "-c",
            'PATH="$1:$PATH"; export PATH; bash "$2"',
            "bootstrap-test",
            bash_path(fake_bin),
            bash_path(Path(__file__).parents[2] / "bootstrap.sh"),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert result.returncode != 0
    assert "macOS" in result.stderr
    assert not marker.exists()


def test_macos_check_flow_uses_existing_tools_without_installing(tmp_path: Path) -> None:
    git_bash = Path("C:/Program Files/Git/bin/bash.exe")
    bash = str(git_bash) if git_bash.is_file() else shutil.which("bash")
    if bash is None:
        pytest.skip("Bash is not available")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    marker = tmp_path / "python-arguments"
    fake_python = fake_bin / "python"
    fake_python.write_text(
        f"#!/usr/bin/env bash\nprintf '%s' \"$*\" > '{bash_path(marker)}'\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)
    scripts = {
        "uname": "#!/usr/bin/env bash\necho Darwin\n",
        "git": "#!/usr/bin/env bash\nexit 0\n",
        "brew": "#!/usr/bin/env bash\nexit 99\n",
        "uv": (
            "#!/usr/bin/env bash\n"
            "case \"$*\" in\n"
            "  '--version') echo 'uv 0.12.17' ;;\n"
            "  'tool list') echo 'hatch v1.18.1' ;;\n"
            "  '--no-python-downloads python find --system --managed-python 3.12') "
            f"echo '{bash_path(fake_python)}' ;;\n"
            "  *) exit 98 ;;\n"
            "esac\n"
        ),
    }
    for name, content in scripts.items():
        executable = fake_bin / name
        executable.write_text(content, encoding="utf-8")
        executable.chmod(0o755)
    result = subprocess.run(
        [
            bash,
            "-c",
            'PATH="$1:$PATH"; export PATH; bash "$2" --check',
            "bootstrap-test",
            bash_path(fake_bin),
            bash_path(Path(__file__).parents[2] / "bootstrap.sh"),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert result.returncode == 0, result.stderr
    assert marker.read_text(encoding="utf-8").endswith("scripts/bootstrap.py --check")
