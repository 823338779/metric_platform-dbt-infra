from __future__ import annotations

import logging
from pathlib import Path

import pytest
import yaml

from dbt_metricflow_service.settings import Settings

logger = logging.getLogger(__name__)

# 内联 profile 保留 dbt 模板，加载服务配置时不展开连接密码或任务 schema。
INLINE_PROFILES = {
    "ecommerce_metrics": {
        "target": "starrocks",
        "outputs": {"starrocks": {
            "type": "starrocks", "host": "127.0.0.1", "port": 9030,
            "schema": "{{ env_var('DBT_PLATFORM_SCHEMA', 'dbt_ecom') }}",
            "password": "{{ env_var('DBT_ENV_SECRET_STARROCKS_PASSWORD', '') }}",
        }},
    },
}


def test_settings_reads_paths_and_limits(monkeypatch, tmp_path: Path) -> None:
    """Environment overrides must define resolved paths and numeric limits."""
    projects = tmp_path / "projects"
    profiles = tmp_path / "profiles"
    projects.mkdir()
    profiles.mkdir()
    artifacts = tmp_path / "artifacts"
    monkeypatch.setenv("PROJECTS_ROOT", str(projects))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles))
    monkeypatch.setenv("COMMAND_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("MAX_OUTPUT_BYTES", "4096")
    monkeypatch.setenv("JOB_ARTIFACTS_ROOT", str(artifacts))

    settings = Settings.from_environment()

    assert settings.projects_root == projects.resolve()
    assert settings.profiles_dir == profiles.resolve()
    assert settings.command_timeout_seconds == 45
    assert settings.max_output_bytes == 4096
    assert settings.job_artifacts_root == artifacts.resolve()


def test_settings_defaults_to_local_directories(monkeypatch, tmp_path: Path) -> None:
    """A local checkout should use directories under its working directory by default."""
    monkeypatch.chdir(tmp_path)
    for name in ("PROJECTS_ROOT", "DBT_PROFILES_DIR", "JOB_ARTIFACTS_ROOT"):
        monkeypatch.delenv(name, raising=False)

    settings = Settings.from_environment()

    assert settings.projects_root == tmp_path / "projects"
    assert settings.profiles_dir == tmp_path / "profiles"
    assert settings.job_artifacts_root == tmp_path / "job-artifacts"


# 配置文件应决定服务参数及路径，不能被启动时的工作目录改变。
def test_settings_loads_file_relative_paths_and_server(monkeypatch, tmp_path: Path) -> None:
    config = tmp_path / "config" / "service.yaml"
    config.parent.mkdir()
    config.write_text(
        "SERVICE_HOST: 127.0.0.1\nSERVICE_PORT: 8123\nPROJECTS_ROOT: ../projects\n"
        "DBT_PROFILES_DIR: ../profiles\nJOB_ARTIFACTS_ROOT: ../artifacts\n"
        "PLATFORM_BINDINGS_FILE: bindings.json\nPLATFORM_DB_PATH: jobs.sqlite\n"
        "SERVICE_TEMP_ROOT: ../scratch\nWORKER_CONCURRENCY: 4\nCOMMAND_TIMEOUT_SECONDS: 45\n"
        "SERVICE_DATABASE_URL: postgresql://localhost/example\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path.parent)

    settings = Settings.from_file(config)

    assert settings.server_host == "127.0.0.1"
    assert settings.server_port == 8123
    assert settings.projects_root == tmp_path / "projects"
    assert settings.profiles_dir == tmp_path / "profiles"
    assert settings.job_artifacts_root == tmp_path / "artifacts"
    assert settings.platform_bindings_file == config.parent / "bindings.json"
    assert settings.platform_db_path == config.parent / "jobs.sqlite"
    assert settings.temp_root == tmp_path / "scratch"
    assert settings.worker_concurrency == 4
    assert settings.command_timeout_seconds == 45
    assert settings.database_url == "postgresql://localhost/example"


# 部署环境应覆盖文件；环境变量中的相对路径延续原有工作目录语义。
def test_settings_environment_overrides_selected_file(monkeypatch, tmp_path: Path) -> None:
    config = tmp_path / "service.yaml"
    config.write_text("SERVICE_PORT: 8123\nPROJECTS_ROOT: ignored\nSERVICE_DATABASE_URL: null\n", encoding="utf-8")
    monkeypatch.setenv("SERVICE_CONFIG_FILE", str(config))
    monkeypatch.setenv("SERVICE_PORT", "8234")
    monkeypatch.setenv("PROJECTS_ROOT", "environment-projects")
    monkeypatch.setenv("SERVICE_DATABASE_URL", "postgresql://localhost/override")
    monkeypatch.chdir(tmp_path)

    settings = Settings.from_file()

    assert settings.server_port == 8234
    assert settings.projects_root == tmp_path / "environment-projects"
    assert settings.database_url == "postgresql://localhost/override"


# 错误配置必须在启动时失败，不能静默使用默认值或在错误中泄露连接信息。
@pytest.mark.parametrize("content", ["UNKNOWN_SETTING: 1", "- item", "SERVICE_PORT: [8123]", "SERVICE_PORT: ["])
def test_settings_rejects_invalid_file(tmp_path: Path, content: str) -> None:
    config = tmp_path / "service.yaml"
    config.write_text(content, encoding="utf-8")
    with pytest.raises(ValueError):
        Settings.from_file(config)


def test_settings_requires_explicit_config_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        Settings.from_file(tmp_path / "missing.yaml")


def test_inline_profiles_are_generated_from_single_file(monkeypatch, tmp_path: Path) -> None:
    # 切换工作目录也必须按服务文件位置生成，并保留后续 dbt 才展开的模板。
    config = tmp_path / "service.yaml"
    config.write_text(yaml.safe_dump({"SERVICE_TEMP_ROOT": "scratch", "DBT_PROFILES": INLINE_PROFILES}))
    monkeypatch.chdir(tmp_path.parent)
    monkeypatch.setenv("DBT_ENV_SECRET_STARROCKS_PASSWORD", "must-not-be-materialized")

    settings = Settings.from_file(config)
    generated = settings.profiles_dir / "profiles.yml"

    assert generated.is_relative_to(tmp_path / "scratch")
    assert yaml.safe_load(generated.read_text(encoding="utf-8")) == INLINE_PROFILES
    assert "must-not-be-materialized" not in generated.read_text(encoding="utf-8")
    assert not (tmp_path / "profiles.yml").exists()
    timestamp = generated.stat().st_mtime_ns
    assert Settings.from_file(config).profiles_dir == settings.profiles_dir
    assert generated.stat().st_mtime_ns == timestamp


def test_inline_profile_update_preserves_running_instance_file(tmp_path: Path) -> None:
    # 配置编辑生成新快照，不改写仍在运行的任务所持有的 profile。
    config = tmp_path / "service.yaml"
    content = {"SERVICE_TEMP_ROOT": "scratch", "DBT_PROFILES": INLINE_PROFILES}
    config.write_text(yaml.safe_dump(content))
    old = Settings.from_file(config)
    changed = yaml.safe_load(config.read_text())
    changed["DBT_PROFILES"]["ecommerce_metrics"]["outputs"]["starrocks"]["host"] = "localhost"
    config.write_text(yaml.safe_dump(changed))
    new = Settings.from_file(config)

    assert old.profiles_dir != new.profiles_dir
    assert yaml.safe_load((old.profiles_dir / "profiles.yml").read_text()) == INLINE_PROFILES
    assert yaml.safe_load((new.profiles_dir / "profiles.yml").read_text()) == changed["DBT_PROFILES"]


def test_profiles_directory_environment_override_does_not_overwrite_external_file(monkeypatch, tmp_path):
    # 现有部署显式指定外部目录时，不生成也不覆盖用户维护的文件。
    external = tmp_path / "external"
    external.mkdir()
    original = "external-profile: unchanged\n"
    (external / "profiles.yml").write_text(original)
    config = tmp_path / "service.yaml"
    config.write_text(yaml.safe_dump({"SERVICE_TEMP_ROOT": "scratch", "DBT_PROFILES": INLINE_PROFILES}))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(external))

    settings = Settings.from_file(config)

    assert settings.profiles_dir == external
    assert (external / "profiles.yml").read_text() == original
    assert not (tmp_path / "scratch").exists()


@pytest.mark.parametrize("profiles", [
    {}, [], "secret-marker", {"demo": None},
    {"demo": {"target": "dev", "outputs": []}},
    {"demo": {"target": "dev", "outputs": {"dev": {"password": "secret-marker"}}}},
])
def test_invalid_inline_profiles_fail_without_leaking_values(tmp_path, profiles):
    # 缺少可用 profile 时必须拒绝启动，不能靠写空文件通过就绪检查。
    config = tmp_path / "service.yaml"
    config.write_text(yaml.safe_dump({"SERVICE_TEMP_ROOT": "scratch", "DBT_PROFILES": profiles}))
    with pytest.raises(ValueError) as error:
        Settings.from_file(config)
    assert "secret-marker" not in str(error.value)
    assert not (tmp_path / "scratch").exists()


def test_inline_profiles_and_file_directory_are_rejected_as_ambiguous(tmp_path):
    config = tmp_path / "service.yaml"
    config.write_text(yaml.safe_dump({"DBT_PROFILES_DIR": "external", "DBT_PROFILES": INLINE_PROFILES}))
    with pytest.raises(ValueError):
        Settings.from_file(config)
