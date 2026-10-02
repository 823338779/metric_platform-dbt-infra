from __future__ import annotations

import hashlib
import logging
import os
import tempfile
from dataclasses import dataclass, field, replace
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

DEFAULT_PROJECTS_ROOT = "projects"
DEFAULT_PROFILES_DIR = "profiles"
DEFAULT_JOB_ARTIFACTS_ROOT = "job-artifacts"
DEFAULT_PLATFORM_DB_PATH = "platform-jobs.sqlite"
PLATFORM_BINDINGS_FILE_ENV = "PLATFORM_BINDINGS_FILE"
DEFAULT_COMMAND_TIMEOUT_SECONDS = 1800
DEFAULT_MAX_OUTPUT_BYTES = 1_048_576
MAX_OUTPUT_BYTES_ENV = "MAX_OUTPUT_BYTES"
# 默认启动文件独立于调用者的工作目录，部署时可用环境变量替换。
CONFIG_FILE_ENV = "SERVICE_CONFIG_FILE"
DEFAULT_CONFIG_FILE = Path(__file__).resolve().parents[2] / "config" / "service.yaml"
UTF8 = "utf-8"
INLINE_PROFILES_KEY = "DBT_PROFILES"
PROFILES_DIR_KEY = "DBT_PROFILES_DIR"
PROFILES_FILENAME = "profiles.yml"
GENERATED_PROFILES_DIR = "profiles"
PROFILE_TARGET_KEY, PROFILE_OUTPUTS_KEY, ADAPTER_TYPE_KEY = "target", "outputs", "type"
WRITE_MODE = "wb"
CONFIG_KEYS = frozenset({
    "SERVICE_HOST", "SERVICE_PORT", "PROJECTS_ROOT", "DBT_PROFILES_DIR", "COMMAND_TIMEOUT_SECONDS",
    "MAX_OUTPUT_BYTES", "JOB_ARTIFACTS_ROOT", "PLATFORM_BINDINGS_FILE", "PLATFORM_DB_PATH",
    "SERVICE_DATABASE_URL", "SERVICE_TEMP_ROOT", "WORKER_CONCURRENCY", "JOB_LEASE_SECONDS",
    "JOB_HEARTBEAT_SECONDS", "SERVICE_CONFIG_VERSION", "SERVICE_TOOLCHAIN_VERSION", "MAX_RESULT_BYTES",
    "MAX_ARTIFACT_FILE_BYTES", "MAX_ARTIFACT_BYTES", "SYNCHRONOUS_WAIT_SECONDS", INLINE_PROFILES_KEY,
})


def _validate_profiles(profiles: object) -> None:
    # 仅校验 dbt profile 必需结构；具体适配器字段仍由 dbt 校验，不输出连接内容。
    if not isinstance(profiles, dict) or not profiles:
        raise ValueError("DBT_PROFILES 必须是非空的 profile 映射")
    for name, profile in profiles.items():
        if not isinstance(name, str) or not name or not isinstance(profile, dict):
            raise ValueError("DBT_PROFILES 的名称和 profile 结构无效")
        target = profile.get(PROFILE_TARGET_KEY)
        outputs = profile.get(PROFILE_OUTPUTS_KEY)
        if not isinstance(target, str) or not target or not isinstance(outputs, dict) or not outputs:
            raise ValueError("DBT_PROFILES 的每个 profile 必须提供 target 和非空 outputs")
        for output_name, output in outputs.items():
            if (not isinstance(output_name, str) or not output_name or not isinstance(output, dict)
                    or not isinstance(output.get(ADAPTER_TYPE_KEY), str) or not output[ADAPTER_TYPE_KEY]):
                raise ValueError("DBT_PROFILES 的每个 output 必须提供适配器 type")


def _materialize_profiles(profiles: dict, temp_root: Path) -> Path:
    # 保留 Jinja 模板，密码和任务 schema 仍由 dbt 进程从环境读取。
    content = yaml.safe_dump(profiles, allow_unicode=True, sort_keys=True).encode(UTF8)
    # 内容变化使用新目录，避免管理命令或另一实例改写正在执行任务的连接配置。
    directory = temp_root / GENERATED_PROFILES_DIR / hashlib.sha256(content).hexdigest()
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / PROFILES_FILENAME
    if destination.exists() and destination.read_bytes() == content:
        return directory
    # 同目录临时文件原子替换，防止并发加载时 dbt 读取半写入的 YAML。
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode=WRITE_MODE, dir=directory, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(content)
        temporary.replace(destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return directory


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime paths and resource limits for one service process."""

    # Root containing dbt project directories addressable by project ID.
    projects_root: Path
    # dbt profiles.yml 所在目录，可来自部署挂载或服务配置生成的临时快照。
    profiles_dir: Path
    # Maximum wall-clock duration allowed for one CLI subprocess.
    command_timeout_seconds: int
    # Maximum number of bytes retained for each subprocess output stream.
    max_output_bytes: int
    # Service-owned root for task-isolated derived dbt and MetricFlow artifacts.
    job_artifacts_root: Path = Path(DEFAULT_JOB_ARTIFACTS_ROOT)
    # 服务拥有的项目 Git 与 profile 绑定配置；不由 HTTP 请求覆盖。
    platform_bindings_file: Path | None = None
    # 平台构建与查询的可重启 SQLite 任务索引。
    platform_db_path: Path = Path(DEFAULT_PLATFORM_DB_PATH)
    # 启用 PostgreSQL 运行时的连接信息；repr 不包含凭据。
    database_url: str | None = field(default=None, repr=False)
    # 可丢弃的实例本地工作目录，不作为持久产物定位符。
    temp_root: Path = Path("runtime-tmp")
    # 本机同时执行的任务数，临时 resources 同样受此上限约束。
    worker_concurrency: int = 2
    # 数据库租约有效期及续租间隔，单位秒。
    lease_seconds: int = 90
    heartbeat_seconds: int = 15
    # 当前实例可执行的连接配置版本。
    config_version: str = "1"
    # 部署镜像可显式指定工具链标识，默认由安装版本及服务代码计算。
    toolchain_version: str | None = None
    # 单结果、单文件和单产物集的原始字节上限。
    max_result_bytes: int = 16 * 1024 * 1024
    max_artifact_file_bytes: int = 64 * 1024 * 1024
    max_artifact_bytes: int = 256 * 1024 * 1024
    # 同步选项/清理接口等待持久任务的最大秒数。
    synchronous_wait_seconds: int = 30
    # HTTP 服务监听地址与端口，不影响管理命令的数据库连接。
    server_host: str = "0.0.0.0"
    server_port: int = 8000

    def __post_init__(self) -> None:
        # 监听配置错误应在创建服务前暴露。
        if not self.server_host or not 1 <= self.server_port <= 65535:
            raise ValueError("server host must be nonempty and port must be between 1 and 65535")
        # 拒绝无法在租约到期前及时自停的配置。
        if self.worker_concurrency < 1 or self.heartbeat_seconds <= 0:
            raise ValueError("worker concurrency and heartbeat must be positive")
        if self.lease_seconds < self.heartbeat_seconds * 3:
            raise ValueError("lease must cover at least three heartbeat intervals")
        if min(self.max_result_bytes, self.max_artifact_file_bytes, self.max_artifact_bytes) <= 0:
            raise ValueError("runtime storage limits must be positive")

    @classmethod
    def from_file(cls, config_file: Path | None = None) -> Settings:
        """服务和管理命令共享启动文件，环境变量覆盖文件中的配置。"""
        # 显式文件优先于环境变量选择的文件，未指定时加载仓库默认文件。
        selected = (
            config_file if config_file is not None else Path(os.getenv(CONFIG_FILE_ENV, str(DEFAULT_CONFIG_FILE)))
        )
        return cls.from_environment(config_file=selected)

    @classmethod
    def from_environment(cls, *, config_file: Path | None = None) -> Settings:
        """Build settings from environment, optionally over a YAML configuration file."""
        # 服务参数保持标量，仅 DBT_PROFILES 允许嵌套的标准 dbt profile。
        config = {}
        base_dir = Path.cwd()
        if config_file is not None:
            config_file = config_file.resolve()
            base_dir = config_file.parent
            try:
                config = yaml.safe_load(config_file.read_text(encoding=UTF8))
            except yaml.YAMLError:
                raise ValueError("服务配置文件不是有效 YAML") from None
            if not isinstance(config, dict) or not config.keys() <= CONFIG_KEYS:
                raise ValueError("服务配置必须是配置项映射，且不能包含未知配置项")
            if any(value is not None and type(value) not in (str, int)
                   for key, value in config.items() if key != INLINE_PROFILES_KEY):
                raise ValueError("服务配置值只能是字符串、整数或 null")
        profiles = config.get(INLINE_PROFILES_KEY)
        if profiles is not None:
            _validate_profiles(profiles)
            if config.get(PROFILES_DIR_KEY) is not None:
                raise ValueError("配置文件不能同时指定 DBT_PROFILES 和 DBT_PROFILES_DIR")

        # 环境变量具有最高优先级；文件中的 null 使用该配置项的默认值。
        def value(name: str, default: str | None = None) -> str | None:
            configured = config.get(name)
            return os.getenv(name, str(configured) if configured is not None else default)

        # 文件路径以文件目录为基准，环境变量路径保留原有工作目录语义。
        def path(name: str, default: str) -> Path:
            configured = Path(value(name, default))
            return (configured if name in os.environ else base_dir / configured).resolve()

        settings = cls(
            projects_root=path("PROJECTS_ROOT", DEFAULT_PROJECTS_ROOT),
            profiles_dir=path("DBT_PROFILES_DIR", DEFAULT_PROFILES_DIR),
            command_timeout_seconds=int(
                value("COMMAND_TIMEOUT_SECONDS", str(DEFAULT_COMMAND_TIMEOUT_SECONDS))
            ),
            max_output_bytes=int(value(MAX_OUTPUT_BYTES_ENV, str(DEFAULT_MAX_OUTPUT_BYTES))),
            job_artifacts_root=path("JOB_ARTIFACTS_ROOT", DEFAULT_JOB_ARTIFACTS_ROOT),
            platform_bindings_file=(
                path(PLATFORM_BINDINGS_FILE_ENV, "") if value(PLATFORM_BINDINGS_FILE_ENV) else None
            ),
            platform_db_path=path("PLATFORM_DB_PATH", DEFAULT_PLATFORM_DB_PATH),
            database_url=value("SERVICE_DATABASE_URL") or None,
            temp_root=path("SERVICE_TEMP_ROOT", "runtime-tmp"),
            worker_concurrency=int(value("WORKER_CONCURRENCY", "2")),
            lease_seconds=int(value("JOB_LEASE_SECONDS", "90")),
            heartbeat_seconds=int(value("JOB_HEARTBEAT_SECONDS", "15")),
            config_version=value("SERVICE_CONFIG_VERSION", "1"),
            toolchain_version=value("SERVICE_TOOLCHAIN_VERSION") or None,
            max_result_bytes=int(value("MAX_RESULT_BYTES", str(16 * 1024 * 1024))),
            max_artifact_file_bytes=int(value("MAX_ARTIFACT_FILE_BYTES", str(64 * 1024 * 1024))),
            max_artifact_bytes=int(value("MAX_ARTIFACT_BYTES", str(256 * 1024 * 1024))),
            synchronous_wait_seconds=int(value("SYNCHRONOUS_WAIT_SECONDS", "30")),
            server_host=value("SERVICE_HOST", "0.0.0.0"),
            server_port=int(value("SERVICE_PORT", "8000")),
        )
        # 显式环境变量可切换到外部 profile；仅在全部服务参数校验完成后生成文件。
        if profiles is not None and PROFILES_DIR_KEY not in os.environ:
            settings = replace(settings, profiles_dir=_materialize_profiles(profiles, settings.temp_root))
        return settings
