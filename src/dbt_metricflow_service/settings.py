from __future__ import annotations

import hashlib
import logging
import os
import tempfile
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Final, overload

import yaml
from pydantic import ValidationError

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.configuration import (
    DBT_PROFILES_ADAPTER,
    EXECUTION_CREDENTIALS_ADAPTER,
    EXECUTION_INSTANCES_ADAPTER,
    ConfigurationScalar,
    DbtProfiles,
    ExecutionCredential,
    ExecutionInstance,
    ServiceConfiguration,
)

INVALID_LISTEN_ADDRESS: Final[str] = "server host must be nonempty and port must be between 1 and 65535"
INVALID_WORKER_TIMING: Final[str] = "worker concurrency and heartbeat must be positive"
INVALID_LEASE_DURATION: Final[str] = "lease must cover at least three heartbeat intervals"
INVALID_STORAGE_LIMITS: Final[str] = "runtime storage limits must be positive"
INVALID_CONFIGURATION_YAML: Final[str] = "服务配置文件不是有效 YAML"
INVALID_CONFIGURATION_VALUES: Final[str] = "服务配置结构或配置值无效"
AMBIGUOUS_PROFILE_CONFIGURATION: Final[str] = "配置文件不能同时指定 DBT_PROFILES 和 DBT_PROFILES_DIR"


logger = logging.getLogger(__name__)

DEFAULT_PROFILES_DIR = strings.PROFILES_DIRECTORY
DEFAULT_MAX_OUTPUT_BYTES = 1_048_576
MAX_OUTPUT_BYTES_ENV = strings.MAX_OUTPUT_BYTES
# 默认启动文件独立于调用者的工作目录，部署时可用环境变量替换。
CONFIG_FILE_ENV = strings.SERVICE_CONFIG_FILE
DEFAULT_CONFIG_FILE = Path(__file__).resolve().parents[2] / strings.CONFIG_DIRECTORY / strings.SERVICE_CONFIG_FILE_NAME
UTF8 = strings.UTF8
PROFILES_DIR_KEY = strings.DBT_PROFILES_DIR
PROFILES_FILENAME = strings.DBT_PROFILES_FILE
GENERATED_PROFILES_DIR = strings.PROFILES_DIRECTORY
WRITE_MODE = strings.BINARY_WRITE_MODE
EXECUTION_INSTANCES = strings.EXECUTION_INSTANCES
EXECUTION_CREDENTIALS = strings.EXECUTION_CREDENTIALS


def _materialize_profiles(profiles: DbtProfiles, temp_root: Path) -> Path:
    # 保留 Jinja 模板，密码和任务 schema 仍由 dbt 进程从环境读取。
    content = yaml.safe_dump(
        DBT_PROFILES_ADAPTER.dump_python(profiles), allow_unicode=True, sort_keys=True,
    ).encode(UTF8)
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

    # dbt profiles.yml 所在目录，可来自部署挂载或服务配置生成的临时快照。
    profiles_dir: Path
    # Maximum number of bytes retained for each subprocess output stream.
    max_output_bytes: int
    # 启用 PostgreSQL 运行时的连接信息；repr 不包含凭据。
    database_url: str | None = field(default=None, repr=False)
    # 可丢弃的实例本地工作目录，不作为持久产物定位符。
    temp_root: Path = Path(strings.RUNTIME_TEMP_DIRECTORY)
    # 本机同时执行的任务数。
    worker_concurrency: int = 2
    # 数据库租约有效期及续租间隔，单位秒。
    lease_seconds: int = 90
    heartbeat_seconds: int = 15
    # 当前实例可执行的连接配置版本。
    config_version: str = strings.DEFAULT_CONFIG_VERSION
    # 部署镜像可显式指定工具链标识，默认由安装版本及服务代码计算。
    toolchain_version: str | None = None
    # 单结果、单文件和单产物集的原始字节上限。
    max_result_bytes: int = 8 * 1024 * 1024
    max_artifact_file_bytes: int = 64 * 1024 * 1024
    max_artifact_bytes: int = 256 * 1024 * 1024
    # HTTP 服务监听地址与端口，不影响管理命令的数据库连接。
    server_host: str = strings.LISTEN_HOST
    server_port: int = 8000

    # 物理实例身份与地址别名，仅为基础设施准入，不含环境落点配置。
    execution_instances: dict[str, ExecutionInstance] = field(default_factory=dict)
    # 允许使用的凭据引用到受保护环境变量名的映射，不包含明文值。
    execution_credentials: dict[str, ExecutionCredential] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            strings.EXECUTION_INSTANCES_FIELD,
            EXECUTION_INSTANCES_ADAPTER.validate_python(self.execution_instances),
        )
        object.__setattr__(
            self,
            strings.EXECUTION_CREDENTIALS_FIELD,
            EXECUTION_CREDENTIALS_ADAPTER.validate_python(self.execution_credentials),
        )
        # 监听配置错误应在创建服务前暴露。
        if not self.server_host or not 1 <= self.server_port <= 65535:
            raise ValueError(INVALID_LISTEN_ADDRESS)
        # 拒绝无法在租约到期前及时自停的配置。
        if self.worker_concurrency < 1 or self.heartbeat_seconds <= 0:
            raise ValueError(INVALID_WORKER_TIMING)
        if self.lease_seconds < self.heartbeat_seconds * 3:
            raise ValueError(INVALID_LEASE_DURATION)
        if min(self.max_result_bytes, self.max_artifact_file_bytes, self.max_artifact_bytes) <= 0:
            raise ValueError(INVALID_STORAGE_LIMITS)

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
        config = ServiceConfiguration()
        base_dir = Path.cwd()
        if config_file is not None:
            config_file = config_file.resolve()
            base_dir = config_file.parent
            try:
                config = ServiceConfiguration.model_validate(yaml.safe_load(config_file.read_text(encoding=UTF8)))
            except yaml.YAMLError:
                raise ValueError(INVALID_CONFIGURATION_YAML) from None
            except ValidationError:
                raise ValueError(INVALID_CONFIGURATION_VALUES) from None
        profiles = config.DBT_PROFILES
        if profiles is not None and config.DBT_PROFILES_DIR is not None:
            raise ValueError(AMBIGUOUS_PROFILE_CONFIGURATION)

        # 环境变量具有最高优先级；文件中的 null 使用该配置项的默认值。
        @overload
        def value(name: str, configured: ConfigurationScalar, default: str) -> str: ...

        @overload
        def value(name: str, configured: ConfigurationScalar, default: None = None) -> str | None: ...

        def value(name: str, configured: ConfigurationScalar, default: str | None = None) -> str | None:
            return os.getenv(name, str(configured) if configured is not None else default)

        # 文件路径以文件目录为基准，环境变量路径保留原有工作目录语义。
        def path(name: str, configured: ConfigurationScalar, default: str) -> Path:
            selected = Path(value(name, configured, default))
            return (selected if name in os.environ else base_dir / selected).resolve()

        instances = (EXECUTION_INSTANCES_ADAPTER.validate_json(os.environ[EXECUTION_INSTANCES])
                     if EXECUTION_INSTANCES in os.environ else config.EXECUTION_INSTANCES)
        credentials = (EXECUTION_CREDENTIALS_ADAPTER.validate_json(os.environ[EXECUTION_CREDENTIALS])
                       if EXECUTION_CREDENTIALS in os.environ else config.EXECUTION_CREDENTIALS)
        settings = cls(
            execution_instances=instances,
            execution_credentials=credentials,
            profiles_dir=path(strings.DBT_PROFILES_DIR, config.DBT_PROFILES_DIR, DEFAULT_PROFILES_DIR),
            max_output_bytes=int(value(MAX_OUTPUT_BYTES_ENV, config.MAX_OUTPUT_BYTES, str(DEFAULT_MAX_OUTPUT_BYTES))),
            database_url=value(strings.SERVICE_DATABASE_URL, config.SERVICE_DATABASE_URL) or None,
            temp_root=path(strings.SERVICE_TEMP_ROOT, config.SERVICE_TEMP_ROOT, strings.RUNTIME_TEMP_DIRECTORY),
            worker_concurrency=int(
                value(strings.WORKER_CONCURRENCY, config.WORKER_CONCURRENCY, strings.DEFAULT_WORKER_CONCURRENCY)
            ),
            lease_seconds=int(
                value(strings.JOB_LEASE_SECONDS, config.JOB_LEASE_SECONDS, strings.DEFAULT_LEASE_SECONDS)
            ),
            heartbeat_seconds=int(
                value(strings.JOB_HEARTBEAT_SECONDS, config.JOB_HEARTBEAT_SECONDS, strings.DEFAULT_HEARTBEAT_SECONDS)
            ),
            config_version=value(
                strings.SERVICE_CONFIG_VERSION, config.SERVICE_CONFIG_VERSION, strings.DEFAULT_CONFIG_VERSION
            ),
            toolchain_version=value(strings.SERVICE_TOOLCHAIN_VERSION, config.SERVICE_TOOLCHAIN_VERSION) or None,
            max_result_bytes=int(value(strings.MAX_RESULT_BYTES, config.MAX_RESULT_BYTES, str(8 * 1024 * 1024))),
            max_artifact_file_bytes=int(
                value(
                    strings.MAX_ARTIFACT_FILE_BYTES,
                    config.MAX_ARTIFACT_FILE_BYTES,
                    str(64 * 1024 * 1024),
                )
            ),
            max_artifact_bytes=int(
                value(strings.MAX_ARTIFACT_BYTES, config.MAX_ARTIFACT_BYTES, str(256 * 1024 * 1024))
            ),
            server_host=value(strings.SERVICE_HOST, config.SERVICE_HOST, strings.LISTEN_HOST),
            server_port=int(value(strings.SERVICE_PORT, config.SERVICE_PORT, strings.DEFAULT_PORT)),
        )
        # 显式环境变量可切换到外部 profile；仅在全部服务参数校验完成后生成文件。
        if profiles is not None and PROFILES_DIR_KEY not in os.environ:
            settings = replace(settings, profiles_dir=_materialize_profiles(profiles, settings.temp_root))
        return settings
