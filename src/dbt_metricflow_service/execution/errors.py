"""后台执行失败及可持久化诊断。"""
from dbt_metricflow_service.storage.records import JobErrorDetail


class ExecutionError(RuntimeError):
    """向 worker 提供稳定错误码和可以安全保存的引擎诊断。"""

    def __init__(self, code: str, payload: JobErrorDetail | None = None, *, stopped: bool = True) -> None:
        super().__init__(code)
        # 错误码不包含环境、输入 YAML 或执行目录绝对路径。
        self.code = code
        # 引擎输出已由进程内执行器脱敏并限制字节数。
        self.payload = payload or JobErrorDetail()
        # 连接异常等失败不能证明目标仓库已经停止执行。
        self.stopped = stopped
