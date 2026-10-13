"""跨平台的链接及 Windows reparse point 检查。"""

import os
import stat
from pathlib import Path

from dbt_metricflow_service import constants as strings


def _is_link(path: Path) -> bool:
    is_symlink = path.is_symlink()
    if is_symlink or os.name != strings.WINDOWS_OS_NAME:
        return is_symlink
    is_junction = getattr(os.path, strings.PATH_IS_JUNCTION_METHOD, None)
    if is_junction is not None:
        return bool(is_junction(path))
    try:
        return bool(path.lstat().st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT)
    except (AttributeError, FileNotFoundError):
        return False
