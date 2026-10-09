"""从固定创建基线和封存输入计算有界差异，不推进任何发布指针。"""

import difflib
import json
import tempfile
from pathlib import Path

from ..publications.service import PublicationService
from ..storage.branches import BranchStore
from ..storage.publications import CATALOG_PATH

UTF8 = "utf-8"
MAX_FILES = 200
MAX_FILE_BYTES = 64 * 1024
MAX_DIFF_BYTES = 256 * 1024
ADDED = "ADDED"
DELETED = "DELETED"
MODIFIED = "MODIFIED"
BEFORE = "a/"
AFTER = "b/"
EMPTY = ""
RESOURCE_FIELDS = ("name", "kind", "description", "attributes", "nativeDetails")


class BranchDiffService:
    """版本状态与源码差异可读历史；它们不会重新激活历史目录。"""

    def __init__(self, runtime):
        self.runtime = runtime

    def compare(self, project_id: str, branch_id: str, release_id: str) -> dict:
        # 显式验证目标归属，并从 run 读取已封存输入，不读取远端最新工作树。
        branches = BranchStore(self.runtime.db)
        branch = branches.get(project_id, branch_id)
        release = PublicationService(self.runtime, branch_id=branch_id)._release(project_id, release_id)
        run = self.runtime.jobs.get(release["run_id"]) if release["run_id"] else None
        if not run or not run["input_set_id"]:
            raise ValueError("候选输入尚未封存，请稍后重试")
        if not branch["base_commit_sha"]:
            raise ValueError("分支尚无固定比较基线")
        if not branch["base_input_set_id"]:
            raise ValueError("历史分支尚未封存固定基线，不能以当前远端内容替代")
        with tempfile.TemporaryDirectory(dir=self.runtime.settings.temp_root) as temporary:
            baseline = Path(temporary) / BASELINE_DIRECTORY
            target = Path(temporary) / SOURCE_DIRECTORY
            self.runtime.artifacts.materialize(branch["base_input_set_id"], baseline)
            self.runtime.artifacts.materialize(run["input_set_id"], target)
            files, truncated = self._files(baseline, target)
        main = branches.production(project_id)
        # 资源差异只使用原生身份，绝不把同名资源的不同版本重新编号。
        resources = self._resources(branch["base_release_id"], release)
        return {"branchId": branch_id, "releaseId": release_id, "baseCommitSha": branch["base_commit_sha"],
                "targetCommitSha": release["request_json"]["commitSha"],
                "productionAdvanced": main["active_release_id"] != branch["production_base_release_id"],
                "resourceBaselineAvailable": branch["base_release_id"] is not None,
                "files": files, "resources": resources[:MAX_FILES],
                "truncated": truncated or len(resources) > MAX_FILES}

    @staticmethod
    def _files(baseline, target):
        # 文件和总输出均有硬上限；超限显示截断，不能伪装为完整对比。
        left = {p.relative_to(baseline).as_posix(): p for p in baseline.rglob(FILE_GLOB) if p.is_file()}
        right = {p.relative_to(target).as_posix(): p for p in target.rglob(FILE_GLOB) if p.is_file()}
        result, used, truncated = [], 0, False
        for path in sorted(left.keys() | right.keys()):
            a = left[path].read_bytes() if path in left else b""
            b = right[path].read_bytes() if path in right else b""
            if a == b and (path in left) == (path in right):
                continue
            if len(result) >= MAX_FILES or used >= MAX_DIFF_BYTES:
                truncated = True
                break
            status = ADDED if path not in left else DELETED if path not in right else MODIFIED
            limited = len(a) > MAX_FILE_BYTES or len(b) > MAX_FILE_BYTES
            diff = EMPTY
            if not limited:
                try:
                    diff = EMPTY.join(difflib.unified_diff(a.decode(UTF8).splitlines(keepends=True),
                                                         b.decode(UTF8).splitlines(keepends=True),
                                                         fromfile=BEFORE + path, tofile=AFTER + path))
                except UnicodeDecodeError:
                    limited = True
            raw = diff.encode(UTF8)
            if len(raw) + used > MAX_DIFF_BYTES:
                raw = raw[:MAX_DIFF_BYTES - used]
                limited = True
            diff = raw.decode(UTF8, errors=IGNORE)
            used += len(raw)
            truncated |= limited
            result.append({"path": path, "status": status, "diff": diff, "truncated": limited})
        return result, truncated

    def _resources(self, base_release_id, target):
        # 基线无需是当前生产版本；读取原封存目录不改变其历史状态。
        from ..storage.publications import PublicationStore

        def resources(row):
            if not row or not row["artifact_set_id"]:
                return []
            document = json.loads(self.runtime.artifacts.read_file(row["artifact_set_id"], CATALOG_PATH))
            return document["resources"]

        if base_release_id is None:
            return []
        base = PublicationStore(self.runtime.db).get_release(target["project_id"], base_release_id)
        return self._compare_resources(resources(base), resources(target))

    @staticmethod
    def _compare_resources(base, target):
        # 对比稳定的业务属性，同一个原生身份的口径变化报告为 MODIFIED。
        left = {item["nativeId"]: item for item in base}
        right = {item["nativeId"]: item for item in target}
        result = []
        for native_id in sorted(left.keys() | right.keys()):
            a, b = left.get(native_id), right.get(native_id)
            if a is not None and b is not None and all(a.get(key) == b.get(key) for key in RESOURCE_FIELDS):
                continue
            result.append({"nativeId": native_id, "kind": (b or a)["kind"],
                           "status": ADDED if a is None else DELETED if b is None else MODIFIED})
        return result


SOURCE_DIRECTORY = "source"
BASELINE_DIRECTORY = "baseline"
FILE_GLOB = "*"
IGNORE = "ignore"
