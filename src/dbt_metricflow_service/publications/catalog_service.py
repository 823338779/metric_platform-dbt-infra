"""服务拥有发布输入与查询映射。"""
import hashlib
import json

from ..storage.publications import (
    CATALOG_PATH,
)
from .service import InvalidPublishedArtifact, PublicationService

UTF8 = "utf-8"
CATALOG_SEARCH_FIELDS = ("name", "displayName", "description")


class CatalogService:
    def __init__(self, runtime):
        self.runtime = runtime
        self.publications = PublicationService(runtime)

    def _catalog(self, project_id: str, release_id: str) -> tuple[dict, dict]:
        release = self.publications._active_release(project_id, release_id)
        # 文件读取校验摘要；不在请求过程中重新解析原生 manifest 或建立投影。
        try:
            raw = self.runtime.artifacts.read_file(release["artifact_set_id"], CATALOG_PATH)
            if hashlib.sha256(raw).hexdigest() != release["catalog_digest"]:
                raise ValueError("发布目录摘要不匹配")
            return release, json.loads(raw)
        except (ValueError, KeyError) as error:
            raise InvalidPublishedArtifact("无法读取已发布目录") from error


    def catalog(self, project_id: str, release_id: str, q: str = "", kind: str | None = None,
                page: int = 1, size: int = 50) -> dict:
        _, catalog = self._catalog(project_id, release_id)
        needle = q.casefold()
        resources = [item for item in catalog["resources"] if (kind is None or item["kind"] == kind)
                     and (not needle or any(needle in (item.get(field) or "").casefold()
                                            for field in CATALOG_SEARCH_FIELDS))]
        resources.sort(key=lambda item: item["resourceId"])
        return {"releaseId": str(release_id), "page": page, "size": size, "total": len(resources),
                "resources": resources[(page - 1) * size:page * size]}


    def resource(self, project_id: str, release_id: str, resource_id: str, view: str | None = None) -> dict:
        release, catalog = self._catalog(project_id, release_id)
        resource = next((item for item in catalog["resources"] if item["resourceId"] == resource_id), None)
        if resource is None:
            raise KeyError(resource_id)
        envelope = {"releaseId": str(release_id), "resourceId": resource_id}
        if view == "lineage":
            return {**envelope, "dependencies": [edge for edge in catalog["relations"]
                                                 if resource_id in (edge["upstreamResourceId"],
                                                                    edge["downstreamResourceId"])]}
        if view == "native-details":
            return {**envelope, "nativeDetails": resource["nativeDetails"]}
        if view == "source":
            path = resource.get("sourcePath")
            if not path:
                raise KeyError(resource_id)
            run = self.runtime.jobs.get(release["run_id"])
            content = self.runtime.artifacts.read_file(run["input_set_id"], path).decode(UTF8)
            return {**envelope, "path": path, "content": content}
        return {**envelope, **resource}


