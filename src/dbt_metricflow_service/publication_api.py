"""已发布目录的 HTTP 边界。"""

import asyncio
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Query

from .publication import InvalidPublishedArtifact, PublicationService, ReleaseGone
from .publication_compatibility import legacy_selection, submit_legacy
from .publication_errors import PublicationError
from .publication_models import (
    OptionsTask,
    PublishedQueryRequest,
    QueryOptionsRequest,
    QueryResultPage,
    QueryStatus,
    ResourceKind,
)
from .storage.jobs import StoreConflict

PREFIX = "/v2/projects"
PUBLICATION = "/{project_id}/publication"
RELEASES = "/{project_id}/releases"
RELEASE = RELEASES + "/{release_id}"
CATALOG = RELEASE + "/catalog"
RESOURCE = RELEASE + "/resources/{resource_id}"
VIEWS = ("lineage", "source", "native-details")
QUERY_OPTIONS = "/{project_id}/query-options"
QUERY_OPTION_JOBS = "/{project_id}/query-option-jobs"
QUERIES = "/{project_id}/queries"
PLATFORM_IDENTITY = "platform"


def public_error(status, code, message, recovery, retryable=False):
    """旧 code 不变，增量诊断使用固定文案，不复制异常正文。"""
    return HTTPException(status, detail=PublicationError(
        code, code, None, message, retryable, recovery, status).detail())


async def call(function, *args):
    try:
        return await asyncio.to_thread(function, *args)
    except PublicationError as error:
        raise HTTPException(error.status_code, detail=error.detail()) from error
    except ReleaseGone as error:
        raise public_error(410, "release_gone", "发布已被替代，请重新读取目录。", "reload_catalog") from error
    except StoreConflict as error:
        raise public_error(409, "idempotency_conflict", "同一受理键不能用于不同输入。",
                           "fix_query_selection") from error
    except InvalidPublishedArtifact as error:
        raise public_error(503, "published_artifact_unavailable", "发布产物暂不可用。",
                           "retry_same_key", True) from error
    except KeyError as error:
        raise public_error(404, "not_found", "项目、资源或任务不存在。", "reload_catalog") from error
    except ValueError as error:
        raise public_error(422, "invalid_query_selection", "请求参数或定义无效。", "fix_query_selection") from error


def create_publication_router(runtime) -> APIRouter:
    router = APIRouter(prefix=PREFIX)
    service = PublicationService(runtime)

    @router.get("")
    async def projects():
        return await call(service.projects)

    @router.get(PUBLICATION)
    async def publication(project_id: str):
        return await call(service.publication, project_id)

    @router.get(RELEASES)
    async def releases(project_id: str):
        return await call(service.releases, project_id)

    @router.get(RELEASE)
    async def release(project_id: str, release_id: UUID):
        return await call(service.release, project_id, str(release_id))

    @router.get(CATALOG)
    async def catalog(project_id: str, release_id: UUID, q: str = "", kind: ResourceKind | None = None,
                      page: int = Query(1, ge=1), size: int = Query(50, ge=1, le=200)):
        return await call(service.catalog, project_id, str(release_id), q, kind, page, size)

    @router.get(RESOURCE)
    async def resource(project_id: str, release_id: UUID, resource_id: str):
        return await call(service.resource, project_id, str(release_id), resource_id)

    # 固定白名单子资源，不允许客户端将路径变成任意产物文件读取。
    def view_handler(view):
        async def handler(project_id: str, release_id: UUID, resource_id: str):
            return await call(service.resource, project_id, str(release_id), resource_id, view)
        return handler

    for view in VIEWS:
        router.add_api_route(RESOURCE + "/" + view, view_handler(view), methods=["GET"])

    @router.post(QUERY_OPTIONS)
    async def options(project_id: str, request: QueryOptionsRequest):
        return await call(service.query_options, project_id, request)

    @router.post(QUERIES, status_code=202)
    async def submit(project_id: str, request: PublishedQueryRequest):
        # 当前服务仅面向受控平台绑定；不接受客户端伪造身份作为幂等域。
        return await call(service.submit_query, project_id, request, PLATFORM_IDENTITY)

    @router.post(QUERY_OPTION_JOBS, status_code=202, response_model=OptionsTask, response_model_exclude_unset=True)
    async def submit_options(project_id: str, request: QueryOptionsRequest):
        return await call(service.submit_options, project_id, request)

    @router.get(QUERY_OPTION_JOBS + "/{options_job_id}", response_model=OptionsTask, response_model_exclude_unset=True)
    async def get_options(project_id: str, options_job_id: UUID):
        return await call(service.get_options, project_id, str(options_job_id))

    @router.get(QUERIES + "/{query_id}/status", response_model=QueryStatus, response_model_exclude_unset=True)
    async def query_status(project_id: str, query_id: UUID):
        return await call(service.query_status, project_id, str(query_id))

    @router.get(QUERIES + "/{query_id}/results", response_model=QueryResultPage | QueryStatus,
                response_model_exclude_unset=True)
    async def query_results(project_id: str, query_id: UUID, offset: int = Query(0, ge=0),
                            limit: int = Query(100, ge=1, le=200)):
        return await call(service.query_result_page, project_id, str(query_id), offset, limit)

    @router.get(QUERIES + "/{query_id}")
    async def query(project_id: str, query_id: UUID):
        return await call(service.get_query, project_id, str(query_id))

    @router.post("/{project_id}/compatibility/query-options")
    async def legacy_options(project_id: str, body: dict):
        release, _, _, _ = await call(legacy_selection, service, project_id, body.get("releaseId"), body.get("metrics"))
        native = await call(runtime.options, release["run_id"], tuple(body["metrics"]))
        return {**native, "releaseId": body["releaseId"]}

    @router.post("/{project_id}/compatibility/queries", status_code=202)
    async def legacy_submit(project_id: str, body: dict):
        return await call(submit_legacy, service, project_id, body.get("mode"), body.get("request"),
                          body.get("idempotencyKey") or uuid4().hex, body.get("resourceId"))
    return router
