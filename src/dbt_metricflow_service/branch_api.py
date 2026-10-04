"""分支 HTTP 门面；写入仅允许受信服务持有的部署凭据。"""

import hmac
from functools import partial
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query

from .branch_diff import BranchDiffService
from .branch_models import BranchView, CreateBranchRequest, RegisterBranchRequest
from .branches import BranchService
from .draft_validation import DraftValidationService
from .draft_validation_models import BranchDraftValidationRequest, BranchValidationResult, ValidationReceipt
from .publication import PublicationService, release_descriptor
from .publication_api import call
from .publication_models import Contract, PublishedQueryRequest, QueryOptionsRequest, ResourceKind

PREFIX = "/v2/projects/{project_id}/branches"
REGISTER = ":register"
BRANCH = "/{branch_id}"
AUTHORIZATION = "Authorization"
BEARER = "Bearer "
UTF8 = "utf-8"
EXPECTED_VERSION = "expectedVersion"
DISABLED = "branch_mutations_disabled"
UNAUTHORIZED = "service_credential_required"
DETAIL_CODE = "code"
PUBLICATION = BRANCH + "/publication"
RELEASES = BRANCH + "/releases"
RELEASE = RELEASES + "/{release_id}"
CATALOG = RELEASE + "/catalog"
RESOURCE = RELEASE + "/resources/{resource_id}"
VIEWS = ("lineage", "source", "native-details")
QUERIES = BRANCH + "/queries"
OPTIONS = BRANCH + "/query-options"
OPTION_JOBS = BRANCH + "/query-option-jobs"
DIFF = RELEASE + "/diff"
VALIDATIONS = BRANCH + "/validations"
PLATFORM = "platform"
GET = "GET"


class BranchReleaseRequest(Contract):
    # 显式重试使用新键，旧键恢复原候选；输入版本由服务核实。
    idempotency_key: str


def mutation_guard(settings):
    # 凭据只来自进程部署配置，未配置时关闭写入入口。
    async def require_service(authorization: str | None = Header(default=None, alias=AUTHORIZATION)):
        token = settings.branch_service_token
        if not token:
            raise HTTPException(503, detail={DETAIL_CODE: DISABLED})
        expected = (BEARER + token).encode(UTF8)
        if authorization is None or not hmac.compare_digest(authorization.encode(UTF8), expected):
            raise HTTPException(401, detail={DETAIL_CODE: UNAUTHORIZED})
    return require_service


def create_branch_router(runtime) -> APIRouter:
    # 保持无分支旧路由不变，所有分支上下文均从此显式路径解析。
    router = APIRouter(prefix=PREFIX)
    service = BranchService(runtime)
    mutation = [Depends(mutation_guard(runtime.settings))]

    @router.get("", response_model=list[BranchView])
    async def branches(project_id: str):
        return await call(service.list, project_id)

    @router.post("", status_code=202, response_model=BranchView, dependencies=mutation)
    async def create(project_id: str, request: CreateBranchRequest):
        return await call(service.create, project_id, request)

    @router.post(REGISTER, status_code=202, response_model=BranchView, dependencies=mutation)
    async def register(project_id: str, request: RegisterBranchRequest):
        return await call(service.register, project_id, request)

    @router.get(BRANCH, response_model=BranchView)
    async def branch(project_id: str, branch_id: UUID):
        return await call(service.get, project_id, str(branch_id))

    @router.delete(BRANCH, response_model=BranchView, dependencies=mutation)
    async def delete(project_id: str, branch_id: UUID, expected_version: int = Query(ge=1, alias=EXPECTED_VERSION)):
        return await call(service.delete, project_id, str(branch_id), expected_version)

    async def selected(method, project_id, branch_id, *args):
        # 请求实例固定一个分支，避免共享服务对象的选择状态污染并发调用。
        target = PublicationService(runtime, branch_id=str(branch_id))
        result = await call(getattr(target, method), project_id, *args)
        return {**result, "branchId": str(branch_id)} if isinstance(result, dict) else result

    @router.get(PUBLICATION)
    async def publication(project_id: str, branch_id: UUID):
        return await selected("publication", project_id, branch_id)

    @router.get(RELEASES)
    async def releases(project_id: str, branch_id: UUID):
        return await selected("releases", project_id, branch_id)

    @router.post(RELEASES, status_code=202, dependencies=mutation)
    async def submit_release(project_id: str, branch_id: UUID, request: BranchReleaseRequest):
        row = await call(partial(PublicationService(runtime).submit, branch_id=str(branch_id)),
                         project_id, request.idempotency_key)
        view = await call(service.get, project_id, str(branch_id))
        return {**release_descriptor(row), "branchId": str(branch_id), "gitRef": view.git_ref}

    @router.get(RELEASE)
    async def release(project_id: str, branch_id: UUID, release_id: UUID):
        return await selected("release", project_id, branch_id, str(release_id))

    @router.get(CATALOG)
    async def catalog(project_id: str, branch_id: UUID, release_id: UUID, q: str = "", kind: ResourceKind | None = None,
                      page: int = Query(1, ge=1), size: int = Query(50, ge=1, le=200)):
        return await selected("catalog", project_id, branch_id, str(release_id), q, kind, page, size)

    @router.get(RESOURCE)
    async def resource(project_id: str, branch_id: UUID, release_id: UUID, resource_id: str):
        return await selected("resource", project_id, branch_id, str(release_id), resource_id)

    def view_handler(view):
        # 固定子资源白名单，与生产路由保持相同的来源和血缘边界。
        async def handler(project_id: str, branch_id: UUID, release_id: UUID, resource_id: str):
            return await selected("resource", project_id, branch_id, str(release_id), resource_id, view)
        return handler

    for view in VIEWS:
        router.add_api_route(RESOURCE + "/" + view, view_handler(view), methods=[GET])

    @router.get(DIFF)
    async def diff(project_id: str, branch_id: UUID, release_id: UUID):
        return await call(BranchDiffService(runtime).compare, project_id, str(branch_id), str(release_id))

    @router.post(OPTIONS)
    async def options(project_id: str, branch_id: UUID, request: QueryOptionsRequest):
        return await selected("query_options", project_id, branch_id, request)

    @router.post(OPTION_JOBS, status_code=202)
    async def submit_options(project_id: str, branch_id: UUID, request: QueryOptionsRequest):
        return await selected("submit_options", project_id, branch_id, request)

    @router.get(OPTION_JOBS + "/{options_job_id}")
    async def get_options(project_id: str, branch_id: UUID, options_job_id: UUID):
        return await selected("get_options", project_id, branch_id, str(options_job_id))

    @router.post(QUERIES, status_code=202)
    async def submit_query(project_id: str, branch_id: UUID, request: PublishedQueryRequest):
        return await selected("submit_query", project_id, branch_id, request, PLATFORM)

    @router.get(QUERIES + "/{query_id}")
    async def query(project_id: str, branch_id: UUID, query_id: UUID):
        return await selected("get_query", project_id, branch_id, str(query_id))

    @router.get(QUERIES + "/{query_id}/status")
    async def status(project_id: str, branch_id: UUID, query_id: UUID):
        return await selected("query_status", project_id, branch_id, str(query_id))

    @router.get(QUERIES + "/{query_id}/results")
    async def results(project_id: str, branch_id: UUID, query_id: UUID,
                      offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200)):
        return await selected("query_result_page", project_id, branch_id, str(query_id), offset, limit)

    @router.post(VALIDATIONS, status_code=202, response_model=ValidationReceipt, dependencies=mutation)
    async def validate(project_id: str, branch_id: UUID, request: BranchDraftValidationRequest):
        return await call(partial(DraftValidationService(runtime).submit, branch_id=str(branch_id)),
                          project_id, request)

    @router.get(VALIDATIONS + "/{validation_id}", response_model=BranchValidationResult)
    async def validation(project_id: str, branch_id: UUID, validation_id: UUID):
        return await call(partial(DraftValidationService(runtime).get, branch_id=str(branch_id)),
                          project_id, str(validation_id))

    return router
