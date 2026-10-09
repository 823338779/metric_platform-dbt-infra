"""项目作用域草稿校验接口；提交与轮询均不执行发布。"""

from uuid import UUID

from fastapi import APIRouter, Depends

from ..api.auth import mutation_guard
from ..publications.api import PREFIX, call
from ..publications.models import FixedCommitRequest
from .models import ValidationReceipt, ValidationResult
from .service import CommitValidationService

VALIDATIONS = "/{project_id}/validations"


def create_commit_validation_router(runtime) -> APIRouter:
    router = APIRouter(prefix=PREFIX)
    service = CommitValidationService(runtime)

    @router.post(VALIDATIONS, status_code=202, response_model=ValidationReceipt,
                 dependencies=[Depends(mutation_guard(runtime.settings))])
    async def submit(project_id: str, request: FixedCommitRequest):
        return await call(service.submit, project_id, request)

    @router.get(VALIDATIONS + "/{validation_id}", response_model=ValidationResult)
    async def get(project_id: str, validation_id: UUID):
        return await call(service.get, project_id, str(validation_id))

    return router
