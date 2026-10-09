"""项目作用域草稿校验接口；提交与轮询均不执行发布。"""

from uuid import UUID

from fastapi import APIRouter

from ..publications.api import PREFIX, call
from .models import DraftValidationRequest, ValidationReceipt, ValidationResult
from .service import DraftValidationService

VALIDATIONS = "/{project_id}/validations"


def create_draft_validation_router(runtime) -> APIRouter:
    router = APIRouter(prefix=PREFIX)
    service = DraftValidationService(runtime)

    @router.post(VALIDATIONS, status_code=202, response_model=ValidationReceipt)
    async def submit(project_id: str, request: DraftValidationRequest):
        return await call(service.submit, project_id, request)

    @router.get(VALIDATIONS + "/{validation_id}", response_model=ValidationResult)
    async def get(project_id: str, validation_id: UUID):
        return await call(service.get, project_id, str(validation_id))

    return router
