from __future__ import annotations

import logging
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dbt_metricflow_service.models import DbtJobRequest, MetricFlowJobRequest

logger = logging.getLogger(__name__)


class WorkerRequest(BaseModel):
    """Private stdin envelope; paths are exclusively supplied by the parent."""

    model_config = ConfigDict(extra="forbid")
    kind: Literal["dbt", "metricflow"] = Field(description="Allowed execution backend.")
    request: DbtJobRequest | MetricFlowJobRequest = Field(description="Validated request input.", repr=False)

    @model_validator(mode="after")
    def validate_request_kind(self) -> WorkerRequest:
        expected = DbtJobRequest if self.kind == "dbt" else MetricFlowJobRequest
        if not isinstance(self.request, expected) or not self.request.resources:
            raise ValueError("worker requires matching kind and nonblank resources")
        return self
