"""Client-neutral stdio tools for the public dbt-metricflow-service API."""

import asyncio
import json
import os
from datetime import datetime
from typing import Annotated
from urllib.parse import quote
from uuid import UUID

import httpx
from mcp import types
from mcp.server import Server
from mcp.server.stdio import stdio_server
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .mcp_client import ToolFailure, request
from .models.contracts import JsonObject
from .models.queries import BoundDimension, SelectedDimension, SelectedFilter, SelectedOrder


class Arguments(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CurrentDeployment(Arguments):
    """读取仓库、环境、分支实际生效的部署；后续分析应固定返回的 activeBuildId。"""

    repository: str
    environment_id: UUID
    branch_name: str


class BuildIdentity(Arguments):
    build_id: UUID = Field(description="已固定的构建 ID")


class SearchCatalog(BuildIdentity):
    """在固定构建中搜索资源；保留同名候选，按 nextCursor 继续相同条件的搜索。"""

    keyword: str = Field(min_length=1)
    resource_type: str | None = None
    limit: int = Field(default=50, ge=1, le=200)
    cursor: str | None = None


class GetResource(BuildIdentity):
    """读取所选资源在固定构建中的完整定义与公开属性。"""

    resource_id: str = Field(pattern=r"^[^/\\?#]+\.[^/\\?#]+$")


class MetricSelection(BuildIdentity):
    """获取固定构建和指标组合的可用维度、optionId 及粒度。"""

    metric_resource_ids: list[Annotated[str, Field(min_length=1)]] = Field(min_length=1, max_length=100)


class QueryMetrics(MetricSelection):
    """查询所选指标。分组与过滤使用该选择的维度 optionId；复合绑定按指标资源 ID 指定单指标选项。"""

    group_by: list[SelectedDimension | BoundDimension] = Field(default_factory=list, max_length=100)
    filters: list[SelectedFilter] = Field(default_factory=list, max_length=100)
    order_by: list[SelectedOrder] = Field(default_factory=list, max_length=100)
    start_time: datetime | None = None
    end_time: datetime | None = None
    limit: int = Field(default=1000, ge=1, le=10000)


TOOLS: dict[str, type[Arguments]] = {
    "get_current_deployment": CurrentDeployment, "search_catalog": SearchCatalog, "get_resource": GetResource,
    "get_available_dimensions": MetricSelection, "query_metrics": QueryMetrics,
}


class Gateway:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self.http = http

    async def metrics(self, args: MetricSelection) -> JsonObject:
        build = str(args.build_id)
        names: dict[str, str] = {}
        for uid in dict.fromkeys(args.metric_resource_ids):
            result = await request(self.http, "GET", f"/builds/{build}/catalog/search",
                                   params={"resourceId": uid, "resourceType": "metric"}, build_id=build)
            item = result["items"][0]
            if item.get("queryNameUnique") is not True:
                raise ToolFailure({"code": "AMBIGUOUS_METRIC", "message": "Metric query name is not unique.",
                                   "retryable": False, "buildId": build})
            names[uid] = item["name"]
        body: JsonObject = {"metrics": list(names.values())}
        path = "/query/available-dimensions"
        if isinstance(args, QueryMetrics):
            path = "/query"
            body.update(args.model_dump(mode="json", by_alias=True, exclude_none=True, exclude={
                "metric_resource_ids", "build_id", "start_time", "end_time", "group_by", "order_by"}))
            groups = []
            for group in args.group_by:
                entry = group.model_dump(mode="json", by_alias=True, exclude_none=True)
                if isinstance(group, BoundDimension):
                    if set(group.bindings) != names.keys():
                        raise ToolFailure({"code": "INVALID_REQUEST", "message": "Bindings must cover selected IDs.",
                                           "retryable": False, "buildId": build})
                    entry["bindings"] = {names[key]: value for key, value in entry["bindings"].items()}
                groups.append(entry)
            body.update(mode="QUERY", groupBy=groups, orderBy=[
                {"fieldId": names.get(order.field_id, order.field_id), "direction": order.direction}
                for order in args.order_by])
            if args.start_time:
                body["startTime"] = args.start_time.isoformat()
            if args.end_time:
                body["endTime"] = args.end_time.isoformat()
        return await request(self.http, "POST", path, params={"build_id": build}, body=body, build_id=build)

    async def execute(self, args: Arguments) -> JsonObject:
        if isinstance(args, MetricSelection):
            return await self.metrics(args)
        if isinstance(args, SearchCatalog):
            params = {"keyword": args.keyword, "limit": args.limit, "resourceType": args.resource_type,
                      "cursor": args.cursor}
            return await request(self.http, "GET", f"/builds/{args.build_id}/catalog/search",
                                 params={k: v for k, v in params.items() if v is not None}, build_id=str(args.build_id))
        if isinstance(args, GetResource):
            return await request(self.http, "GET", f"/builds/{args.build_id}/resources/{quote(args.resource_id, safe='')}",
                                 build_id=str(args.build_id))
        if isinstance(args, CurrentDeployment):
            result = await request(self.http, "GET", "/deployments/current", params={
                "repository": args.repository, "environmentId": str(args.environment_id), "branchName": args.branch_name})
            result["status"] = "active" if result.get("activeBuildId") else "undeployed"
            return result
        raise ValueError("unknown tool")

    async def call(self, name: str, arguments: JsonObject) -> types.CallToolResult:
        failed = False
        try:
            model = TOOLS.get(name)
            if model is None:
                raise ToolFailure({"code": "UNKNOWN_TOOL", "message": "Unknown tool.", "retryable": False})
            data = await self.execute(model.model_validate(arguments))
        except ValidationError:
            failed = True
            data = {"error": {"code": "INVALID_REQUEST", "message": "Invalid tool arguments.", "retryable": False}}
        except ToolFailure as exc:
            failed = True
            data = {"error": exc.error}
        return types.CallToolResult(isError=failed, structuredContent=data, content=[
            types.TextContent(type="text", text=json.dumps(data, ensure_ascii=False, separators=(",", ":")))])


async def serve() -> None:
    url = os.environ.get("DBT_METRICFLOW_SERVICE_URL")
    if not url:
        raise SystemExit("DBT_METRICFLOW_SERVICE_URL is required")
    token = os.environ.get("DBT_METRICFLOW_SERVICE_TOKEN")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with httpx.AsyncClient(base_url=url, headers=headers, timeout=120) as http:
        gateway = Gateway(http)
        server = Server("dbt-metricflow-service")

        @server.list_tools()
        async def list_tools() -> list[types.Tool]:
            return [types.Tool(name=name, description=model.__doc__, inputSchema=model.model_json_schema())
                    for name, model in TOOLS.items()]

        server.call_tool(validate_input=False)(gateway.call)
        async with stdio_server() as (reader, writer):
            await server.run(reader, writer, server.create_initialization_options())


def run() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    run()
