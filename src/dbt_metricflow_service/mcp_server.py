"""Client-neutral stdio tools for the public dbt-metricflow-service API."""

import asyncio
import json
import os
from uuid import UUID

import httpx
from mcp import types
from mcp.server import Server
from mcp.server.stdio import stdio_server
from pydantic import BaseModel, ConfigDict, ValidationError

from .mcp_client import ToolFailure, request
from .models.contracts import JsonObject


class Arguments(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CurrentDeployment(Arguments):
    """读取仓库、环境、分支实际生效的部署；后续分析应固定返回的 activeBuildId。"""

    repository: str
    environment_id: UUID
    branch_name: str


TOOLS: dict[str, type[Arguments]] = {"get_current_deployment": CurrentDeployment}


class Gateway:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self.http = http

    async def execute(self, args: Arguments) -> JsonObject:
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
