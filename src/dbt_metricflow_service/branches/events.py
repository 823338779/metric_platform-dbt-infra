"""Forgejo webhook 边界，签名验证后只提交持久核对信号。"""

import asyncio
import hashlib
import hmac
import json

from fastapi import APIRouter, HTTPException, Request, Response

from ..platform.bindings import HEADS_PREFIX, validate_git_ref
from .sync import SQL_SIGNAL

PATH = "/internal/git-branch-events"
SIGNATURE = "X-Forgejo-Signature"
EVENT = "X-Forgejo-Event"
DELIVERY = "X-Forgejo-Delivery"
DELIVERY_COMPAT = "X-Gitea-Delivery"
PUSH = "push"
DELETE = "delete"
BRANCH = "branch"
UTF8 = "utf-8"
CODE = "code"
SQL_REPOSITORY = "SELECT project_id FROM runtime_project WHERE binding_config->>'remote'=%s"
MAX_EVENT_BYTES = 1024 * 1024
SQL_EVENT = """INSERT INTO runtime_branch_event(delivery_id,payload_digest) VALUES(%s,%s)
 ON CONFLICT DO NOTHING RETURNING delivery_id"""
SQL_EVENT_DIGEST = "SELECT payload_digest FROM runtime_branch_event WHERE delivery_id=%s"
SQL_DELETE_EVENT = """UPDATE runtime_branch SET status='DELETED',version=version+1,
 scan_token=NULL,scan_expires_at=NULL,signal_version=signal_version+1
 WHERE project_id=%s AND git_ref=%s AND mode='PREVIEW' AND status IN ('ACTIVE','PROVISIONING','DELETING')"""


def create_branch_event_router(runtime) -> APIRouter:
    router = APIRouter()

    @router.post(PATH, status_code=202)
    async def event(request: Request):
        # 未启用或无 Secret 不接受事件，不将请求头当成可信身份。
        settings = runtime.settings
        if not settings.branch_events_enabled or not settings.branch_event_secret:
            raise HTTPException(404, detail={CODE: "branch_events_disabled"})
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > MAX_EVENT_BYTES:
                raise HTTPException(413, detail={CODE: "event_too_large"})
        signature = hmac.new(settings.branch_event_secret.encode(UTF8), raw, hashlib.sha256).hexdigest()
        supplied = request.headers.get(SIGNATURE, "")
        if not hmac.compare_digest(signature.encode(UTF8), supplied.encode(UTF8)):
            raise HTTPException(401, detail={CODE: "invalid_event_signature"})
        kind = request.headers.get(EVENT)
        if kind not in (PUSH, DELETE):
            raise HTTPException(422, detail={CODE: "unsupported_event"})
        delivery = request.headers.get(DELIVERY) or request.headers.get(DELIVERY_COMPAT)
        if (kind == DELETE and not delivery) or (delivery and len(delivery) > 200):
            raise HTTPException(422, detail={CODE: "invalid_delivery_identity"})
        try:
            payload = json.loads(raw)
            remote = payload["repository"]["clone_url"]
            ref = payload["ref"]
            if kind == DELETE:
                if payload.get("ref_type") != BRANCH:
                    return Response(status_code=204)
                ref = HEADS_PREFIX + ref
            validate_git_ref(ref)
            if not isinstance(remote, str):
                raise ValueError("repository must be a string")
        except (ValueError, KeyError, TypeError) as error:
            raise HTTPException(422, detail={CODE: "invalid_event"}) from error

        def signal():
            # 仓库身份从服务配置核对；同事务更新已登记分支后才返回 202。
            with runtime.db.transaction() as cursor:
                cursor.execute(SQL_REPOSITORY, (remote,))
                projects = cursor.fetchall()
                if not projects:
                    raise HTTPException(403, detail={CODE: "untrusted_repository"})
                # 删除事实先持久化；相同 delivery 的重放不能关闭后来新登记的身份。
                if delivery:
                    digest = hashlib.sha256(raw).hexdigest()
                    cursor.execute(SQL_EVENT, (delivery, digest))
                    if not cursor.fetchone():
                        cursor.execute(SQL_EVENT_DIGEST, (delivery,))
                        if cursor.fetchone()["payload_digest"] != digest:
                            raise HTTPException(409, detail={CODE: "event_identity_conflict"})
                        return 0
                changed = 0
                for project in projects:
                    cursor.execute(SQL_DELETE_EVENT if kind == DELETE else SQL_SIGNAL, (project["project_id"], ref))
                    changed += cursor.rowcount
                return changed

        count = await asyncio.to_thread(signal)
        return Response(status_code=202 if count else 204)

    return router
