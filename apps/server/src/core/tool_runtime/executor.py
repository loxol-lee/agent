from __future__ import annotations

import json
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from typing import Any

from config import Settings
from core.contracts.chat import ChatRequest
from core.contracts.tools import (
    ERROR_TOOL_ARGS_INVALID,
    ERROR_TOOL_NOT_ALLOWED,
    ERROR_TOOL_NOT_FOUND,
    ERROR_TOOL_RUNTIME_ERROR,
    ERROR_TOOL_TIMEOUT,
    ToolCall,
    ToolError,
    ToolResult,
    tool_error_user_message,
)
from core.contracts.storage import Storage
from core.contracts.observability import Tracer
from core.infra.observability.tracer import now_ms
from core.tool_runtime.policy import ToolPolicy
from core.tool_runtime.redact import redact
from core.tool_runtime.registry import ToolRegistry


class ToolExecutor:
    def __init__(self, *, registry: ToolRegistry, policy: ToolPolicy):
        self.registry = registry
        self.policy = policy

    def new_call_id(self) -> str:
        return uuid.uuid4().hex

    def tool_call_event_payload(self, *, call: ToolCall) -> dict[str, Any]:
        args_redacted = redact(call.args)
        args_json = json.dumps(args_redacted, ensure_ascii=False)
        return {"call_id": call.call_id, "name": call.name, "arguments_json": args_json, "args": args_redacted}

    def execute(
        self,
        *,
        settings: Settings,
        store: Storage,
        tracer: Tracer,
        req: ChatRequest,
        call: ToolCall,
    ) -> ToolResult:
        spec = self.registry.get_spec(call.name)
        handler = self.registry.get_handler(call.name)

        decision = self.policy.decide(spec=spec, name=call.name, args=call.args)
        allowed = decision.allowed

        risk = spec.risk if spec else "unknown"
        side_effect = bool(spec.side_effect) if spec else False
        timeout_ms = int(spec.timeout_ms) if spec else 1000

        args_redacted = redact(call.args)
        args_json = json.dumps(args_redacted, ensure_ascii=False)

        started = now_ms()
        finished = started

        span_attrs: dict[str, Any] = {
            "tool_name": call.name,
            "allowed": allowed,
            "latency_ms": None,
            "ok": None,
            "error_code": None,
            "risk": risk,
            "side_effect": side_effect,
        }

        def run_handler() -> dict[str, Any]:
            if handler is None or spec is None:
                raise RuntimeError("tool_not_found")
            return handler(settings, store, tracer, req, call.args)

        error: ToolError | None = None
        content: dict[str, Any] = {}
        ok = False

        with tracer.span(req.trace_id, name=f"tool.{call.name}", kind="tool", attrs=span_attrs):
            if not allowed:
                code = decision.error_code or ERROR_TOOL_NOT_ALLOWED
                error = ToolError(code=code, message=decision.reason)
                ok = False
                finished = now_ms()
            else:
                with ThreadPoolExecutor(max_workers=1) as ex:
                    fut = ex.submit(run_handler)
                    try:
                        res = fut.result(timeout=max(0.001, timeout_ms / 1000.0))
                        if not isinstance(res, dict):
                            raise ValueError("invalid_result")
                        content = res
                        ok = True
                        finished = now_ms()
                    except TimeoutError:
                        error = ToolError(code=ERROR_TOOL_TIMEOUT, message="timeout")
                        ok = False
                        finished = now_ms()
                    except ValueError as e:
                        error = ToolError(code=ERROR_TOOL_ARGS_INVALID, message=str(e)[:200])
                        ok = False
                        finished = now_ms()
                    except Exception as e:
                        msg = str(e)
                        if msg == "tool_not_found":
                            error = ToolError(code=ERROR_TOOL_NOT_FOUND, message=msg)
                        else:
                            error = ToolError(code=ERROR_TOOL_RUNTIME_ERROR, message=msg[:200])
                        ok = False
                        finished = now_ms()

            latency = max(0, int(finished) - int(started))
            span_attrs["latency_ms"] = latency
            span_attrs["ok"] = ok
            if not ok and error is not None:
                span_attrs["error_code"] = error.code

        result = ToolResult(call_id=call.call_id, name=call.name, ok=ok, content=content if ok else {}, error=error)

        result_redacted = redact(
            {
                "call_id": result.call_id,
                "name": result.name,
                "ok": result.ok,
                "content": result.content,
                "error": ({"code": result.error.code, "message": result.error.message} if result.error else None),
            }
        )

        result_json = json.dumps(result_redacted, ensure_ascii=False)

        store.insert_tool_run(
            trace_id=req.trace_id,
            conversation_id=req.conversation_id,
            call_id=call.call_id,
            tool_name=call.name,
            allowed=allowed,
            risk=risk,
            side_effect=side_effect,
            args_json=args_json,
            result_json=result_json,
            ok=ok,
            error_code=(error.code if error else None),
            error_message=(error.message if error else None),
            started_at_ms=started,
            finished_at_ms=finished,
        )

        return result

    def tool_result_event_payload(self, *, result: ToolResult) -> dict[str, Any]:
        if result.ok:
            return {
                "call_id": result.call_id,
                "name": result.name,
                "ok": True,
                "content_json": json.dumps(redact(result.content), ensure_ascii=False),
            }

        code = result.error.code if result.error else None
        return {
            "call_id": result.call_id,
            "name": result.name,
            "ok": False,
            "content_json": "{}",
            "error": {
                "code": code,
                "message": (result.error.message if result.error else None),
                "user_message": tool_error_user_message(code),
            },
        }

