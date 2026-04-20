from __future__ import annotations

import hashlib
import json
from typing import Any, Iterator

from config import Settings
from core.contracts.chat import ChatRequest, StreamEvent
from core.contracts.model import Message, ModelChatRequest
from core.contracts.observability import Trace, Tracer
from core.contracts.storage import Storage
from core.contracts.tools import ToolCall
from core.infra.observability.tracer import now_ms
from core.model_layer.client import complete_chat, stream_chat
from core.tool_runtime.executor import ToolExecutor
from core.tool_runtime.redact import redact
from core.orchestrator.budgets import Budgets
from core.orchestrator.context_builder import build_messages
from core.orchestrator.stop_reason import StopReason


SYSTEM_PROMPT = "你是一个可靠的工作助理。回答要简洁、可执行；如果信息不足，先提出最少的澄清问题再行动。"


def _decision_prompt(tools_spec: dict[str, Any]) -> str:
    return (
        "你可以在回答前选择调用工具。\n"
        "可用工具（JSON）：" + json.dumps(tools_spec, ensure_ascii=False) + "\n\n"
        "当你需要工具时：请直接使用工具调用（function calling），不要在 content 里输出 JSON。\n"
        "当你不需要工具时：直接输出最终回答文本即可（不必输出 JSON）。\n"
        "如果你仍然输出 JSON，也允许两种格式：\n"
        "1) {\"type\":\"final\",\"content\":\"...\"}\n"
        "2) {\"type\":\"tool_call\",\"name\":\"<tool_name>\",\"args\":{...}}\n"
    )


def _json_object_from_text(text: str) -> dict[str, Any] | None:
    s = text.strip()
    if not s:
        return None

    dec = json.JSONDecoder()
    idx = s.find("{")
    while idx != -1:
        try:
            obj, _ = dec.raw_decode(s[idx:])
        except Exception:
            idx = s.find("{", idx + 1)
            continue
        if isinstance(obj, dict):
            return obj
        idx = s.find("{", idx + 1)

    return None


def _dict_from_json(text: str) -> dict[str, Any] | None:
    try:
        obj = json.loads(text)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _openai_tools_from_tools_spec(tools_spec: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name, spec in tools_spec.items():
        if not isinstance(name, str) or not name:
            continue
        if not isinstance(spec, dict):
            continue
        schema = spec.get("json_schema")
        if not isinstance(schema, dict):
            continue
        desc = spec.get("desc")
        if not isinstance(desc, str):
            desc = ""
        out.append({"type": "function", "function": {"name": name, "description": desc, "parameters": schema}})
    return out


def _summarize_messages(messages: list[Message]) -> dict:
    items: list[dict] = []
    for m in messages:
        role = m.role
        content = m.content
        if not isinstance(role, str) or not isinstance(content, str):
            continue
        head = content[:120]
        h = hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()
        row: dict[str, Any] = {"role": role, "len": len(content), "sha256": h, "head": head}
        if m.name:
            row["name"] = m.name
        if m.tool_call_id:
            row["tool_call_id"] = m.tool_call_id
        items.append(row)
    return {"count": len(items), "items": items}


def _normalize_usage(usage: Any) -> dict[str, int] | None:
    if not isinstance(usage, dict):
        return None
    out: dict[str, int] = {}
    for k in ("prompt_tokens", "completion_tokens", "total_tokens"):
        v = usage.get(k)
        if isinstance(v, int) and v >= 0:
            out[k] = v
    return out or None


def _merge_usage(total: dict[str, int], usage: dict[str, int] | None) -> None:
    if usage is None:
        return
    for k, v in usage.items():
        total[k] = total.get(k, 0) + int(v)


def _done_payload(stop_reason: str, usage_total: dict[str, int]) -> dict[str, Any]:
    p: dict[str, Any] = {"stop_reason": stop_reason}
    if usage_total:
        p["usage"] = dict(usage_total)
    return p


def _request_meta_payload(req: ChatRequest) -> dict[str, Any]:
    md = req.metadata if isinstance(req.metadata, dict) else {}
    atts = md.get("attachments")
    if not isinstance(atts, list) or not atts:
        return {}
    safe_items: list[dict[str, Any]] = []
    for a in atts:
        if not isinstance(a, dict):
            continue
        row: dict[str, Any] = {}
        n = a.get("file_name")
        t = a.get("mime_type")
        s = a.get("size_bytes")
        if isinstance(n, str):
            row["file_name"] = n[:255]
        if isinstance(t, str):
            row["mime_type"] = t[:120]
        if isinstance(s, int) and s >= 0:
            row["size_bytes"] = s
        if row:
            safe_items.append(row)
    if not safe_items:
        return {}
    return {"attachments": safe_items, "attachment_count": len(safe_items)}


def _error_payload(message: str, *, stage: str, recoverable: bool, error_code: str, trace_hint: str | None = None) -> dict[str, Any]:
    p: dict[str, Any] = {
        "message": message,
        "recoverable": bool(recoverable),
        "stage": stage,
        "error_code": error_code,
    }
    if isinstance(trace_hint, str) and trace_hint:
        p["trace_hint"] = trace_hint
    return p


def run_chat_stream(
    *,
    settings: Settings,
    store: Storage,
    tracer: Tracer,
    req: ChatRequest,
    tool_executor: ToolExecutor,
    tools_spec: dict[str, Any],
) -> Iterator[StreamEvent]:
    store.insert_trace(
        Trace(
            trace_id=req.trace_id,
            conversation_id=req.conversation_id,
            started_at_ms=now_ms(),
        )
    )

    budgets = Budgets()
    run_started_ms = now_ms()

    if len(req.input_text) > budgets.max_input_chars:
        store.finish_trace(
            req.trace_id,
            finished_at_ms=now_ms(),
            status="error",
            stop_reason=StopReason.max_tokens.value,
            error_message="input_too_long",
        )
        yield StreamEvent(
            trace_id=req.trace_id,
            type="error",
            payload=_error_payload(
                "输入过长，请缩短后重试",
                stage="connector",
                recoverable=False,
                error_code="INPUT_TOO_LONG",
            ),
        )
        yield StreamEvent(trace_id=req.trace_id, type="done", payload={"stop_reason": StopReason.max_tokens.value})
        return

    history = store.list_chat_messages(req.conversation_id, limit=budgets.history_limit)
    store.insert_message(req.conversation_id, req.trace_id, "user", req.input_text)

    mem_rows = store.list_memories(req.conversation_id, limit=budgets.memory_limit)
    mem_texts: list[str] = []
    for r in mem_rows:
        c = r.get("content")
        if isinstance(c, str) and c.strip():
            mem_texts.append(c.strip())

    md = req.metadata if isinstance(req.metadata, dict) else {}
    raw_attachments = md.get("attachments")
    attachments: list[dict[str, Any]] = []
    if isinstance(raw_attachments, list):
        for a in raw_attachments:
            if isinstance(a, dict):
                attachments.append(a)

    base_messages = build_messages(
        system_prompt=SYSTEM_PROMPT,
        history=history,
        user_text=req.input_text,
        memories=mem_texts,
        attachments=attachments,
    )

    seq = 0
    assistant_text_parts: list[str] = []
    model_usage_total: dict[str, int] = {}

    request_meta = _request_meta_payload(req)
    if request_meta:
        store.insert_replay_event(req.trace_id, seq, "request_meta", json.dumps(request_meta, ensure_ascii=False))
        seq += 1

    with tracer.span(req.trace_id, name="orchestrator.run", kind="orchestrator"):
        try:
            final_messages: list[Message] = list(base_messages)

            executor = tool_executor

            max_tool_steps = budgets.tool_call_limit
            max_steps = budgets.max_steps
            tool_steps = 0
            step_count = 0
            final_stop_reason = StopReason.completed.value

            while step_count < max_steps and tool_steps < max_tool_steps:
                step_count += 1
                if now_ms() - run_started_ms > budgets.time_budget_ms:
                    final_stop_reason = StopReason.time_budget.value
                    break

                decision_messages = final_messages + [Message(role="system", content=_decision_prompt(tools_spec))]
                store.insert_replay_event(
                    req.trace_id,
                    seq,
                    "model_request",
                    json.dumps(_summarize_messages(decision_messages), ensure_ascii=False),
                )
                seq += 1

                retry_events: list[dict[str, Any]] = []

                def on_model_retry(n: int, e: Exception) -> None:
                    retry_events.append({"retry_count": int(n), "hint": str(e)[:200]})

                model_attrs: dict[str, Any] = {"provider": "siliconflow", "model": None, "model_alias": "chat"}
                with tracer.span(
                    req.trace_id,
                    name="model.decide",
                    kind="model",
                    attrs=model_attrs,
                ):
                    openai_tools = _openai_tools_from_tools_spec(tools_spec)
                    model_resp = complete_chat(
                        settings,
                        ModelChatRequest(
                            trace_id=req.trace_id,
                            conversation_id=req.conversation_id,
                            messages=decision_messages,
                            tools=openai_tools,
                            tool_choice="auto",
                        ),
                        model_alias="chat",
                        on_retry=on_model_retry,
                    )
                    decision_raw = model_resp.message.content
                    tool_calls = list(model_resp.message.tool_calls or [])
                    usage = _normalize_usage(model_resp.usage)
                    _merge_usage(model_usage_total, usage)
                    model_attrs["model"] = (model_resp.raw or {}).get("model")
                    model_attrs["retry_count"] = (model_resp.raw or {}).get("retry_count")
                    model_attrs["usage"] = usage

                if retry_events:
                    for it in retry_events:
                        store.insert_replay_event(
                            req.trace_id,
                            seq,
                            "model_retry",
                            json.dumps({"kind": "decide", "retry_count": it.get("retry_count"), "hint": it.get("hint")}, ensure_ascii=False),
                        )
                        seq += 1
                        yield StreamEvent(
                            trace_id=req.trace_id,
                            type="error",
                            payload=_error_payload(
                                "模型请求失败，正在重试…",
                                stage="model",
                                recoverable=True,
                                error_code="MODEL_RETRYING",
                                trace_hint=it.get("hint"),
                            ),
                        )

                raw_s = str(decision_raw or "")
                raw_head = raw_s[:200]
                raw_sha = hashlib.sha256(raw_s.encode("utf-8", errors="ignore")).hexdigest()

                tool_calls_summary: list[dict[str, Any]] = []
                for tc in tool_calls[:5]:
                    args_json = str(getattr(tc, "arguments_json", "") or "")
                    tool_calls_summary.append(
                        {
                            "call_id": tc.call_id,
                            "name": tc.name,
                            "arguments_len": len(args_json),
                            "arguments_sha256": hashlib.sha256(args_json.encode("utf-8", errors="ignore")).hexdigest(),
                            "arguments_head": args_json[:200],
                        }
                    )

                resp_payload: dict[str, Any] = {"kind": "decide", "len": len(raw_s), "sha256": raw_sha, "head": raw_head}
                usage_payload = _normalize_usage(model_resp.usage)
                if usage_payload:
                    resp_payload["usage"] = usage_payload
                if tool_calls_summary:
                    resp_payload["tool_calls"] = tool_calls_summary

                store.insert_replay_event(
                    req.trace_id,
                    seq,
                    "model_response",
                    json.dumps(resp_payload, ensure_ascii=False),
                )
                seq += 1

                if tool_calls:
                    executed_calls = 0
                    for tc in tool_calls:
                        if tool_steps >= max_tool_steps:
                            final_stop_reason = StopReason.tool_call_limit.value
                            final_messages.append(
                                Message(
                                    role="system",
                                    content="已达到工具调用上限。请在不再调用工具的前提下，直接给出最终回答。",
                                )
                            )
                            break

                        name = tc.name if isinstance(tc.name, str) else ""
                        if not name:
                            continue

                        args_obj = _dict_from_json(tc.arguments_json) or {}
                        if not isinstance(args_obj, dict):
                            args_obj = {}

                        call_id = tc.call_id if isinstance(tc.call_id, str) and tc.call_id else executor.new_call_id()
                        call = ToolCall(call_id=call_id, name=name, args=args_obj)

                        call_payload = executor.tool_call_event_payload(call=call)
                        store.insert_replay_event(
                            req.trace_id,
                            seq,
                            "tool_call",
                            json.dumps({"call_id": call_id, "name": call.name, "arguments_json": call_payload.get("arguments_json")}, ensure_ascii=False),
                        )
                        yield StreamEvent(trace_id=req.trace_id, type="tool_call", payload=call_payload)
                        seq += 1

                        tool_result = executor.execute(settings=settings, store=store, tracer=tracer, req=req, call=call)
                        tool_payload = executor.tool_result_event_payload(result=tool_result)

                        store.insert_replay_event(
                            req.trace_id,
                            seq,
                            "tool_result",
                            json.dumps(
                                {
                                    "call_id": tool_payload.get("call_id"),
                                    "name": tool_payload.get("name"),
                                    "ok": tool_payload.get("ok"),
                                    "content_json": tool_payload.get("content_json"),
                                    "error": ((tool_payload.get("error") or {}).get("code") if isinstance(tool_payload.get("error"), dict) else None),
                                },
                                ensure_ascii=False,
                            ),
                        )
                        yield StreamEvent(trace_id=req.trace_id, type="tool_result", payload=tool_payload)
                        seq += 1

                        tool_msg = {
                            "call_id": call_id,
                            "name": call.name,
                            "ok": tool_result.ok,
                            "content": redact(tool_result.content),
                            "error": ({"code": tool_result.error.code, "message": tool_result.error.message} if tool_result.error else None),
                        }

                        final_messages.append(
                            Message(
                                role="tool",
                                content=json.dumps(tool_msg, ensure_ascii=False),
                                name=call.name,
                                tool_call_id=call_id,
                            )
                        )

                        tool_steps += 1
                        executed_calls += 1

                    if executed_calls > 0 and final_stop_reason != StopReason.tool_call_limit.value:
                        final_messages.append(
                            Message(
                                role="system",
                                content="工具已执行完成。工具结果已作为 tool 消息提供。请决定是否需要继续调用工具；如果不需要，请直接给出 final。",
                            )
                        )
                        continue

                    if final_stop_reason == StopReason.tool_call_limit.value:
                        break

                decision = _json_object_from_text(str(decision_raw or ""))
                if decision is None:
                    if isinstance(decision_raw, str) and decision_raw.strip():
                        content = decision_raw.strip()
                        assistant_text_parts.append(content)
                        store.insert_replay_event(req.trace_id, seq, "token", json.dumps({"delta": content}, ensure_ascii=False))
                        seq += 1
                        yield StreamEvent(trace_id=req.trace_id, type="token", payload={"delta": content})

                        assistant_text = content
                        store.insert_message(req.conversation_id, req.trace_id, "assistant", assistant_text)

                        done_payload = _done_payload(final_stop_reason, model_usage_total)
                        store.insert_replay_event(req.trace_id, seq, "model_done", json.dumps(done_payload, ensure_ascii=False))
                        seq += 1

                        store.finish_trace(
                            req.trace_id,
                            finished_at_ms=now_ms(),
                            status="ok",
                            stop_reason=final_stop_reason,
                            error_message=None,
                        )
                        yield StreamEvent(trace_id=req.trace_id, type="done", payload=done_payload)
                        return

                    decision = {"type": "final", "content": ""}

                if not isinstance(decision, dict):
                    break

                if decision.get("type") == "final":
                    content = decision.get("content")
                    if isinstance(content, str) and content.strip():
                        assistant_text_parts.append(content)
                        store.insert_replay_event(req.trace_id, seq, "token", json.dumps({"delta": content}, ensure_ascii=False))
                        seq += 1
                        yield StreamEvent(trace_id=req.trace_id, type="token", payload={"delta": content})

                        assistant_text = content
                        store.insert_message(req.conversation_id, req.trace_id, "assistant", assistant_text)

                        done_payload = _done_payload(final_stop_reason, model_usage_total)
                        store.insert_replay_event(req.trace_id, seq, "model_done", json.dumps(done_payload, ensure_ascii=False))
                        seq += 1

                        store.finish_trace(
                            req.trace_id,
                            finished_at_ms=now_ms(),
                            status="ok",
                            stop_reason=final_stop_reason,
                            error_message=None,
                        )
                        yield StreamEvent(trace_id=req.trace_id, type="done", payload=done_payload)
                        return
                    break

                if decision.get("type") != "tool_call":
                    break

                name = decision.get("name")
                args = decision.get("args")
                if not isinstance(name, str) or not name.strip():
                    break
                if not isinstance(args, dict):
                    break

                call_id = executor.new_call_id()
                call = ToolCall(call_id=call_id, name=name, args=args)

                call_payload = executor.tool_call_event_payload(call=call)
                store.insert_replay_event(req.trace_id, seq, "tool_call", json.dumps({"call_id": call_id, "name": name, "arguments_json": call_payload.get("arguments_json")}, ensure_ascii=False))
                yield StreamEvent(trace_id=req.trace_id, type="tool_call", payload=call_payload)
                seq += 1

                tool_result = executor.execute(settings=settings, store=store, tracer=tracer, req=req, call=call)
                tool_payload = executor.tool_result_event_payload(result=tool_result)

                store.insert_replay_event(
                    req.trace_id,
                    seq,
                    "tool_result",
                    json.dumps(
                        {
                            "call_id": tool_payload.get("call_id"),
                            "name": tool_payload.get("name"),
                            "ok": tool_payload.get("ok"),
                            "content_json": tool_payload.get("content_json"),
                            "error": ((tool_payload.get("error") or {}).get("code") if isinstance(tool_payload.get("error"), dict) else None),
                        },
                        ensure_ascii=False,
                    ),
                )
                yield StreamEvent(trace_id=req.trace_id, type="tool_result", payload=tool_payload)
                seq += 1

                tool_msg = {
                    "call_id": call_id,
                    "name": name,
                    "ok": tool_result.ok,
                    "content": redact(tool_result.content),
                    "error": ({"code": tool_result.error.code, "message": tool_result.error.message} if tool_result.error else None),
                }

                final_messages.append(
                    Message(
                        role="tool",
                        content=json.dumps(tool_msg, ensure_ascii=False),
                        name=name,
                        tool_call_id=call_id,
                    )
                )
                final_messages.append(
                    Message(
                        role="system",
                        content="工具已执行完成。工具结果已作为 tool 消息提供。请决定是否需要继续调用工具；如果不需要，请直接给出 final。",
                    )
                )

                tool_steps += 1
                if tool_steps >= max_tool_steps:
                    final_stop_reason = StopReason.tool_call_limit.value
                    final_messages.append(
                        Message(
                            role="system",
                            content="已达到工具调用上限。请在不再调用工具的前提下，直接给出最终回答。",
                        )
                    )
                    break

            if final_stop_reason == StopReason.completed.value and step_count >= max_steps:
                final_stop_reason = StopReason.max_steps.value
                final_messages.append(
                    Message(
                        role="system",
                        content="已达到推理步数上限。请在不再调用工具的前提下，直接给出最终回答。",
                    )
                )

            if final_stop_reason == StopReason.time_budget.value:
                done_payload = _done_payload(final_stop_reason, model_usage_total)
                store.insert_replay_event(req.trace_id, seq, "model_done", json.dumps(done_payload, ensure_ascii=False))
                seq += 1
                store.finish_trace(
                    req.trace_id,
                    finished_at_ms=now_ms(),
                    status="error",
                    stop_reason=final_stop_reason,
                    error_message="time_budget_exceeded",
                )
                yield StreamEvent(
                    trace_id=req.trace_id,
                    type="error",
                    payload=_error_payload(
                        "执行超时（预算耗尽），请缩小问题或重试",
                        stage="orchestrator",
                        recoverable=False,
                        error_code="TIME_BUDGET_EXCEEDED",
                    ),
                )
                yield StreamEvent(trace_id=req.trace_id, type="done", payload=done_payload)
                return

            store.insert_replay_event(req.trace_id, seq, "model_request", json.dumps(_summarize_messages(final_messages), ensure_ascii=False))
            seq += 1

            retry_events2: list[dict[str, Any]] = []

            def on_model_retry2(n: int, e: Exception) -> None:
                retry_events2.append({"retry_count": int(n), "hint": str(e)[:200]})

            model_attrs2: dict[str, Any] = {"provider": "siliconflow", "model": None, "model_alias": "chat"}
            with tracer.span(
                req.trace_id,
                name="model.stream",
                kind="model",
                attrs=model_attrs2,
            ):
                model_stream, meta2 = stream_chat(
                    settings,
                    ModelChatRequest(
                        trace_id=req.trace_id,
                        conversation_id=req.conversation_id,
                        messages=final_messages,
                        tool_choice="none",
                    ),
                    model_alias="chat",
                    on_retry=on_model_retry2,
                )
                model_attrs2["model"] = meta2.get("model")
                model_attrs2["retry_count"] = meta2.get("retry_count")

                if retry_events2:
                    for it2 in retry_events2:
                        store.insert_replay_event(
                            req.trace_id,
                            seq,
                            "model_retry",
                            json.dumps({"kind": "stream", "retry_count": it2.get("retry_count"), "hint": it2.get("hint")}, ensure_ascii=False),
                        )
                        seq += 1
                        yield StreamEvent(
                            trace_id=req.trace_id,
                            type="error",
                            payload=_error_payload(
                                "模型请求失败，正在重试…",
                                stage="model",
                                recoverable=True,
                                error_code="MODEL_RETRYING",
                                trace_hint=it2.get("hint"),
                            ),
                        )

                for me in model_stream:
                    if me.type == "token":
                        token = (me.payload or {}).get("delta")
                        if not isinstance(token, str) or not token:
                            continue
                        assistant_text_parts.append(token)
                        ev = StreamEvent(trace_id=req.trace_id, type="token", payload={"delta": token})
                        store.insert_replay_event(req.trace_id, seq, "token", json.dumps({"delta": token}, ensure_ascii=False))
                        seq += 1
                        yield ev
                    elif me.type == "done":
                        break

            assistant_text = "".join(assistant_text_parts)
            store.insert_message(req.conversation_id, req.trace_id, "assistant", assistant_text)

            done_payload = _done_payload(final_stop_reason, model_usage_total)
            store.insert_replay_event(req.trace_id, seq, "model_done", json.dumps(done_payload, ensure_ascii=False))
            seq += 1

            store.finish_trace(
                req.trace_id,
                finished_at_ms=now_ms(),
                status="ok",
                stop_reason=final_stop_reason,
                error_message=None,
            )

            yield StreamEvent(trace_id=req.trace_id, type="done", payload=done_payload)
        except Exception as e:
            msg = str(e)
            stop_reason = StopReason.model_error.value
            user_message = "执行失败"
            recoverable = False
            stage = "orchestrator"

            error_code = "ORCHESTRATOR_ERROR"
            if msg == "model_rpm_limited":
                user_message = "模型请求过于频繁，请稍后重试"
                recoverable = True
                stage = "model"
                error_code = "MODEL_RPM_LIMITED"
            elif msg == "model_tpm_limited":
                stop_reason = StopReason.max_tokens.value
                user_message = "模型令牌预算已达上限，请缩短输入或稍后重试"
                recoverable = True
                stage = "model"
                error_code = "MODEL_TPM_LIMITED"

            store.insert_replay_event(req.trace_id, seq, "model_error", json.dumps({"message": msg[:500]}, ensure_ascii=False))
            store.finish_trace(
                req.trace_id,
                finished_at_ms=now_ms(),
                status="error",
                stop_reason=stop_reason,
                error_message=msg[:500],
            )
            yield StreamEvent(
                trace_id=req.trace_id,
                type="error",
                payload=_error_payload(user_message, stage=stage, recoverable=recoverable, error_code=error_code),
            )
            yield StreamEvent(trace_id=req.trace_id, type="done", payload=_done_payload(stop_reason, model_usage_total))
