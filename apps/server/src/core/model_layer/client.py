from __future__ import annotations

import math
import time
from collections import deque
from collections.abc import Callable
from threading import BoundedSemaphore, Lock
from typing import Any, Iterator

from config import Settings
from core.contracts.model import AssistantMessage, Message, ModelChatRequest, ModelChatResponse, ModelStreamEvent, ModelToolCall
from core.model_layer.limits import ModelLimits, load_model_limits
from core.model_layer.providers.siliconflow import complete_chat_message, complete_chat_text, stream_chat_deltas, stream_chat_tokens
from core.model_layer.retry import with_retry_info
from core.model_layer.router import route_models

_GATE_LOCK = Lock()
_CONCURRENCY_SEM: BoundedSemaphore | None = None
_CONCURRENCY_SIZE = 0
_RPM_BUCKET: deque[float] = deque()
_TPM_BUCKET: deque[tuple[float, int]] = deque()


class _Lease:
    def __init__(self, sem: BoundedSemaphore | None):
        self._sem = sem
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        if self._sem is not None:
            self._sem.release()
        self._closed = True


def _estimate_tokens(messages: list[dict[str, Any]], *, chars_per_token: float) -> int:
    chars = 0
    for m in messages:
        if not isinstance(m, dict):
            continue
        for k in ("content", "name", "tool_call_id"):
            v = m.get(k)
            if isinstance(v, str):
                chars += len(v)
    ratio = max(0.5, float(chars_per_token))
    return max(1, int(math.ceil(chars / ratio)))


def _acquire_concurrency(max_concurrency: int) -> _Lease:
    if max_concurrency <= 0:
        return _Lease(None)
    global _CONCURRENCY_SEM, _CONCURRENCY_SIZE
    with _GATE_LOCK:
        if _CONCURRENCY_SEM is None or _CONCURRENCY_SIZE != max_concurrency:
            _CONCURRENCY_SEM = BoundedSemaphore(value=max_concurrency)
            _CONCURRENCY_SIZE = max_concurrency
        sem = _CONCURRENCY_SEM
    if sem is None:
        return _Lease(None)
    sem.acquire()
    return _Lease(sem)


def _enforce_throughput(limits: ModelLimits, estimated_tokens: int) -> None:
    now = time.time()
    with _GATE_LOCK:
        while _RPM_BUCKET and (now - _RPM_BUCKET[0]) > 60.0:
            _RPM_BUCKET.popleft()
        while _TPM_BUCKET and (now - _TPM_BUCKET[0][0]) > 60.0:
            _TPM_BUCKET.popleft()

        if limits.rpm > 0 and len(_RPM_BUCKET) >= limits.rpm:
            raise RuntimeError("model_rpm_limited")

        if limits.tpm > 0:
            used = sum(t for _, t in _TPM_BUCKET)
            if used + max(1, estimated_tokens) > limits.tpm:
                raise RuntimeError("model_tpm_limited")

        _RPM_BUCKET.append(now)
        if limits.tpm > 0:
            _TPM_BUCKET.append((now, max(1, estimated_tokens)))


def _run_with_limits(limits: ModelLimits, messages: list[dict[str, Any]], fn: Callable[[], Any]) -> Any:
    estimated_tokens = _estimate_tokens(messages, chars_per_token=limits.chars_per_token)
    _enforce_throughput(limits, estimated_tokens)
    lease = _acquire_concurrency(limits.max_concurrency)
    try:
        return fn()
    finally:
        lease.close()


def _messages_to_dicts(messages: list[dict[str, Any]] | list[Message]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for m in messages:
        if isinstance(m, dict):
            out.append(m)
            continue
        out.append({"role": m.role, "content": m.content, **({"name": m.name} if m.name else {}), **({"tool_call_id": m.tool_call_id} if m.tool_call_id else {})})
    return out


def complete_text(
    settings: Settings,
    messages: list[dict[str, Any]] | list[Message],
    *,
    model_alias: str | None = None,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> tuple[str, str, int]:
    if not settings.siliconflow_api_key:
        raise RuntimeError("Missing SILICONFLOW_API_KEY")

    limits = load_model_limits()
    last_err: Exception | None = None

    for model in route_models(settings, alias=model_alias):
        try:
            msg_dicts = _messages_to_dicts(messages)
            text, retry_count = with_retry_info(
                fn=lambda: _run_with_limits(
                    limits,
                    msg_dicts,
                    lambda: complete_chat_text(
                        base_url=settings.siliconflow_base_url,
                        api_key=settings.siliconflow_api_key,
                        model=model,
                        messages=msg_dicts,
                    ),
                ),
                max_retries=limits.max_retries,
                backoff_s=limits.backoff_s,
                on_retry=on_retry,
            )
            return text, model, retry_count
        except Exception as e:
            last_err = e
            continue

    if last_err is not None:
        raise last_err
    raise RuntimeError("no_model_routed")


def stream_chat(
    settings: Settings,
    req: ModelChatRequest,
    *,
    model_alias: str | None = None,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> tuple[Iterator[ModelStreamEvent], dict[str, Any]]:
    if not settings.siliconflow_api_key:
        raise RuntimeError("Missing SILICONFLOW_API_KEY")

    limits = load_model_limits()
    last_err: Exception | None = None

    msg_dicts = _messages_to_dicts(req.messages)

    def _open_stream_first(model: str) -> tuple[_Lease, dict[str, Any] | None, Iterator[dict[str, Any]]]:
        estimated_tokens = _estimate_tokens(msg_dicts, chars_per_token=limits.chars_per_token)
        _enforce_throughput(limits, estimated_tokens)
        lease = _acquire_concurrency(limits.max_concurrency)
        try:
            it = stream_chat_deltas(
                base_url=settings.siliconflow_base_url,
                api_key=settings.siliconflow_api_key,
                model=model,
                messages=msg_dicts,
                tools=req.tools,
                tool_choice=req.tool_choice,
            )

            try:
                first = next(it)
            except StopIteration:
                return lease, None, iter(())

            return lease, first, it
        except Exception:
            lease.close()
            raise

    for model in route_models(settings, alias=model_alias):
        try:
            (lease, first, it), retry_count = with_retry_info(
                fn=lambda: _open_stream_first(model),
                max_retries=limits.max_retries,
                backoff_s=limits.backoff_s,
                on_retry=on_retry,
            )

            def gen() -> Iterator[ModelStreamEvent]:
                tool_calls_by_index: dict[int, dict[str, Any]] = {}
                finish_reason: str | None = None

                def consume(ev: dict[str, Any]) -> Iterator[ModelStreamEvent]:
                    nonlocal finish_reason

                    t = ev.get("type")
                    if t == "token":
                        delta = ev.get("delta")
                        if isinstance(delta, str) and delta:
                            yield ModelStreamEvent(trace_id=req.trace_id, type="token", payload={"delta": delta})
                        return

                    if t == "tool_call_delta":
                        idx = ev.get("index")
                        if not isinstance(idx, int):
                            return

                        rec = tool_calls_by_index.get(idx)
                        if rec is None:
                            rec = {"call_id": None, "name": None, "arguments_parts": []}
                            tool_calls_by_index[idx] = rec

                        call_id = ev.get("call_id")
                        name = ev.get("name")
                        args = ev.get("arguments")

                        if isinstance(call_id, str) and call_id:
                            rec["call_id"] = call_id
                        if isinstance(name, str) and name:
                            rec["name"] = name
                        if isinstance(args, str) and args:
                            rec["arguments_parts"].append(args)
                        return

                    if t == "finish":
                        fr = ev.get("finish_reason")
                        if isinstance(fr, str) and fr:
                            finish_reason = fr
                        return

                try:
                    if first is not None:
                        yield from consume(first)
                    for ev in it:
                        yield from consume(ev)

                    for idx in sorted(tool_calls_by_index.keys()):
                        rec = tool_calls_by_index[idx]
                        call_id = rec.get("call_id")
                        name = rec.get("name")
                        arguments_parts = rec.get("arguments_parts")
                        arguments_json = "".join(arguments_parts) if isinstance(arguments_parts, list) else ""

                        if isinstance(call_id, str) and isinstance(name, str) and arguments_json:
                            yield ModelStreamEvent(
                                trace_id=req.trace_id,
                                type="tool_call",
                                payload={"call_id": call_id, "name": name, "arguments_json": arguments_json},
                            )

                    yield ModelStreamEvent(trace_id=req.trace_id, type="done", payload={"finish_reason": finish_reason})
                finally:
                    lease.close()

            meta: dict[str, Any] = {"provider": "siliconflow", "model": model, "retry_count": retry_count}
            return gen(), meta
        except Exception as e:
            last_err = e
            continue

    if last_err is not None:
        raise last_err
    raise RuntimeError("no_model_routed")


def complete_chat(
    settings: Settings,
    req: ModelChatRequest,
    *,
    model_alias: str | None = None,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> ModelChatResponse:
    if not settings.siliconflow_api_key:
        raise RuntimeError("Missing SILICONFLOW_API_KEY")

    limits = load_model_limits()
    last_err: Exception | None = None

    msg_dicts = _messages_to_dicts(req.messages)

    for model in route_models(settings, alias=model_alias):
        try:
            (content, tool_calls_raw, usage), retry_count = with_retry_info(
                fn=lambda: _run_with_limits(
                    limits,
                    msg_dicts,
                    lambda: complete_chat_message(
                        base_url=settings.siliconflow_base_url,
                        api_key=settings.siliconflow_api_key,
                        model=model,
                        messages=msg_dicts,
                        tools=req.tools,
                        tool_choice=req.tool_choice,
                    ),
                ),
                max_retries=limits.max_retries,
                backoff_s=limits.backoff_s,
                on_retry=on_retry,
            )

            tool_calls: list[ModelToolCall] = []
            for tc in tool_calls_raw:
                call_id = tc.get("call_id")
                name = tc.get("name")
                arguments_json = tc.get("arguments_json")
                if isinstance(call_id, str) and isinstance(name, str) and isinstance(arguments_json, str):
                    tool_calls.append(ModelToolCall(call_id=call_id, name=name, arguments_json=arguments_json))

            return ModelChatResponse(
                trace_id=req.trace_id,
                message=AssistantMessage(content=content or "", tool_calls=tool_calls),
                usage=usage,
                raw={"provider": "siliconflow", "model": model, "retry_count": retry_count, "usage": usage},
            )
        except Exception as e:
            last_err = e
            continue

    if last_err is not None:
        raise last_err
    raise RuntimeError("no_model_routed")



def stream_tokens(
    settings: Settings,
    messages: list[dict[str, Any]] | list[Message],
    *,
    model_alias: str | None = None,
    tools: list[dict[str, Any]] | None = None,
    tool_choice: Any | None = None,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> tuple[Iterator[str], str, int]:
    if not settings.siliconflow_api_key:
        raise RuntimeError("Missing SILICONFLOW_API_KEY")

    limits = load_model_limits()
    last_err: Exception | None = None

    msg_dicts = _messages_to_dicts(messages)

    def _open_stream_first(model: str) -> tuple[_Lease, str | None, Iterator[str]]:
        estimated_tokens = _estimate_tokens(msg_dicts, chars_per_token=limits.chars_per_token)
        _enforce_throughput(limits, estimated_tokens)
        lease = _acquire_concurrency(limits.max_concurrency)
        try:
            it = stream_chat_tokens(
                base_url=settings.siliconflow_base_url,
                api_key=settings.siliconflow_api_key,
                model=model,
                messages=msg_dicts,
                tools=tools,
                tool_choice=tool_choice,
            )

            try:
                first = next(it)
            except StopIteration:
                return lease, None, iter(())

            return lease, first, it
        except Exception:
            lease.close()
            raise

    for model in route_models(settings, alias=model_alias):
        try:
            (lease, first, it), retry_count = with_retry_info(
                fn=lambda: _open_stream_first(model),
                max_retries=limits.max_retries,
                backoff_s=limits.backoff_s,
                on_retry=on_retry,
            )

            def gen() -> Iterator[str]:
                try:
                    if first is not None:
                        yield first
                    for t in it:
                        yield t
                finally:
                    lease.close()

            return gen(), model, retry_count
        except Exception as e:
            last_err = e
            continue

    if last_err is not None:
        raise last_err
    raise RuntimeError("no_model_routed")
