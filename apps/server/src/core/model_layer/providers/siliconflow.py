from __future__ import annotations

from typing import Any, Iterator

from openai import OpenAI


def _tool_calls_from_msg(msg: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    tool_calls = getattr(msg, "tool_calls", None)
    if not tool_calls:
        return out

    for tc in tool_calls:
        call_id = getattr(tc, "id", None)
        fn = getattr(tc, "function", None)
        name = getattr(fn, "name", None) if fn is not None else None
        arguments_json = getattr(fn, "arguments", None) if fn is not None else None

        if isinstance(call_id, str) and isinstance(name, str) and isinstance(arguments_json, str):
            out.append({"call_id": call_id, "name": name, "arguments_json": arguments_json})

    return out


def _usage_from_resp(resp: Any) -> dict[str, int] | None:
    usage = getattr(resp, "usage", None)
    if usage is None:
        return None

    out: dict[str, int] = {}
    for k in ("prompt_tokens", "completion_tokens", "total_tokens"):
        v = getattr(usage, k, None)
        if isinstance(v, int) and v >= 0:
            out[k] = v
    return out or None


def complete_chat_message(
    *,
    base_url: str,
    api_key: str,
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    tool_choice: Any | None = None,
) -> tuple[str, list[dict[str, str]], dict[str, int] | None]:
    client = OpenAI(api_key=api_key, base_url=base_url)

    kwargs: dict[str, Any] = {"model": model, "messages": messages, "stream": False, "temperature": 0}
    if tools is not None:
        kwargs["tools"] = tools
    if tool_choice is not None:
        kwargs["tool_choice"] = tool_choice

    resp = client.chat.completions.create(**kwargs)
    usage = _usage_from_resp(resp)
    if not resp.choices:
        return "", [], usage

    msg = resp.choices[0].message
    content = getattr(msg, "content", None) or ""
    return content, _tool_calls_from_msg(msg), usage


def complete_chat_text(
    *,
    base_url: str,
    api_key: str,
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    tool_choice: Any | None = None,
) -> str:
    content, _tool_calls, _usage = complete_chat_message(
        base_url=base_url,
        api_key=api_key,
        model=model,
        messages=messages,
        tools=tools,
        tool_choice=tool_choice,
    )
    return content


def stream_chat_deltas(
    *,
    base_url: str,
    api_key: str,
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    tool_choice: Any | None = None,
) -> Iterator[dict[str, Any]]:
    client = OpenAI(api_key=api_key, base_url=base_url)

    kwargs: dict[str, Any] = {"model": model, "messages": messages, "stream": True}
    if tools is not None:
        kwargs["tools"] = tools
    if tool_choice is not None:
        kwargs["tool_choice"] = tool_choice

    stream = client.chat.completions.create(**kwargs)
    for chunk in stream:
        if not chunk.choices:
            continue

        choice = chunk.choices[0]
        delta = choice.delta

        text = getattr(delta, "content", None)
        if text:
            yield {"type": "token", "delta": text}

        tcs = getattr(delta, "tool_calls", None)
        if tcs:
            for tc in tcs:
                fn = getattr(tc, "function", None)
                yield {
                    "type": "tool_call_delta",
                    "index": getattr(tc, "index", None),
                    "call_id": getattr(tc, "id", None),
                    "name": (getattr(fn, "name", None) if fn is not None else None),
                    "arguments": (getattr(fn, "arguments", None) if fn is not None else None),
                }

        finish_reason = getattr(choice, "finish_reason", None)
        if finish_reason:
            yield {"type": "finish", "finish_reason": finish_reason}


def stream_chat_tokens(
    *,
    base_url: str,
    api_key: str,
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    tool_choice: Any | None = None,
) -> Iterator[str]:
    for ev in stream_chat_deltas(
        base_url=base_url,
        api_key=api_key,
        model=model,
        messages=messages,
        tools=tools,
        tool_choice=tool_choice,
    ):
        if ev.get("type") == "token":
            text = ev.get("delta")
            if isinstance(text, str) and text:
                yield text
