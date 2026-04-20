from __future__ import annotations

from typing import Any

from config import Settings
from core.contracts.chat import ChatRequest
from core.contracts.observability import Tracer
from core.contracts.storage import Storage


def _memory_ids_from_args(args: dict[str, Any]) -> list[int]:
    raw = args.get("memory_ids")
    if not isinstance(raw, list):
        raise ValueError("memory_ids must be array")

    out: list[int] = []
    seen: set[int] = set()
    for it in raw:
        if not isinstance(it, int) or it <= 0:
            raise ValueError("memory_ids must contain positive integers")
        if it in seen:
            continue
        seen.add(it)
        out.append(it)

    if not out:
        raise ValueError("memory_ids is empty")
    if len(out) > 50:
        raise ValueError("memory_ids too many")
    return out


def remember(settings: Settings, store: Storage, tracer: Tracer, req: ChatRequest, args: dict[str, Any]) -> dict[str, Any]:
    note = str(args.get("note") or "").strip()
    if not store.get_memory_write_enabled(req.conversation_id):
        return {"ok": True, "skipped": True, "reason": "memory_write_disabled"}
    store.insert_memory(req.conversation_id, req.trace_id, note)
    return {"ok": True}


def memory_forget(settings: Settings, store: Storage, tracer: Tracer, req: ChatRequest, args: dict[str, Any]) -> dict[str, Any]:
    memory_ids = _memory_ids_from_args(args)
    deleted = store.delete_memories(req.conversation_id, memory_ids)
    return {"ok": True, "deleted": int(deleted), "memory_ids": memory_ids}
