from __future__ import annotations

from typing import Any

from config import Settings
from core.contracts.chat import ChatRequest
from core.contracts.observability import Tracer
from core.contracts.storage import Storage


def recall(settings: Settings, store: Storage, tracer: Tracer, req: ChatRequest, args: dict[str, Any]) -> dict[str, Any]:
    limit = int(args.get("limit"))
    items = store.list_memories(req.conversation_id, limit=limit)
    notes: list[str] = []
    for it in items:
        notes.append(str(it.get("content") or ""))
    return {"notes": notes}
