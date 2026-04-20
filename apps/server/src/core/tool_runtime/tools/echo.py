from __future__ import annotations

from typing import Any

from config import Settings
from core.contracts.chat import ChatRequest
from core.contracts.observability import Tracer
from core.contracts.storage import Storage


def echo(settings: Settings, store: Storage, tracer: Tracer, req: ChatRequest, args: dict[str, Any]) -> dict[str, Any]:
    return {"text": str(args.get("text") or "")}
