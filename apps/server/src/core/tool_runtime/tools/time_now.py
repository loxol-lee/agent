from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from config import Settings
from core.contracts.chat import ChatRequest
from core.contracts.observability import Tracer
from core.contracts.storage import Storage


def time_now(settings: Settings, store: Storage, tracer: Tracer, req: ChatRequest, args: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {"unix": int(now.timestamp()), "iso": now.isoformat()}
