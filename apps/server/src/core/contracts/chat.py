from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


StreamEventType = Literal["token", "tool_call", "tool_result", "error", "done"]


@dataclass(frozen=True)
class ChatRequest:
    trace_id: str
    conversation_id: str
    input_text: str
    user_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StreamEvent:
    trace_id: str
    type: StreamEventType
    payload: dict[str, Any]
