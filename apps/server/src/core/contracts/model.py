from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


MessageRole = Literal["system", "user", "assistant", "tool"]


@dataclass(frozen=True)
class Message:
    role: MessageRole
    content: str
    name: str | None = None
    tool_call_id: str | None = None


@dataclass(frozen=True)
class ModelToolCall:
    call_id: str
    name: str
    arguments_json: str


@dataclass(frozen=True)
class AssistantMessage:
    role: Literal["assistant"] = "assistant"
    content: str = ""
    tool_calls: list[ModelToolCall] = field(default_factory=list)


@dataclass(frozen=True)
class ModelChatRequest:
    trace_id: str
    conversation_id: str
    messages: list[Message]
    tools: list[dict[str, Any]] | None = None
    tool_choice: Any | None = None
    generation: dict[str, Any] | None = None
    routing: dict[str, Any] | None = None


@dataclass(frozen=True)
class ModelChatResponse:
    trace_id: str
    message: AssistantMessage
    usage: dict[str, int] | None = None
    raw: dict[str, Any] = field(default_factory=dict)


ModelStreamEventType = Literal["token", "tool_call", "error", "done"]


@dataclass(frozen=True)
class ModelStreamEvent:
    trace_id: str
    type: ModelStreamEventType
    payload: dict[str, Any]
