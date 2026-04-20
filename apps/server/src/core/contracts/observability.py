from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ContextManager, Protocol


@dataclass(frozen=True)
class Trace:
    trace_id: str
    conversation_id: str
    started_at_ms: int
    finished_at_ms: int | None = None
    status: str = "running"
    stop_reason: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class Span:
    span_id: str
    trace_id: str
    name: str
    kind: str
    started_at_ms: int
    finished_at_ms: int | None = None
    ok: bool = True
    attrs: dict[str, Any] = field(default_factory=dict)


class Tracer(Protocol):
    def span(self, trace_id: str, name: str, kind: str, attrs: dict[str, Any] | None = None) -> ContextManager[None]: ...
