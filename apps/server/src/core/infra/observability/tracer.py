from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

from core.contracts.observability import Span
from core.contracts.storage import Storage


def now_ms() -> int:
    return int(time.time() * 1000)


class Tracer:
    def __init__(self, store: Storage):
        self.store = store

    @contextmanager
    def span(self, trace_id: str, name: str, kind: str, attrs: dict[str, Any] | None = None) -> Iterator[None]:
        span_id = uuid.uuid4().hex
        started = now_ms()
        ok = True
        span_attrs = attrs or {}
        try:
            yield
        except Exception:
            ok = False
            raise
        finally:
            finished = now_ms()
            self.store.insert_span(
                Span(
                    span_id=span_id,
                    trace_id=trace_id,
                    name=name,
                    kind=kind,
                    started_at_ms=started,
                    finished_at_ms=finished,
                    ok=ok,
                    attrs=span_attrs,
                )
            )
