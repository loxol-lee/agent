from __future__ import annotations

import argparse
from pathlib import Path


BASE = Path("/Users/lixiaoyi/Documents/newtext/agunt")


def write_file(path: Path, content: str, force: bool) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        if path.stat().st_size > 0:
            return f"skip (non-empty): {path}"
    path.write_text(content, encoding="utf-8")
    return f"write: {path}"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    files: dict[str, str] = {}

    files["apps/server/requirements.txt"] = "\n".join(
        [
            "fastapi>=0.110",
            "uvicorn[standard]>=0.27",
            "openai>=1.0.0",
        ]
    ) + "\n"

    files["apps/server/src/config.py"] = """from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    sqlite_path: str
    siliconflow_base_url: str
    siliconflow_api_key: str | None
    siliconflow_model: str


def load_settings() -> Settings:
    sqlite_path = os.environ.get("AGUNT_SQLITE_PATH", "/Users/lixiaoyi/Documents/newtext/agunt/agunt_v0.sqlite3")
    base_url = os.environ.get("SILICONFLOW_BASE_URL", "https://api.siliconflow.cn/v1")
    api_key = os.environ.get("SILICONFLOW_API_KEY")
    model = os.environ.get("SILICONFLOW_MODEL", "Qwen/Qwen2.5-7B-Instruct")
    return Settings(
        sqlite_path=sqlite_path,
        siliconflow_base_url=base_url,
        siliconflow_api_key=api_key,
        siliconflow_model=model,
    )
"""

    files["apps/server/src/core/contracts/chat.py"] = """from __future__ import annotations

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
"""

    files["apps/server/src/core/contracts/observability.py"] = """from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


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
"""

    files["apps/server/src/core/infra/storage/schema.sql"] = """PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS traces (
  trace_id TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL,
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER,
  status TEXT NOT NULL,
  stop_reason TEXT,
  error_message TEXT
);

CREATE TABLE IF NOT EXISTS spans (
  span_id TEXT PRIMARY KEY,
  trace_id TEXT NOT NULL,
  name TEXT NOT NULL,
  kind TEXT NOT NULL,
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER,
  ok INTEGER NOT NULL,
  attrs_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_spans_trace_id ON spans(trace_id);

CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  conversation_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON messages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_trace_id ON messages(trace_id);

CREATE TABLE IF NOT EXISTS replay_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trace_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_replay_trace_id_seq ON replay_events(trace_id, seq);
"""

    files["apps/server/src/core/infra/storage/sqlite_store.py"] = """from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from core.contracts.observability import Span, Trace


def now_ms() -> int:
    return int(time.time() * 1000)


class SQLiteStore:
    def __init__(self, sqlite_path: str, schema_sql_path: str):
        self.sqlite_path = sqlite_path
        self.schema_sql_path = schema_sql_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        Path(self.sqlite_path).parent.mkdir(parents=True, exist_ok=True)
        schema = Path(self.schema_sql_path).read_text(encoding="utf-8")
        with self._connect() as conn:
            conn.executescript(schema)
            conn.commit()

    def insert_trace(self, trace: Trace) -> None:
        with self._connect() as conn:
            conn.execute(
                \"\"\"
                INSERT INTO traces(trace_id, conversation_id, started_at_ms, finished_at_ms, status, stop_reason, error_message)
                VALUES(?,?,?,?,?,?,?)
                \"\"\",
                (
                    trace.trace_id,
                    trace.conversation_id,
                    trace.started_at_ms,
                    trace.finished_at_ms,
                    trace.status,
                    trace.stop_reason,
                    trace.error_message,
                ),
            )
            conn.commit()

    def finish_trace(self, trace_id: str, finished_at_ms: int, status: str, stop_reason: str | None, error_message: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                \"\"\"
                UPDATE traces
                SET finished_at_ms=?, status=?, stop_reason=?, error_message=?
                WHERE trace_id=?
                \"\"\",
                (finished_at_ms, status, stop_reason, error_message, trace_id),
            )
            conn.commit()

    def insert_span(self, span: Span) -> None:
        with self._connect() as conn:
            conn.execute(
                \"\"\"
                INSERT INTO spans(span_id, trace_id, name, kind, started_at_ms, finished_at_ms, ok, attrs_json)
                VALUES(?,?,?,?,?,?,?,?)
                \"\"\",
                (
                    span.span_id,
                    span.trace_id,
                    span.name,
                    span.kind,
                    span.started_at_ms,
                    span.finished_at_ms,
                    1 if span.ok else 0,
                    json.dumps(span.attrs, ensure_ascii=False),
                ),
            )
            conn.commit()

    def insert_message(self, conversation_id: str, trace_id: str, role: str, content: str) -> None:
        with self._connect() as conn:
            conn.execute(
                \"\"\"
                INSERT INTO messages(conversation_id, trace_id, role, content, created_at_ms)
                VALUES(?,?,?,?,?)
                \"\"\",
                (conversation_id, trace_id, role, content, now_ms()),
            )
            conn.commit()

    def insert_replay_event(self, trace_id: str, seq: int, type_: str, payload_json: str) -> None:
        with self._connect() as conn:
            conn.execute(
                \"\"\"
                INSERT INTO replay_events(trace_id, seq, type, payload_json, created_at_ms)
                VALUES(?,?,?,?,?)
                \"\"\",
                (trace_id, seq, type_, payload_json, now_ms()),
            )
            conn.commit()
"""

    files["apps/server/src/core/infra/observability/tracer.py"] = """from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

from core.contracts.observability import Span
from core.infra.storage.sqlite_store import SQLiteStore


def now_ms() -> int:
    return int(time.time() * 1000)


class Tracer:
    def __init__(self, store: SQLiteStore):
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
"""

    files["apps/server/src/core/model_layer/providers/siliconflow.py"] = """from __future__ import annotations

from typing import Iterator

from openai import OpenAI


def stream_chat_tokens(
    *,
    base_url: str,
    api_key: str,
    model: str,
    user_text: str,
) -> Iterator[str]:
    client = OpenAI(api_key=api_key, base_url=base_url)
    stream = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": user_text}],
        stream=True,
    )
    for chunk in stream:
        if not chunk.choices:
            continue
        delta = chunk.choices[0].delta
        text = getattr(delta, "content", None)
        if text:
            yield text
"""

    files["apps/server/src/core/model_layer/client.py"] = """from __future__ import annotations

from typing import Iterator

from config import Settings
from core.model_layer.providers.siliconflow import stream_chat_tokens


def stream_tokens(settings: Settings, user_text: str) -> Iterator[str]:
    if not settings.siliconflow_api_key:
        raise RuntimeError("Missing SILICONFLOW_API_KEY")
    return stream_chat_tokens(
        base_url=settings.siliconflow_base_url,
        api_key=settings.siliconflow_api_key,
        model=settings.siliconflow_model,
        user_text=user_text,
    )
"""

    files["apps/server/src/core/orchestrator/agent_runtime.py"] = """from __future__ import annotations

import json
import uuid
from typing import Iterator

from config import Settings
from core.contracts.chat import ChatRequest, StreamEvent
from core.contracts.observability import Trace
from core.infra.observability.tracer import Tracer, now_ms
from core.infra.storage.sqlite_store import SQLiteStore
from core.model_layer.client import stream_tokens


def run_chat_stream(
    *,
    settings: Settings,
    store: SQLiteStore,
    tracer: Tracer,
    req: ChatRequest,
) -> Iterator[StreamEvent]:
    store.insert_trace(
        Trace(
            trace_id=req.trace_id,
            conversation_id=req.conversation_id,
            started_at_ms=now_ms(),
        )
    )

    store.insert_message(req.conversation_id, req.trace_id, "user", req.input_text)

    seq = 0
    assistant_text_parts: list[str] = []

    with tracer.span(req.trace_id, name="orchestrator.run", kind="orchestrator"):
        try:
            with tracer.span(
                req.trace_id,
                name="model.stream",
                kind="model",
                attrs={"provider": "siliconflow", "model": settings.siliconflow_model},
            ):
                for token in stream_tokens(settings, req.input_text):
                    assistant_text_parts.append(token)
                    ev = StreamEvent(trace_id=req.trace_id, type="token", payload={"delta": token})
                    payload_json = json.dumps({"delta": token}, ensure_ascii=False)
                    store.insert_replay_event(req.trace_id, seq, "token", payload_json)
                    seq += 1
                    yield ev

            assistant_text = "".join(assistant_text_parts)
            store.insert_message(req.conversation_id, req.trace_id, "assistant", assistant_text)

            store.finish_trace(
                req.trace_id,
                finished_at_ms=now_ms(),
                status="ok",
                stop_reason="completed",
                error_message=None,
            )

            yield StreamEvent(trace_id=req.trace_id, type="done", payload={"stop_reason": "completed"})
        except Exception as e:
            msg = str(e)
            store.finish_trace(
                req.trace_id,
                finished_at_ms=now_ms(),
                status="error",
                stop_reason="model_error",
                error_message=msg[:500],
            )
            yield StreamEvent(trace_id=req.trace_id, type="error", payload={"message": "执行失败", "recoverable": False, "stage": "orchestrator"})
            yield StreamEvent(trace_id=req.trace_id, type="done", payload={"stop_reason": "model_error"})
"""

    files["apps/server/src/connectors/web/routes.py"] = """from __future__ import annotations

import json
import uuid
from typing import Iterator

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from config import load_settings
from core.contracts.chat import ChatRequest, StreamEvent
from core.infra.observability.tracer import Tracer
from core.infra.storage.sqlite_store import SQLiteStore
from core.orchestrator.agent_runtime import run_chat_stream


router = APIRouter()


class ChatIn(BaseModel):
    input_text: str = Field(min_length=1, max_length=20000)
    conversation_id: str | None = None
    user_id: str | None = None


def sse_bytes(event: StreamEvent) -> bytes:
    data = json.dumps({"trace_id": event.trace_id, "type": event.type, "payload": event.payload}, ensure_ascii=False)
    return f"data: {data}\\n\\n".encode("utf-8")


@router.get("/", response_class=HTMLResponse)
def index() -> str:
    return \"\"\"<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>agunt v0</title>
</head>
<body>
  <div style="max-width: 820px; margin: 24px auto; font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial;">
    <h2>agunt V0</h2>
    <div style="display:flex; gap:8px;">
      <input id="q" style="flex:1; padding:10px; font-size:16px;" placeholder="输入一句话，然后回车或点发送" />
      <button id="send" style="padding:10px 14px; font-size:16px;">发送</button>
    </div>
    <pre id="out" style="margin-top:16px; padding:12px; background:#111; color:#0f0; min-height:240px; white-space:pre-wrap;"></pre>
  </div>
<script>
const out = document.getElementById("out");
const q = document.getElementById("q");
const send = document.getElementById("send");

async function run() {
  const text = (q.value || "").trim();
  if (!text) return;
  out.textContent = "";
  const resp = await fetch("/chat/stream", {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({input_text: text})
  });
  const reader = resp.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let buf = "";
  while (true) {
    const {value, done} = await reader.read();
    if (done) break;
    buf += decoder.decode(value, {stream:true});
    while (true) {
      const idx = buf.indexOf("\\n\\n");
      if (idx < 0) break;
      const frame = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      const line = frame.split("\\n").find(l => l.startsWith("data: "));
      if (!line) continue;
      const data = line.slice(6);
      let ev;
      try { ev = JSON.parse(data); } catch(e) { continue; }
      if (ev.type === "token") out.textContent += (ev.payload && ev.payload.delta) ? ev.payload.delta : "";
      if (ev.type === "error") out.textContent += "\\n[error]\\n";
    }
  }
}

send.onclick = run;
q.addEventListener("keydown", (e) => { if (e.key === "Enter") run(); });
</script>
</body>
</html>\"\"\"


@router.get("/health")
def health() -> dict:
    return {"ok": True}


@router.post("/chat/stream")
def chat_stream(body: ChatIn) -> StreamingResponse:
    settings = load_settings()
    store = SQLiteStore(
        settings.sqlite_path,
        schema_sql_path="/Users/lixiaoyi/Documents/newtext/agunt/apps/server/src/core/infra/storage/schema.sql",
    )
    tracer = Tracer(store)

    trace_id = uuid.uuid4().hex
    conversation_id = body.conversation_id or uuid.uuid4().hex
    req = ChatRequest(
        trace_id=trace_id,
        conversation_id=conversation_id,
        input_text=body.input_text,
        user_id=body.user_id,
    )

    def gen() -> Iterator[bytes]:
        for ev in run_chat_stream(settings=settings, store=store, tracer=tracer, req=req):
            yield sse_bytes(ev)

    return StreamingResponse(gen(), media_type="text/event-stream")
"""

    files["apps/server/src/main.py"] = """from __future__ import annotations

from fastapi import FastAPI
from connectors.web.routes import router as web_router


def create_app() -> FastAPI:
    app = FastAPI()
    app.include_router(web_router)
    return app


app = create_app()
"""

    wrote = []
    for rel, content in files.items():
        wrote.append(write_file(BASE / rel, content, force=args.force))

    for line in wrote:
        print(line)

    print("\\nNext:")
    print('  cd "/Users/lixiaoyi/Documents/newtext/agunt/apps/server"')
    print("  python3 -m pip install -r requirements.txt")
    print("  export SILICONFLOW_API_KEY=你的key")
    print('  uvicorn main:app --app-dir "src" --host 127.0.0.1 --port 8000')
    print("  open http://127.0.0.1:8000/")


if __name__ == "__main__":
    main()