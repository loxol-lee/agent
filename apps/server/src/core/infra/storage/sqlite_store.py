from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from core.contracts.observability import Span, Trace


def now_ms() -> int:
    return int(time.time() * 1000)


class SQLiteStore:
    def __init__(self, sqlite_path: str, schema_sql_path: str):
        self.sqlite_path = sqlite_path
        self.schema_sql_path = schema_sql_path
        self._init_db()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self) -> None:
        Path(self.sqlite_path).parent.mkdir(parents=True, exist_ok=True)
        schema = Path(self.schema_sql_path).read_text(encoding="utf-8")
        with self._connect() as conn:
            conn.executescript(schema)
            conn.commit()

    def insert_trace(self, trace: Trace) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO traces(trace_id, conversation_id, started_at_ms, finished_at_ms, status, stop_reason, error_message)
                VALUES(?,?,?,?,?,?,?)
                """,
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
                """
                UPDATE traces
                SET finished_at_ms=?, status=?, stop_reason=?, error_message=?
                WHERE trace_id=?
                """,
                (finished_at_ms, status, stop_reason, error_message, trace_id),
            )
            conn.commit()

    def insert_span(self, span: Span) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO spans(span_id, trace_id, name, kind, started_at_ms, finished_at_ms, ok, attrs_json)
                VALUES(?,?,?,?,?,?,?,?)
                """,
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
        ts = now_ms()
        title_guess: str | None = None
        if role == "user":
            t = content.strip().replace("\n", " ")
            if len(t) > 36:
                t = t[:36] + "…"
            title_guess = t or None

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO messages(conversation_id, trace_id, role, content, created_at_ms)
                VALUES(?,?,?,?,?)
                """,
                (conversation_id, trace_id, role, content, ts),
            )

            conn.execute(
                """
                INSERT INTO conversations(conversation_id, title, created_at_ms, updated_at_ms, archived)
                VALUES(?,?,?,?,0)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    updated_at_ms=excluded.updated_at_ms,
                    title=COALESCE(conversations.title, excluded.title),
                    archived=0
                """,
                (conversation_id, title_guess, ts, ts),
            )

            conn.commit()

    def insert_memory(self, conversation_id: str, trace_id: str, content: str) -> None:
        ts = now_ms()
        c = content.strip()
        if not c:
            return
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO memories(conversation_id, trace_id, content, created_at_ms)
                VALUES(?,?,?,?)
                """,
                (conversation_id, trace_id, c, ts),
            )
            conn.commit()

    def search_memories(self, conversation_id: str, query: str, limit: int = 10) -> list[dict]:
        q = query.strip()
        if not q:
            return []

        parts = [p for p in q.split() if p]
        if not parts:
            parts = [q]

        where = " OR ".join(["content LIKE ?" for _ in parts])
        params = ["%" + p + "%" for p in parts]

        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, conversation_id, trace_id, content, created_at_ms
                FROM memories
                WHERE conversation_id=? AND ({where})
                ORDER BY created_at_ms DESC, id DESC
                LIMIT ?
                """,
                (conversation_id, *params, limit),
            ).fetchall()

        return [dict(r) for r in rows]

    def list_memories(self, conversation_id: str, limit: int = 10) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, conversation_id, trace_id, content, created_at_ms
                FROM memories
                WHERE conversation_id=?
                ORDER BY created_at_ms DESC, id DESC
                LIMIT ?
                """,
                (conversation_id, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_memories(self, conversation_id: str, memory_ids: list[int]) -> int:
        ids: list[int] = []
        seen: set[int] = set()
        for it in memory_ids:
            try:
                mid = int(it)
            except Exception:
                continue
            if mid <= 0 or mid in seen:
                continue
            seen.add(mid)
            ids.append(mid)

        if not ids:
            return 0

        placeholders = ",".join(["?" for _ in ids])
        sql = f"DELETE FROM memories WHERE conversation_id=? AND id IN ({placeholders})"

        with self._connect() as conn:
            cur = conn.execute(sql, (conversation_id, *ids))
            conn.commit()
            return int(cur.rowcount or 0)

    def get_memory_write_enabled(self, conversation_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT memory_write_enabled FROM conversation_settings WHERE conversation_id=?",
                (conversation_id,),
            ).fetchone()
        if row is None:
            return True
        return bool(int(row[0]))

    def set_memory_write_enabled(self, conversation_id: str, enabled: bool) -> None:
        ts = now_ms()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO conversation_settings(conversation_id, memory_write_enabled, updated_at_ms)
                VALUES(?,?,?)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    memory_write_enabled=excluded.memory_write_enabled,
                    updated_at_ms=excluded.updated_at_ms
                """,
                (conversation_id, 1 if enabled else 0, ts),
            )
            conn.commit()

    def insert_tool_run(
        self,
        *,
        trace_id: str,
        conversation_id: str,
        call_id: str,
        tool_name: str,
        allowed: bool,
        risk: str,
        side_effect: bool,
        args_json: str,
        result_json: str,
        ok: bool,
        error_code: str | None,
        error_message: str | None,
        started_at_ms: int,
        finished_at_ms: int,
    ) -> None:
        latency_ms = max(0, int(finished_at_ms) - int(started_at_ms))
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO tool_runs(
                  trace_id, conversation_id, call_id, tool_name,
                  allowed, risk, side_effect,
                  args_json, result_json,
                  ok, error_code, error_message,
                  started_at_ms, finished_at_ms, latency_ms,
                  created_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    trace_id,
                    conversation_id,
                    call_id,
                    tool_name,
                    1 if allowed else 0,
                    risk,
                    1 if side_effect else 0,
                    args_json,
                    result_json,
                    1 if ok else 0,
                    error_code,
                    error_message,
                    started_at_ms,
                    finished_at_ms,
                    latency_ms,
                    now_ms(),
                ),
            )
            conn.commit()

    def list_tool_runs(self, trace_id: str, limit: int = 200) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, trace_id, conversation_id, call_id, tool_name, allowed, risk, side_effect,
                       args_json, result_json, ok, error_code, error_message,
                       started_at_ms, finished_at_ms, latency_ms, created_at_ms
                FROM tool_runs
                WHERE trace_id=?
                ORDER BY id ASC
                LIMIT ?
                """,
                (trace_id, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_messages(self, conversation_id: str, limit: int = 50) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, conversation_id, trace_id, role, content, created_at_ms
                FROM messages
                WHERE conversation_id=?
                ORDER BY created_at_ms DESC, id DESC
                LIMIT ?
                """,
                (conversation_id, limit),
            ).fetchall()

        items = [dict(r) for r in rows]
        items.reverse()
        return items

    def list_chat_messages(self, conversation_id: str, limit: int = 20) -> list[dict[str, str]]:
        items = self.list_messages(conversation_id=conversation_id, limit=limit)
        out: list[dict[str, str]] = []
        for m in items:
            role = m.get("role")
            content = m.get("content")
            if isinstance(role, str) and isinstance(content, str):
                out.append({"role": role, "content": content})
        return out

    def insert_replay_event(self, trace_id: str, seq: int, type_: str, payload_json: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO replay_events(trace_id, seq, type, payload_json, created_at_ms)
                VALUES(?,?,?,?,?)
                """,
                (trace_id, seq, type_, payload_json, now_ms()),
            )
            conn.commit()

    def get_trace(self, trace_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT trace_id, conversation_id, started_at_ms, finished_at_ms, status, stop_reason, error_message
                FROM traces
                WHERE trace_id=?
                """,
                (trace_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_spans(self, trace_id: str) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT span_id, trace_id, name, kind, started_at_ms, finished_at_ms, ok, attrs_json
                FROM spans
                WHERE trace_id=?
                ORDER BY started_at_ms ASC
                """,
                (trace_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_replay_events(self, trace_id: str, limit: int = 2000) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT seq, type, payload_json, created_at_ms
                FROM replay_events
                WHERE trace_id=?
                ORDER BY seq ASC
                LIMIT ?
                """,
                (trace_id, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_conversations(self, limit: int = 50) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                  m.conversation_id,
                  COALESCE(c.title,
                    (SELECT content
                     FROM messages m2
                     WHERE m2.conversation_id = m.conversation_id AND m2.role='user'
                     ORDER BY m2.created_at_ms ASC, m2.id ASC
                     LIMIT 1)
                  ) AS title,
                  MAX(m.created_at_ms) AS updated_at_ms,
                  COUNT(*) AS message_count
                FROM messages m
                LEFT JOIN conversations c ON c.conversation_id = m.conversation_id
                WHERE COALESCE(c.archived, 0) = 0
                GROUP BY m.conversation_id
                ORDER BY updated_at_ms DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        out: list[dict] = []
        for r in rows:
            d = dict(r)
            title_src = d.get("title") or ""
            title = str(title_src).strip().replace("\n", " ")
            if len(title) > 36:
                title = title[:36] + "…"
            if not title:
                title = d.get("conversation_id")
            out.append(
                {
                    "conversation_id": d.get("conversation_id"),
                    "title": title,
                    "updated_at_ms": d.get("updated_at_ms"),
                    "message_count": d.get("message_count"),
                }
            )
        return out

    def rename_conversation(self, conversation_id: str, title: str) -> None:
        ts = now_ms()
        t = title.strip().replace("\n", " ")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO conversations(conversation_id, title, created_at_ms, updated_at_ms, archived)
                VALUES(?,?,?,?,0)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    title=excluded.title,
                    updated_at_ms=excluded.updated_at_ms,
                    archived=0
                """,
                (conversation_id, t, ts, ts),
            )
            conn.commit()

    def archive_conversation(self, conversation_id: str) -> None:
        ts = now_ms()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO conversations(conversation_id, title, created_at_ms, updated_at_ms, archived)
                VALUES(?,?,?, ?, 1)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    archived=1,
                    updated_at_ms=excluded.updated_at_ms
                """,
                (conversation_id, None, ts, ts),
            )
            conn.commit()

    def purge_conversation(self, conversation_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM replay_events WHERE trace_id IN (SELECT trace_id FROM traces WHERE conversation_id=?)", (conversation_id,))
            conn.execute("DELETE FROM spans WHERE trace_id IN (SELECT trace_id FROM traces WHERE conversation_id=?)", (conversation_id,))
            conn.execute("DELETE FROM tool_runs WHERE conversation_id=?", (conversation_id,))
            conn.execute("DELETE FROM memories WHERE conversation_id=?", (conversation_id,))
            conn.execute("DELETE FROM messages WHERE conversation_id=?", (conversation_id,))
            conn.execute("DELETE FROM traces WHERE conversation_id=?", (conversation_id,))
            conn.execute("DELETE FROM conversations WHERE conversation_id=?", (conversation_id,))
            conn.execute("DELETE FROM conversation_settings WHERE conversation_id=?", (conversation_id,))
            conn.commit()

    def purge_all_conversations(self) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM replay_events")
            conn.execute("DELETE FROM spans")
            conn.execute("DELETE FROM tool_runs")
            conn.execute("DELETE FROM memories")
            conn.execute("DELETE FROM messages")
            conn.execute("DELETE FROM traces")
            conn.execute("DELETE FROM conversations")
            conn.execute("DELETE FROM conversation_settings")
            conn.commit()
