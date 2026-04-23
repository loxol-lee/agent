from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

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
            cols = [r[1] for r in conn.execute("PRAGMA table_info(agent_tasks)").fetchall()]
            if "logs_json" not in cols:
                conn.execute("ALTER TABLE agent_tasks ADD COLUMN logs_json TEXT")
            if "working_dir" not in cols:
                conn.execute("ALTER TABLE agent_tasks ADD COLUMN working_dir TEXT")
            if "actions_json" not in cols:
                conn.execute("ALTER TABLE agent_tasks ADD COLUMN actions_json TEXT")
            if "rollback_on_action_failure" not in cols:
                conn.execute("ALTER TABLE agent_tasks ADD COLUMN rollback_on_action_failure INTEGER NOT NULL DEFAULT 1")
            if "actor_role" not in cols:
                conn.execute("ALTER TABLE agent_tasks ADD COLUMN actor_role TEXT NOT NULL DEFAULT 'coder_agent'")
            art_cols = [r[1] for r in conn.execute("PRAGMA table_info(agent_task_artifacts)").fetchall()]
            if art_cols and "step_results_json" not in art_cols:
                conn.execute("ALTER TABLE agent_task_artifacts ADD COLUMN step_results_json TEXT")
            step_cols = [r[1] for r in conn.execute("PRAGMA table_info(agent_task_steps)").fetchall()]
            if step_cols and "attempt" not in step_cols:
                conn.execute("ALTER TABLE agent_task_steps ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0")
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

    def insert_agent_task(self, task: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO agent_tasks(
                  task_id, goal, status, current_step, attempt, max_retry,
                  acceptance_cmd, working_dir, dry_run, rollback_on_action_failure, actor_role, request_id, trace_id,
                  stop_reason, error_code, scope_paths_json, forbidden_paths_json,
                  actions_json, last_verify_json, logs_json, created_at_ms, updated_at_ms, finished_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                  status=excluded.status,
                  current_step=excluded.current_step,
                  attempt=excluded.attempt,
                  stop_reason=excluded.stop_reason,
                  error_code=excluded.error_code,
                  last_verify_json=excluded.last_verify_json,
                  logs_json=excluded.logs_json,
                  updated_at_ms=excluded.updated_at_ms,
                  finished_at_ms=excluded.finished_at_ms
                """,
                (
                    task.get("task_id"),
                    task.get("goal") or "",
                    task.get("status") or "queued",
                    task.get("current_step") or "QUEUED",
                    int(task.get("attempt") or 0),
                    int(task.get("max_retry") or 1),
                    task.get("acceptance_cmd") or "",
                    task.get("working_dir") or "",
                    1 if bool(task.get("dry_run")) else 0,
                    1 if bool(task.get("rollback_on_action_failure", True)) else 0,
                    str(task.get("actor_role") or "coder_agent"),
                    task.get("request_id") or "",
                    task.get("trace_id") or "",
                    task.get("stop_reason"),
                    task.get("error_code"),
                    json.dumps(task.get("scope_paths") or [], ensure_ascii=False),
                    json.dumps(task.get("forbidden_paths") or [], ensure_ascii=False),
                    json.dumps(task.get("actions") or [], ensure_ascii=False),
                    json.dumps(task.get("last_verify"), ensure_ascii=False) if task.get("last_verify") is not None else None,
                    json.dumps(task.get("logs") or [], ensure_ascii=False),
                    int(task.get("created_at_ms") or now_ms()),
                    int(task.get("updated_at_ms") or now_ms()),
                    int(task.get("finished_at_ms")) if task.get("finished_at_ms") is not None else None,
                ),
            )
            conn.commit()

    def update_agent_task(self, task_id: str, fields: dict[str, Any]) -> None:
        if not fields:
            return
        cols: list[str] = []
        vals: list[Any] = []
        mapping = {
            "status": "status",
            "current_step": "current_step",
            "attempt": "attempt",
            "stop_reason": "stop_reason",
            "error_code": "error_code",
            "actor_role": "actor_role",
            "updated_at_ms": "updated_at_ms",
            "finished_at_ms": "finished_at_ms",
            "last_verify": "last_verify_json",
        }
        for k, col in mapping.items():
            if k not in fields:
                continue
            v = fields.get(k)
            if k == "last_verify":
                v = (json.dumps(v, ensure_ascii=False) if v is not None else None)
            cols.append(f"{col}=?")
            vals.append(v)
        if not cols:
            return
        vals.append(task_id)
        with self._connect() as conn:
            conn.execute(f"UPDATE agent_tasks SET {', '.join(cols)} WHERE task_id=?", vals)
            conn.commit()

    def get_agent_task(self, task_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM agent_tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["dry_run"] = bool(int(d.get("dry_run") or 0))
        d["rollback_on_action_failure"] = bool(int(d.get("rollback_on_action_failure") or 1))
        d["actor_role"] = str(d.get("actor_role") or "coder_agent")
        d["scope_paths"] = json.loads(d.get("scope_paths_json") or "[]")
        d["forbidden_paths"] = json.loads(d.get("forbidden_paths_json") or "[]")
        d["actions"] = json.loads(d.get("actions_json") or "[]")
        d["last_verify"] = (json.loads(d["last_verify_json"]) if d.get("last_verify_json") else None)
        d["logs"] = json.loads(d.get("logs_json") or "[]")
        return d

    def list_agent_tasks(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM agent_tasks ORDER BY created_at_ms DESC LIMIT ?",
                (max(1, min(int(limit), 200)),),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["dry_run"] = bool(int(d.get("dry_run") or 0))
            d["rollback_on_action_failure"] = bool(int(d.get("rollback_on_action_failure") or 1))
            d["actor_role"] = str(d.get("actor_role") or "coder_agent")
            d["scope_paths"] = json.loads(d.get("scope_paths_json") or "[]")
            d["forbidden_paths"] = json.loads(d.get("forbidden_paths_json") or "[]")
            d["actions"] = json.loads(d.get("actions_json") or "[]")
            d["last_verify"] = (json.loads(d["last_verify_json"]) if d.get("last_verify_json") else None)
            d["logs"] = json.loads(d.get("logs_json") or "[]")
            out.append(d)
        return out

    def upsert_agent_task_artifacts(self, task_id: str, artifacts: dict[str, Any]) -> None:
        changed_files = artifacts.get("changed_files") if isinstance(artifacts, dict) else []
        step_results = artifacts.get("step_results") if isinstance(artifacts, dict) else []
        test_result = artifacts.get("test_result") if isinstance(artifacts, dict) else None
        summary = artifacts.get("summary") if isinstance(artifacts, dict) else None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO agent_task_artifacts(task_id, changed_files_json, step_results_json, test_result, summary, updated_at_ms)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                  changed_files_json=excluded.changed_files_json,
                  step_results_json=excluded.step_results_json,
                  test_result=excluded.test_result,
                  summary=excluded.summary,
                  updated_at_ms=excluded.updated_at_ms
                """,
                (task_id, json.dumps(changed_files or [], ensure_ascii=False), json.dumps(step_results or [], ensure_ascii=False), test_result, summary, now_ms()),
            )
            conn.commit()

    def get_agent_task_artifacts(self, task_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM agent_task_artifacts WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            return None
        d = dict(row)
        return {
            "changed_files": json.loads(d.get("changed_files_json") or "[]"),
            "step_results": json.loads(d.get("step_results_json") or "[]"),
            "test_result": d.get("test_result"),
            "summary": d.get("summary") or "",
        }

    def insert_agent_task_log(self, task_id: str, ts: int, message: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO agent_task_logs(task_id, ts, message) VALUES(?,?,?)",
                (task_id, int(ts), str(message)[:1000]),
            )
            conn.commit()

    def list_agent_task_logs(self, task_id: str, limit: int = 500) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT ts, message FROM agent_task_logs WHERE task_id=? ORDER BY id ASC LIMIT ?",
                (task_id, max(1, min(int(limit), 5000))),
            ).fetchall()
        return [dict(r) for r in rows]

    def insert_agent_task_step(self, task_id: str, step_no: int, step_type: str, attempt: int, input_json: str, output_json: str, ok: bool, latency_ms: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO agent_task_steps(task_id, step_no, step_type, attempt, input_json, output_json, ok, latency_ms, created_at_ms)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (task_id, int(step_no), step_type, int(attempt), input_json, output_json, 1 if ok else 0, int(latency_ms), now_ms()),
            )
            conn.commit()

    def list_agent_task_steps(self, task_id: str, limit: int = 500, offset: int = 0, cursor: int | None = None, attempt: int | None = None, step_type: str | None = None, ok: bool | None = None, order: str = "asc", from_ts: int | None = None, to_ts: int | None = None) -> list[dict[str, Any]]:
        limit_v = max(1, min(int(limit), 5000))
        offset_v = max(0, int(offset))
        order_v = "DESC" if str(order).strip().lower() == "desc" else "ASC"
        sql = "SELECT id AS _cursor, step_no, step_type, attempt, input_json, output_json, ok, latency_ms, created_at_ms FROM agent_task_steps WHERE task_id=?"
        params: list[Any] = [task_id]
        if attempt is not None:
            sql += " AND attempt=?"
            params.append(int(attempt))
        if cursor is not None:
            sql += (" AND id>?" if order_v == "ASC" else " AND id<?")
            params.append(int(cursor))
        if step_type is not None and str(step_type).strip():
            sql += " AND step_type=?"
            params.append(str(step_type).strip().upper())
        if ok is not None:
            sql += " AND ok=?"
            params.append(1 if bool(ok) else 0)
        if from_ts is not None:
            sql += " AND created_at_ms>=?"
            params.append(int(from_ts))
        if to_ts is not None:
            sql += " AND created_at_ms<=?"
            params.append(int(to_ts))
        sql += f" ORDER BY id {order_v} LIMIT ? OFFSET ?"
        params.extend([limit_v, offset_v])
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["ok"] = bool(int(d.get("ok") or 0))
            out.append(d)
        return out

    def aggregate_agent_task_steps(self, task_id: str, attempt: int | None = None, step_type: str | None = None, ok: bool | None = None, from_ts: int | None = None, to_ts: int | None = None, error_top_n: int = 5, success_top_n: int = 5, p95_threshold_ms: int = 1000, penalty_per_100ms: int = 1, penalty_cap: int = 40, green_min_score: int = 90, yellow_min_score: int = 70, yellow_max_failure_rate: float = 20.0, empty_score: int = 50) -> dict[str, Any]:
        where = " WHERE task_id=?"
        params: list[Any] = [task_id]
        if attempt is not None:
            where += " AND attempt=?"
            params.append(int(attempt))
        if step_type is not None and str(step_type).strip():
            where += " AND step_type=?"
            params.append(str(step_type).strip().upper())
        if ok is not None:
            where += " AND ok=?"
            params.append(1 if bool(ok) else 0)
        if from_ts is not None:
            where += " AND created_at_ms>=?"
            params.append(int(from_ts))
        if to_ts is not None:
            where += " AND created_at_ms<=?"
            params.append(int(to_ts))

        with self._connect() as conn:
            base = conn.execute(
                f"SELECT COUNT(*) AS total, SUM(CASE WHEN ok=1 THEN 1 ELSE 0 END) AS success_count, SUM(CASE WHEN ok=0 THEN 1 ELSE 0 END) AS failed_count, AVG(latency_ms) AS avg_latency_ms, MIN(latency_ms) AS min_latency_ms, MAX(latency_ms) AS max_latency_ms FROM agent_task_steps{where}",
                params,
            ).fetchone()
            grp = conn.execute(
                f"SELECT step_type, COUNT(*) AS cnt FROM agent_task_steps{where} GROUP BY step_type",
                params,
            ).fetchall()
            grp_err = conn.execute(
                f"SELECT step_type, COUNT(*) AS cnt FROM agent_task_steps{where} AND ok=0 GROUP BY step_type ORDER BY cnt DESC, step_type ASC LIMIT ?",
                [*params, max(1, min(int(error_top_n), 20))],
            ).fetchall()
            grp_succ = conn.execute(
                f"SELECT step_type, COUNT(*) AS cnt FROM agent_task_steps{where} AND ok=1 GROUP BY step_type ORDER BY cnt DESC, step_type ASC LIMIT ?",
                [*params, max(1, min(int(success_top_n), 20))],
            ).fetchall()

            b = dict(base) if base is not None else {}
            total = int(b["total"]) if "total" in b else 0
            p95_latency_ms = 0
            if total > 0:
                p95_idx = int((total - 1) * 0.95)
                p95_row = conn.execute(
                    f"SELECT latency_ms FROM agent_task_steps{where} ORDER BY latency_ms ASC LIMIT 1 OFFSET ?",
                    [*params, p95_idx],
                ).fetchone()
                if p95_row is not None:
                    p95_latency_ms = int(dict(p95_row).get("latency_ms") or 0)

        by_type: dict[str, int] = {}
        for r in grp:
            d = dict(r)
            by_type[str(d.get("step_type") or "UNKNOWN")] = int(d.get("cnt") or 0)
        error_top: list[dict[str, Any]] = []
        for r in grp_err:
            d = dict(r)
            error_top.append({"step_type": str(d.get("step_type") or "UNKNOWN"), "count": int(d.get("cnt") or 0)})
        success_top: list[dict[str, Any]] = []
        for r in grp_succ:
            d = dict(r)
            success_top.append({"step_type": str(d.get("step_type") or "UNKNOWN"), "count": int(d.get("cnt") or 0)})
        success_count = int(b.get("success_count") or 0)
        failed_count = int(b.get("failed_count") or 0)
        success_rate = (round((success_count * 100.0) / total, 2) if total > 0 else 0.0)
        failure_rate = (round((failed_count * 100.0) / total, 2) if total > 0 else 0.0)
        if total <= 0:
            top_issue_summary = "no step data"
        elif failed_count <= 0:
            top_issue_summary = f"healthy run: {success_rate:.2f}% success ({success_count}/{total})"
        else:
            first = (error_top[0] if error_top else {"step_type": "UNKNOWN", "count": failed_count})
            st = str(first.get("step_type") or "UNKNOWN")
            cnt = int(first.get("count") or 0)
            top_issue_summary = f"top failure: {st} x{cnt} (failure_rate={failure_rate:.2f}%)"

        p95_v = max(1, int(p95_threshold_ms))
        penalty_step = max(1, int(penalty_per_100ms))
        penalty_cap_v = max(0, int(penalty_cap))
        green_min = max(0, min(100, int(green_min_score)))
        yellow_min = max(0, min(100, int(yellow_min_score)))
        yellow_fail = max(0.0, float(yellow_max_failure_rate))
        empty_score_v = max(0, min(100, int(empty_score)))

        latency_penalty = 0
        if p95_latency_ms > p95_v:
            latency_penalty = min(penalty_cap_v, int((p95_latency_ms - p95_v) / 100) * penalty_step)

        if total <= 0:
            health_score = empty_score_v
            health_level = "yellow"
        else:
            health_score = max(0, min(100, int(round(success_rate)) - latency_penalty))
            if failed_count <= 0 and health_score >= green_min:
                health_level = "green"
            elif health_score >= yellow_min or failure_rate <= yellow_fail:
                health_level = "yellow"
            else:
                health_level = "red"

        return {
            "total": total,
            "success_count": success_count,
            "failed_count": failed_count,
            "success_rate": success_rate,
            "failure_rate": failure_rate,
            "health_level": health_level,
            "health_score": health_score,
            "avg_latency_ms": int(float(b.get("avg_latency_ms") or 0)),
            "min_latency_ms": int(float(b.get("min_latency_ms") or 0)),
            "max_latency_ms": int(float(b.get("max_latency_ms") or 0)),
            "p95_latency_ms": p95_latency_ms,
            "by_step_type": by_type,
            "error_top": error_top,
            "success_top": success_top,
            "top_issue_summary": top_issue_summary,
        }

    def insert_group_task(self, task: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO group_tasks(
                  task_id, goal, status, phase, owner_id,
                  request_id, trace_id,
                  stop_reason, error_code,
                  created_at_ms, updated_at_ms, finished_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                  status=excluded.status,
                  phase=excluded.phase,
                  stop_reason=excluded.stop_reason,
                  error_code=excluded.error_code,
                  updated_at_ms=excluded.updated_at_ms,
                  finished_at_ms=excluded.finished_at_ms
                """,
                (
                    str(task.get("task_id") or ""),
                    str(task.get("goal") or ""),
                    str(task.get("status") or "created"),
                    str(task.get("phase") or "planning"),
                    str(task.get("owner_id") or "owner"),
                    str(task.get("request_id") or ""),
                    str(task.get("trace_id") or ""),
                    task.get("stop_reason"),
                    task.get("error_code"),
                    int(task.get("created_at_ms") or now_ms()),
                    int(task.get("updated_at_ms") or now_ms()),
                    (int(task.get("finished_at_ms")) if task.get("finished_at_ms") is not None else None),
                ),
            )
            conn.commit()

    def get_group_task(self, task_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM group_tasks WHERE task_id=?", (task_id,)).fetchone()
        return (dict(row) if row is not None else None)

    def list_group_tasks(self, limit: int = 50, status: str | None = None, phase: str | None = None, owner_id: str | None = None, cursor_updated_at_ms: int | None = None) -> list[dict[str, Any]]:
        limit_v = max(1, min(int(limit), 200))
        sql = "SELECT * FROM group_tasks"
        wheres: list[str] = []
        params: list[Any] = []
        if status is not None and str(status).strip():
            wheres.append("status=?")
            params.append(str(status).strip())
        if phase is not None and str(phase).strip():
            wheres.append("phase=?")
            params.append(str(phase).strip())
        if owner_id is not None and str(owner_id).strip():
            wheres.append("owner_id=?")
            params.append(str(owner_id).strip())
        if cursor_updated_at_ms is not None:
            wheres.append("updated_at_ms < ?")
            params.append(int(cursor_updated_at_ms))
        if wheres:
            sql += " WHERE " + " AND ".join(wheres)
        sql += " ORDER BY updated_at_ms DESC, task_id DESC LIMIT ?"
        params.append(limit_v)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def aggregate_group_tasks(self, owner_id: str | None = None) -> dict[str, Any]:
        owner_v = (str(owner_id).strip() if owner_id is not None else None)
        where = ""
        params: list[Any] = []
        if owner_v:
            where = " WHERE owner_id=?"
            params.append(owner_v)
        with self._connect() as conn:
            total_row = conn.execute(f"SELECT COUNT(1) AS c FROM group_tasks{where}", params).fetchone()
            by_status_rows = conn.execute(f"SELECT status, COUNT(1) AS c FROM group_tasks{where} GROUP BY status", params).fetchall()
            by_phase_rows = conn.execute(f"SELECT phase, COUNT(1) AS c FROM group_tasks{where} GROUP BY phase", params).fetchall()
            by_owner_rows = conn.execute(f"SELECT owner_id, COUNT(1) AS c FROM group_tasks{where} GROUP BY owner_id ORDER BY c DESC LIMIT 20", params).fetchall()
        total = int(total_row["c"] or 0) if total_row is not None else 0
        by_status = {str(r["status"] or "unknown"): int(r["c"] or 0) for r in by_status_rows}
        by_phase = {str(r["phase"] or "unknown"): int(r["c"] or 0) for r in by_phase_rows}
        by_owner = [{"owner_id": str(r["owner_id"] or ""), "count": int(r["c"] or 0)} for r in by_owner_rows]
        return {"total": total, "by_status": by_status, "by_phase": by_phase, "by_owner": by_owner}

    def insert_group_task_round(self, task_id: str, round_no: int, phase: str, actor_role: str, note: str | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO group_task_rounds(task_id, round_no, phase, actor_role, note, created_at_ms)
                VALUES(?,?,?,?,?,?)
                """,
                (task_id, int(round_no), str(phase), str(actor_role), (None if note is None else str(note)), now_ms()),
            )
            conn.commit()

    def list_group_task_rounds(self, task_id: str, limit: int = 500, cursor: int | None = None, order: str = "asc") -> list[dict[str, Any]]:
        limit_v = max(1, min(int(limit), 2000))
        order_v = "DESC" if str(order).strip().lower() == "desc" else "ASC"
        sql = "SELECT id AS _cursor, task_id, round_no, phase, actor_role, note, created_at_ms FROM group_task_rounds WHERE task_id=?"
        params: list[Any] = [task_id]
        if cursor is not None:
            sql += (" AND id>?" if order_v == "ASC" else " AND id<?")
            params.append(int(cursor))
        sql += f" ORDER BY id {order_v} LIMIT ?"
        params.append(limit_v)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def insert_group_agent_message(self, task_id: str, round_no: int, agent_id: str, role: str, content: str, refs_json: str = "{}") -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO group_agent_messages(task_id, round_no, agent_id, role, content, refs_json, created_at_ms)
                VALUES(?,?,?,?,?,?,?)
                """,
                (task_id, int(round_no), agent_id, role, content, refs_json, now_ms()),
            )
            conn.commit()

    def list_group_agent_messages(self, task_id: str, limit: int = 500, cursor: int | None = None, order: str = "asc", role: str | None = None) -> list[dict[str, Any]]:
        limit_v = max(1, min(int(limit), 2000))
        order_v = "DESC" if str(order).strip().lower() == "desc" else "ASC"
        sql = "SELECT id AS _cursor, round_no, agent_id, role, content, refs_json, created_at_ms FROM group_agent_messages WHERE task_id=?"
        params: list[Any] = [task_id]
        if role is not None and str(role).strip():
            sql += " AND role=?"
            params.append(str(role).strip())
        if cursor is not None:
            sql += (" AND id>?" if order_v == "ASC" else " AND id<?")
            params.append(int(cursor))
        sql += f" ORDER BY id {order_v} LIMIT ?"
        params.append(limit_v)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def upsert_group_artifacts(self, task_id: str, artifacts: dict[str, Any]) -> None:
        changed_files = artifacts.get("changed_files") if isinstance(artifacts, dict) else []
        test_result = artifacts.get("test_result") if isinstance(artifacts, dict) else None
        summary = artifacts.get("summary") if isinstance(artifacts, dict) else None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO group_artifacts(task_id, changed_files_json, test_result, summary, updated_at_ms)
                VALUES(?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                  changed_files_json=excluded.changed_files_json,
                  test_result=excluded.test_result,
                  summary=excluded.summary,
                  updated_at_ms=excluded.updated_at_ms
                """,
                (task_id, json.dumps(changed_files or [], ensure_ascii=False), test_result, summary, now_ms()),
            )
            conn.commit()

    def get_group_artifacts(self, task_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT task_id, changed_files_json, test_result, summary FROM group_artifacts WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            return None
        d = dict(row)
        return {
            "changed_files": json.loads(d.get("changed_files_json") or "[]"),
            "test_result": d.get("test_result"),
            "summary": d.get("summary") or "",
        }

    def insert_group_decision(self, task_id: str, decision_type: str, winner_plan_id: str | None, reason: str | None, approved_by: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO group_decisions(task_id, decision_type, winner_plan_id, reason, approved_by, created_at_ms)
                VALUES(?,?,?,?,?,?)
                """,
                (task_id, decision_type, winner_plan_id, reason, approved_by, now_ms()),
            )
            conn.commit()

    def list_group_decisions(self, task_id: str, limit: int = 100, cursor: int | None = None, order: str = "asc") -> list[dict[str, Any]]:
        limit_v = max(1, min(int(limit), 2000))
        order_v = "DESC" if str(order).strip().lower() == "desc" else "ASC"
        sql = "SELECT id AS _cursor, task_id, decision_type, winner_plan_id, reason, approved_by, created_at_ms FROM group_decisions WHERE task_id=?"
        params: list[Any] = [task_id]
        if cursor is not None:
            sql += (" AND id>?" if order_v == "ASC" else " AND id<?")
            params.append(int(cursor))
        sql += f" ORDER BY id {order_v} LIMIT ?"
        params.append(limit_v)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
