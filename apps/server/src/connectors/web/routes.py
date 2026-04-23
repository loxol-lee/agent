from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
import shlex
import subprocess
import threading
import time
import uuid
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Iterator

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from config import Settings
from connectors.web.auth import require_api_key
from connectors.web.group_audit_routes import (
    handle_group_task_audit,
    handle_group_task_audit_closure,
    handle_group_task_export,
    handle_group_task_trace_map,
    register_group_audit_routes,
)
from connectors.web.group_kpi_routes import (
    handle_group_task_kpi,
    handle_group_tasks_kpi_alerts,
    handle_group_tasks_kpi_batch,
    handle_group_tasks_kpi_batch_get,
    handle_group_tasks_kpi_distribution,
    handle_group_tasks_kpi_leaderboard,
    handle_group_tasks_kpi_overview,
    handle_group_tasks_kpi_owners,
    register_group_kpi_routes,
)
from connectors.web.group_prechecks_routes import (
    handle_group_task_approve_precheck,
    handle_group_task_artifacts_update_precheck,
    handle_group_task_can,
    handle_group_task_cancel_precheck,
    handle_group_task_execution_submit_precheck,
    handle_group_task_prechecks,
    handle_group_task_qa_review_precheck,
    handle_group_task_rerun_precheck,
    handle_group_task_thread_message_precheck,
    handle_group_tasks_can_batch,
    handle_group_tasks_prechecks_batch,
    register_group_prechecks_routes,
)
from connectors.web.group_tasks_routes import (
    get_group_artifacts_or_default,
    get_group_task_or_raise,
    handle_group_task_approve,
    handle_group_task_artifacts_update,
    handle_group_task_cancel,
    handle_group_task_execution_submit,
    handle_group_task_qa_review,
    handle_group_task_rerun,
    handle_group_task_thread_message,
    list_group_tasks_or_raise,
    register_group_tasks_routes,
)
from connectors.web.rate_limit import enforce_rate_limit
from core.contracts.chat import ChatRequest, StreamEvent
from core.contracts.observability import Tracer
from core.contracts.storage import Storage
from core.contracts.tools import (
    ERROR_TOOL_ARGS_INVALID,
    ERROR_TOOL_NOT_ALLOWED,
    ERROR_TOOL_NOT_FOUND,
    ERROR_TOOL_RUNTIME_ERROR,
    ERROR_TOOL_TIMEOUT,
)
from core.orchestrator.agent_runtime import run_chat_stream
from core.tool_runtime.executor import ToolExecutor


router = APIRouter(dependencies=[Depends(require_api_key), Depends(enforce_rate_limit)])

register_group_tasks_routes(router)
register_group_kpi_routes(router)
register_group_prechecks_routes(router)
register_group_audit_routes(router)


class AttachmentIn(BaseModel):
    file_name: str | None = Field(default=None, max_length=255)
    mime_type: str = Field(min_length=1, max_length=100)
    content_base64: str = Field(min_length=1, max_length=10_000_000)


class ChatIn(BaseModel):
    input_text: str = Field(min_length=1, max_length=20000)
    conversation_id: str | None = None
    user_id: str | None = None
    attachments: list[AttachmentIn] | None = None


def sse_bytes(event: StreamEvent) -> bytes:
    req_id = event.trace_id
    payload = dict(event.payload or {})
    payload.setdefault("request_id", req_id)

    if event.type == "done":
        payload.setdefault("stop_reason", "completed")
    elif event.type == "error":
        if not isinstance(payload.get("error_code"), str) or not payload.get("error_code"):
            stage = payload.get("stage")
            if stage == "connector":
                payload["error_code"] = "CONNECTOR_ERROR"
            elif stage == "model":
                payload["error_code"] = "MODEL_ERROR"
            else:
                payload["error_code"] = "ORCHESTRATOR_ERROR"

    data = json.dumps({"request_id": req_id, "trace_id": event.trace_id, "type": event.type, "payload": payload}, ensure_ascii=False)
    return f"data: {data}\n\n".encode("utf-8")


_ALLOWED_ATTACHMENT_MIME_TYPES: set[str] = {
    "text/plain",
    "text/markdown",
    "application/pdf",
    "application/json",
    "image/png",
    "image/jpeg",
    "image/webp",
}
_MAX_ATTACHMENTS = 5
_MAX_ATTACHMENT_BYTES = 5 * 1024 * 1024
_MAX_ATTACHMENTS_TOTAL_BYTES = 20 * 1024 * 1024
_SUMMARY_SIGNATURE_ALGO = "sha1-16"
_SUMMARY_FINGERPRINT_ALGO = "sha1-16-stable"
_DEFAULT_PROTECTED_FILE_PATTERNS = [
    ".env",
    ".env.",
    "id_rsa",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    ".pem",
    ".key",
    "token",
]


def _summary_key(prefix: str, payload: dict[str, Any]) -> str:
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}{digest}"


def _summary_payload(
    *,
    task_id: str,
    attempt: int | None,
    step_type: str | None,
    step_ok: bool | None,
    from_ts: int | None,
    to_ts: int | None,
    window_ms: int | None,
    config_version: str,
    salt: str,
    generated_at_ms: int | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    p: dict[str, Any] = {
        "task_id": task_id,
        "attempt": attempt,
        "step_type": step_type,
        "step_ok": step_ok,
        "from_ts": from_ts,
        "to_ts": to_ts,
        "window_ms": window_ms,
        "config_version": config_version,
        "salt": salt,
    }
    if generated_at_ms is not None:
        p["summary_generated_at_ms"] = int(generated_at_ms)
    if isinstance(extra, dict) and extra:
        p.update(extra)
    return p


def _validate_attachments(attachments: list[AttachmentIn] | None) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    if not attachments:
        return [], None
    if len(attachments) > _MAX_ATTACHMENTS:
        return [], {"message": "附件数量超过上限", "recoverable": True, "stage": "connector", "error_code": "ATTACHMENTS_TOO_MANY"}

    out: list[dict[str, Any]] = []
    total = 0
    for i, a in enumerate(attachments):
        mt = (a.mime_type or "").strip().lower()
        if mt not in _ALLOWED_ATTACHMENT_MIME_TYPES:
            return [], {"message": "包含不支持的附件类型", "recoverable": True, "stage": "connector", "error_code": "ATTACHMENT_TYPE_NOT_ALLOWED", "attachment_index": i}
        try:
            raw = base64.b64decode(a.content_base64, validate=True)
        except (binascii.Error, ValueError):
            return [], {"message": "附件编码不合法", "recoverable": True, "stage": "connector", "error_code": "ATTACHMENT_BASE64_INVALID", "attachment_index": i}

        size = len(raw)
        if size <= 0:
            return [], {"message": "附件内容为空", "recoverable": True, "stage": "connector", "error_code": "ATTACHMENT_EMPTY", "attachment_index": i}
        if size > _MAX_ATTACHMENT_BYTES:
            return [], {"message": "单个附件大小超过上限", "recoverable": True, "stage": "connector", "error_code": "ATTACHMENT_TOO_LARGE", "attachment_index": i}

        total += size
        if total > _MAX_ATTACHMENTS_TOTAL_BYTES:
            return [], {"message": "附件总大小超过上限", "recoverable": True, "stage": "connector", "error_code": "ATTACHMENTS_TOTAL_TOO_LARGE"}

        out.append({"file_name": a.file_name, "mime_type": mt, "size_bytes": size})

    return out, None


def _norm_usage(obj: object) -> dict[str, int] | None:
    if not isinstance(obj, dict):
        return None
    out: dict[str, int] = {}
    for k in ("prompt_tokens", "completion_tokens", "total_tokens"):
        v = obj.get(k)
        if isinstance(v, int) and v >= 0:
            out[k] = v
    return out or None


def _usage_from_payload_json(payload_json: str) -> dict[str, int] | None:
    try:
        obj = json.loads(payload_json)
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    return _norm_usage(obj.get("usage")) or _norm_usage(obj)


def _sum_usage_from_replay(events: list[dict]) -> dict[str, int]:
    done_usage: dict[str, int] | None = None
    sum_usage: dict[str, int] = {}
    for ev in events:
        t = ev.get("type")
        pj = ev.get("payload_json")
        if not isinstance(pj, str):
            continue
        u = _usage_from_payload_json(pj)
        if not u:
            continue
        if t == "model_done":
            done_usage = u
            continue
        if t != "model_response":
            continue
        for k, v in u.items():
            sum_usage[k] = sum_usage.get(k, 0) + int(v)
    return done_usage or sum_usage


def _attachments_from_payload_json(payload_json: str) -> list[dict]:
    try:
        obj = json.loads(payload_json)
    except Exception:
        return []
    if not isinstance(obj, dict):
        return []
    atts = obj.get("attachments")
    if not isinstance(atts, list):
        return []

    out: list[dict] = []
    for a in atts:
        if not isinstance(a, dict):
            continue
        row: dict[str, Any] = {}
        n = a.get("file_name")
        t = a.get("mime_type")
        s = a.get("size_bytes")
        if isinstance(n, str):
            row["file_name"] = n
        if isinstance(t, str):
            row["mime_type"] = t
        if isinstance(s, int) and s >= 0:
            row["size_bytes"] = s
        if row:
            out.append(row)
    return out


def _attachments_from_replay(events: list[dict]) -> list[dict]:
    for ev in events:
        if ev.get("type") != "request_meta":
            continue
        pj = ev.get("payload_json")
        if not isinstance(pj, str):
            continue
        items = _attachments_from_payload_json(pj)
        if items:
            return items
    return []


_ATTACHMENT_REF_RE = re.compile(r"\[附件#(\d+)\]")
_ALLOWED_STOP_REASONS = {
    "completed",
    "max_steps",
    "tool_call_limit",
    "time_budget",
    "max_tokens",
    "model_error",
    "tool_error",
    "user_cancelled",
}
_ALLOWED_ERROR_CODES = {
    ERROR_TOOL_NOT_FOUND,
    ERROR_TOOL_NOT_ALLOWED,
    ERROR_TOOL_ARGS_INVALID,
    ERROR_TOOL_TIMEOUT,
    ERROR_TOOL_RUNTIME_ERROR,
    "CONNECTOR_ERROR",
    "MODEL_ERROR",
    "ORCHESTRATOR_ERROR",
}


def _attachment_refs_from_messages(messages: list[dict], *, trace_id: str, attachment_count: int) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for m in messages:
        if not isinstance(m, dict):
            continue
        if m.get("trace_id") != trace_id:
            continue
        if m.get("role") != "assistant":
            continue
        content = m.get("content")
        if not isinstance(content, str) or not content:
            continue
        for g in _ATTACHMENT_REF_RE.findall(content):
            try:
                idx = int(g)
            except Exception:
                continue
            if idx <= 0:
                continue
            if attachment_count > 0 and idx > attachment_count:
                continue
            k = str(idx)
            counts[k] = counts.get(k, 0) + 1

    ids = sorted(int(k) for k in counts.keys())
    total_mentions = sum(int(v) for v in counts.values())
    return {"ids": ids, "counts": counts, "total_mentions": total_mentions}


def _inc_count(counter: dict[str, int], key: object) -> None:
    if not isinstance(key, str):
        return
    k = key.strip()
    if not k:
        return
    counter[k] = counter.get(k, 0) + 1


def _stop_reason_counts(traces_by_id: dict[str, dict | None]) -> dict[str, int]:
    out: dict[str, int] = {}
    for t in traces_by_id.values():
        if not isinstance(t, dict):
            continue
        _inc_count(out, t.get("stop_reason"))
    return out


def _tool_error_code_counts(tool_runs_by_trace: dict[str, list[dict]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for runs in tool_runs_by_trace.values():
        if not isinstance(runs, list):
            continue
        for r in runs:
            if not isinstance(r, dict):
                continue
            if bool(r.get("ok")):
                continue
            _inc_count(out, r.get("error_code"))
    return out


def _sum_counter(counter: dict[str, int]) -> int:
    total = 0
    for v in counter.values():
        total += int(v)
    return total


def _quality_signals(*, trace_count: int, stop_reason_counts: dict[str, int], tool_error_code_counts: dict[str, int], attachments_total: dict[str, int]) -> dict[str, Any]:
    timeout_count = int(stop_reason_counts.get("time_budget", 0))
    tool_error_total = _sum_counter(tool_error_code_counts)
    top_tool_error_code = ""
    top_tool_error_count = 0
    for code, cnt in tool_error_code_counts.items():
        c = int(cnt)
        if c > top_tool_error_count:
            top_tool_error_code = code
            top_tool_error_count = c

    attachment_total_bytes = int(attachments_total.get("total_bytes", 0))

    metrics = {
        "trace_count": int(trace_count),
        "timeout_count": timeout_count,
        "timeout_ratio": (timeout_count / trace_count) if trace_count > 0 else 0.0,
        "tool_error_total": tool_error_total,
        "tool_error_ratio": (tool_error_total / trace_count) if trace_count > 0 else 0.0,
        "top_tool_error_code": top_tool_error_code,
        "top_tool_error_ratio": (top_tool_error_count / tool_error_total) if tool_error_total > 0 else 0.0,
        "attachment_total_bytes": attachment_total_bytes,
    }

    thresholds = {
        "timeout_ratio": 0.3,
        "tool_error_ratio": 0.3,
        "tool_error_concentrated_ratio": 0.7,
        "tool_error_concentrated_min_count": 5,
        "attachment_total_bytes": 10 * 1024 * 1024,
    }

    hits: list[str] = []
    if metrics["timeout_ratio"] >= thresholds["timeout_ratio"]:
        hits.append("TIME_BUDGET_HIGH")
    if metrics["tool_error_ratio"] >= thresholds["tool_error_ratio"]:
        hits.append("TOOL_ERROR_HIGH")
    if (
        metrics["tool_error_total"] >= thresholds["tool_error_concentrated_min_count"]
        and metrics["top_tool_error_ratio"] >= thresholds["tool_error_concentrated_ratio"]
        and metrics["top_tool_error_code"]
    ):
        hits.append("TOOL_ERROR_CONCENTRATED")
    if metrics["attachment_total_bytes"] >= thresholds["attachment_total_bytes"]:
        hits.append("ATTACHMENT_BYTES_HIGH")

    return {"status": ("warn" if hits else "ok"), "hits": hits, "thresholds": thresholds, "metrics": metrics}


def _inspect_summary(
    *,
    conversation_id: str,
    trace_count: int,
    usage_total: dict[str, int],
    attachments_total: dict[str, int],
    attachment_refs_total: dict[str, int],
    quality_signals: dict[str, Any],
) -> dict[str, Any]:
    p = int(usage_total.get("prompt_tokens", 0))
    c = int(usage_total.get("completion_tokens", 0))
    t = int(usage_total.get("total_tokens", p + c))
    ac = int(attachments_total.get("count", 0))
    ab = int(attachments_total.get("total_bytes", 0))
    art = int(attachment_refs_total.get("traces_with_refs", 0))
    arm = int(attachment_refs_total.get("total_mentions", 0))
    status = str(quality_signals.get("status", "ok")) if isinstance(quality_signals, dict) else "ok"
    hits = quality_signals.get("hits", []) if isinstance(quality_signals, dict) else []
    hits_list = [str(x) for x in hits] if isinstance(hits, list) else []
    risk = "|".join(hits_list) if status == "warn" and hits_list else "OK"
    line = f"inspect: cid={conversation_id} traces={trace_count} usage={p}/{c}/{t} att={ac}/{ab} att_refs={art}/{arm} risk={risk}"
    return {
        "conversation_id": conversation_id,
        "trace_count": trace_count,
        "usage": {"prompt_tokens": p, "completion_tokens": c, "total_tokens": t},
        "attachments": {"count": ac, "total_bytes": ab},
        "attachment_refs": {"traces_with_refs": art, "total_mentions": arm},
        "risk": {"status": status, "hits": hits_list},
        "line": line,
    }


def _payload_obj(payload_json: object) -> dict[str, Any]:
    if not isinstance(payload_json, str):
        return {}
    try:
        obj = json.loads(payload_json)
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _tool_run_content_obj(tool_run: dict) -> dict[str, Any]:
    p = _payload_obj(tool_run.get("result_json"))
    c = p.get("content")
    return c if isinstance(c, dict) else {}


def _attachment_policy_violations(replay_events: list[dict]) -> int:
    bad = 0
    for ev in replay_events:
        if ev.get("type") != "request_meta":
            continue
        p = _payload_obj(ev.get("payload_json"))
        atts = p.get("attachments")
        if not isinstance(atts, list):
            continue
        if len(atts) > _MAX_ATTACHMENTS:
            bad += 1
            continue
        total = 0
        for a in atts:
            if not isinstance(a, dict):
                bad += 1
                continue
            mt = str(a.get("mime_type") or "").strip().lower()
            s = a.get("size_bytes")
            if mt not in _ALLOWED_ATTACHMENT_MIME_TYPES:
                bad += 1
            if not isinstance(s, int) or s <= 0 or s > _MAX_ATTACHMENT_BYTES:
                bad += 1
            if isinstance(s, int) and s > 0:
                total += s
        if total > _MAX_ATTACHMENTS_TOTAL_BYTES:
            bad += 1
    return bad


def _gate_level(status: str) -> int:
    if status == "fail":
        return 2
    if status == "warn":
        return 1
    return 0


def _quality_gate_ci_view(quality_gate: dict[str, Any], quality_gate_mode: str = "off") -> dict[str, Any]:
    status = str(quality_gate.get("status") or "ok")
    rules = quality_gate.get("rules")
    if not isinstance(rules, list):
        rules = []
    blocking_rule_ids: list[str] = []
    warn_rule_ids: list[str] = []
    for r in rules:
        if not isinstance(r, dict):
            continue
        rid = r.get("id")
        if not isinstance(rid, str) or not rid:
            continue
        rs = str(r.get("status") or "ok")
        if rs == "fail":
            blocking_rule_ids.append(rid)
        elif rs == "warn":
            warn_rule_ids.append(rid)
    min_level = 0
    if quality_gate_mode == "warn":
        min_level = 1
    elif quality_gate_mode == "fail":
        min_level = 2
    blocked = min_level > 0 and _gate_level(status) >= min_level
    passed = not blocked
    trace_audit_index = quality_gate.get("trace_audit_index")
    trace_fail_count = 0
    if isinstance(trace_audit_index, dict):
        trace_fail_count = sum(1 for v in trace_audit_index.values() if isinstance(v, dict) and str(v.get("status") or "ok") == "fail")
    top = quality_gate.get("trace_audit_top_issues")
    top_parts: list[str] = []
    if isinstance(top, list):
        for row in top[:3]:
            if not isinstance(row, dict):
                continue
            issue = row.get("issue")
            count = row.get("count")
            if isinstance(issue, str) and issue and isinstance(count, int):
                top_parts.append(f"{issue}:{count}")
    top_issue_summary = ",".join(top_parts) if top_parts else "none"
    contract_catalog = quality_gate.get("contract_catalog")
    contract_version = ""
    if isinstance(contract_catalog, dict):
        v = contract_catalog.get("contract_version")
        if isinstance(v, str) and v:
            contract_version = v
    contract_fingerprint = quality_gate.get("contract_fingerprint")
    if not isinstance(contract_fingerprint, str):
        contract_fingerprint = ""
    expected_contract_fingerprint = quality_gate.get("expected_contract_fingerprint")
    if not isinstance(expected_contract_fingerprint, str):
        expected_contract_fingerprint = ""
    contract_drift = bool(quality_gate.get("contract_drift"))
    contract_match = not contract_drift
    summary_line = f"quality_gate: mode={quality_gate_mode} status={status} pass={str(passed).lower()} fail={len(blocking_rule_ids)} warn={len(warn_rule_ids)} trace_fail={trace_fail_count} top={top_issue_summary} contract={contract_version} drift={str(contract_drift).lower()}"
    return {
        "mode": quality_gate_mode,
        "status": status,
        "pass": passed,
        "blocked": blocked,
        "blocking_rule_ids": blocking_rule_ids,
        "warn_rule_ids": warn_rule_ids,
        "trace_fail_count": trace_fail_count,
        "top_issue_summary": top_issue_summary,
        "contract_version": contract_version,
        "contract_fingerprint": contract_fingerprint,
        "expected_contract_fingerprint": expected_contract_fingerprint,
        "contract_drift": contract_drift,
        "contract_match": contract_match,
        "summary_line": summary_line,
    }


def _trace_audit_index(
    *,
    trace_ids: list[str],
    traces_by_id: dict[str, dict | None],
    replay_events_by_trace: dict[str, list[dict]],
    tool_runs_by_trace: dict[str, list[dict]],
    msg_trace_ids: set[str],
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for tid in trace_ids:
        issues: list[str] = []
        trace = traces_by_id.get(tid)
        replay_events = replay_events_by_trace.get(tid) or []
        tool_runs = tool_runs_by_trace.get(tid) or []
        if not isinstance(trace, dict):
            issues.append("trace_missing_row")
        stop_reason = str(trace.get("stop_reason") or "") if isinstance(trace, dict) else ""
        if not stop_reason:
            issues.append("trace_stop_reason_missing")
        elif stop_reason not in _ALLOWED_STOP_REASONS:
            issues.append("trace_stop_reason_unknown")
        if tid not in msg_trace_ids:
            issues.append("trace_without_message")
        if not replay_events:
            issues.append("trace_without_replay")
        has_done = False
        done_match = False
        replay_call_ids: set[str] = set()
        replay_tool_result_call_ids: set[str] = set()
        for ev in replay_events:
            p = _payload_obj(ev.get("payload_json"))
            req_id = p.get("request_id")
            if isinstance(req_id, str) and req_id and req_id != tid and "request_id_mismatch" not in issues:
                issues.append("request_id_mismatch")
            if ev.get("type") == "model_done":
                has_done = True
                if isinstance(p.get("stop_reason"), str) and p.get("stop_reason") == stop_reason and stop_reason:
                    done_match = True
            if ev.get("type") == "tool_result":
                cid = p.get("call_id")
                if isinstance(cid, str) and cid:
                    replay_call_ids.add(cid)
                    replay_tool_result_call_ids.add(cid)
        if not has_done:
            issues.append("trace_without_done_event")
        elif stop_reason and not done_match:
            issues.append("done_stop_reason_mismatch")
        tool_run_call_ids: set[str] = set()
        for r in tool_runs:
            cid = r.get("call_id")
            if isinstance(cid, str) and cid:
                tool_run_call_ids.add(cid)
                if cid not in replay_call_ids and "tool_runs_unmapped" not in issues:
                    issues.append("tool_runs_unmapped")
        if any((cid not in tool_run_call_ids) for cid in replay_tool_result_call_ids):
            issues.append("replay_tool_results_unmapped")
        out[tid] = {"status": ("fail" if issues else "ok"), "issues": sorted(set(issues))}
    return out


def _trace_issue_top_counts(trace_audit_index: dict[str, dict[str, Any]], top_n: int = 8) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in trace_audit_index.values():
        if not isinstance(row, dict):
            continue
        issues = row.get("issues")
        if not isinstance(issues, list):
            continue
        for it in issues:
            if not isinstance(it, str) or not it:
                continue
            counts[it] = counts.get(it, 0) + 1
    items = [{"issue": k, "count": int(v)} for k, v in counts.items()]
    items.sort(key=lambda x: (-int(x.get("count", 0)), str(x.get("issue", ""))))
    return items[: max(0, int(top_n))]


def _contract_fingerprint(contract_catalog: dict[str, Any]) -> str:
    raw = json.dumps(contract_catalog, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _normalize_fingerprint(v: object) -> str:
    if not isinstance(v, str):
        return ""
    return v.strip().lower()


@router.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    html = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
  <meta http-equiv="Pragma" content="no-cache" />
  <meta http-equiv="Expires" content="0" />
  <title>agunt v0</title>
</head>
<body>
<style>
  :root { --bg:#343541; --panel:#202123; --text:#ececf1; --muted:#9aa1a9; --bubble:#444654; --bubble2:#2b2d31; --border:#2f3037; }
  html,body{height:100%;}
  body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial;}
  #app{display:flex;height:100vh;}
  #sidebar{width:280px;background:var(--panel);border-right:1px solid var(--border);display:flex;flex-direction:column;}
  #sideTop{padding:12px;display:flex;gap:8px;align-items:center;}
  #newChat{flex:1;background:#2a2b32;border:1px solid var(--border);color:var(--text);padding:10px 12px;border-radius:8px;cursor:pointer;}
  #chatList{flex:1;overflow:auto;padding:8px;}
  .chatItem{padding:10px 10px;border-radius:8px;cursor:pointer;color:var(--text);border:1px solid transparent;}
  .chatItem:hover{background:#2a2b32;}
  .chatItem.active{background:#2a2b32;border-color:var(--border);}
  .chatTitle{font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
  .chatMeta{font-size:12px;color:var(--muted);margin-top:3px;}
  #main{flex:1;display:flex;flex-direction:column;min-width:0;}
  #header{padding:12px 16px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;gap:12px;align-items:center;}
  #cidText{color:var(--muted);font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
  #messages{flex:1;overflow:auto;padding:18px 0;}
  .msgRow{max-width:900px;margin:0 auto;padding:10px 16px;}
  .msgRole{font-size:12px;color:var(--muted);margin-bottom:6px;display:flex;gap:10px;align-items:center;}
  .msgMeta{font-size:12px;color:var(--muted);}
  .msgActions{margin-left:auto;display:flex;gap:8px;}
  .msgActions button{background:#2a2b32;border:1px solid var(--border);color:var(--text);padding:4px 8px;border-radius:8px;cursor:pointer;font-size:12px;}
  .msgActions a{color:var(--muted);text-decoration:none;font-size:12px;}
  .msgActions a:hover{color:var(--text);}
  .msgBubble{background:var(--bubble);border:1px solid var(--border);border-radius:10px;padding:12px 12px;white-space:pre-wrap;word-wrap:break-word;}
  .msgRow.user .msgBubble{background:var(--bubble2);}
  #composerWrap{border-top:1px solid var(--border);padding:12px 16px;}
  #composer{max-width:900px;margin:0 auto;display:flex;gap:10px;align-items:flex-end;}
  #q{flex:1;resize:none;min-height:44px;max-height:200px;background:#40414f;color:var(--text);border:1px solid var(--border);border-radius:10px;padding:12px;font-size:14px;outline:none;}
  #send{background:#19c37d;color:#062c1a;border:0;border-radius:10px;padding:10px 14px;font-weight:600;cursor:pointer;}
  #send:disabled{opacity:.6;cursor:not-allowed;}
  #tip{max-width:900px;margin:8px auto 0;color:var(--muted);font-size:12px;}
</style>
<div id="app">
  <aside id="sidebar">
    <div id="sideTop">
      <button id="newChat" type="button">新建会话</button>
    </div>
    <div id="chatList"></div>
  </aside>
  <main id="main">
    <div id="header">
      <div style="display:flex; gap:10px; align-items:center; min-width:0;">
        <div>agunt</div>
        <button id="renameChat" type="button" style="background:#2a2b32;border:1px solid var(--border);color:var(--text);padding:6px 10px;border-radius:8px;cursor:pointer;">重命名</button>
        <button id="exportChat" type="button" style="background:#2a2b32;border:1px solid var(--border);color:var(--text);padding:6px 10px;border-radius:8px;cursor:pointer;">导出</button>
        <button id="deleteChat" type="button" style="background:#2a2b32;border:1px solid var(--border);color:#ffb4b4;padding:6px 10px;border-radius:8px;cursor:pointer;">归档</button>
        <button id="purgeChat" type="button" style="background:#402226;border:1px solid #6a2a33;color:#ffb4b4;padding:6px 10px;border-radius:8px;cursor:pointer;">彻底删当前</button>
        <button id="purgeAllChats" type="button" style="background:#5a1e26;border:1px solid #8b2b38;color:#ffd7d7;padding:6px 10px;border-radius:8px;cursor:pointer;">清空全部</button>
      </div>
      <div id="cidText"></div>
    </div>
    <div id="messages"><div class="msgRow"><div class="msgBubble">JS 启动中…</div></div></div>
    <div id="composerWrap">
      <form id="composer" method="GET" action="/chat/plain">
        <textarea id="q" name="input_text" placeholder="输入内容，回车发送（Shift+Enter 换行）"></textarea>
        <input type="hidden" id="cid" name="conversation_id" value="" />
        <button id="send" type="button">发送</button>
      </form>
    </div>
  </main>
</div>
<script>
(function(){
  if (window.__AGUNT_APP_READY == null) window.__AGUNT_APP_READY = false;
  window.__AGUNT_BOOT_FAILED = false;

  function escHtml(s){
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function setMessagesHtml(html){
    var el = document.getElementById("messages");
    if (!el) return;
    el.innerHTML = html;
  }

  function showBootError(msg){
    window.__AGUNT_BOOT_FAILED = true;
    setMessagesHtml('<div class="msgRow"><div class="msgBubble">前端脚本执行失败：<br/>' + escHtml(msg) + '<br/><br/>请把这段错误原样发我。</div></div>');
  }

  window.__AGUNT_BOOT_ERROR = showBootError;

  setMessagesHtml('<div class="msgRow"><div class="msgBubble">JS 启动中…</div></div>');

  window.addEventListener('error', function(e){
    try {
      var msg = (e && e.message) ? e.message : 'unknown_error';
      var file = (e && e.filename) ? e.filename : '';
      var pos = (e && (e.lineno || e.colno)) ? ('@' + (e.lineno||0) + ':' + (e.colno||0)) : '';
      showBootError(msg + (file ? ('\\n' + file + pos) : ''));
    } catch(_) {}
  }, true);

  window.addEventListener('unhandledrejection', function(e){
    try {
      var r = e && e.reason ? e.reason : 'unhandled_rejection';
      showBootError(r && r.stack ? r.stack : String(r));
    } catch(_) {}
  });

  setTimeout(function(){
    if (window.__AGUNT_APP_READY) return;
    if (window.__AGUNT_BOOT_FAILED) return;
    var s = document.createElement("script");
    s.src = "/static/app.js?v=v0-ui-5&cb=" + Date.now();
    s.onerror = function(){ showBootError("load /static/app.js network error (retry)"); };
    document.head.appendChild(s);
  }, 600);
})();
</script>
<script src="/static/app.js?v=v0-ui-5" defer onerror="window.__AGUNT_BOOT_ERROR && window.__AGUNT_BOOT_ERROR('load /static/app.js network error')"></script>
</body>
</html>"""

    return HTMLResponse(
        content=html,
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@router.get("/favicon.ico")
def favicon() -> HTMLResponse:
    return HTMLResponse(status_code=204)


@router.get("/static/app.js")
def static_app_js() -> Response:
    js = """
(function(){
  window.__AGUNT_APP_READY = true;
  var BUILD = "v0-ui-6";

  var chatList = document.getElementById("chatList");
  var messages = document.getElementById("messages");
  if (messages) messages.innerHTML = '<div class="msgRow"><div class="msgBubble">加载中…</div></div>';
  var composer = document.getElementById("composer");
  var q = document.getElementById("q");
  var send = document.getElementById("send");
  var newChat = document.getElementById("newChat");
  var renameChat = document.getElementById("renameChat");
  var exportChat = document.getElementById("exportChat");
  var deleteChat = document.getElementById("deleteChat");
  var purgeChat = document.getElementById("purgeChat");
  var purgeAllChats = document.getElementById("purgeAllChats");
  var cidEl = document.getElementById("cid");
  var cidText = document.getElementById("cidText");

  function newId(){ return (Date.now().toString(16) + Math.random().toString(16).slice(2)); }
  function esc(s){ return (s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
  function scrollBottom(){ if (messages) messages.scrollTop = messages.scrollHeight; }

  var cid = null;
  var lastUsage = null;
  var lastRequestId = "";
  var lastStopReason = "";
  var lastErrorCode = "";
  var lastCopiedInspectLine = "";
  try {
    cid = localStorage.getItem("agunt_active_conversation_id");
  } catch(e) {}

  function fmtUsage(u){
    if (!u) return "";
    var p = (typeof u.prompt_tokens === "number") ? u.prompt_tokens : 0;
    var c = (typeof u.completion_tokens === "number") ? u.completion_tokens : 0;
    var t = (typeof u.total_tokens === "number") ? u.total_tokens : (p + c);
    if (t <= 0 && p <= 0 && c <= 0) return "";
    return " | usage p/c/t: " + p + "/" + c + "/" + t;
  }

  function fmtRunMeta(){
    var parts = [];
    if (lastRequestId) parts.push("request_id: " + lastRequestId);
    if (lastStopReason) parts.push("stop_reason: " + lastStopReason);
    if (lastErrorCode) parts.push("error_code: " + lastErrorCode);
    if (!parts.length) return "";
    return " | " + parts.join(" | ");
  }

  function renderCid(){
    if (!cidText) return;
    cidText.textContent = "conversation_id: " + cid + " (" + BUILD + ")" + fmtUsage(lastUsage) + fmtRunMeta();
  }

  function setUsage(u){
    if (u && typeof u === "object") {
      lastUsage = {
        prompt_tokens: Number(u.prompt_tokens || 0),
        completion_tokens: Number(u.completion_tokens || 0),
        total_tokens: Number(u.total_tokens || 0)
      };
    } else {
      lastUsage = null;
    }
    renderCid();
  }

  function setRunMeta(meta){
    var m = meta || {};
    lastRequestId = m.request_id ? String(m.request_id) : lastRequestId;
    lastStopReason = m.stop_reason ? String(m.stop_reason) : lastStopReason;
    lastErrorCode = m.error_code ? String(m.error_code) : lastErrorCode;
    renderCid();
  }

  function setCid(id){
    cid = id;
    if (cidEl) cidEl.value = cid;
    lastRequestId = "";
    lastStopReason = "";
    lastErrorCode = "";
    setUsage(null);
    try { localStorage.setItem("agunt_active_conversation_id", cid); } catch(e) {}
  }

  if (!cid) setCid(newId()); else setCid(cid);

  function renderChats(items){
    if (!chatList) return;
    var html = "";
    for (var i=0;i<items.length;i++){
      var it = items[i] || {};
      var id = it.conversation_id;
      var title = it.title || id;
      var meta = (it.message_count!=null? (it.message_count+" msgs") : "");
      var cls = (id===cid) ? "chatItem active" : "chatItem";
      html += '<div class="'+cls+'" data-cid="'+esc(id)+'"><div class="chatTitle">'+esc(title)+'</div><div class="chatMeta">'+esc(meta)+'</div></div>';
    }
    chatList.innerHTML = html || '<div class="chatItem"><div class="chatTitle">暂无会话</div><div class="chatMeta">发送一条消息后会出现在这里</div></div>';
  }

  function loadChats(){
    return fetch("/api/conversations?limit=80").then(function(r){return r.json();}).then(function(j){
      var items = (j && j.items) ? j.items : [];
      renderChats(items);
    }).catch(function(){});
  }

  var msgCache = {};

  function fmtTime(ms){
    try { return new Date(ms).toLocaleString(); } catch(e){ return ""; }
  }

  function renderMessages(items){
    if (!messages) return;
    msgCache = {};
    var html = "";
    for (var i=0;i<items.length;i++){
      var m = items[i] || {};
      var id = m.id;
      var role = m.role || "";
      var content = m.content || "";
      var traceId = m.trace_id || "";
      var t = fmtTime(m.created_at_ms);

      if (id != null) msgCache[String(id)] = String(content);

      var cls = role === "user" ? "msgRow user" : "msgRow";
      var actions = '';
      if (id != null) actions += '<button type="button" class="copyBtn" data-mid="'+esc(String(id))+'">复制</button>';
      if (traceId) actions += '<a href="/debug/trace/'+esc(String(traceId))+'" target="_blank">trace</a>';

      html += '<div class="'+cls+'">'
        + '<div class="msgRole"><span>'+esc(role)+'</span><span class="msgMeta">'+esc(t)+'</span><span class="msgActions">'+actions+'</span></div>'
        + '<div class="msgBubble">'+esc(content)+'</div>'
        + '</div>';
    }
    messages.innerHTML = html || '<div class="msgRow"><div class="msgBubble">新会话：输入一句话开始</div></div>';
    scrollBottom();
  }

  function loadMessages(){
    return fetch("/api/conversations/" + encodeURIComponent(cid) + "/messages?limit=200").then(function(r){return r.json();}).then(function(j){
      renderMessages((j && j.items) ? j.items : []);
    }).catch(function(){
      renderMessages([]);
    });
  }

  function renameActive(){
    var title = prompt("会话名称：");
    if (!title) return;
    return fetch("/api/conversations/" + encodeURIComponent(cid) + "/rename", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({title: title})
    }).then(function(r){return r.json();}).then(function(){
      return loadChats();
    }).catch(function(){});
  }

  function archiveActive(){
    var ok = confirm("归档会话（仅隐藏，不会真正删除）？");
    if (!ok) return;
    return fetch("/api/conversations/" + encodeURIComponent(cid) + "/archive", {method: "POST"}).then(function(r){return r.json();}).then(function(){
      setCid(newId());
      renderMessages([]);
      return loadChats();
    }).catch(function(){});
  }

  function purgeActive(){
    var ok = confirm("彻底删除当前会话及其全部轨迹数据？该操作不可恢复。");
    if (!ok) return;
    return fetch("/api/conversations/" + encodeURIComponent(cid) + "/purge", {method: "POST"}).then(function(r){return r.json();}).then(function(){
      setCid(newId());
      renderMessages([]);
      return loadChats();
    }).catch(function(){});
  }

  function purgeAll(){
    var ok = confirm("彻底删除全部会话与历史数据？该操作不可恢复。");
    if (!ok) return;
    return fetch("/api/conversations/purge_all", {method: "POST"}).then(function(r){return r.json();}).then(function(){
      setCid(newId());
      renderMessages([]);
      return loadChats();
    }).catch(function(){});
  }

  function append(role, text){
    if (!messages) return null;
    var cls = role === "user" ? "msgRow user" : "msgRow";
    var div = document.createElement("div");
    div.className = cls;
    div.innerHTML = '<div class="msgRole">'+esc(role)+'</div><div class="msgBubble"></div>';
    var bubble = div.querySelector(".msgBubble");
    bubble.textContent = text || "";
    messages.appendChild(div);
    scrollBottom();
    return bubble;
  }

  function startStream(userText){
    if (typeof EventSource === "undefined") {
      append("assistant", "当前环境不支持 EventSource (SSE)，已停留在聊天页。\\n请点击右侧的『结果页』按钮进行降级请求。\\n\\n提示：如果你用的是代理/特殊网络层，可能会影响 SSE 连接。");
      send.disabled = false;
      return;
    }

    send.disabled = true;
    append("user", userText);
    var bubble = append("assistant", "");

    var url = "/chat/stream_get?input_text=" + encodeURIComponent(userText) + "&conversation_id=" + encodeURIComponent(cid);
    var es = new EventSource(url);

    es.onmessage = function(msg){
      var ev;
      try { ev = JSON.parse(msg.data); } catch (e) { return; }
      var payload = ev && ev.payload ? ev.payload : {};
      var reqId = ev.request_id || ev.trace_id || payload.request_id || "";
      if (reqId) setRunMeta({request_id: reqId});

      if (ev.type === "token") {
        var d = ev.payload && ev.payload.delta ? ev.payload.delta : "";
        if (bubble) bubble.textContent += d;
        scrollBottom();
      } else if (ev.type === "tool_call") {
        var p1 = ev.payload || {};
        var name1 = p1.name || "";
        var args1 = p1.arguments_json || "";
        append("tool", "[tool_call] " + name1 + (args1 ? " " + args1 : ""));
      } else if (ev.type === "tool_result") {
        var p2 = ev.payload || {};
        var ok2 = (p2.ok === true);
        var cid2 = p2.call_id || "";
        var name2 = p2.name || "";
        var c2 = p2.content_json || "";
        var e2 = p2.error || {};
        var head2 = "[tool_result]" + (name2 ? " " + name2 : "") + (cid2 ? " " + cid2 : "");
        if (ok2) {
          append("tool", head2 + " ok " + c2);
        } else {
          var um2 = e2.user_message || "";
          var dm2 = e2.message || "";
          append("tool", head2 + " error " + (e2.code || "") + " " + (um2 || dm2));
        }
      } else if (ev.type === "error") {
        var m = (payload && payload.message) ? payload.message : "error";
        var ec = (payload && payload.error_code) ? payload.error_code : "";
        setRunMeta({request_id: reqId, error_code: ec});
        if (bubble) bubble.textContent += "\\n[error" + (ec ? (":" + ec) : "") + "] " + m + (reqId ? (" (request_id=" + reqId + ")") : "");
      } else if (ev.type === "done") {
        var doneUsage = payload && payload.usage ? payload.usage : null;
        var stopReason = payload && payload.stop_reason ? payload.stop_reason : "";
        setRunMeta({request_id: reqId, stop_reason: stopReason});
        setUsage(doneUsage);
        try { es.close(); } catch (e) {}
        send.disabled = false;
        loadChats().then(loadMessages);
      }
    };

    es.onerror = function(){
      try { es.close(); } catch (e) {}
      send.disabled = false;
      loadChats().then(loadMessages);
    };
  }

  if (chatList) {
    chatList.addEventListener("click", function(e){
      var el = e.target;
      while (el && el !== chatList && !el.dataset.cid) el = el.parentNode;
      if (!el || !el.dataset.cid) return;
      setCid(el.dataset.cid);
      loadChats();
      loadMessages();
    });
  }

  if (newChat) {
    newChat.addEventListener("click", function(){
      setCid(newId());
      renderMessages([]);
      loadChats();
      if (q) q.focus();
    });
  }

  if (renameChat) {
    renameChat.addEventListener("click", function(){
      renameActive();
    });
  }

  if (deleteChat) {
    deleteChat.addEventListener("click", function(){
      archiveActive();
    });
  }

  if (purgeChat) {
    purgeChat.addEventListener("click", function(){
      purgeActive();
    });
  }

  if (purgeAllChats) {
    purgeAllChats.addEventListener("click", function(){
      purgeAll();
    });
  }

  if (exportChat) {
    exportChat.addEventListener("click", function(){
      fetch("/api/conversations/" + encodeURIComponent(cid) + "/export?limit=2000&tool_limit=2000").then(function(r){return r.json();}).then(function(j){
        var usage = (j && j.usage_total) ? j.usage_total : {};
        var p = (typeof usage.prompt_tokens === "number") ? usage.prompt_tokens : 0;
        var c = (typeof usage.completion_tokens === "number") ? usage.completion_tokens : 0;
        var t = (typeof usage.total_tokens === "number") ? usage.total_tokens : (p + c);

        function topPairs(obj, n){
          var arr = [];
          if (obj && typeof obj === "object") {
            for (var k in obj) {
              if (!Object.prototype.hasOwnProperty.call(obj, k)) continue;
              var v = Number(obj[k] || 0);
              arr.push([k, v]);
            }
          }
          arr.sort(function(a,b){ return b[1]-a[1]; });
          return arr.slice(0, n || 5);
        }

        var stopCounts = (j && j.stop_reason_counts) ? j.stop_reason_counts : {};
        var errCounts = (j && j.tool_error_code_counts) ? j.tool_error_code_counts : {};
        var stopTop = topPairs(stopCounts, 5);
        var errTop = topPairs(errCounts, 5);
        var att = (j && j.attachments_total) ? j.attachments_total : {};
        var attCount = Number(att.count || 0);
        var attBytes = Number(att.total_bytes || 0);
        var attRefs = (j && j.attachment_refs_total) ? j.attachment_refs_total : {};
        var attRefTraces = Number(attRefs.traces_with_refs || 0);
        var attRefMentions = Number(attRefs.total_mentions || 0);
        var traceCount = ((j && j.trace_ids && j.trace_ids.length) ? j.trace_ids.length : 0);

        var qs = (j && j.quality_signals && typeof j.quality_signals === "object") ? j.quality_signals : null;
        var qsStatus = (qs && qs.status) ? String(qs.status) : "ok";
        var qsHits = (qs && Array.isArray(qs.hits)) ? qs.hits : [];
        var qsMetrics = (qs && qs.metrics && typeof qs.metrics === "object") ? qs.metrics : {};
        var qsThresholds = (qs && qs.thresholds && typeof qs.thresholds === "object") ? qs.thresholds : {};

        var inspectSummary = (j && j.inspect_summary && typeof j.inspect_summary === "object") ? j.inspect_summary : null;
        var backendInspectLine = (j && typeof j.inspect_line === "string" && j.inspect_line.trim()) ? j.inspect_line.trim() : "";
        var inspectLine = backendInspectLine || (
          "inspect:"
          + " cid=" + (j && j.conversation_id ? j.conversation_id : cid)
          + " traces=" + traceCount
          + " usage=" + p + "/" + c + "/" + t
          + " att=" + attCount + "/" + attBytes
          + " att_refs=" + attRefTraces + "/" + attRefMentions
          + " risk=" + ((qsStatus === "warn" && qsHits.length) ? qsHits.join("|") : "OK")
        );

        var inspectSummaryText = "none";
        if (inspectSummary) {
          var su = (inspectSummary.usage && typeof inspectSummary.usage === "object") ? inspectSummary.usage : {};
          var sa = (inspectSummary.attachments && typeof inspectSummary.attachments === "object") ? inspectSummary.attachments : {};
          var sar = (inspectSummary.attachment_refs && typeof inspectSummary.attachment_refs === "object") ? inspectSummary.attachment_refs : {};
          var sr = (inspectSummary.risk && typeof inspectSummary.risk === "object") ? inspectSummary.risk : {};
          var sh = (Array.isArray(sr.hits) && sr.hits.length) ? sr.hits.join(",") : "none";
          inspectSummaryText = "conversation_id=" + (inspectSummary.conversation_id || "")
            + "; trace_count=" + Number(inspectSummary.trace_count || 0)
            + "; usage=" + Number(su.prompt_tokens || 0) + "/" + Number(su.completion_tokens || 0) + "/" + Number(su.total_tokens || 0)
            + "; attachments=" + Number(sa.count || 0) + "/" + Number(sa.total_bytes || 0)
            + "; attachment_refs=" + Number(sar.traces_with_refs || 0) + "/" + Number(sar.total_mentions || 0)
            + "; risk_status=" + String(sr.status || "ok")
            + "; risk_hits=" + sh;
        }

        var inspectCopied = false;
        var inspectCopySkipped = false;
        if (inspectLine && inspectLine === lastCopiedInspectLine) {
          inspectCopySkipped = true;
        } else if (navigator && navigator.clipboard && navigator.clipboard.writeText) {
          try {
            navigator.clipboard.writeText(inspectLine);
            inspectCopied = true;
            lastCopiedInspectLine = inspectLine;
          } catch(e) {}
        }

        var msg = "导出摘要\\n"
          + "conversation_id: " + (j && j.conversation_id ? j.conversation_id : cid) + "\\n"
          + "messages: " + ((j && j.messages && j.messages.length) ? j.messages.length : 0) + "\\n"
          + "trace_ids: " + traceCount + "\\n"
          + "usage p/c/t: " + p + "/" + c + "/" + t + "\\n"
          + "attachments count/bytes: " + attCount + "/" + attBytes + "\\n"
          + "attachment_refs traces/mentions: " + attRefTraces + "/" + attRefMentions + "\\n"
          + "stop_reason top: " + (stopTop.length ? stopTop.map(function(it){return it[0]+":"+it[1];}).join(", ") : "none") + "\\n"
          + "tool_error top: " + (errTop.length ? errTop.map(function(it){return it[0]+":"+it[1];}).join(", ") : "none") + "\\n"
          + "risk_signals: " + ((qsStatus === "warn" && qsHits.length) ? ("⚠ " + qsHits.join(", ")) : "OK") + "\\n"
          + inspectLine + "\\n"
          + (inspectCopied ? "inspect_line 已复制到剪贴板\\n\\n" : (inspectCopySkipped ? "inspect_line 本会话已复制过，本次不覆盖剪贴板\\n\\n" : "inspect_line 复制失败，可手动复制上面一行\\n\\n"))
          + "查看详细质量指标？点击【确定】查看，点击【取消】继续下载。";

        var showDetail = false;
        try { showDetail = confirm(msg); } catch(e) {}
        if (showDetail) {
          var detail = "quality_signals 详情\\n"
            + "status: " + qsStatus + "\\n"
            + "hits: " + (qsHits.length ? qsHits.join(", ") : "none") + "\\n"
            + "thresholds: " + JSON.stringify(qsThresholds) + "\\n"
            + "metrics: " + JSON.stringify(qsMetrics) + "\\n"
            + "attachment_refs traces/mentions: " + attRefTraces + "/" + attRefMentions + "\\n"
            + "inspect_summary: " + inspectSummaryText + "\\n"
            + inspectLine;

          var copied = false;
          if (navigator && navigator.clipboard && navigator.clipboard.writeText) {
            try {
              navigator.clipboard.writeText(detail);
              copied = true;
            } catch(e) {}
          }

          if (copied) {
            try { alert("详细质量指标已复制到剪贴板\\n\\n" + detail); } catch(e) {}
          } else {
            try { window.prompt("复制以下质量指标文本", detail); } catch(e) { try { alert(detail); } catch(_) {} }
          }
        }

        function shortHash(s){
          var h = 2166136261;
          var x = String(s || "");
          for (var i = 0; i < x.length; i++) {
            h ^= x.charCodeAt(i);
            h += (h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24);
          }
          return (h >>> 0).toString(16).slice(0, 8);
        }

        var riskTag = (qsStatus === "warn") ? "warn" : "ok";
        var hashTag = shortHash(inspectLine);
        var data = JSON.stringify(j, null, 2);
        var blob = new Blob([data], {type: "application/json"});
        var a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = "conversation-" + cid + "-" + riskTag + "-" + hashTag + "-full.json";
        document.body.appendChild(a);
        a.click();
        a.remove();
      }).catch(function(){});
    });
  }

  if (messages) {
    messages.addEventListener("click", function(e){
      var el = e.target;
      if (!el) return;
      if (el.classList && el.classList.contains("copyBtn")) {
        var mid = el.getAttribute("data-mid");
        var txt = msgCache[mid] || "";
        if (!txt) return;
        if (navigator && navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(txt).catch(function(){});
        } else {
          var ta = document.createElement("textarea");
          ta.value = txt;
          document.body.appendChild(ta);
          ta.select();
          try { document.execCommand("copy"); } catch(err) {}
          ta.remove();
        }
      }
    });
  }

  function handleSend(){
    var text = (q && q.value) ? q.value.trim() : "";
    if (!text) return;
    if (q) q.value = "";
    startStream(text);
  }

  if (composer) {
    composer.addEventListener("submit", function(e){
      e.preventDefault();
      handleSend();
    });
  }

  if (send) {
    send.addEventListener("click", function(){
      handleSend();
    });
  }

  if (q) {
    q.addEventListener("keydown", function(e){
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        handleSend();
      }
    });
  }

  loadChats().then(loadMessages).catch(function(){
    loadMessages();
  });
})();
""".strip() + "\n"

    return Response(
        content=js,
        media_type="application/javascript; charset=utf-8",
        headers={"Cache-Control": "no-store"},
    )


@router.get("/debug/trace/{trace_id}")
def debug_trace(request: Request, trace_id: str) -> dict:
    store = _get_store(request)

    trace = store.get_trace(trace_id)
    if not trace:
        return {"ok": False, "error": "trace_not_found", "trace_id": trace_id}

    replay_events = store.list_replay_events(trace_id)
    attachments = _attachments_from_replay(replay_events)
    conv_id = trace.get("conversation_id") if isinstance(trace, dict) else None
    msgs = store.list_messages(conv_id, limit=2000) if isinstance(conv_id, str) and conv_id else []
    attachment_refs = _attachment_refs_from_messages(msgs, trace_id=trace_id, attachment_count=len(attachments))
    return {
        "ok": True,
        "trace": trace,
        "spans": store.list_spans(trace_id),
        "replay_events": replay_events,
        "tool_runs": store.list_tool_runs(trace_id),
        "attachments": attachments,
        "attachment_refs": attachment_refs,
    }


@router.get("/api/conversations")
def api_conversations(request: Request, limit: int = 50) -> dict:
    store = _get_store(request)
    return {"ok": True, "items": store.list_conversations(limit=limit)}


@router.get("/api/conversations/{conversation_id}/messages")
def api_conversation_messages(request: Request, conversation_id: str, limit: int = 200) -> dict:
    store = _get_store(request)
    return {"ok": True, "conversation_id": conversation_id, "items": store.list_messages(conversation_id=conversation_id, limit=limit)}


@router.get("/api/conversations/{conversation_id}/export")
def api_conversation_export(
    request: Request,
    conversation_id: str,
    limit: int = 2000,
    tool_limit: int = 2000,
    span_limit: int = 2000,
    replay_limit: int = 2000,
    quality_gate_mode: str = Query(default="off", pattern="^(off|warn|fail)$"),
    expected_contract_fingerprint: str | None = Query(default=None, min_length=8, max_length=128),
) -> dict:
    store = _get_store(request)

    items = store.list_messages(conversation_id=conversation_id, limit=limit)
    trace_ids: list[str] = []
    seen: set[str] = set()
    for m in items:
        tid = m.get("trace_id")
        if not isinstance(tid, str) or not tid:
            continue
        if tid in seen:
            continue
        seen.add(tid)
        trace_ids.append(tid)

    traces_by_id: dict[str, dict | None] = {}
    spans_by_trace: dict[str, list[dict]] = {}
    replay_events_by_trace: dict[str, list[dict]] = {}
    tool_runs_by_trace: dict[str, list[dict]] = {}
    usage_by_trace: dict[str, dict[str, int]] = {}
    usage_total: dict[str, int] = {}
    attachments_by_trace: dict[str, list[dict]] = {}
    attachments_total = {"count": 0, "total_bytes": 0}
    attachment_refs_by_trace: dict[str, dict[str, Any]] = {}
    attachment_refs_total = {"traces_with_refs": 0, "total_mentions": 0}
    msg_trace_ids = {str(m.get("trace_id")) for m in items if isinstance(m.get("trace_id"), str) and m.get("trace_id")}
    contract_checks = {
        "contract_version": "v1",
        "request_id_strategy": "trace_id",
        "trace_stop_reason_missing": 0,
        "trace_stop_reason_unknown": 0,
        "request_id_mismatch": 0,
        "done_event_missing": 0,
        "done_stop_reason_mismatch": 0,
        "tool_error_code_missing": 0,
        "tool_error_code_unknown": 0,
    }
    security_checks = {
        "tool_not_allowed_count": 0,
        "tool_not_allowed_without_error_count": 0,
        "tool_runtime_error_count": 0,
        "silent_tool_failure_count": 0,
        "attachment_policy_violation_count": 0,
        "remember_write_block_count": 0,
    }
    audit_checks = {
        "trace_missing_row": 0,
        "trace_without_replay": 0,
        "trace_without_message": 0,
        "trace_without_done_event": 0,
        "tool_runs_unmapped": 0,
        "replay_tool_results_unmapped": 0,
        "replay_seq_gap_count": 0,
    }
    audit_samples = {
        "trace_missing_row": [],
        "trace_without_done_event": [],
        "tool_runs_unmapped": [],
        "replay_tool_results_unmapped": [],
        "replay_seq_gap": [],
    }

    for tid in trace_ids:
        trace = store.get_trace(tid)
        traces_by_id[tid] = trace
        spans_by_trace[tid] = store.list_spans(tid)[: max(0, int(span_limit))]
        replay_events = store.list_replay_events(tid, limit=replay_limit)
        replay_events_by_trace[tid] = replay_events
        security_checks["attachment_policy_violation_count"] += _attachment_policy_violations(replay_events)
        tool_runs = store.list_tool_runs(tid, limit=tool_limit)
        tool_runs_by_trace[tid] = tool_runs

        if not isinstance(trace, dict):
            audit_checks["trace_missing_row"] += 1
            if len(audit_samples["trace_missing_row"]) < 10:
                audit_samples["trace_missing_row"].append(tid)
        if not isinstance(trace, dict) or not isinstance(trace.get("stop_reason"), str) or not str(trace.get("stop_reason") or "").strip():
            contract_checks["trace_stop_reason_missing"] += 1

        if not replay_events:
            audit_checks["trace_without_replay"] += 1
        if tid not in msg_trace_ids:
            audit_checks["trace_without_message"] += 1

        replay_call_ids: set[str] = set()
        replay_tool_result_call_ids: set[str] = set()
        has_done = False
        done_match = False
        prev_seq: int | None = None
        trace_stop_reason = str(trace.get("stop_reason") or "") if isinstance(trace, dict) else ""
        if trace_stop_reason and trace_stop_reason not in _ALLOWED_STOP_REASONS:
            contract_checks["trace_stop_reason_unknown"] += 1
        for ev in replay_events:
            seq = ev.get("seq")
            if isinstance(seq, int):
                if prev_seq is not None and seq != prev_seq + 1:
                    audit_checks["replay_seq_gap_count"] += 1
                    if len(audit_samples["replay_seq_gap"]) < 10:
                        audit_samples["replay_seq_gap"].append({"trace_id": tid, "prev_seq": prev_seq, "seq": seq})
                prev_seq = seq
            ev_type = ev.get("type")
            p = _payload_obj(ev.get("payload_json"))
            req_id = p.get("request_id")
            if isinstance(req_id, str) and req_id and req_id != tid:
                contract_checks["request_id_mismatch"] += 1
            if ev_type == "tool_result":
                cid = p.get("call_id")
                if isinstance(cid, str) and cid:
                    replay_call_ids.add(cid)
                    replay_tool_result_call_ids.add(cid)
            if ev_type == "model_done":
                has_done = True
                sr = p.get("stop_reason")
                if isinstance(sr, str) and sr == trace_stop_reason and sr:
                    done_match = True

        if not has_done:
            contract_checks["done_event_missing"] += 1
            audit_checks["trace_without_done_event"] += 1
            if len(audit_samples["trace_without_done_event"]) < 10:
                audit_samples["trace_without_done_event"].append(tid)
        elif trace_stop_reason and not done_match:
            contract_checks["done_stop_reason_mismatch"] += 1

        tool_run_call_ids: set[str] = set()
        for r in tool_runs:
            allowed = bool(r.get("allowed"))
            ok = bool(r.get("ok"))
            code = r.get("error_code")
            if not allowed:
                security_checks["tool_not_allowed_count"] += 1
                if code != ERROR_TOOL_NOT_ALLOWED:
                    security_checks["tool_not_allowed_without_error_count"] += 1
            if not ok:
                msg = r.get("error_message")
                if not isinstance(code, str) or not code.strip():
                    contract_checks["tool_error_code_missing"] += 1
                    if not isinstance(msg, str) or not msg.strip():
                        security_checks["silent_tool_failure_count"] += 1
                elif code == "TOOL_RUNTIME_ERROR":
                    security_checks["tool_runtime_error_count"] += 1
                if isinstance(code, str) and code.strip() and code not in _ALLOWED_ERROR_CODES:
                    contract_checks["tool_error_code_unknown"] += 1
            if str(r.get("tool_name") or "") == "remember" and ok:
                c = _tool_run_content_obj(r)
                if bool(c.get("skipped")) and str(c.get("reason") or "") == "memory_write_disabled":
                    security_checks["remember_write_block_count"] += 1
            call_id = r.get("call_id")
            if isinstance(call_id, str) and call_id:
                tool_run_call_ids.add(call_id)
                if call_id not in replay_call_ids:
                    audit_checks["tool_runs_unmapped"] += 1
                    if len(audit_samples["tool_runs_unmapped"]) < 10:
                        audit_samples["tool_runs_unmapped"].append({"trace_id": tid, "call_id": call_id})

        for call_id in replay_tool_result_call_ids:
            if call_id not in tool_run_call_ids:
                audit_checks["replay_tool_results_unmapped"] += 1
                if len(audit_samples["replay_tool_results_unmapped"]) < 10:
                    audit_samples["replay_tool_results_unmapped"].append({"trace_id": tid, "call_id": call_id})

        usage = _sum_usage_from_replay(replay_events)
        usage_by_trace[tid] = usage
        for k, v in usage.items():
            usage_total[k] = usage_total.get(k, 0) + int(v)

        attachments = _attachments_from_replay(replay_events)
        attachments_by_trace[tid] = attachments
        attachments_total["count"] += len(attachments)
        for a in attachments:
            s = a.get("size_bytes")
            if isinstance(s, int) and s >= 0:
                attachments_total["total_bytes"] += s

        refs = _attachment_refs_from_messages(items, trace_id=tid, attachment_count=len(attachments))
        attachment_refs_by_trace[tid] = refs
        if isinstance(refs.get("ids"), list) and refs.get("ids"):
            attachment_refs_total["traces_with_refs"] += 1
        tm = refs.get("total_mentions")
        if isinstance(tm, int) and tm > 0:
            attachment_refs_total["total_mentions"] += tm

    stop_reason_counts = _stop_reason_counts(traces_by_id)
    tool_error_code_counts = _tool_error_code_counts(tool_runs_by_trace)
    quality_signals = _quality_signals(
        trace_count=len(trace_ids),
        stop_reason_counts=stop_reason_counts,
        tool_error_code_counts=tool_error_code_counts,
        attachments_total=attachments_total,
    )
    inspect_summary = _inspect_summary(
        conversation_id=conversation_id,
        trace_count=len(trace_ids),
        usage_total=usage_total,
        attachments_total=attachments_total,
        attachment_refs_total=attachment_refs_total,
        quality_signals=quality_signals,
    )

    m = quality_signals.get("metrics", {}) if isinstance(quality_signals, dict) else {}
    t = quality_signals.get("thresholds", {}) if isinstance(quality_signals, dict) else {}
    hits = quality_signals.get("hits", []) if isinstance(quality_signals, dict) else []
    timeout_ratio = float(m.get("timeout_ratio", 0.0))
    tool_error_ratio = float(m.get("tool_error_ratio", 0.0))
    timeout_thr = float(t.get("timeout_ratio", 0.3))
    tool_error_thr = float(t.get("tool_error_ratio", 0.3))
    contract_error_total = (
        contract_checks["trace_stop_reason_missing"]
        + contract_checks["trace_stop_reason_unknown"]
        + contract_checks["request_id_mismatch"]
        + contract_checks["done_event_missing"]
        + contract_checks["done_stop_reason_mismatch"]
        + contract_checks["tool_error_code_missing"]
        + contract_checks["tool_error_code_unknown"]
    )
    security_error_total = (
        security_checks["silent_tool_failure_count"]
        + security_checks["tool_not_allowed_without_error_count"]
        + security_checks["attachment_policy_violation_count"]
    )
    rules = [
        {"id": "timeout_ratio", "status": ("warn" if timeout_ratio >= timeout_thr else "ok"), "value": timeout_ratio, "threshold": timeout_thr},
        {"id": "tool_error_ratio", "status": ("warn" if tool_error_ratio >= tool_error_thr else "ok"), "value": tool_error_ratio, "threshold": tool_error_thr},
        {"id": "tool_error_concentrated", "status": ("warn" if "TOOL_ERROR_CONCENTRATED" in hits else "ok"), "value": (1 if "TOOL_ERROR_CONCENTRATED" in hits else 0), "threshold": 1},
        {"id": "contract_integrity", "status": ("fail" if contract_error_total > 0 else "ok"), "value": contract_error_total, "threshold": 0},
        {"id": "security_integrity", "status": ("fail" if security_error_total > 0 else "ok"), "value": security_error_total, "threshold": 0},
        {
            "id": "audit_closure",
            "status": (
                "fail"
                if (
                    audit_checks["trace_missing_row"]
                    + audit_checks["trace_without_replay"]
                    + audit_checks["trace_without_message"]
                    + audit_checks["trace_without_done_event"]
                    + audit_checks["tool_runs_unmapped"]
                    + audit_checks["replay_tool_results_unmapped"]
                    + audit_checks["replay_seq_gap_count"]
                )
                > 0
                else "ok"
            ),
        },
    ]
    quality_gate_status = "ok"
    if any((r.get("status") == "fail") for r in rules):
        quality_gate_status = "fail"
    elif any((r.get("status") == "warn") for r in rules) or str(quality_signals.get("status", "ok")) == "warn":
        quality_gate_status = "warn"
    contract_catalog = {
        "contract_version": "v1",
        "stop_reason": sorted(_ALLOWED_STOP_REASONS),
        "error_code": sorted(_ALLOWED_ERROR_CODES),
        "request_id_strategy": "trace_id",
    }
    contract_fingerprint = _contract_fingerprint(contract_catalog)
    trace_audit_index = _trace_audit_index(
        trace_ids=trace_ids,
        traces_by_id=traces_by_id,
        replay_events_by_trace=replay_events_by_trace,
        tool_runs_by_trace=tool_runs_by_trace,
        msg_trace_ids=msg_trace_ids,
    )
    trace_audit_top_issues = _trace_issue_top_counts(trace_audit_index)
    quality_gate = {
        "status": quality_gate_status,
        "rules": rules,
        "contract_checks": contract_checks,
        "security_checks": security_checks,
        "audit_checks": audit_checks,
        "audit_samples": audit_samples,
        "contract_catalog": contract_catalog,
        "contract_fingerprint": contract_fingerprint,
        "trace_audit_index": trace_audit_index,
        "trace_audit_top_issues": trace_audit_top_issues,
    }
    expected_fp = _normalize_fingerprint(expected_contract_fingerprint)
    actual_fp = _normalize_fingerprint(contract_fingerprint)
    contract_drift = bool(expected_fp and actual_fp and expected_fp != actual_fp)
    rules.append({"id": "contract_drift", "status": ("fail" if contract_drift else "ok"), "value": (1 if contract_drift else 0), "threshold": 0})
    quality_gate_status = "ok"
    if any((r.get("status") == "fail") for r in rules):
        quality_gate_status = "fail"
    elif any((r.get("status") == "warn") for r in rules) or str(quality_signals.get("status", "ok")) == "warn":
        quality_gate_status = "warn"
    quality_gate["status"] = quality_gate_status
    quality_gate["expected_contract_fingerprint"] = expected_contract_fingerprint
    quality_gate["contract_drift"] = contract_drift
    quality_gate_ci = _quality_gate_ci_view(quality_gate, quality_gate_mode)

    gate_blocked = False
    if quality_gate_mode != "off":
        min_level = 1 if quality_gate_mode == "warn" else 2
        gate_blocked = _gate_level(quality_gate_status) >= min_level
    if gate_blocked or contract_drift:
        raise HTTPException(
            status_code=409,
            detail={
                "code": ("CONTRACT_DRIFT_BLOCKED" if contract_drift else "QUALITY_GATE_BLOCKED"),
                "quality_gate_mode": quality_gate_mode,
                "quality_gate": quality_gate,
                "quality_gate_ci": quality_gate_ci,
                "contract_catalog": contract_catalog,
                "contract_fingerprint": contract_fingerprint,
                "expected_contract_fingerprint": expected_contract_fingerprint,
                "contract_drift": contract_drift,
                "inspect_line": inspect_summary.get("line", ""),
            },
        )

    return {
        "ok": True,
        "conversation_id": conversation_id,
        "messages": items,
        "trace_ids": trace_ids,
        "traces_by_id": traces_by_id,
        "spans_by_trace": spans_by_trace,
        "replay_events_by_trace": replay_events_by_trace,
        "tool_runs_by_trace": tool_runs_by_trace,
        "usage_by_trace": usage_by_trace,
        "usage_total": usage_total,
        "attachments_by_trace": attachments_by_trace,
        "attachments_total": attachments_total,
        "attachment_refs_by_trace": attachment_refs_by_trace,
        "attachment_refs_total": attachment_refs_total,
        "stop_reason_counts": stop_reason_counts,
        "tool_error_code_counts": tool_error_code_counts,
        "quality_signals": quality_signals,
        "quality_gate_mode": quality_gate_mode,
        "quality_gate": quality_gate,
        "quality_gate_ci": quality_gate_ci,
        "contract_catalog": contract_catalog,
        "contract_fingerprint": contract_fingerprint,
        "expected_contract_fingerprint": expected_contract_fingerprint,
        "contract_drift": contract_drift,
        "trace_audit_index": trace_audit_index,
        "trace_audit_top_issues": trace_audit_top_issues,
        "inspect_summary": inspect_summary,
        "inspect_line": inspect_summary.get("line", ""),
    }


@router.get("/api/conversations/{conversation_id}/quality-gate")
def api_conversation_quality_gate(
    request: Request,
    conversation_id: str,
    quality_gate_mode: str = Query(default="fail", pattern="^(warn|fail)$"),
    expected_contract_fingerprint: str | None = Query(default=None, min_length=8, max_length=128),
    include_details: bool = False,
    limit: int = 2000,
    tool_limit: int = 2000,
    span_limit: int = 2000,
    replay_limit: int = 2000,
) -> dict:
    payload = api_conversation_export(
        request=request,
        conversation_id=conversation_id,
        limit=limit,
        tool_limit=tool_limit,
        span_limit=span_limit,
        replay_limit=replay_limit,
        quality_gate_mode="off",
        expected_contract_fingerprint=None,
    )
    quality_gate = payload.get("quality_gate") if isinstance(payload, dict) else {}
    quality_gate = quality_gate if isinstance(quality_gate, dict) else {}
    inspect_line = payload.get("inspect_line", "") if isinstance(payload, dict) else ""

    contract_catalog = payload.get("contract_catalog") if isinstance(payload, dict) else {}
    contract_catalog = contract_catalog if isinstance(contract_catalog, dict) else {}
    contract_fingerprint = payload.get("contract_fingerprint") if isinstance(payload, dict) else ""
    if not isinstance(contract_fingerprint, str) or not contract_fingerprint:
        contract_fingerprint = _contract_fingerprint(contract_catalog) if contract_catalog else ""
    expected_fp = _normalize_fingerprint(expected_contract_fingerprint)
    actual_fp = _normalize_fingerprint(contract_fingerprint)
    contract_drift = bool(expected_fp and actual_fp and expected_fp != actual_fp)
    quality_gate["expected_contract_fingerprint"] = expected_contract_fingerprint
    quality_gate["contract_drift"] = contract_drift
    if contract_fingerprint:
        quality_gate["contract_fingerprint"] = contract_fingerprint
    quality_gate_ci = _quality_gate_ci_view(quality_gate, quality_gate_mode)

    if bool(quality_gate_ci.get("blocked")) or contract_drift:
        raise HTTPException(
            status_code=409,
            detail={
                "code": ("CONTRACT_DRIFT_BLOCKED" if contract_drift else "QUALITY_GATE_BLOCKED"),
                "quality_gate_mode": quality_gate_mode,
                "quality_gate_ci": quality_gate_ci,
                "contract_catalog": contract_catalog,
                "contract_fingerprint": contract_fingerprint,
                "expected_contract_fingerprint": expected_contract_fingerprint,
                "contract_drift": contract_drift,
                "inspect_line": inspect_line,
            },
        )

    if include_details:
        return {
            "ok": True,
            "conversation_id": conversation_id,
            "quality_gate_mode": quality_gate_mode,
            "quality_gate": quality_gate,
            "quality_gate_ci": quality_gate_ci,
            "contract_catalog": contract_catalog,
            "contract_fingerprint": contract_fingerprint,
            "expected_contract_fingerprint": expected_contract_fingerprint,
            "contract_drift": contract_drift,
            "inspect_line": inspect_line,
        }
    return {
        "ok": True,
        "conversation_id": conversation_id,
        "quality_gate_mode": quality_gate_mode,
        "quality_gate_ci": quality_gate_ci,
        "contract_catalog": contract_catalog,
        "contract_fingerprint": contract_fingerprint,
        "expected_contract_fingerprint": expected_contract_fingerprint,
        "contract_drift": contract_drift,
        "inspect_line": inspect_line,
    }


GROUP_PHASE_PLANNING = "planning"
GROUP_PHASE_DISCUSSION = "discussion"
GROUP_PHASE_EXECUTION = "execution"
GROUP_PHASE_QA_VERIFYING = "qa_verifying"
GROUP_PHASE_DECISION = "decision"

GROUP_PHASE_ALLOWED = {
    GROUP_PHASE_PLANNING,
    GROUP_PHASE_DISCUSSION,
    GROUP_PHASE_EXECUTION,
    GROUP_PHASE_QA_VERIFYING,
    GROUP_PHASE_DECISION,
}
GROUP_PHASE_RUNNING = {GROUP_PHASE_EXECUTION, GROUP_PHASE_QA_VERIFYING}

GROUP_STATUS_CREATED = "created"
GROUP_STATUS_RUNNING = "running"
GROUP_STATUS_DONE = "done"

GROUP_DECISION_TYPE_APPROVE = "approve"
GROUP_DECISION_TYPE_CANCEL = "cancel"

GROUP_ERROR_PHASE_NOT_ALLOWED = "GROUP_PHASE_NOT_ALLOWED"
GROUP_ERROR_TRANSITION_NOT_ALLOWED = "GROUP_TRANSITION_NOT_ALLOWED"
GROUP_ERROR_APPROVE_PHASE_INVALID = "GROUP_APPROVE_PHASE_INVALID"
GROUP_ERROR_QA_PHASE_INVALID = "GROUP_QA_PHASE_INVALID"
GROUP_ERROR_QUALITY_GATE_BLOCKED = "GROUP_QUALITY_GATE_BLOCKED"
GROUP_ERROR_TASK_CLOSED = "GROUP_TASK_CLOSED"
GROUP_ERROR_EXEC_PHASE_INVALID = "GROUP_EXEC_PHASE_INVALID"

GROUP_PHASE_TRANSITIONS: dict[str, set[str]] = {
    GROUP_PHASE_PLANNING: {GROUP_PHASE_DISCUSSION},
    GROUP_PHASE_DISCUSSION: {GROUP_PHASE_EXECUTION},
    GROUP_PHASE_EXECUTION: {GROUP_PHASE_QA_VERIFYING},
    GROUP_PHASE_QA_VERIFYING: {GROUP_PHASE_DECISION, GROUP_PHASE_EXECUTION},
    GROUP_PHASE_DECISION: {GROUP_PHASE_DISCUSSION, GROUP_PHASE_EXECUTION, GROUP_PHASE_QA_VERIFYING},
}


class GroupTaskCreateIn(BaseModel):
    goal: str = Field(min_length=1, max_length=2000)
    owner_id: str = Field(default="owner", min_length=1, max_length=200)


class GroupTaskApproveIn(BaseModel):
    owner_id: str = Field(default="owner", min_length=1, max_length=200)
    actor_role: str = Field(default="owner", min_length=1, max_length=50)
    winner_plan_id: str | None = Field(default=None, max_length=200)
    reason: str = Field(default="", max_length=2000)


class GroupTaskRerunIn(BaseModel):
    phase: str = Field(default=GROUP_PHASE_DISCUSSION, min_length=1, max_length=50)
    actor_role: str = Field(default="owner", min_length=1, max_length=50)


class GroupTaskQaReviewIn(BaseModel):
    passed: bool
    reason: str = Field(default="", max_length=1000)
    actor_role: str = Field(default="qa", min_length=1, max_length=50)


class GroupTaskCancelIn(BaseModel):
    owner_id: str = Field(default="owner", min_length=1, max_length=200)
    actor_role: str = Field(default="owner", min_length=1, max_length=50)
    reason: str = Field(default="", max_length=1000)


class GroupTaskThreadMessageIn(BaseModel):
    actor_role: str = Field(default="pm", min_length=1, max_length=50)
    content: str = Field(min_length=1, max_length=4000)
    refs: dict[str, Any] = Field(default_factory=dict)


class GroupTaskExecutionSubmitIn(BaseModel):
    actor_role: str = Field(default="rd", min_length=1, max_length=50)
    changed_files: list[str] = Field(default_factory=list, max_length=200)
    summary: str = Field(default="", max_length=4000)


class GroupTaskArtifactsUpsertIn(BaseModel):
    actor_role: str = Field(default="owner", min_length=1, max_length=50)
    changed_files: list[str] = Field(default_factory=list, max_length=500)
    test_result: str | None = Field(default=None, max_length=200)
    summary: str = Field(default="", max_length=8000)


class GroupTaskExportVerifyIn(BaseModel):
    expected_signature: str = Field(min_length=16, max_length=200)
    thread_limit: int = Field(default=2000, ge=1, le=5000)
    decisions_limit: int = Field(default=500, ge=1, le=2000)
    rounds_limit: int = Field(default=2000, ge=1, le=5000)


class AgentTaskActionIn(BaseModel):
    type: str = Field(min_length=1, max_length=50)
    params: dict[str, Any] = Field(default_factory=dict)


class AgentTaskCreateIn(BaseModel):
    goal: str = Field(min_length=1, max_length=2000)
    scope_paths: list[str] = Field(default_factory=list, max_length=50)
    forbidden_paths: list[str] = Field(default_factory=list, max_length=50)
    actions: list[AgentTaskActionIn] = Field(default_factory=list, max_length=100)
    acceptance_cmd: str = Field(min_length=1, max_length=1000)
    working_dir: str | None = Field(default=None, max_length=2000)
    max_retry: int = Field(default=3, ge=1, le=5)
    dry_run: bool = False
    rollback_on_action_failure: bool = True
    actor_role: str = "coder_agent"
    auto_start: bool = True


_AGENT_TASKS: dict[str, dict[str, Any]] = {}
_AGENT_TASKS_LOCK = threading.Lock()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _task_public(task: dict[str, Any]) -> dict[str, Any]:
    artifacts = task.get("artifacts") or {}
    return {
        "task_id": task.get("task_id"),
        "status": task.get("status"),
        "goal": task.get("goal"),
        "scope_paths": list(task.get("scope_paths") or []),
        "forbidden_paths": list(task.get("forbidden_paths") or []),
        "acceptance_cmd": task.get("acceptance_cmd"),
        "working_dir": task.get("working_dir"),
        "actor_role": str(task.get("actor_role") or "coder_agent"),
        "dry_run": bool(task.get("dry_run")),
        "rollback_on_action_failure": bool(task.get("rollback_on_action_failure", True)),
        "actions_count": len(list(task.get("actions") or [])),
        "request_id": task.get("request_id"),
        "trace_id": task.get("trace_id"),
        "current_step": task.get("current_step"),
        "attempt": task.get("attempt"),
        "max_retry": task.get("max_retry"),
        "stop_reason": task.get("stop_reason"),
        "error_code": task.get("error_code"),
        "created_at_ms": task.get("created_at_ms"),
        "updated_at_ms": task.get("updated_at_ms"),
        "finished_at_ms": task.get("finished_at_ms"),
        "logs": list(task.get("logs") or []),
        "last_verify": task.get("last_verify"),
        "test_result": artifacts.get("test_result"),
        "summary": artifacts.get("summary"),
        "step_results": list(artifacts.get("step_results") or []),
    }


def _task_log(task: dict[str, Any], message: str, store: Storage | None = None) -> None:
    ts = _now_ms()
    logs = task.setdefault("logs", [])
    if isinstance(logs, list):
        logs.append({"ts": ts, "message": message[:1000]})
    if store is not None:
        fn = getattr(store, "insert_agent_task_log", None)
        if callable(fn):
            fn(str(task.get("task_id") or ""), ts, message[:1000])


def _persist_task(store: Storage | None, task: dict[str, Any]) -> None:
    if store is None:
        return
    fn = getattr(store, "insert_agent_task", None)
    if callable(fn):
        fn(task)
    fn2 = getattr(store, "upsert_agent_task_artifacts", None)
    if callable(fn2):
        fn2(str(task.get("task_id") or ""), task.get("artifacts") or {"changed_files": [], "step_results": [], "test_result": None, "summary": ""})


def _task_step(
    store: Storage | None,
    task: dict[str, Any],
    step_type: str,
    inp: dict[str, Any],
    out: dict[str, Any],
    ok: bool,
    latency_ms: int,
) -> None:
    seq = int(task.get("_step_seq") or 0) + 1
    task["_step_seq"] = seq
    attempt = int(task.get("attempt") or 0)
    if store is None:
        return
    fn = getattr(store, "insert_agent_task_step", None)
    if callable(fn):
        fn(
            str(task.get("task_id") or ""),
            seq,
            step_type,
            attempt,
            json.dumps(inp, ensure_ascii=False),
            json.dumps(out, ensure_ascii=False),
            ok,
            latency_ms,
        )


def _normalize_task_error_code(code: object) -> str:
    c = str(code or "").strip().upper()
    if not c:
        return "TASK_RUNTIME_ERROR"
    mapping = {
        "COMMAND_EMPTY": "COMMAND_EMPTY",
        "COMMAND_NOT_ALLOWED": "COMMAND_NOT_ALLOWED",
        "COMMAND_TIMEOUT": "COMMAND_TIMEOUT",
        "COMMAND_FAILED": "COMMAND_FAILED",
        "WORKING_DIR_NOT_ALLOWED": "WORKING_DIR_NOT_ALLOWED",
        "PATH_NOT_ALLOWED": "PATH_NOT_ALLOWED",
        "FILE_NOT_FOUND": "FILE_NOT_FOUND",
        "FILE_PROTECTED": "FILE_PROTECTED",
        "SEARCH_TARGET_NOT_FOUND": "SEARCH_TARGET_NOT_FOUND",
        "ACTION_TYPE_NOT_ALLOWED": "ACTION_TYPE_NOT_ALLOWED",
        "ACTION_ARGS_INVALID": "ACTION_ARGS_INVALID",
        "ACTION_FAILED": "ACTION_FAILED",
        "ROLE_PERMISSION_DENIED": "ROLE_PERMISSION_DENIED",
        "RETRY_EXHAUSTED": "RETRY_EXHAUSTED",
    }
    return mapping.get(c, "TASK_RUNTIME_ERROR")


def _task_summary_from_verify(verify: dict[str, Any], attempt: int, max_retry: int) -> str:
    ok = bool(verify.get("ok"))
    if ok:
        return f"acceptance command passed on attempt {attempt}"
    code = _normalize_task_error_code(verify.get("error_code"))
    exit_code = verify.get("exit_code")
    return f"acceptance failed: code={code} exit={exit_code} attempt={attempt}/{max_retry}"


def _task_not_found(task_id: str) -> HTTPException:
    return HTTPException(status_code=404, detail={"code": "TASK_NOT_FOUND", "task_id": task_id})


def _group_task_not_found(task_id: str) -> HTTPException:
    return HTTPException(status_code=404, detail={"code": "GROUP_TASK_NOT_FOUND", "task_id": task_id})


def _ensure_group_mode_enabled(settings: Settings) -> None:
    if not bool(getattr(settings, "group_mode_enabled", True)):
        raise HTTPException(status_code=503, detail={"code": "GROUP_MODE_DISABLED", "message": "group mode is disabled by AGUNT_GROUP_MODE_ENABLED"})


def _group_task_public(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_id": task.get("task_id"),
        "goal": task.get("goal"),
        "owner_id": task.get("owner_id"),
        "status": task.get("status"),
        "phase": task.get("phase"),
        "request_id": task.get("request_id"),
        "trace_id": task.get("trace_id"),
        "stop_reason": task.get("stop_reason"),
        "error_code": task.get("error_code"),
        "created_at_ms": task.get("created_at_ms"),
        "updated_at_ms": task.get("updated_at_ms"),
        "finished_at_ms": task.get("finished_at_ms"),
    }


def _group_round_no_from_phase(phase: str) -> int:
    phase_v = str(phase or "").strip().lower()
    if phase_v == GROUP_PHASE_PLANNING:
        return 1
    if phase_v == GROUP_PHASE_DISCUSSION:
        return 2
    if phase_v == GROUP_PHASE_EXECUTION:
        return 3
    if phase_v == GROUP_PHASE_QA_VERIFYING:
        return 4
    if phase_v == GROUP_PHASE_DECISION:
        return 5
    return 0


def _write_group_round(store: Storage | None, task_id: str, phase: str, actor_role: str, note: str | None = None) -> None:
    if store is None:
        return
    fn = getattr(store, "insert_group_task_round", None)
    if callable(fn):
        fn(task_id, _group_round_no_from_phase(phase), phase, actor_role, note)


def _is_group_task_closed(task: dict[str, Any]) -> bool:
    return str(task.get("status") or "") in {GROUP_STATUS_DONE, "cancelled"}


def _normalize_group_phase(v: str) -> str:
    p = str(v or "").strip().lower()
    if p not in GROUP_PHASE_ALLOWED:
        raise ValueError("GROUP_PHASE_NOT_ALLOWED")
    return p


def _group_status_from_phase(phase: str) -> str:
    return (GROUP_STATUS_RUNNING if phase in GROUP_PHASE_RUNNING else GROUP_STATUS_CREATED)


def _normalize_group_actor_role(actor_role: str | None) -> str:
    raw = str(actor_role or "owner").strip().lower()
    role_map = {
        "owner": "owner",
        "rd": "rd",
        "qa": "qa",
        "pm": "pm",
        "ba": "ba",
        "finance": "finance",
        "thinktank": "thinktank",
        "sme": "sme",
        "coder_agent": "rd",
        "reviewer_agent": "qa",
    }
    role = role_map.get(raw)
    if not role:
        raise ValueError("GROUP_ROLE_NOT_ALLOWED")
    return role


def _assert_group_role_can_approve(actor_role: str) -> None:
    if actor_role != "owner":
        raise ValueError("ROLE_PERMISSION_DENIED")


def _assert_group_role_can_rerun(actor_role: str, phase: str) -> None:
    if actor_role == "owner":
        return
    if phase == GROUP_PHASE_EXECUTION and actor_role == "rd":
        return
    if phase == GROUP_PHASE_QA_VERIFYING and actor_role == "qa":
        return
    raise ValueError("ROLE_PERMISSION_DENIED")


def _assert_group_role_can_qa_review(actor_role: str) -> None:
    if actor_role in {"qa", "owner"}:
        return
    raise ValueError("ROLE_PERMISSION_DENIED")


def _assert_group_role_can_cancel(actor_role: str) -> None:
    if actor_role == "owner":
        return
    raise ValueError("ROLE_PERMISSION_DENIED")


def _assert_group_role_can_post_thread(actor_role: str) -> None:
    if actor_role in {"owner", "pm", "rd", "qa", "ba", "finance", "thinktank", "sme"}:
        return
    raise ValueError("ROLE_PERMISSION_DENIED")


def _assert_group_role_can_submit_execution(actor_role: str) -> None:
    if actor_role in {"rd", "owner"}:
        return
    raise ValueError("ROLE_PERMISSION_DENIED")


def _assert_group_owner_identity(task: dict[str, Any], owner_id: str | None) -> None:
    expected = str(task.get("owner_id") or "").strip()
    provided = str(owner_id or "").strip()
    if expected and provided and expected != provided:
        raise ValueError("GROUP_OWNER_MISMATCH")


def _assert_group_role_can_upsert_artifacts(actor_role: str) -> None:
    if actor_role in {"owner", "rd", "qa"}:
        return
    raise ValueError("ROLE_PERMISSION_DENIED")


def _assert_group_phase_transition(current_phase: str, next_phase: str) -> None:
    cur = _normalize_group_phase(current_phase)
    nxt = _normalize_group_phase(next_phase)
    if cur == nxt:
        return
    allowed_next = GROUP_PHASE_TRANSITIONS.get(cur) or set()
    if nxt not in allowed_next:
        raise ValueError(GROUP_ERROR_TRANSITION_NOT_ALLOWED)


def _group_state_machine_view(task: dict[str, Any]) -> dict[str, Any]:
    phase = str(task.get("phase") or GROUP_PHASE_PLANNING)
    status = str(task.get("status") or GROUP_STATUS_CREATED)
    closed = _is_group_task_closed(task)
    allowed_next = sorted(list(GROUP_PHASE_TRANSITIONS.get(phase) or set()))
    if closed:
        allowed_next = []
    can = {
        "owner": {
            "approve": (not closed and phase == GROUP_PHASE_DECISION),
            "rerun": (not closed),
            "cancel": (not closed),
            "execution_submit": (not closed and phase == GROUP_PHASE_EXECUTION),
            "qa_review": (not closed and phase == GROUP_PHASE_QA_VERIFYING),
            "thread_message": (not closed),
        },
        "rd": {
            "execution_submit": (not closed and phase == GROUP_PHASE_EXECUTION),
            "rerun_to_execution": (not closed),
            "thread_message": (not closed),
        },
        "qa": {
            "qa_review": (not closed and phase == GROUP_PHASE_QA_VERIFYING),
            "rerun_to_qa_verifying": (not closed),
            "thread_message": (not closed),
        },
    }
    return {"phase": phase, "status": status, "closed": closed, "allowed_next_phases": allowed_next, "capabilities": can}


def _group_actions_for_role(task: dict[str, Any], actor_role: str) -> dict[str, Any]:
    role = _normalize_group_actor_role(actor_role)
    phase = str(task.get("phase") or GROUP_PHASE_PLANNING)
    closed = _is_group_task_closed(task)
    return {
        "actor_role": role,
        "closed": closed,
        "approve": bool(role == "owner" and (not closed) and phase == GROUP_PHASE_DECISION),
        "cancel": bool(role == "owner" and (not closed)),
        "rerun_allowed": bool(
            (role == "owner" and (not closed))
            or (role == "rd" and (not closed))
            or (role == "qa" and (not closed))
        ),
        "rerun_phases": (
            ([GROUP_PHASE_DISCUSSION, GROUP_PHASE_EXECUTION, GROUP_PHASE_QA_VERIFYING] if role == "owner" else [GROUP_PHASE_EXECUTION] if role == "rd" else [GROUP_PHASE_QA_VERIFYING] if role == "qa" else [])
            if not closed
            else []
        ),
        "execution_submit": bool(role in {"owner", "rd"} and (not closed) and phase == GROUP_PHASE_EXECUTION),
        "qa_review": bool(role in {"owner", "qa"} and (not closed) and phase == GROUP_PHASE_QA_VERIFYING),
        "thread_message": bool((not closed) and role in {"owner", "pm", "rd", "qa", "ba", "finance", "thinktank", "sme"}),
        "artifacts_update": bool(role in {"owner", "rd", "qa"} and (not closed)),
    }


def _group_quality_gate_ci(task_id: str, artifacts: dict[str, Any] | None) -> dict[str, Any]:
    art = artifacts if isinstance(artifacts, dict) else {}
    test_result_raw = str(art.get("test_result") or "").strip()
    test_result = test_result_raw.lower()
    changed_files = art.get("changed_files") if isinstance(art.get("changed_files"), list) else []
    ok_set = {"qa_passed", "passed", "ok", "success", "green"}
    qa_passed = test_result in ok_set
    blocked_reasons: list[str] = []
    if not qa_passed:
        blocked_reasons.append("qa_review_not_passed")
    return {
        "task_id": task_id,
        "status": ("ok" if not blocked_reasons else "fail"),
        "pass": (len(blocked_reasons) == 0),
        "blocked": (len(blocked_reasons) > 0),
        "blocked_reasons": blocked_reasons,
        "test_result": test_result_raw,
        "changed_files_count": len(changed_files),
        "summary_line": f"group_quality_gate: status={'ok' if not blocked_reasons else 'fail'} pass={str(len(blocked_reasons)==0).lower()} test_result={test_result_raw or 'none'} changed_files={len(changed_files)}",
    }


def _safe_project_path(settings: Settings, raw_path: str) -> Path:
    root = Path(str(settings.project_root)).resolve()
    p = Path(raw_path or ".")
    target = (root / p).resolve() if not p.is_absolute() else p.resolve()
    if root != target and root not in target.parents:
        raise ValueError("PATH_NOT_ALLOWED")
    return target


def _normalize_task_paths(settings: Settings, paths: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for it in paths:
        s = str(it or "").strip()
        if not s:
            continue
        p = _safe_project_path(settings, s)
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _is_allowed_working_dir(cwd: Path, scope_paths: list[str], forbidden_paths: list[str]) -> bool:
    for fp in forbidden_paths:
        p = Path(fp)
        if cwd == p or p in cwd.parents:
            return False
    if not scope_paths:
        return True
    for sp in scope_paths:
        p = Path(sp)
        if cwd == p or p in cwd.parents:
            return True
    return False


def _is_allowed_target_path(p: Path, scope_paths: list[str], forbidden_paths: list[str]) -> bool:
    for fp in forbidden_paths:
        f = Path(fp)
        if p == f or f in p.parents:
            return False
    for sp in scope_paths:
        s = Path(sp)
        if p == s or s in p.parents:
            return True
    return False


def _is_default_protected_target_path(p: Path) -> bool:
    name = p.name.lower()
    if name == ".env" or name.startswith(".env."):
        return True
    for pat in _DEFAULT_PROTECTED_FILE_PATTERNS:
        pp = pat.lower()
        if pp in {".env", ".env."}:
            continue
        if pp.startswith(".") and name.endswith(pp):
            return True
        if pp in name:
            return True
    return False


def _normalize_actor_role(actor_role: str | None) -> str:
    r = str(actor_role or "coder_agent").strip().lower()
    if r not in {"coder_agent", "reviewer_agent", "owner"}:
        raise ValueError("ROLE_NOT_ALLOWED")
    return r


def _normalize_actions(actions: list[AgentTaskActionIn], actor_role: str = "coder_agent") -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    role = _normalize_actor_role(actor_role)
    for a in actions:
        t = str(a.type or "").strip().lower()
        if t not in {"read_file", "write_file", "run_command", "search_replace"}:
            raise ValueError("ACTION_TYPE_NOT_ALLOWED")
        if role in {"reviewer_agent", "owner"} and t in {"write_file", "search_replace"}:
            raise ValueError("ROLE_PERMISSION_DENIED")
        out.append({"type": t, "params": dict(a.params or {})})
    return out


def _execute_task_actions(
    settings: Settings,
    actions: list[dict[str, Any]],
    scope_paths: list[str],
    forbidden_paths: list[str],
    working_dir: str,
    actor_role: str = "coder_agent",
) -> dict[str, Any]:
    changed_files: list[str] = []
    action_logs: list[str] = []
    step_results: list[dict[str, Any]] = []
    backups: list[dict[str, Any]] = []
    for idx, action in enumerate(actions, start=1):
        action_started = _now_ms()
        t = str(action.get("type") or "")
        params = action.get("params") if isinstance(action.get("params"), dict) else {}
        role_v = _normalize_actor_role(actor_role)
        if role_v in {"reviewer_agent", "owner"} and t in {"write_file", "search_replace"}:
            return _fail("ROLE_PERMISSION_DENIED", f"role {role_v} cannot execute {t}")

        def _fail(code: str, message: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
            sr = {
                "index": idx,
                "type": t,
                "ok": False,
                "error_code": code,
                "message": message[:500],
                "latency_ms": max(0, _now_ms() - action_started),
            }
            if isinstance(extra, dict):
                sr.update(extra)
            step_results.append(sr)
            return {"ok": False, "error_code": code, "message": message[:500], "step_results": step_results, "backups": backups}

        if t == "read_file":
            fp = str(params.get("file_path") or "").strip()
            if not fp:
                return _fail("ACTION_ARGS_INVALID", "read_file.file_path required")
            try:
                p = _safe_project_path(settings, fp)
            except Exception:
                return _fail("PATH_NOT_ALLOWED", "read_file path outside project")
            if not _is_allowed_target_path(p, scope_paths, forbidden_paths):
                return _fail("PATH_NOT_ALLOWED", "read_file path not in scope")
            if not p.exists() or not p.is_file():
                return _fail("FILE_NOT_FOUND", "read_file target not found")
            _ = p.read_text(encoding="utf-8")
            action_logs.append(f"action_{idx}:read_file:{p}")
            step_results.append({"index": idx, "type": t, "ok": True, "file_path": str(p), "latency_ms": max(0, _now_ms() - action_started)})
            continue

        if t == "write_file":
            fp = str(params.get("file_path") or "").strip()
            content = params.get("content")
            if not fp or not isinstance(content, str):
                return _fail("ACTION_ARGS_INVALID", "write_file args invalid")
            try:
                p = _safe_project_path(settings, fp)
            except Exception:
                return _fail("PATH_NOT_ALLOWED", "write_file path outside project")
            if not _is_allowed_target_path(p, scope_paths, forbidden_paths):
                return _fail("PATH_NOT_ALLOWED", "write_file path not in scope")
            if _is_default_protected_target_path(p):
                return _fail("FILE_PROTECTED", "write_file target is protected by default policy")
            prev_exists = p.exists() and p.is_file()
            prev_content = (p.read_text(encoding="utf-8") if prev_exists else None)
            backups.append({"type": "file", "file_path": str(p), "prev_exists": prev_exists, "prev_content": prev_content})
            p.parent.mkdir(parents=True, exist_ok=True)
            with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(p.parent)) as tmp:
                tmp.write(content)
                tmp_path = Path(tmp.name)
            tmp_path.replace(p)
            changed_files.append(str(p))
            action_logs.append(f"action_{idx}:write_file:{p}")
            step_results.append({"index": idx, "type": t, "ok": True, "file_path": str(p), "latency_ms": max(0, _now_ms() - action_started)})
            continue

        if t == "run_command":
            cmd = str(params.get("command") or "").strip()
            if not cmd:
                return _fail("ACTION_ARGS_INVALID", "run_command.command required")
            res = _run_acceptance_command(settings, cmd, working_dir, scope_paths, forbidden_paths)
            action_logs.append(f"action_{idx}:run_command:{res.get('error_code') or 'OK'}")
            ok = bool(res.get("ok"))
            step_results.append({"index": idx, "type": t, "ok": ok, "command": cmd, "exit_code": res.get("exit_code"), "latency_ms": max(0, _now_ms() - action_started)})
            if not ok:
                return _fail(_normalize_task_error_code(res.get("error_code")), str(res.get("stderr") or "run_command failed"), {"command": cmd, "exit_code": res.get("exit_code")})
            continue

        if t == "search_replace":
            fp = str(params.get("file_path") or "").strip()
            old_str = params.get("old_str")
            new_str = params.get("new_str")
            if (not fp) or (not isinstance(old_str, str)) or (not isinstance(new_str, str)) or (old_str == ""):
                return _fail("ACTION_ARGS_INVALID", "search_replace args invalid")
            try:
                p = _safe_project_path(settings, fp)
            except Exception:
                return _fail("PATH_NOT_ALLOWED", "search_replace path outside project")
            if not _is_allowed_target_path(p, scope_paths, forbidden_paths):
                return _fail("PATH_NOT_ALLOWED", "search_replace path not in scope")
            if _is_default_protected_target_path(p):
                return _fail("FILE_PROTECTED", "search_replace target is protected by default policy")
            if not p.exists() or not p.is_file():
                return _fail("FILE_NOT_FOUND", "search_replace target not found")
            src = p.read_text(encoding="utf-8")
            pos = src.find(old_str)
            if pos < 0:
                return _fail("SEARCH_TARGET_NOT_FOUND", "old_str not found", {"file_path": str(p)})
            backups.append({"type": "file", "file_path": str(p), "prev_exists": True, "prev_content": src})
            out = src[:pos] + new_str + src[pos + len(old_str) :]
            with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(p.parent)) as tmp:
                tmp.write(out)
                tmp_path = Path(tmp.name)
            tmp_path.replace(p)
            changed_files.append(str(p))
            action_logs.append(f"action_{idx}:search_replace:{p}")
            step_results.append({"index": idx, "type": t, "ok": True, "file_path": str(p), "latency_ms": max(0, _now_ms() - action_started)})
            continue

    return {"ok": True, "changed_files": changed_files, "logs": action_logs, "step_results": step_results, "backups": backups}


def _rollback_action_backups(backups: list[dict[str, Any]]) -> list[str]:
    logs: list[str] = []
    for item in reversed(backups):
        if not isinstance(item, dict) or item.get("type") != "file":
            continue
        p = Path(str(item.get("file_path") or ""))
        prev_exists = bool(item.get("prev_exists"))
        prev_content = item.get("prev_content")
        try:
            if prev_exists:
                with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(p.parent)) as tmp:
                    tmp.write(str(prev_content or ""))
                    tmp_path = Path(tmp.name)
                tmp_path.replace(p)
                logs.append(f"rollback_restored:{p}")
            else:
                if p.exists() and p.is_file():
                    p.unlink()
                logs.append(f"rollback_deleted:{p}")
        except Exception as e:
            logs.append(f"rollback_failed:{p}:{e}")
    return logs


def _is_allowed_command_parts(parts: list[str]) -> tuple[bool, str]:
    if not parts:
        return False, "empty command"
    b = parts[0]
    if b in {"python", "python3"}:
        if len(parts) >= 4 and parts[1] == "-m" and parts[2] == "unittest":
            return True, ""
        return False, "python_only_unittest_allowed"
    if b == "pip":
        if len(parts) == 4 and parts[1] == "install" and parts[2] == "-r" and parts[3] and not str(parts[3]).startswith("-"):
            return True, ""
        return False, "pip_only_install_requirements_allowed"
    if b == "git":
        if len(parts) == 2 and parts[1] == "status":
            return True, ""
        return False, "git_only_status_allowed"
    return False, f"bin_not_allowed:{b}"


def _run_acceptance_command(settings: Settings, command: str, working_dir: str, scope_paths: list[str], forbidden_paths: list[str]) -> dict[str, Any]:
    if any(x in command for x in ["&&", "||", ";", "|", "`"]):
        return {"ok": False, "error_code": "COMMAND_NOT_ALLOWED", "stdout": "", "stderr": "complex_shell_not_allowed", "exit_code": -1}
    parts = shlex.split(command)
    if not parts:
        return {"ok": False, "error_code": "COMMAND_EMPTY", "stdout": "", "stderr": "empty command", "exit_code": -1}
    ok_cmd, deny_reason = _is_allowed_command_parts(parts)
    if not ok_cmd:
        return {"ok": False, "error_code": "COMMAND_NOT_ALLOWED", "stdout": "", "stderr": deny_reason, "exit_code": -1}
    try:
        cwd_path = _safe_project_path(settings, working_dir or ".")
    except Exception:
        return {"ok": False, "error_code": "WORKING_DIR_NOT_ALLOWED", "stdout": "", "stderr": "working_dir_outside_project", "exit_code": -1}
    if not _is_allowed_working_dir(cwd_path, scope_paths, forbidden_paths):
        return {"ok": False, "error_code": "WORKING_DIR_NOT_ALLOWED", "stdout": "", "stderr": "working_dir_not_in_scope_or_forbidden", "exit_code": -1}
    cwd = str(cwd_path)
    try:
        proc = subprocess.run(parts, cwd=cwd, capture_output=True, text=True, timeout=120)
        return {
            "ok": proc.returncode == 0,
            "error_code": (None if proc.returncode == 0 else "COMMAND_FAILED"),
            "stdout": (proc.stdout or "")[:8000],
            "stderr": (proc.stderr or "")[:8000],
            "exit_code": int(proc.returncode),
            "command": command,
            "cwd": cwd,
        }
    except subprocess.TimeoutExpired:
        return {"ok": False, "error_code": "COMMAND_TIMEOUT", "stdout": "", "stderr": "timeout", "exit_code": -1, "command": command, "cwd": cwd}


def _run_agent_task(task_id: str, settings: Settings, store: Storage | None = None) -> None:
    with _AGENT_TASKS_LOCK:
        task = _AGENT_TASKS.get(task_id)
        if not isinstance(task, dict) or task.get("status") != "queued":
            return
        task["status"] = "running"
        task["current_step"] = "PLAN"
        task["updated_at_ms"] = _now_ms()
        _task_log(task, "plan_started", store)
        _task_step(store, task, "PLAN", {"task_id": task_id}, {"status": "running"}, True, 0)
        _persist_task(store, task)

    while True:
        with _AGENT_TASKS_LOCK:
            task = _AGENT_TASKS.get(task_id)
            if not isinstance(task, dict) or task.get("status") == "cancelled":
                return
            attempt = int(task.get("attempt") or 0) + 1
            task["attempt"] = attempt
            task["status"] = "verifying"
            task["current_step"] = "VERIFY"
            task["updated_at_ms"] = _now_ms()
            _task_log(task, f"verify_started_attempt_{attempt}", store)
            cmd = str(task.get("acceptance_cmd") or "")
            max_retry = int(task.get("max_retry") or 1)
            dry_run = bool(task.get("dry_run"))
            rollback_on_action_failure = bool(task.get("rollback_on_action_failure", True))
            working_dir = str(task.get("working_dir") or ".")
            scope_paths = list(task.get("scope_paths") or [])
            forbidden_paths = list(task.get("forbidden_paths") or [])
            actions = list(task.get("actions") or [])
            actor_role = str(task.get("actor_role") or "coder_agent")
            actions_done = bool(task.get("actions_done"))
            _persist_task(store, task)

        if not dry_run and (not actions_done):
            with _AGENT_TASKS_LOCK:
                task = _AGENT_TASKS.get(task_id)
                if not isinstance(task, dict) or task.get("status") == "cancelled":
                    return
                task["current_step"] = "ACT"
                task["updated_at_ms"] = _now_ms()
                _task_log(task, "act_started", store)
                _persist_task(store, task)

            act_started = _now_ms()
            act_res = _execute_task_actions(settings, actions, scope_paths, forbidden_paths, working_dir, actor_role=actor_role)
            with _AGENT_TASKS_LOCK:
                task = _AGENT_TASKS.get(task_id)
                if not isinstance(task, dict) or task.get("status") == "cancelled":
                    return
                _task_step(
                    store,
                    task,
                    "ACT",
                    {"actions": actions, "working_dir": working_dir},
                    {"ok": bool(act_res.get("ok")), "error_code": act_res.get("error_code"), "changed_files": act_res.get("changed_files") or [], "step_results": act_res.get("step_results") or []},
                    bool(act_res.get("ok")),
                    max(0, _now_ms() - act_started),
                )
                for sr in list(act_res.get("step_results") or []):
                    idx = int(sr.get("index") or 0)
                    stype = str(sr.get("type") or "unknown").upper()
                    s_ok = bool(sr.get("ok"))
                    s_latency = int(sr.get("latency_ms") or 0)
                    _task_step(
                        store,
                        task,
                        f"ACTION_{stype}",
                        {"index": idx, "type": str(sr.get("type") or ""), "working_dir": working_dir},
                        sr,
                        s_ok,
                        max(0, s_latency),
                    )
                if not bool(act_res.get("ok")):
                    artifacts = task.setdefault("artifacts", {"changed_files": [], "step_results": [], "test_result": None, "summary": ""})
                    artifacts["step_results"] = list(act_res.get("step_results") or [])
                    if rollback_on_action_failure:
                        for lg in _rollback_action_backups(list(act_res.get("backups") or [])):
                            _task_log(task, lg, store)
                    task["status"] = "failed"
                    task["current_step"] = "FINISH"
                    task["stop_reason"] = "action_failed"
                    task["error_code"] = _normalize_task_error_code(act_res.get("error_code") or "ACTION_FAILED")
                    task["finished_at_ms"] = _now_ms()
                    artifacts["test_result"] = "failed"
                    artifacts["summary"] = str(act_res.get("message") or "action failed")[:200]
                    _task_log(task, "act_failed", store)
                    _persist_task(store, task)
                    return
                task["actions_done"] = True
                artifacts = task.setdefault("artifacts", {"changed_files": [], "step_results": [], "test_result": None, "summary": ""})
                artifacts["changed_files"] = list(dict.fromkeys((artifacts.get("changed_files") or []) + list(act_res.get("changed_files") or [])))
                artifacts["step_results"] = list(act_res.get("step_results") or [])
                for m in list(act_res.get("logs") or []):
                    _task_log(task, str(m), store)
                _task_log(task, "act_finished", store)
                _persist_task(store, task)

        verify_started = _now_ms()
        if dry_run:
            verify = {"ok": True, "error_code": None, "stdout": "", "stderr": "", "exit_code": 0, "command": "<dry_run>", "cwd": working_dir}
        else:
            verify = _run_acceptance_command(settings, cmd, working_dir, scope_paths, forbidden_paths)

        with _AGENT_TASKS_LOCK:
            task = _AGENT_TASKS.get(task_id)
            if not isinstance(task, dict) or task.get("status") == "cancelled":
                return
            task["last_verify"] = verify
            task["updated_at_ms"] = _now_ms()
            _task_step(store, task, "VERIFY", {"command": cmd, "working_dir": working_dir}, verify, bool(verify.get("ok")), max(0, _now_ms() - verify_started))
            artifacts = task.setdefault("artifacts", {"changed_files": [], "step_results": [], "test_result": None, "summary": ""})
            if bool(verify.get("ok")):
                task["status"] = "succeeded"
                task["current_step"] = "FINISH"
                task["stop_reason"] = "completed"
                task["error_code"] = None
                task["finished_at_ms"] = _now_ms()
                artifacts["test_result"] = "passed"
                artifacts["summary"] = _task_summary_from_verify(verify, attempt, max_retry)
                _task_log(task, "task_finished", store)
                _persist_task(store, task)
                return
            if attempt < max_retry:
                task["status"] = "retrying"
                task["current_step"] = "RETRY"
                task["error_code"] = _normalize_task_error_code(verify.get("error_code") or "COMMAND_FAILED")
                _task_step(
                    store,
                    task,
                    "RETRY",
                    {"attempt": attempt, "max_retry": max_retry, "error_code": task.get("error_code")},
                    {"status": "retrying"},
                    True,
                    0,
                )
                _task_log(task, f"retrying_attempt_{attempt}", store)
                _persist_task(store, task)
            else:
                task["status"] = "failed"
                task["current_step"] = "FINISH"
                task["stop_reason"] = "retry_exhausted"
                task["error_code"] = _normalize_task_error_code(verify.get("error_code") or "RETRY_EXHAUSTED")
                task["finished_at_ms"] = _now_ms()
                artifacts["test_result"] = "failed"
                artifacts["summary"] = _task_summary_from_verify(verify, attempt, max_retry)
                _task_log(task, "task_failed", store)
                _persist_task(store, task)
                return


@router.post("/api/agent/tasks")
def api_agent_task_create(request: Request, body: AgentTaskCreateIn) -> dict:
    task_id = f"task_{uuid.uuid4().hex[:12]}"
    now = _now_ms()
    settings = _get_settings(request)
    if not bool(getattr(settings, "agent_exec_enabled", True)):
        raise HTTPException(status_code=503, detail={"code": "AGENT_EXEC_DISABLED", "message": "agent execution is disabled by AGUNT_AGENT_EXEC_ENABLED"})
    try:
        scope_paths = _normalize_task_paths(settings, list(body.scope_paths))
        forbidden_paths = _normalize_task_paths(settings, list(body.forbidden_paths))
        actor_role = _normalize_actor_role(body.actor_role)
        actions = _normalize_actions(list(body.actions), actor_role=actor_role)
        default_wd = "apps/server/src" if (Path(str(settings.project_root)).resolve() / "apps/server/src").exists() else "."
        working_dir = str(_safe_project_path(settings, body.working_dir or default_wd))
        if not scope_paths:
            scope_paths = [str(Path(str(settings.project_root)).resolve())]
        if not _is_allowed_working_dir(Path(working_dir), scope_paths, forbidden_paths):
            raise ValueError("WORKING_DIR_NOT_ALLOWED")
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": str(e), "message": "invalid task scope/working_dir"})

    task = {
        "task_id": task_id,
        "status": "queued",
        "goal": body.goal,
        "scope_paths": scope_paths,
        "forbidden_paths": forbidden_paths,
        "acceptance_cmd": body.acceptance_cmd,
        "working_dir": working_dir,
        "max_retry": int(body.max_retry),
        "attempt": 0,
        "dry_run": bool(body.dry_run),
        "rollback_on_action_failure": bool(body.rollback_on_action_failure),
        "actor_role": actor_role,
        "actions": actions,
        "actions_done": False,
        "_step_seq": 0,
        "current_step": "QUEUED",
        "request_id": task_id,
        "trace_id": task_id,
        "stop_reason": None,
        "error_code": None,
        "created_at_ms": now,
        "updated_at_ms": now,
        "finished_at_ms": None,
        "logs": [],
        "last_verify": None,
        "artifacts": {"changed_files": [], "step_results": [], "test_result": None, "summary": ""},
    }
    _task_log(task, "task_created", _get_store(request))
    with _AGENT_TASKS_LOCK:
        _AGENT_TASKS[task_id] = task

    store = _get_store(request)
    _persist_task(store, task)

    if body.auto_start:
        th = threading.Thread(target=_run_agent_task, args=(task_id, settings, store), daemon=True)
        th.start()
    return {"ok": True, **_task_public(task)}


@router.get("/api/agent/tasks")
def api_agent_tasks(request: Request, limit: int = 50) -> dict:
    store = _get_store(request)
    fn = getattr(store, "list_agent_tasks", None)
    if callable(fn):
        rows = fn(limit=max(1, min(int(limit), 200))) or []
        fn_art = getattr(store, "get_agent_task_artifacts", None)
        out: list[dict[str, Any]] = []
        for t in rows:
            task = dict(t)
            task["artifacts"] = (fn_art(task.get("task_id")) if callable(fn_art) else None) or {"changed_files": [], "step_results": [], "test_result": None, "summary": ""}
            fn_logs = getattr(store, "list_agent_task_logs", None)
            if callable(fn_logs):
                task["logs"] = fn_logs(str(task.get("task_id") or ""), limit=500)
            out.append(_task_public(task))
        return {"ok": True, "items": out}
    with _AGENT_TASKS_LOCK:
        rows = sorted(_AGENT_TASKS.values(), key=lambda x: int(x.get("created_at_ms") or 0), reverse=True)
        out = [_task_public(t) for t in rows[: max(1, min(int(limit), 200))]]
        return {"ok": True, "items": out}


@router.get("/api/agent/tasks/{task_id}")
def api_agent_task_get(request: Request, task_id: str) -> dict:
    store = _get_store(request)
    fn = getattr(store, "get_agent_task", None)
    if callable(fn):
        task = fn(task_id)
        if isinstance(task, dict):
            fn_art = getattr(store, "get_agent_task_artifacts", None)
            task["artifacts"] = (fn_art(task_id) if callable(fn_art) else None) or {"changed_files": [], "step_results": [], "test_result": None, "summary": ""}
            fn_logs = getattr(store, "list_agent_task_logs", None)
            if callable(fn_logs):
                task["logs"] = fn_logs(task_id, limit=500)
            return {"ok": True, **_task_public(task)}
    with _AGENT_TASKS_LOCK:
        task = _AGENT_TASKS.get(task_id)
        if not isinstance(task, dict):
            raise _task_not_found(task_id)
        return {"ok": True, **_task_public(task)}


@router.post("/api/group/tasks")
def api_group_task_create(request: Request, body: GroupTaskCreateIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    task_id = f"gtask_{uuid.uuid4().hex[:12]}"
    now = _now_ms()
    task = {
        "task_id": task_id,
        "goal": body.goal,
        "owner_id": body.owner_id,
        "status": GROUP_STATUS_CREATED,
        "phase": GROUP_PHASE_PLANNING,
        "request_id": task_id,
        "trace_id": task_id,
        "stop_reason": None,
        "error_code": None,
        "created_at_ms": now,
        "updated_at_ms": now,
        "finished_at_ms": None,
    }
    store = _get_store(request)
    fn = getattr(store, "insert_group_task", None)
    if not callable(fn):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    fn(task)
    _write_group_round(store, task_id, str(task.get("phase") or GROUP_PHASE_PLANNING), "owner", "task_created")

    fn_msg = getattr(store, "insert_group_agent_message", None)
    if callable(fn_msg):
        fn_msg(task_id, 0, "system", "system", "group task created", "{}")

    fn_art = getattr(store, "upsert_group_artifacts", None)
    if callable(fn_art):
        fn_art(task_id, {"changed_files": [], "test_result": None, "summary": ""})

    return {"ok": True, **_group_task_public(task)}


@router.get("/api/group/tasks")
def api_group_tasks(request: Request, limit: int = 50, status: str | None = None, phase: str | None = None, owner_id: str | None = None, cursor_updated_at_ms: int | None = None) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    fn = getattr(store, "list_group_tasks", None)
    if not callable(fn):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})

    limit_v = max(1, min(int(limit), 200))
    status_v = (str(status).strip() if status is not None else None)
    phase_v = (str(phase).strip() if phase is not None else None)
    owner_v = (str(owner_id).strip() if owner_id is not None else None)
    cursor_v = (None if cursor_updated_at_ms is None else int(cursor_updated_at_ms))

    rows = fn(limit=limit_v, status=status_v, phase=phase_v, owner_id=owner_v, cursor_updated_at_ms=cursor_v) or []
    items = [_group_task_public(dict(r)) for r in rows]

    by_status: dict[str, int] = {}
    for it in items:
        st = str(it.get("status") or "unknown")
        by_status[st] = int(by_status.get(st) or 0) + 1

    next_cursor = None
    if len(items) == limit_v and items:
        last_updated = items[-1].get("updated_at_ms")
        if isinstance(last_updated, int):
            next_cursor = last_updated

    return {
        "ok": True,
        "items": items,
        "total": len(items),
        "stats": {"by_status": by_status},
        "filters": {"status": status_v, "phase": phase_v, "owner_id": owner_v},
        "paging": {"limit": limit_v, "cursor_updated_at_ms": cursor_v, "next_cursor_updated_at_ms": next_cursor},
    }


@router.get("/api/group/tasks/metrics")
def api_group_tasks_metrics(request: Request, owner_id: str | None = None) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    fn = getattr(store, "aggregate_group_tasks", None)
    if callable(fn):
        metrics = fn(owner_id=(str(owner_id).strip() if owner_id is not None else None))
        return {"ok": True, "metrics": metrics}

    fn_list = getattr(store, "list_group_tasks", None)
    if not callable(fn_list):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    rows = fn_list(limit=200, status=None, phase=None, owner_id=(str(owner_id).strip() if owner_id is not None else None), cursor_updated_at_ms=None) or []
    by_status: dict[str, int] = {}
    by_phase: dict[str, int] = {}
    by_owner: dict[str, int] = {}
    for r in rows:
        st = str(r.get("status") or "unknown")
        ph = str(r.get("phase") or "unknown")
        ow = str(r.get("owner_id") or "")
        by_status[st] = int(by_status.get(st) or 0) + 1
        by_phase[ph] = int(by_phase.get(ph) or 0) + 1
        by_owner[ow] = int(by_owner.get(ow) or 0) + 1
    return {"ok": True, "metrics": {"total": len(rows), "by_status": by_status, "by_phase": by_phase, "by_owner": [{"owner_id": k, "count": v} for k, v in sorted(by_owner.items(), key=lambda x: (-x[1], x[0]))[:20]]}}


@router.get("/api/group/tasks/{task_id}/phase-history")
def api_group_task_phase_history(request: Request, task_id: str, limit: int = 2000) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    fn_rounds = getattr(store, "list_group_task_rounds", None)
    if not callable(fn_rounds):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    task = get_group_task_or_raise(store, task_id, not_found=_group_task_not_found)

    rows = fn_rounds(task_id, limit=max(1, min(int(limit), 5000)), cursor=None, order="asc") or []
    items: list[dict[str, Any]] = []
    prev_phase = None
    for r in rows:
        phase = str(r.get("phase") or "")
        if phase != prev_phase:
            items.append({
                "phase": phase,
                "at_ms": int(r.get("created_at_ms") or 0),
                "actor_role": str(r.get("actor_role") or ""),
                "note": str(r.get("note") or ""),
            })
            prev_phase = phase

    return {
        "ok": True,
        "task_id": task_id,
        "current_phase": str(task.get("phase") or ""),
        "total_rounds": len(rows),
        "phase_changes": items,
    }


@router.get("/api/group/meta")
def api_group_meta(request: Request) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    return {
        "ok": True,
        "phases": [GROUP_PHASE_PLANNING, GROUP_PHASE_DISCUSSION, GROUP_PHASE_EXECUTION, GROUP_PHASE_QA_VERIFYING, GROUP_PHASE_DECISION],
        "statuses": [GROUP_STATUS_CREATED, GROUP_STATUS_RUNNING, GROUP_STATUS_DONE, "cancelled"],
        "decision_types": [GROUP_DECISION_TYPE_APPROVE, GROUP_DECISION_TYPE_CANCEL],
        "limits": {
            "thread_limit_max": 5000,
            "rounds_limit_max": 5000,
            "decisions_limit_max": 2000,
            "changed_files_max": 500,
            "summary_max": 8000,
        },
        "roles": ["owner", "pm", "rd", "qa", "ba", "finance", "thinktank", "sme"],
        "phase_transitions": GROUP_PHASE_TRANSITIONS,
    }


@router.get("/api/group/transitions")
def api_group_transitions(request: Request) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    role_next_phases = {
        "owner": [GROUP_PHASE_DISCUSSION, GROUP_PHASE_EXECUTION, GROUP_PHASE_QA_VERIFYING, GROUP_PHASE_DECISION],
        "rd": [GROUP_PHASE_EXECUTION],
        "qa": [GROUP_PHASE_QA_VERIFYING],
        "pm": [],
        "ba": [],
        "finance": [],
        "thinktank": [],
        "sme": [],
    }
    return {"ok": True, "phase_transitions": GROUP_PHASE_TRANSITIONS, "role_next_phases": role_next_phases}


class GroupTasksBatchSummaryIn(BaseModel):
    task_ids: list[str] = Field(default_factory=list, max_length=200)
    actor_role: str = Field(default="owner", min_length=1, max_length=64)


class GroupTasksBatchHealthIn(BaseModel):
    task_ids: list[str] = Field(default_factory=list, max_length=200)


class GroupTasksBatchKpiIn(BaseModel):
    task_ids: list[str] = Field(default_factory=list, max_length=200)
    owner_id: str | None = None


@router.post("/api/group/tasks/health:batch")
def api_group_tasks_health_batch(request: Request, body: GroupTasksBatchHealthIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    fn_get = getattr(store, "get_group_task", None)
    fn_art = getattr(store, "get_group_artifacts", None)
    fn_thread = getattr(store, "list_group_agent_messages", None)
    fn_decisions = getattr(store, "list_group_decisions", None)
    fn_rounds = getattr(store, "list_group_task_rounds", None)
    if not callable(fn_get):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})

    ids = [str(x).strip() for x in list(body.task_ids or []) if str(x).strip()][:200]
    items: list[dict[str, Any]] = []
    for tid in ids:
        task = fn_get(tid)
        if not isinstance(task, dict):
            continue
        artifacts = (fn_art(tid) if callable(fn_art) else None) or {"changed_files": [], "test_result": None, "summary": ""}
        quality_gate_ci = _group_quality_gate_ci(tid, artifacts)
        phase = str(task.get("phase") or "")
        closed = _is_group_task_closed(task)
        has_thread = bool((fn_thread(tid, limit=1, cursor=None, order="asc", role=None) if callable(fn_thread) else []) or [])
        has_decisions = bool((fn_decisions(tid, limit=1, cursor=None, order="asc") if callable(fn_decisions) else []) or [])
        has_rounds = bool((fn_rounds(tid, limit=1, cursor=None, order="asc") if callable(fn_rounds) else []) or [])
        approve_ready = bool((phase == GROUP_PHASE_DECISION) and (not closed) and (not bool(quality_gate_ci.get("blocked"))))
        blockers: list[str] = []
        if closed:
            blockers.append("GROUP_TASK_CLOSED")
        if bool(quality_gate_ci.get("blocked")):
            blockers.append(GROUP_ERROR_QUALITY_GATE_BLOCKED)
        if not has_thread:
            blockers.append("GROUP_EXPORT_THREAD_EMPTY")
        if not has_decisions:
            blockers.append("GROUP_EXPORT_DECISIONS_EMPTY")
        if not has_rounds:
            blockers.append("GROUP_EXPORT_ROUNDS_EMPTY")
        items.append({"task_id": tid, "phase": phase, "status": str(task.get("status") or ""), "approve_ready": approve_ready, "blockers": blockers})

    return {"ok": True, "total": len(items), "items": items}


@router.post("/api/group/tasks/summary:batch")
def api_group_tasks_summary_batch(request: Request, body: GroupTasksBatchSummaryIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    fn_get = getattr(store, "get_group_task", None)
    fn_art = getattr(store, "get_group_artifacts", None)
    if not callable(fn_get):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})

    actor_role = _normalize_group_actor_role(body.actor_role)
    items: list[dict[str, Any]] = []
    ids = [str(x).strip() for x in list(body.task_ids or []) if str(x).strip()]
    ids = ids[:200]
    for tid in ids:
        task = fn_get(tid)
        if not isinstance(task, dict):
            continue
        artifacts = (fn_art(tid) if callable(fn_art) else None) or {"changed_files": [], "test_result": None, "summary": ""}
        quality_gate_ci = _group_quality_gate_ci(tid, artifacts)
        approve_ready = bool(
            (str(task.get("phase") or "") == GROUP_PHASE_DECISION)
            and (not _is_group_task_closed(task))
            and (not bool(quality_gate_ci.get("blocked")))
        )
        items.append({
            "task": _group_task_public(task),
            "kpi": {
                "changed_files_count": len(list(artifacts.get("changed_files") or [])),
                "quality_gate_pass": not bool(quality_gate_ci.get("blocked")),
                "approve_ready": approve_ready,
            },
            "actions": _group_actions_for_role(task, actor_role),
        })

    return {"ok": True, "actor_role": actor_role, "total": len(items), "items": items}


@router.get("/api/group/tasks/summary")
def api_group_tasks_summary(request: Request, limit: int = 20, status: str | None = None, phase: str | None = None, owner_id: str | None = None, actor_role: str = "owner") -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)

    try:
        role_v = _normalize_group_actor_role(actor_role)
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": str(e), "actor_role": str(actor_role or "")})

    rows = list_group_tasks_or_raise(
        store,
        limit=limit,
        status=status,
        phase=phase,
        owner_id=owner_id,
    )

    out: list[dict[str, Any]] = []
    for row in rows:
        task = dict(row)
        task_id = str(task.get("task_id") or "")
        artifacts = get_group_artifacts_or_default(store, task_id)
        qg = _group_quality_gate_ci(task_id, artifacts)
        out.append(
            {
                "task": _group_task_public(task),
                "kpi": {
                    "changed_files_count": len(list(artifacts.get("changed_files") or [])),
                    "quality_gate_pass": bool(qg.get("pass")),
                    "approve_ready": bool((str(task.get("phase") or "") == GROUP_PHASE_DECISION) and (not _is_group_task_closed(task)) and (not bool(qg.get("blocked")))),
                },
                "actions": _group_actions_for_role(task, role_v),
            }
        )

    return {"ok": True, "items": out, "total": len(out), "actor_role": role_v}


@router.get("/api/group/tasks/{task_id}")
def api_group_task_get(request: Request, task_id: str) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    task = get_group_task_or_raise(store, task_id, not_found=_group_task_not_found)
    return {"ok": True, **_group_task_public(task)}


@router.get("/api/group/tasks/{task_id}/state-machine")
def api_group_task_state_machine(request: Request, task_id: str) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    task = get_group_task_or_raise(store, task_id, not_found=_group_task_not_found)
    return {"ok": True, "task_id": task_id, "state_machine": _group_state_machine_view(task)}


@router.get("/api/group/tasks/{task_id}/actions")
def api_group_task_actions(request: Request, task_id: str, actor_role: str = "owner") -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    task = get_group_task_or_raise(store, task_id, not_found=_group_task_not_found)
    try:
        actions = _group_actions_for_role(task, actor_role)
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": str(e), "actor_role": str(actor_role or "")})
    return {"ok": True, "task_id": task_id, "phase": str(task.get("phase") or ""), "status": str(task.get("status") or ""), "actions": actions}


@router.post("/api/group/tasks/{task_id}/thread/messages:precheck")
def api_group_task_thread_message_precheck(request: Request, task_id: str, body: GroupTaskThreadMessageIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_task_thread_message_precheck(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        normalize_role=_normalize_group_actor_role,
        assert_can_post_thread=_assert_group_role_can_post_thread,
        is_closed=_is_group_task_closed,
        err_task_closed=GROUP_ERROR_TASK_CLOSED,
    )


@router.post("/api/group/tasks/{task_id}/thread/messages")
def api_group_task_thread_message(request: Request, task_id: str, body: GroupTaskThreadMessageIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    fn_put = getattr(store, "insert_group_task", None)
    if not callable(fn_put):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    return handle_group_task_thread_message(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        normalize_role=_normalize_group_actor_role,
        assert_can_post_thread=_assert_group_role_can_post_thread,
        status_done=GROUP_STATUS_DONE,
        status_cancelled="cancelled",
        err_task_closed=GROUP_ERROR_TASK_CLOSED,
        phase_planning=GROUP_PHASE_PLANNING,
        phase_discussion=GROUP_PHASE_DISCUSSION,
        status_created=GROUP_STATUS_CREATED,
        now_ms=_now_ms,
        put_task=lambda _, t: fn_put(t),
        write_round=_write_group_round,
        public_task=_group_task_public,
    )


@router.get("/api/group/tasks/{task_id}/thread")
def api_group_task_thread(request: Request, task_id: str, limit: int = 500, cursor: int | None = None, order: str = "asc", role: str | None = None) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    _ = get_group_task_or_raise(store, task_id, not_found=_group_task_not_found)

    limit_v = max(1, min(int(limit), 2000))
    order_v = ("desc" if str(order).strip().lower() == "desc" else "asc")
    cursor_v = (None if cursor is None else int(cursor))
    role_v = (str(role).strip() if role is not None and str(role).strip() else None)

    fn = getattr(store, "list_group_agent_messages", None)
    if not callable(fn):
        return {"ok": True, "task_id": task_id, "items": [], "paging": {"limit": limit_v, "order": order_v, "cursor": cursor_v, "next_cursor": None, "role": role_v}}

    items = fn(task_id, limit=limit_v, cursor=cursor_v, order=order_v, role=role_v)
    next_cursor = None
    if len(items) == limit_v and items:
        last_cursor = items[-1].get("_cursor")
        if isinstance(last_cursor, int):
            next_cursor = last_cursor

    return {
        "ok": True,
        "task_id": task_id,
        "items": items,
        "paging": {"limit": limit_v, "order": order_v, "cursor": cursor_v, "next_cursor": next_cursor, "role": role_v},
    }


@router.get("/api/group/tasks/{task_id}/artifacts")
def api_group_task_artifacts(request: Request, task_id: str) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    _ = get_group_task_or_raise(store, task_id, not_found=_group_task_not_found)
    artifacts = get_group_artifacts_or_default(store, task_id)
    return {"ok": True, "task_id": task_id, "artifacts": artifacts}


@router.post("/api/group/tasks/{task_id}/artifacts/update:precheck")
def api_group_task_artifacts_update_precheck(request: Request, task_id: str, body: GroupTaskArtifactsUpsertIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_task_artifacts_update_precheck(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        normalize_role=_normalize_group_actor_role,
        assert_can_upsert_artifacts=_assert_group_role_can_upsert_artifacts,
        is_closed=_is_group_task_closed,
        err_task_closed=GROUP_ERROR_TASK_CLOSED,
    )


@router.post("/api/group/tasks/{task_id}/artifacts/update")
def api_group_task_artifacts_update(request: Request, task_id: str, body: GroupTaskArtifactsUpsertIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_task_artifacts_update(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        get_artifacts_default=get_group_artifacts_or_default,
        is_closed=_is_group_task_closed,
        normalize_role=_normalize_group_actor_role,
        assert_can_upsert_artifacts=_assert_group_role_can_upsert_artifacts,
        err_task_closed=GROUP_ERROR_TASK_CLOSED,
        round_no_from_phase=_group_round_no_from_phase,
    )


@router.get("/api/group/tasks/{task_id}/rounds")
def api_group_task_rounds(request: Request, task_id: str, limit: int = 500, cursor: int | None = None, order: str = "asc") -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    _ = get_group_task_or_raise(store, task_id, not_found=_group_task_not_found)

    limit_v = max(1, min(int(limit), 2000))
    order_v = ("desc" if str(order).strip().lower() == "desc" else "asc")
    cursor_v = (None if cursor is None else int(cursor))

    fn = getattr(store, "list_group_task_rounds", None)
    items = (fn(task_id, limit=limit_v, cursor=cursor_v, order=order_v) if callable(fn) else []) or []
    next_cursor = None
    if len(items) == limit_v and items:
        last_cursor = items[-1].get("_cursor")
        if isinstance(last_cursor, int):
            next_cursor = last_cursor

    return {"ok": True, "task_id": task_id, "items": items, "paging": {"limit": limit_v, "order": order_v, "cursor": cursor_v, "next_cursor": next_cursor}}


@router.get("/api/group/tasks/{task_id}/decisions")
def api_group_task_decisions(request: Request, task_id: str, limit: int = 100, cursor: int | None = None, order: str = "asc") -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    _ = get_group_task_or_raise(store, task_id, not_found=_group_task_not_found)

    limit_v = max(1, min(int(limit), 2000))
    order_v = ("desc" if str(order).strip().lower() == "desc" else "asc")
    cursor_v = (None if cursor is None else int(cursor))

    fn = getattr(store, "list_group_decisions", None)
    items = (fn(task_id, limit=limit_v, cursor=cursor_v, order=order_v) if callable(fn) else []) or []
    next_cursor = None
    if len(items) == limit_v and items:
        last_cursor = items[-1].get("_cursor")
        if isinstance(last_cursor, int):
            next_cursor = last_cursor

    return {"ok": True, "task_id": task_id, "items": items, "paging": {"limit": limit_v, "order": order_v, "cursor": cursor_v, "next_cursor": next_cursor}}


@router.get("/api/group/tasks/{task_id}/timeline")
def api_group_task_timeline(request: Request, task_id: str, rounds_limit: int = 200, thread_limit: int = 200, decisions_limit: int = 100) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    task = get_group_task_or_raise(store, task_id, not_found=_group_task_not_found)

    fn_rounds = getattr(store, "list_group_task_rounds", None)
    rounds = (fn_rounds(task_id, limit=max(1, min(int(rounds_limit), 2000)), cursor=None, order="asc") if callable(fn_rounds) else []) or []

    fn_thread = getattr(store, "list_group_agent_messages", None)
    thread = (fn_thread(task_id, limit=max(1, min(int(thread_limit), 2000)), cursor=None, order="asc", role=None) if callable(fn_thread) else []) or []

    fn_decisions = getattr(store, "list_group_decisions", None)
    decisions = (fn_decisions(task_id, limit=max(1, min(int(decisions_limit), 1000)), cursor=None, order="asc") if callable(fn_decisions) else []) or []

    last_round = (rounds[-1] if rounds else None)
    last_message = (thread[-1] if thread else None)
    last_decision = (decisions[-1] if decisions else None)

    return {
        "ok": True,
        "task": _group_task_public(task),
        "counts": {"rounds": len(rounds), "thread": len(thread), "decisions": len(decisions)},
        "last": {"round": last_round, "message": last_message, "decision": last_decision},
        "rounds": rounds,
        "thread": thread,
        "decisions": decisions,
    }


@router.get("/api/group/tasks/{task_id}/export")
def api_group_task_export(request: Request, task_id: str, thread_limit: int = 2000, decisions_limit: int = 500, rounds_limit: int = 2000) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_task_export(
        task_id=task_id,
        thread_limit=thread_limit,
        decisions_limit=decisions_limit,
        rounds_limit=rounds_limit,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        get_artifacts=get_group_artifacts_or_default,
        public_task=_group_task_public,
    )


@router.post("/api/group/tasks/{task_id}/export/verify")
def api_group_task_export_verify(request: Request, task_id: str, body: GroupTaskExportVerifyIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    out = api_group_task_export(
        request=request,
        task_id=task_id,
        thread_limit=int(body.thread_limit),
        decisions_limit=int(body.decisions_limit),
        rounds_limit=int(body.rounds_limit),
    )
    actual = str(out.get("export_signature") or "")
    expected = str(body.expected_signature or "").strip().lower()
    matched = bool(actual) and (actual == expected)
    return {
        "ok": True,
        "task_id": task_id,
        "matched": matched,
        "expected_signature": expected,
        "actual_signature": actual,
        "signature_algo": str(out.get("export_signature_algo") or "sha256-stable-json-v1"),
    }


@router.get("/api/group/tasks/{task_id}/audit")
def api_group_task_audit(request: Request, task_id: str, thread_limit: int = 2000, decisions_limit: int = 500, rounds_limit: int = 2000) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_task_audit(
        task_id=task_id,
        thread_limit=thread_limit,
        decisions_limit=decisions_limit,
        rounds_limit=rounds_limit,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        get_artifacts=get_group_artifacts_or_default,
        public_task=_group_task_public,
    )


@router.get("/api/group/tasks/{task_id}/trace-map")
def api_group_task_trace_map(request: Request, task_id: str, limit: int = 5000) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_task_trace_map(
        task_id=task_id,
        limit=limit,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
    )


@router.get("/api/group/tasks/{task_id}/audit-closure")
def api_group_task_audit_closure(request: Request, task_id: str) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    export_out = api_group_task_export(request, task_id, thread_limit=2000, decisions_limit=500, rounds_limit=2000)
    audit_out = api_group_task_audit(request, task_id, thread_limit=2000, decisions_limit=500, rounds_limit=2000)
    trace_out = api_group_task_trace_map(request, task_id, limit=5000)
    return handle_group_task_audit_closure(task_id=task_id, export_out=export_out, audit_out=audit_out, trace_out=trace_out)


@router.get("/api/group/tasks/{task_id}/readiness")
def api_group_task_readiness(request: Request, task_id: str) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    task = get_group_task_or_raise(store, task_id, not_found=_group_task_not_found)

    fn_art = getattr(store, "get_group_artifacts", None)
    fn_thread = getattr(store, "list_group_agent_messages", None)
    fn_decisions = getattr(store, "list_group_decisions", None)
    fn_rounds = getattr(store, "list_group_task_rounds", None)

    artifacts = (fn_art(task_id) if callable(fn_art) else None) or {"changed_files": [], "test_result": None, "summary": ""}
    quality_gate_ci = _group_quality_gate_ci(task_id, artifacts)

    closed = _is_group_task_closed(task)
    phase = str(task.get("phase") or "")
    status = str(task.get("status") or "")

    has_thread = bool((fn_thread(task_id, limit=1, cursor=None, order="asc", role=None) if callable(fn_thread) else []) or [])
    has_decisions = bool((fn_decisions(task_id, limit=1, cursor=None, order="asc") if callable(fn_decisions) else []) or [])
    has_rounds = bool((fn_rounds(task_id, limit=1, cursor=None, order="asc") if callable(fn_rounds) else []) or [])

    approve_ready = bool((phase == GROUP_PHASE_DECISION) and (not closed) and (not bool(quality_gate_ci.get("blocked"))))
    qa_review_ready = bool((phase == GROUP_PHASE_QA_VERIFYING) and (not closed))
    execution_submit_ready = bool((phase == GROUP_PHASE_EXECUTION) and (not closed))
    export_ready = bool((not bool(quality_gate_ci.get("blocked"))) and has_thread and has_decisions and has_rounds)

    return {
        "ok": True,
        "task_id": task_id,
        "phase": phase,
        "status": status,
        "closed": closed,
        "quality_gate_ci": quality_gate_ci,
        "readiness": {
            "execution_submit_ready": execution_submit_ready,
            "qa_review_ready": qa_review_ready,
            "approve_ready": approve_ready,
            "export_ready": export_ready,
        },
        "signals": {"has_thread": has_thread, "has_decisions": has_decisions, "has_rounds": has_rounds},
    }


@router.get("/api/group/tasks/{task_id}/blockers")
def api_group_task_blockers(request: Request, task_id: str) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    task = get_group_task_or_raise(store, task_id, not_found=_group_task_not_found)

    fn_art = getattr(store, "get_group_artifacts", None)
    fn_thread = getattr(store, "list_group_agent_messages", None)
    fn_decisions = getattr(store, "list_group_decisions", None)
    fn_rounds = getattr(store, "list_group_task_rounds", None)

    artifacts = (fn_art(task_id) if callable(fn_art) else None) or {"changed_files": [], "test_result": None, "summary": ""}
    quality_gate_ci = _group_quality_gate_ci(task_id, artifacts)
    closed = _is_group_task_closed(task)
    phase = str(task.get("phase") or "")

    has_thread = bool((fn_thread(task_id, limit=1, cursor=None, order="asc", role=None) if callable(fn_thread) else []) or [])
    has_decisions = bool((fn_decisions(task_id, limit=1, cursor=None, order="asc") if callable(fn_decisions) else []) or [])
    has_rounds = bool((fn_rounds(task_id, limit=1, cursor=None, order="asc") if callable(fn_rounds) else []) or [])

    blockers: dict[str, list[str]] = {
        "execution_submit": [],
        "qa_review": [],
        "approve": [],
        "export": [],
    }
    if closed:
        for k in blockers.keys():
            blockers[k].append("GROUP_TASK_CLOSED")

    if phase != GROUP_PHASE_EXECUTION:
        blockers["execution_submit"].append("GROUP_EXECUTION_PHASE_INVALID")
    if phase != GROUP_PHASE_QA_VERIFYING:
        blockers["qa_review"].append(GROUP_ERROR_QA_PHASE_INVALID)
    if phase != GROUP_PHASE_DECISION:
        blockers["approve"].append(GROUP_ERROR_APPROVE_PHASE_INVALID)

    if bool(quality_gate_ci.get("blocked")):
        blockers["approve"].append(GROUP_ERROR_QUALITY_GATE_BLOCKED)
        blockers["export"].append(GROUP_ERROR_QUALITY_GATE_BLOCKED)
    if not has_thread:
        blockers["export"].append("GROUP_EXPORT_THREAD_EMPTY")
    if not has_decisions:
        blockers["export"].append("GROUP_EXPORT_DECISIONS_EMPTY")
    if not has_rounds:
        blockers["export"].append("GROUP_EXPORT_ROUNDS_EMPTY")

    return {
        "ok": True,
        "task_id": task_id,
        "phase": phase,
        "closed": closed,
        "blockers": blockers,
        "quality_gate_ci": quality_gate_ci,
    }


@router.get("/api/group/tasks/{task_id}/integrity")
def api_group_task_integrity(request: Request, task_id: str) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    task = get_group_task_or_raise(store, task_id, not_found=_group_task_not_found)

    fn_thread = getattr(store, "list_group_agent_messages", None)
    fn_decisions = getattr(store, "list_group_decisions", None)
    fn_rounds = getattr(store, "list_group_task_rounds", None)

    thread_items = (fn_thread(task_id, limit=5000, cursor=None, order="asc", role=None) if callable(fn_thread) else []) or []
    decision_items = (fn_decisions(task_id, limit=2000, cursor=None, order="asc") if callable(fn_decisions) else []) or []
    rounds_items = (fn_rounds(task_id, limit=5000, cursor=None, order="asc") if callable(fn_rounds) else []) or []
    artifacts = get_group_artifacts_or_default(store, task_id)

    payload = {"task": _group_task_public(task), "thread": thread_items, "rounds": rounds_items, "decisions": decision_items, "artifacts": artifacts}
    integrity_hash = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()

    phase = str(task.get("phase") or "")
    status = str(task.get("status") or "")
    closed = _is_group_task_closed(task)
    quality_gate_ci = _group_quality_gate_ci(task_id, artifacts)
    phase_valid = phase in GROUP_PHASE_ALLOWED
    status_valid = status in {GROUP_STATUS_CREATED, GROUP_STATUS_RUNNING, GROUP_STATUS_DONE, "cancelled"}
    closed_consistent = (closed and status in {GROUP_STATUS_DONE, "cancelled"}) or ((not closed) and status in {GROUP_STATUS_CREATED, GROUP_STATUS_RUNNING})

    return {
        "ok": True,
        "task_id": task_id,
        "integrity_hash": integrity_hash,
        "integrity_hash_algo": "sha256-stable-json-v1",
        "signals": {
            "phase_valid": phase_valid,
            "status_valid": status_valid,
            "closed_consistent": closed_consistent,
            "has_thread": len(thread_items) > 0,
            "has_decisions": len(decision_items) > 0,
            "has_rounds": len(rounds_items) > 0,
            "quality_gate_blocked": bool(quality_gate_ci.get("blocked")),
        },
    }


@router.get("/api/group/tasks/{task_id}/health")
def api_group_task_health(request: Request, task_id: str) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    fn_task = getattr(store, "get_group_task", None)
    if not callable(fn_task):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    task = fn_task(task_id)
    if not isinstance(task, dict):
        raise _group_task_not_found(task_id)

    fn_art = getattr(store, "get_group_artifacts", None)
    fn_thread = getattr(store, "list_group_agent_messages", None)
    fn_decisions = getattr(store, "list_group_decisions", None)
    fn_rounds = getattr(store, "list_group_task_rounds", None)

    artifacts = (fn_art(task_id) if callable(fn_art) else None) or {"changed_files": [], "test_result": None, "summary": ""}
    quality_gate_ci = _group_quality_gate_ci(task_id, artifacts)

    thread_n = len((fn_thread(task_id, limit=1, cursor=None, order="asc", role=None) if callable(fn_thread) else []) or [])
    decisions_n = len((fn_decisions(task_id, limit=1, cursor=None, order="asc") if callable(fn_decisions) else []) or [])
    rounds_n = len((fn_rounds(task_id, limit=1, cursor=None, order="asc") if callable(fn_rounds) else []) or [])

    phase = str(task.get("phase") or "")
    status = str(task.get("status") or "")
    closed = _is_group_task_closed(task)
    approve_ready = bool((phase == GROUP_PHASE_DECISION) and (not closed) and (not bool(quality_gate_ci.get("blocked"))))

    blockers: list[str] = []
    if closed:
        blockers.append("GROUP_TASK_CLOSED")
    if bool(quality_gate_ci.get("blocked")):
        blockers.append(GROUP_ERROR_QUALITY_GATE_BLOCKED)
    if thread_n == 0:
        blockers.append("GROUP_EXPORT_THREAD_EMPTY")
    if decisions_n == 0:
        blockers.append("GROUP_EXPORT_DECISIONS_EMPTY")
    if rounds_n == 0:
        blockers.append("GROUP_EXPORT_ROUNDS_EMPTY")

    return {
        "ok": True,
        "task_id": task_id,
        "phase": phase,
        "status": status,
        "closed": closed,
        "approve_ready": approve_ready,
        "quality_gate_ci": quality_gate_ci,
        "signals": {"has_thread": thread_n > 0, "has_decisions": decisions_n > 0, "has_rounds": rounds_n > 0},
        "blockers": blockers,
    }


@router.get("/api/group/tasks/{task_id}/prechecks")
def api_group_task_prechecks(request: Request, task_id: str, actor_role: str = "owner", owner_id: str = "owner", next_phase: str | None = None) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_task_prechecks(
        task_id=task_id,
        actor_role=actor_role,
        owner_id=owner_id,
        next_phase=next_phase,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        is_closed=_is_group_task_closed,
        normalize_role=_normalize_group_actor_role,
        normalize_phase=_normalize_group_phase,
        assert_can_approve=_assert_group_role_can_approve,
        assert_owner=_assert_group_owner_identity,
        assert_can_rerun=_assert_group_role_can_rerun,
        assert_transition=_assert_group_phase_transition,
        assert_can_submit_execution=_assert_group_role_can_submit_execution,
        assert_can_qa_review=_assert_group_role_can_qa_review,
        assert_can_post_thread=_assert_group_role_can_post_thread,
        assert_can_upsert_artifacts=_assert_group_role_can_upsert_artifacts,
        assert_can_cancel=_assert_group_role_can_cancel,
        get_artifacts=get_group_artifacts_or_default,
        quality_gate_ci=_group_quality_gate_ci,
        phase_decision=GROUP_PHASE_DECISION,
        phase_execution=GROUP_PHASE_EXECUTION,
        phase_qa_verifying=GROUP_PHASE_QA_VERIFYING,
        phase_planning=GROUP_PHASE_PLANNING,
        err_task_closed=GROUP_ERROR_TASK_CLOSED,
        err_approve_phase_invalid=GROUP_ERROR_APPROVE_PHASE_INVALID,
        err_qa_phase_invalid=GROUP_ERROR_QA_PHASE_INVALID,
        err_quality_gate_blocked=GROUP_ERROR_QUALITY_GATE_BLOCKED,
        err_exec_phase_invalid="GROUP_EXECUTION_PHASE_INVALID",
    )


@router.get("/api/group/tasks/prechecks:batch")
def api_group_tasks_prechecks_batch(request: Request, task_ids: str, actor_role: str = "owner", owner_id: str = "owner", next_phase: str | None = None) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    return handle_group_tasks_prechecks_batch(
        task_ids=task_ids,
        actor_role=actor_role,
        owner_id=owner_id,
        next_phase=next_phase,
        fetch_prechecks=lambda tid, r, oid, p: api_group_task_prechecks(request, tid, actor_role=r, owner_id=oid, next_phase=p),
    )


@router.get("/api/group/tasks/{task_id}/can")
def api_group_task_can(request: Request, task_id: str, actor_role: str = "owner", owner_id: str = "owner", next_phase: str | None = None) -> dict:
    return handle_group_task_can(
        task_id=task_id,
        actor_role=actor_role,
        owner_id=owner_id,
        next_phase=next_phase,
        fetch_prechecks=lambda tid, r, oid, p: api_group_task_prechecks(request, tid, actor_role=r, owner_id=oid, next_phase=p),
    )


@router.get("/api/group/tasks/can:batch")
def api_group_tasks_can_batch(request: Request, task_ids: str, actor_role: str = "owner", owner_id: str = "owner", next_phase: str | None = None) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    return handle_group_tasks_can_batch(
        task_ids=task_ids,
        actor_role=actor_role,
        owner_id=owner_id,
        next_phase=next_phase,
        fetch_can=lambda tid, r, oid, p: api_group_task_can(request, tid, actor_role=r, owner_id=oid, next_phase=p),
    )


@router.get("/api/group/tasks/{task_id}/quality-gate")
def api_group_task_quality_gate(request: Request, task_id: str) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    fn_task = getattr(store, "get_group_task", None)
    if not callable(fn_task):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    task = fn_task(task_id)
    if not isinstance(task, dict):
        raise _group_task_not_found(task_id)

    artifacts = get_group_artifacts_or_default(store, task_id)
    quality_gate_ci = _group_quality_gate_ci(task_id, artifacts)

    approve_ready = bool(
        (str(task.get("phase") or "") == GROUP_PHASE_DECISION)
        and (not _is_group_task_closed(task))
        and (not bool(quality_gate_ci.get("blocked")))
    )

    return {
        "ok": True,
        "task_id": task_id,
        "phase": str(task.get("phase") or ""),
        "status": str(task.get("status") or ""),
        "approve_ready": approve_ready,
        "quality_gate_ci": quality_gate_ci,
    }


@router.get("/api/group/tasks/{task_id}/kpi")
def api_group_task_kpi(request: Request, task_id: str) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_task_kpi(
        task_id=task_id,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        get_artifacts=get_group_artifacts_or_default,
        quality_gate_ci=_group_quality_gate_ci,
        is_task_closed=_is_group_task_closed,
    )


@router.post("/api/group/tasks/kpi:batch")
def api_group_tasks_kpi_batch(request: Request, body: GroupTasksBatchKpiIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    return handle_group_tasks_kpi_batch(list(body.task_ids or []), body.owner_id, lambda tid: api_group_task_kpi(request, tid))


@router.get("/api/group/tasks/kpi:batch")
def api_group_tasks_kpi_batch_get(request: Request, task_ids: str, owner_id: str | None = None) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    return handle_group_tasks_kpi_batch_get(task_ids, owner_id, lambda tid: api_group_task_kpi(request, tid))


@router.get("/api/group/tasks/kpi:leaderboard")
def api_group_tasks_kpi_leaderboard(request: Request, top: int = 20, sort_by: str = "changed_files_count", owner_id: str | None = None, status: str | None = None, phase: str | None = None) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_tasks_kpi_leaderboard(
        top=top,
        sort_by=sort_by,
        owner_id=owner_id,
        status=status,
        phase=phase,
        list_tasks=lambda **kw: list_group_tasks_or_raise(store, **kw),
        fetch_kpi=lambda tid: api_group_task_kpi(request, tid),
    )


@router.get("/api/group/tasks/kpi:distribution")
def api_group_tasks_kpi_distribution(request: Request, owner_id: str | None = None, status: str | None = None, phase: str | None = None) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_tasks_kpi_distribution(
        owner_id=owner_id,
        status=status,
        phase=phase,
        list_tasks=lambda **kw: list_group_tasks_or_raise(store, **kw),
        fetch_kpi=lambda tid: api_group_task_kpi(request, tid),
    )


@router.get("/api/group/tasks/kpi:alerts")
def api_group_tasks_kpi_alerts(request: Request, limit: int = 50, owner_id: str | None = None, status: str | None = None, phase: str | None = None) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_tasks_kpi_alerts(
        limit=limit,
        owner_id=owner_id,
        status=status,
        phase=phase,
        list_tasks=lambda **kw: list_group_tasks_or_raise(store, **kw),
        fetch_kpi=lambda tid: api_group_task_kpi(request, tid),
    )


@router.get("/api/group/tasks/kpi:overview")
def api_group_tasks_kpi_overview(request: Request, owner_id: str | None = None, status: str | None = None, phase: str | None = None) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_tasks_kpi_overview(
        owner_id=owner_id,
        status=status,
        phase=phase,
        list_tasks=lambda **kw: list_group_tasks_or_raise(store, **kw),
        fetch_kpi=lambda tid: api_group_task_kpi(request, tid),
    )


@router.get("/api/group/tasks/kpi:owners")
def api_group_tasks_kpi_owners(request: Request, top: int = 20, min_tasks: int = 1, status: str | None = None, phase: str | None = None) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_tasks_kpi_owners(
        top=top,
        min_tasks=min_tasks,
        status=status,
        phase=phase,
        list_tasks=lambda **kw: list_group_tasks_or_raise(store, **kw),
        fetch_kpi=lambda tid: api_group_task_kpi(request, tid),
    )


@router.get("/api/group/tasks/{task_id}/summary")
def api_group_task_summary(request: Request, task_id: str) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    task = get_group_task_or_raise(store, task_id, not_found=_group_task_not_found)
    artifacts = get_group_artifacts_or_default(store, task_id)
    fn_rounds = getattr(store, "list_group_task_rounds", None)
    rounds = (fn_rounds(task_id, limit=20, cursor=None, order="desc") if callable(fn_rounds) else []) or []
    fn_decisions = getattr(store, "list_group_decisions", None)
    decisions = (fn_decisions(task_id, limit=20, cursor=None, order="desc") if callable(fn_decisions) else []) or []

    quality_gate_ci = _group_quality_gate_ci(task_id, artifacts)
    approve_ready = bool(
        (str(task.get("phase") or "") == GROUP_PHASE_DECISION)
        and (not _is_group_task_closed(task))
        and (not bool(quality_gate_ci.get("blocked")))
    )

    return {
        "ok": True,
        "task": _group_task_public(task),
        "kpi": {
            "changed_files_count": len(list(artifacts.get("changed_files") or [])),
            "rounds_count": len(rounds),
            "decisions_count": len(decisions),
            "approve_ready": approve_ready,
            "quality_gate_pass": bool(quality_gate_ci.get("pass")),
        },
        "latest": {
            "round": (rounds[0] if rounds else None),
            "decision": (decisions[0] if decisions else None),
            "test_result": artifacts.get("test_result"),
        },
    }


@router.post("/api/group/tasks/{task_id}/approve:precheck")
def api_group_task_approve_precheck(request: Request, task_id: str, body: GroupTaskApproveIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_task_approve_precheck(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        normalize_role=_normalize_group_actor_role,
        assert_can_approve=_assert_group_role_can_approve,
        assert_owner=_assert_group_owner_identity,
        is_closed=_is_group_task_closed,
        get_artifacts=get_group_artifacts_or_default,
        quality_gate_ci=_group_quality_gate_ci,
        phase_decision=GROUP_PHASE_DECISION,
        err_task_closed=GROUP_ERROR_TASK_CLOSED,
        err_approve_phase_invalid=GROUP_ERROR_APPROVE_PHASE_INVALID,
        err_quality_gate_blocked=GROUP_ERROR_QUALITY_GATE_BLOCKED,
    )


@router.post("/api/group/tasks/{task_id}/approve")
def api_group_task_approve(request: Request, task_id: str, body: GroupTaskApproveIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    fn_put = getattr(store, "insert_group_task", None)
    if not callable(fn_put):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    return handle_group_task_approve(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        get_artifacts=get_group_artifacts_or_default,
        is_closed=_is_group_task_closed,
        normalize_role=_normalize_group_actor_role,
        assert_can_approve=_assert_group_role_can_approve,
        assert_owner=_assert_group_owner_identity,
        quality_gate_ci=_group_quality_gate_ci,
        phase_decision=GROUP_PHASE_DECISION,
        status_done=GROUP_STATUS_DONE,
        err_task_closed=GROUP_ERROR_TASK_CLOSED,
        err_approve_phase_invalid=GROUP_ERROR_APPROVE_PHASE_INVALID,
        err_quality_gate_blocked=GROUP_ERROR_QUALITY_GATE_BLOCKED,
        public_task=_group_task_public,
        now_ms=_now_ms,
        put_task=lambda _, t: fn_put(t),
        write_round=_write_group_round,
        decision_type_approve=GROUP_DECISION_TYPE_APPROVE,
    )


@router.post("/api/group/tasks/{task_id}/rerun:precheck")
def api_group_task_rerun_precheck(request: Request, task_id: str, body: GroupTaskRerunIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_task_rerun_precheck(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        normalize_phase=_normalize_group_phase,
        normalize_role=_normalize_group_actor_role,
        assert_can_rerun=_assert_group_role_can_rerun,
        assert_transition=_assert_group_phase_transition,
        is_closed=_is_group_task_closed,
        phase_planning=GROUP_PHASE_PLANNING,
        err_task_closed=GROUP_ERROR_TASK_CLOSED,
    )


@router.post("/api/group/tasks/{task_id}/rerun")
def api_group_task_rerun(request: Request, task_id: str, body: GroupTaskRerunIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    fn_put = getattr(store, "insert_group_task", None)
    if not callable(fn_put):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    return handle_group_task_rerun(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        is_closed=_is_group_task_closed,
        normalize_phase=_normalize_group_phase,
        normalize_role=_normalize_group_actor_role,
        assert_can_rerun=_assert_group_role_can_rerun,
        assert_transition=_assert_group_phase_transition,
        phase_planning=GROUP_PHASE_PLANNING,
        err_task_closed=GROUP_ERROR_TASK_CLOSED,
        err_transition_not_allowed=GROUP_ERROR_TRANSITION_NOT_ALLOWED,
        err_phase_not_allowed=GROUP_ERROR_PHASE_NOT_ALLOWED,
        status_from_phase=_group_status_from_phase,
        public_task=_group_task_public,
        now_ms=_now_ms,
        put_task=lambda _, t: fn_put(t),
        write_round=_write_group_round,
    )


@router.post("/api/group/tasks/{task_id}/execution-submit:precheck")
def api_group_task_execution_submit_precheck(request: Request, task_id: str, body: GroupTaskExecutionSubmitIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_task_execution_submit_precheck(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        normalize_role=_normalize_group_actor_role,
        assert_can_submit_execution=_assert_group_role_can_submit_execution,
        is_closed=_is_group_task_closed,
        phase_execution=GROUP_PHASE_EXECUTION,
        err_execution_phase_invalid="GROUP_EXECUTION_PHASE_INVALID",
        err_task_closed=GROUP_ERROR_TASK_CLOSED,
    )


@router.post("/api/group/tasks/{task_id}/execution-submit")
def api_group_task_execution_submit(request: Request, task_id: str, body: GroupTaskExecutionSubmitIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    fn_put = getattr(store, "insert_group_task", None)
    if not callable(fn_put):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    return handle_group_task_execution_submit(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        is_closed=_is_group_task_closed,
        normalize_role=_normalize_group_actor_role,
        assert_can_submit_execution=_assert_group_role_can_submit_execution,
        phase_execution=GROUP_PHASE_EXECUTION,
        phase_qa_verifying=GROUP_PHASE_QA_VERIFYING,
        status_running=GROUP_STATUS_RUNNING,
        err_task_closed=GROUP_ERROR_TASK_CLOSED,
        err_exec_phase_invalid=GROUP_ERROR_EXEC_PHASE_INVALID,
        public_task=_group_task_public,
        now_ms=_now_ms,
        put_task=lambda _, t: fn_put(t),
        write_round=_write_group_round,
    )


@router.post("/api/group/tasks/{task_id}/qa-review:precheck")
def api_group_task_qa_review_precheck(request: Request, task_id: str, body: GroupTaskQaReviewIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_task_qa_review_precheck(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        normalize_role=_normalize_group_actor_role,
        assert_can_qa_review=_assert_group_role_can_qa_review,
        is_closed=_is_group_task_closed,
        phase_qa_verifying=GROUP_PHASE_QA_VERIFYING,
        err_qa_phase_invalid=GROUP_ERROR_QA_PHASE_INVALID,
        err_task_closed=GROUP_ERROR_TASK_CLOSED,
    )


@router.post("/api/group/tasks/{task_id}/qa-review")
def api_group_task_qa_review(request: Request, task_id: str, body: GroupTaskQaReviewIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    fn_put = getattr(store, "insert_group_task", None)
    if not callable(fn_put):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    return handle_group_task_qa_review(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        normalize_role=_normalize_group_actor_role,
        assert_can_qa_review=_assert_group_role_can_qa_review,
        phase_qa_verifying=GROUP_PHASE_QA_VERIFYING,
        phase_decision=GROUP_PHASE_DECISION,
        phase_execution=GROUP_PHASE_EXECUTION,
        status_from_phase=_group_status_from_phase,
        public_task=_group_task_public,
        now_ms=_now_ms,
        put_task=lambda _, t: fn_put(t),
        write_round=_write_group_round,
        err_qa_phase_invalid=GROUP_ERROR_QA_PHASE_INVALID,
    )


@router.post("/api/group/tasks/{task_id}/cancel:precheck")
def api_group_task_cancel_precheck(request: Request, task_id: str, body: GroupTaskCancelIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    return handle_group_task_cancel_precheck(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        normalize_role=_normalize_group_actor_role,
        assert_can_cancel=_assert_group_role_can_cancel,
        assert_owner=_assert_group_owner_identity,
        is_closed=_is_group_task_closed,
        err_task_closed=GROUP_ERROR_TASK_CLOSED,
    )


@router.post("/api/group/tasks/{task_id}/cancel")
def api_group_task_cancel(request: Request, task_id: str, body: GroupTaskCancelIn) -> dict:
    _ensure_group_mode_enabled(_get_settings(request))
    store = _get_store(request)
    fn_put = getattr(store, "insert_group_task", None)
    if not callable(fn_put):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    return handle_group_task_cancel(
        task_id=task_id,
        body=body,
        store=store,
        get_task_or_raise=lambda s, tid: get_group_task_or_raise(s, tid, not_found=_group_task_not_found),
        normalize_role=_normalize_group_actor_role,
        assert_can_cancel=_assert_group_role_can_cancel,
        assert_owner=_assert_group_owner_identity,
        status_done=GROUP_STATUS_DONE,
        status_cancelled="cancelled",
        public_task=_group_task_public,
        now_ms=_now_ms,
        put_task=lambda _, t: fn_put(t),
        write_round=_write_group_round,
        decision_type_cancel=GROUP_DECISION_TYPE_CANCEL,
    )


@router.get("/api/agent/tasks/{task_id}/steps")
def api_agent_task_steps(request: Request, task_id: str, limit: int = 500, offset: int = 0, cursor: int | None = None, attempt: int | None = None, step_type: str | None = None, ok: bool | None = None, order: str = "asc", from_ts: int | None = None, to_ts: int | None = None, fields: str | None = None, format: str | None = None, include_summary: bool = True) -> dict:
    store = _get_store(request)
    settings = _get_settings(request)
    fn = getattr(store, "list_agent_task_steps", None)
    p95_threshold_ms = max(1, int(getattr(settings, "health_p95_threshold_ms", 1000)))
    penalty_per_100ms = max(1, int(getattr(settings, "health_penalty_per_100ms", 1)))
    penalty_cap = max(0, int(getattr(settings, "health_penalty_cap", 40)))
    green_min_score = max(0, min(100, int(getattr(settings, "health_green_min_score", 90))))
    yellow_min_score = max(0, min(100, int(getattr(settings, "health_yellow_min_score", 70))))
    yellow_max_failure_rate = max(0.0, float(getattr(settings, "health_yellow_max_failure_rate", 20.0)))
    empty_score = max(0, min(100, int(getattr(settings, "health_empty_score", 50))))
    health_config = {
        "p95_threshold_ms": p95_threshold_ms,
        "penalty_per_100ms": penalty_per_100ms,
        "penalty_cap": penalty_cap,
        "green_min_score": green_min_score,
        "yellow_min_score": yellow_min_score,
        "yellow_max_failure_rate": yellow_max_failure_rate,
        "empty_score": empty_score,
    }
    config_version = hashlib.sha1(json.dumps(health_config, sort_keys=True).encode("utf-8")).hexdigest()[:12]
    summary_window_ms = (None if from_ts is None or to_ts is None else abs(max(0, int(to_ts)) - max(0, int(from_ts))))
    summary_id_salt = str(getattr(settings, "summary_id_salt", "") or "")
    summary_id_salted = bool(summary_id_salt)
    summary_salt_hint = ("configured" if summary_id_salted else "not_configured")
    allowed_fields = {"step_no", "step_type", "attempt", "input_json", "output_json", "ok", "latency_ms", "created_at_ms"}
    format_v = (str(format or "full").strip().lower() or "full")
    if format_v not in {"full", "compact"}:
        format_v = "full"
    if format_v == "compact":
        selected_fields = ["step_no", "step_type", "attempt", "ok", "latency_ms", "created_at_ms"]
    else:
        selected_fields = [f.strip() for f in str(fields or "").split(",") if f.strip() in allowed_fields]

    attempt_fallback = (None if attempt is None else max(0, int(attempt)))
    step_type_fallback = (None if step_type is None else str(step_type).strip().upper())
    if step_type_fallback == "":
        step_type_fallback = None
    ok_fallback = (None if ok is None else bool(ok))
    order_fallback = ("desc" if str(order).strip().lower() == "desc" else "asc")
    from_ts_fallback = (None if from_ts is None else max(0, int(from_ts)))
    to_ts_fallback = (None if to_ts is None else max(0, int(to_ts)))
    paging_mode_fallback = ("cursor" if cursor is not None else "offset")
    offset_fallback = (0 if cursor is not None else max(0, int(offset)))
    cursor_fallback = (None if cursor is None else max(0, int(cursor)))
    limit_fallback = max(1, min(int(limit), 5000))
    summary_generated_at_ms_fallback = _now_ms()
    fallback_extra = {
        "order": order_fallback,
        "paging_mode": paging_mode_fallback,
        "offset": offset_fallback,
        "cursor": cursor_fallback,
        "limit": limit_fallback,
    }
    summary_fingerprint_fallback = _summary_key(
        "sfp_",
        _summary_payload(
            task_id=task_id,
            attempt=attempt_fallback,
            step_type=step_type_fallback,
            step_ok=ok_fallback,
            from_ts=from_ts_fallback,
            to_ts=to_ts_fallback,
            window_ms=summary_window_ms,
            config_version=config_version,
            salt=summary_id_salt,
            extra=fallback_extra,
        ),
    )
    summary_id_fallback = _summary_key(
        "sum_",
        _summary_payload(
            task_id=task_id,
            attempt=attempt_fallback,
            step_type=step_type_fallback,
            step_ok=ok_fallback,
            from_ts=from_ts_fallback,
            to_ts=to_ts_fallback,
            window_ms=summary_window_ms,
            config_version=config_version,
            salt=summary_id_salt,
            generated_at_ms=summary_generated_at_ms_fallback,
            extra=fallback_extra,
        ),
    )

    if callable(fn):
        attempt_v = (None if attempt is None else max(0, int(attempt)))
        step_type_v = (None if step_type is None else str(step_type).strip().upper())
        if step_type_v == "":
            step_type_v = None
        ok_v = (None if ok is None else bool(ok))
        limit_v = max(1, min(int(limit), 5000))
        offset_v = max(0, int(offset))
        cursor_v = (None if cursor is None else max(0, int(cursor)))
        paging_mode = ("cursor" if cursor_v is not None else "offset")
        effective_offset = (0 if cursor_v is not None else offset_v)
        order_v = ("desc" if str(order).strip().lower() == "desc" else "asc")
        from_ts_v = (None if from_ts is None else max(0, int(from_ts)))
        to_ts_v = (None if to_ts is None else max(0, int(to_ts)))
        if from_ts_v is not None and to_ts_v is not None and from_ts_v > to_ts_v:
            from_ts_v, to_ts_v = to_ts_v, from_ts_v
        summary_id = "sum_" + hashlib.sha1(
            json.dumps(
                {
                    "task_id": task_id,
                    "attempt": attempt_v,
                    "step_type": step_type_v,
                    "step_ok": ok_v,
                    "order": order_v,
                    "from_ts": from_ts_v,
                    "to_ts": to_ts_v,
                    "window_ms": summary_window_ms,
                    "paging_mode": paging_mode,
                    "offset": effective_offset,
                    "cursor": cursor_v,
                    "limit": limit_v,
                    "config_version": config_version,
                    "summary_generated_at_ms": summary_generated_at_ms_fallback,
                    "salt": summary_id_salt,
                },
                sort_keys=True,
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()[:16]
        rows = fn(task_id, limit=limit_v + 1, offset=effective_offset, cursor=cursor_v, attempt=attempt_v, step_type=step_type_v, ok=ok_v, order=order_v, from_ts=from_ts_v, to_ts=to_ts_v)
        has_more = len(rows) > limit_v
        items_raw = rows[:limit_v]
        next_offset = ((effective_offset + limit_v) if (has_more and paging_mode == "offset") else None)
        next_cursor = (int(items_raw[-1].get("_cursor")) if has_more and items_raw else None)
        items = []
        for it in items_raw:
            row = dict(it)
            row.pop("_cursor", None)
            items.append(row)

        summary: dict[str, Any] | None = None
        if bool(include_summary):
            page_count = len(items)
            success_count = sum(1 for it in items if bool(it.get("ok")))
            failed_count = page_count - success_count
            latency_sum = sum(int(it.get("latency_ms") or 0) for it in items)
            avg_latency_ms = (int(latency_sum / page_count) if page_count > 0 else 0)
            p95_latency_ms = 0
            min_latency_ms = 0
            max_latency_ms = 0
            if page_count > 0:
                lats = sorted(int(it.get("latency_ms") or 0) for it in items)
                p95_latency_ms = lats[int((page_count - 1) * 0.95)]
                min_latency_ms = lats[0]
                max_latency_ms = lats[-1]
            sr = (round((success_count * 100.0) / page_count, 2) if page_count > 0 else 0.0)
            fr = (round((failed_count * 100.0) / page_count, 2) if page_count > 0 else 0.0)
            over_threshold_ms = max(0, p95_latency_ms - p95_threshold_ms)
            raw_penalty = max(0, (over_threshold_ms // 100) * penalty_per_100ms)
            latency_penalty = (min(penalty_cap, raw_penalty) if p95_latency_ms > p95_threshold_ms else 0)
            base_score = (empty_score if page_count <= 0 else max(0, min(100, int(round(sr)))))
            health_score = (empty_score if page_count <= 0 else max(0, min(100, base_score - latency_penalty)))
            if page_count <= 0:
                health_level = "yellow"
            elif failed_count <= 0 and health_score >= green_min_score:
                health_level = "green"
            elif health_score >= yellow_min_score or fr <= yellow_max_failure_rate:
                health_level = "yellow"
            else:
                health_level = "red"
            by_type: dict[str, int] = {}
            for it in items:
                k = str(it.get("step_type") or "UNKNOWN")
                by_type[k] = by_type.get(k, 0) + 1
            summary_generated_at_ms_actual = _now_ms()
            runtime_extra = {
                "order": order_v,
                "paging_mode": paging_mode,
                "offset": effective_offset,
                "cursor": cursor_v,
                "limit": limit_v,
            }
            summary_fingerprint = _summary_key(
                "sfp_",
                _summary_payload(
                    task_id=task_id,
                    attempt=attempt_v,
                    step_type=step_type_v,
                    step_ok=ok_v,
                    from_ts=from_ts_v,
                    to_ts=to_ts_v,
                    window_ms=summary_window_ms,
                    config_version=config_version,
                    salt=summary_id_salt,
                    extra=runtime_extra,
                ),
            )
            summary_id = _summary_key(
                "sum_",
                _summary_payload(
                    task_id=task_id,
                    attempt=attempt_v,
                    step_type=step_type_v,
                    step_ok=ok_v,
                    from_ts=from_ts_v,
                    to_ts=to_ts_v,
                    window_ms=summary_window_ms,
                    config_version=config_version,
                    salt=summary_id_salt,
                    generated_at_ms=summary_generated_at_ms_actual,
                    extra=runtime_extra,
                ),
            )
            summary = {
                "page_count": page_count,
                "success_count": success_count,
                "failed_count": failed_count,
                "success_rate": sr,
                "failure_rate": fr,
                "health_level": health_level,
                "health_score": health_score,
                "health_config": health_config,
                "config_version": config_version,
                "summary_signature_algo": _SUMMARY_SIGNATURE_ALGO,
                "summary_generated_at_ms": summary_generated_at_ms_actual,
                "summary_window_ms": summary_window_ms,
                "summary_id": summary_id,
                "summary_fingerprint": summary_fingerprint,
                "summary_fingerprint_algo": _SUMMARY_FINGERPRINT_ALGO,
                "summary_id_salted": summary_id_salted,
                "summary_salt_hint": summary_salt_hint,
                "avg_latency_ms": avg_latency_ms,
                "min_latency_ms": min_latency_ms,
                "max_latency_ms": max_latency_ms,
                "p95_latency_ms": p95_latency_ms,
                "latency_penalty_preview": {
                    "p95_latency_ms": p95_latency_ms,
                    "threshold_ms": p95_threshold_ms,
                    "over_threshold_ms": over_threshold_ms,
                    "raw_penalty": raw_penalty,
                    "capped_penalty": latency_penalty,
                    "penalty_cap": penalty_cap,
                    "base_score": base_score,
                    "final_score": health_score,
                    "applies_penalty": bool(latency_penalty > 0),
                    "reason": ("p95 exceeds threshold" if latency_penalty > 0 else "p95 within threshold"),
                },
                "by_step_type": by_type,
            }

        if selected_fields:
            items = [{k: v for k, v in it.items() if k in set(selected_fields)} for it in items]

        return {
            "ok": True,
            "task_id": task_id,
            "attempt": attempt_v,
            "step_type": step_type_v,
            "step_ok": ok_v,
            "order": order_v,
            "from_ts": from_ts_v,
            "to_ts": to_ts_v,
            "format": format_v,
            "fields": (selected_fields or None),
            "include_summary": bool(include_summary),
            "paging_mode": paging_mode,
            "deprecated_offset": bool(cursor_v is None and offset_v > 0),
            "offset": effective_offset,
            "cursor": cursor_v,
            "limit": limit_v,
            "next_offset": next_offset,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "summary": summary,
            "items": items,
        }
    return {
        "ok": True,
        "task_id": task_id,
        "attempt": attempt,
        "step_type": step_type,
        "step_ok": ok,
        "order": ("desc" if str(order).strip().lower() == "desc" else "asc"),
        "from_ts": (None if from_ts is None else max(0, int(from_ts))),
        "to_ts": (None if to_ts is None else max(0, int(to_ts))),
        "fields": (selected_fields or None),
        "include_summary": bool(include_summary),
        "paging_mode": ("cursor" if cursor is not None else "offset"),
        "deprecated_offset": bool(cursor is None and max(0, int(offset)) > 0),
        "offset": (0 if cursor is not None else max(0, int(offset))),
        "cursor": (None if cursor is None else max(0, int(cursor))),
        "limit": max(1, min(int(limit), 5000)),
        "next_offset": None,
        "next_cursor": None,
        "has_more": False,
        "summary": ({"page_count": 0, "success_count": 0, "failed_count": 0, "success_rate": 0.0, "failure_rate": 0.0, "health_level": "yellow", "health_score": empty_score, "health_config": health_config, "config_version": config_version, "summary_signature_algo": _SUMMARY_SIGNATURE_ALGO, "summary_generated_at_ms": summary_generated_at_ms_fallback, "summary_window_ms": summary_window_ms, "summary_id": summary_id_fallback, "summary_fingerprint": summary_fingerprint_fallback, "summary_fingerprint_algo": "sha1-16-stable", "summary_id_salted": summary_id_salted, "summary_salt_hint": summary_salt_hint, "avg_latency_ms": 0, "min_latency_ms": 0, "max_latency_ms": 0, "p95_latency_ms": 0, "latency_penalty_preview": {"p95_latency_ms": 0, "threshold_ms": p95_threshold_ms, "over_threshold_ms": 0, "raw_penalty": 0, "capped_penalty": 0, "penalty_cap": penalty_cap, "base_score": empty_score, "final_score": empty_score, "applies_penalty": False, "reason": "p95 within threshold"}, "by_step_type": {}} if bool(include_summary) else None),
        "items": [],
    }


@router.get("/api/agent/tasks/{task_id}/steps/summary")
def api_agent_task_steps_summary(
    request: Request,
    task_id: str,
    attempt: int | None = None,
    step_type: str | None = None,
    ok: bool | None = None,
    from_ts: int | None = None,
    to_ts: int | None = None,
    error_top_n: int = 5,
    success_top_n: int = 5,
) -> dict:
    store = _get_store(request)
    settings = _get_settings(request)
    p95_threshold_ms = max(1, int(getattr(settings, "health_p95_threshold_ms", 1000)))
    penalty_per_100ms = max(1, int(getattr(settings, "health_penalty_per_100ms", 1)))
    penalty_cap = max(0, int(getattr(settings, "health_penalty_cap", 40)))
    green_min_score = max(0, min(100, int(getattr(settings, "health_green_min_score", 90))))
    yellow_min_score = max(0, min(100, int(getattr(settings, "health_yellow_min_score", 70))))
    yellow_max_failure_rate = max(0.0, float(getattr(settings, "health_yellow_max_failure_rate", 20.0)))
    empty_score = max(0, min(100, int(getattr(settings, "health_empty_score", 50))) )
    health_config = {
        "p95_threshold_ms": p95_threshold_ms,
        "penalty_per_100ms": penalty_per_100ms,
        "penalty_cap": penalty_cap,
        "green_min_score": green_min_score,
        "yellow_min_score": yellow_min_score,
        "yellow_max_failure_rate": yellow_max_failure_rate,
        "empty_score": empty_score,
    }
    config_version = hashlib.sha1(json.dumps(health_config, sort_keys=True).encode("utf-8")).hexdigest()[:12]
    summary_id_salt = str(getattr(settings, "summary_id_salt", "") or "")
    summary_id_salted = bool(summary_id_salt)
    summary_salt_hint = ("configured" if summary_id_salted else "not_configured")
    summary_generated_at_ms = _now_ms()
    window_ms_raw = None
    if from_ts is not None and to_ts is not None:
        f0 = max(0, int(from_ts))
        t0 = max(0, int(to_ts))
        window_ms_raw = abs(t0 - f0)
    fn = getattr(store, "list_agent_task_steps", None)
    fn_agg = getattr(store, "aggregate_agent_task_steps", None)
    if not callable(fn) and not callable(fn_agg):
        summary_fingerprint = _summary_key(
            "sfp_",
            _summary_payload(
                task_id=task_id,
                attempt=attempt,
                step_type=step_type,
                step_ok=ok,
                from_ts=from_ts,
                to_ts=to_ts,
                window_ms=window_ms_raw,
                config_version=config_version,
                salt=summary_id_salt,
            ),
        )
        summary_id = _summary_key(
            "sum_",
            _summary_payload(
                task_id=task_id,
                attempt=attempt,
                step_type=step_type,
                step_ok=ok,
                from_ts=from_ts,
                to_ts=to_ts,
                window_ms=window_ms_raw,
                config_version=config_version,
                salt=summary_id_salt,
                generated_at_ms=summary_generated_at_ms,
            ),
        )
        health_config_resp = {
            **health_config,
            "latency_penalty_preview": {
                "p95_latency_ms": 0,
                "threshold_ms": p95_threshold_ms,
                "over_threshold_ms": 0,
                "raw_penalty": 0,
                "capped_penalty": 0,
                "penalty_cap": penalty_cap,
                "base_score": 0,
                "final_score": 50,
                "applies_penalty": False,
                "reason": "p95 within threshold",
            },
        }
        return {
            "ok": True,
            "task_id": task_id,
            "attempt": attempt,
            "step_type": step_type,
            "step_ok": ok,
            "from_ts": from_ts,
            "to_ts": to_ts,
            "window_ms": window_ms_raw,
            "health_config": health_config_resp,
            "config_version": config_version,
            "summary_generated_at_ms": summary_generated_at_ms,
            "summary_signature_algo": _SUMMARY_SIGNATURE_ALGO,
            "summary_id_salted": summary_id_salted,
            "summary_salt_hint": summary_salt_hint,
            "summary_id": summary_id,
            "summary_fingerprint": summary_fingerprint,
            "summary_fingerprint_algo": _SUMMARY_FINGERPRINT_ALGO,
            "summary": {"total": 0, "success_count": 0, "failed_count": 0, "success_rate": 0.0, "failure_rate": 0.0, "health_level": "yellow", "health_score": 50, "avg_latency_ms": 0, "min_latency_ms": 0, "max_latency_ms": 0, "p95_latency_ms": 0, "by_step_type": {}, "error_top": [], "success_top": []},
        }

    attempt_v = (None if attempt is None else max(0, int(attempt)))
    step_type_v = (None if step_type is None else str(step_type).strip().upper())
    if step_type_v == "":
        step_type_v = None
    ok_v = (None if ok is None else bool(ok))
    from_ts_v = (None if from_ts is None else max(0, int(from_ts)))
    to_ts_v = (None if to_ts is None else max(0, int(to_ts)))
    if from_ts_v is not None and to_ts_v is not None and from_ts_v > to_ts_v:
        from_ts_v, to_ts_v = to_ts_v, from_ts_v
    window_ms = (None if (from_ts_v is None or to_ts_v is None) else max(0, int(to_ts_v - from_ts_v)))

    top_n_v = max(1, min(int(error_top_n), 20))
    succ_top_n_v = max(1, min(int(success_top_n), 20))
    summary_extra = {"error_top_n": top_n_v, "success_top_n": succ_top_n_v}
    summary_fingerprint = _summary_key(
        "sfp_",
        _summary_payload(
            task_id=task_id,
            attempt=attempt_v,
            step_type=step_type_v,
            step_ok=ok_v,
            from_ts=from_ts_v,
            to_ts=to_ts_v,
            window_ms=window_ms,
            config_version=config_version,
            salt=summary_id_salt,
            extra=summary_extra,
        ),
    )
    summary_id = _summary_key(
        "sum_",
        _summary_payload(
            task_id=task_id,
            attempt=attempt_v,
            step_type=step_type_v,
            step_ok=ok_v,
            from_ts=from_ts_v,
            to_ts=to_ts_v,
            window_ms=window_ms,
            config_version=config_version,
            salt=summary_id_salt,
            generated_at_ms=summary_generated_at_ms,
            extra=summary_extra,
        ),
    )
    if callable(fn_agg):
        summary = fn_agg(
            task_id,
            attempt=attempt_v,
            step_type=step_type_v,
            ok=ok_v,
            from_ts=from_ts_v,
            to_ts=to_ts_v,
            error_top_n=top_n_v,
            success_top_n=succ_top_n_v,
            p95_threshold_ms=p95_threshold_ms,
            penalty_per_100ms=penalty_per_100ms,
            penalty_cap=penalty_cap,
            green_min_score=green_min_score,
            yellow_min_score=yellow_min_score,
            yellow_max_failure_rate=yellow_max_failure_rate,
            empty_score=empty_score,
        )
    else:
        rows = fn(task_id, limit=5000, offset=0, cursor=None, attempt=attempt_v, step_type=step_type_v, ok=ok_v, order="asc", from_ts=from_ts_v, to_ts=to_ts_v)
        items: list[dict[str, Any]] = []
        for it in rows:
            row = dict(it)
            row.pop("_cursor", None)
            items.append(row)
        total = len(items)
        success_count = sum(1 for it in items if bool(it.get("ok")))
        failed_count = total - success_count
        latency_sum = sum(int(it.get("latency_ms") or 0) for it in items)
        avg_latency_ms = (int(latency_sum / total) if total > 0 else 0)
        by_type: dict[str, int] = {}
        for it in items:
            k = str(it.get("step_type") or "UNKNOWN")
            by_type[k] = by_type.get(k, 0) + 1
        p95_latency_ms = 0
        if total > 0:
            lats = sorted(int(it.get("latency_ms") or 0) for it in items)
            p95_latency_ms = lats[int((total - 1) * 0.95)]
        min_latency_ms = (min(int(it.get("latency_ms") or 0) for it in items) if total > 0 else 0)
        max_latency_ms = (max(int(it.get("latency_ms") or 0) for it in items) if total > 0 else 0)
        err_map: dict[str, int] = {}
        succ_map: dict[str, int] = {}
        for it in items:
            k = str(it.get("step_type") or "UNKNOWN")
            if bool(it.get("ok")):
                succ_map[k] = succ_map.get(k, 0) + 1
            else:
                err_map[k] = err_map.get(k, 0) + 1
        error_top = [{"step_type": k, "count": c} for k, c in sorted(err_map.items(), key=lambda x: (-x[1], x[0]))[:top_n_v]]
        success_top = [{"step_type": k, "count": c} for k, c in sorted(succ_map.items(), key=lambda x: (-x[1], x[0]))[:succ_top_n_v]]
        sr = (round((success_count * 100.0) / total, 2) if total > 0 else 0.0)
        fr = (round((failed_count * 100.0) / total, 2) if total > 0 else 0.0)
        latency_penalty = (min(penalty_cap, int((p95_latency_ms - p95_threshold_ms) / 100) * penalty_per_100ms) if p95_latency_ms > p95_threshold_ms else 0)
        if total <= 0:
            health_score = empty_score
            health_level = "yellow"
        else:
            health_score = max(0, min(100, int(round(sr)) - latency_penalty))
            if failed_count <= 0 and health_score >= green_min_score:
                health_level = "green"
            elif health_score >= yellow_min_score or fr <= yellow_max_failure_rate:
                health_level = "yellow"
            else:
                health_level = "red"
        summary = {
            "total": total,
            "success_count": success_count,
            "failed_count": failed_count,
            "success_rate": sr,
            "failure_rate": fr,
            "health_level": health_level,
            "health_score": health_score,
            "avg_latency_ms": avg_latency_ms,
            "min_latency_ms": min_latency_ms,
            "max_latency_ms": max_latency_ms,
            "p95_latency_ms": p95_latency_ms,
            "by_step_type": by_type,
            "error_top": error_top,
            "success_top": success_top,
        }

    total_v = int(summary.get("total") or 0)
    success_v = int(summary.get("success_count") or 0)
    failed_v = int(summary.get("failed_count") or 0)
    if "success_rate" not in summary:
        summary["success_rate"] = (round((success_v * 100.0) / total_v, 2) if total_v > 0 else 0.0)
    if "failure_rate" not in summary:
        summary["failure_rate"] = (round((failed_v * 100.0) / total_v, 2) if total_v > 0 else 0.0)
    p95_v = int(summary.get("p95_latency_ms") or 0)
    latency_penalty_v = (min(penalty_cap, int((p95_v - p95_threshold_ms) / 100) * penalty_per_100ms) if p95_v > p95_threshold_ms else 0)
    if "health_score" not in summary:
        sr_v = float(summary.get("success_rate") or 0.0)
        summary["health_score"] = (empty_score if total_v <= 0 else max(0, min(100, int(round(sr_v)) - latency_penalty_v)))
    if "health_level" not in summary:
        fr_v = float(summary.get("failure_rate") or 0.0)
        hs_v = int(summary.get("health_score") or 0)
        if total_v <= 0:
            summary["health_level"] = "yellow"
        elif failed_v <= 0 and hs_v >= green_min_score:
            summary["health_level"] = "green"
        elif hs_v >= yellow_min_score or fr_v <= yellow_max_failure_rate:
            summary["health_level"] = "yellow"
        else:
            summary["health_level"] = "red"

    over_threshold_ms = max(0, p95_v - p95_threshold_ms)
    raw_penalty_v = max(0, (over_threshold_ms // 100) * penalty_per_100ms)
    base_score_v = (empty_score if total_v <= 0 else max(0, min(100, int(round(float(summary.get("success_rate") or 0.0))))))
    final_score_v = int(summary.get("health_score") or 0)
    applies_penalty_v = bool(latency_penalty_v > 0)
    penalty_reason_v = ("p95 exceeds threshold" if applies_penalty_v else "p95 within threshold")
    health_config_resp = {
        **health_config,
        "latency_penalty_preview": {
            "p95_latency_ms": p95_v,
            "threshold_ms": p95_threshold_ms,
            "over_threshold_ms": over_threshold_ms,
            "raw_penalty": raw_penalty_v,
            "capped_penalty": latency_penalty_v,
            "penalty_cap": penalty_cap,
            "base_score": base_score_v,
            "final_score": final_score_v,
            "applies_penalty": applies_penalty_v,
            "reason": penalty_reason_v,
        },
    }

    top_issue_summary = str(summary.get("top_issue_summary") or "") if isinstance(summary, dict) else ""
    if not top_issue_summary:
        if total_v <= 0:
            top_issue_summary = "no step data"
        elif failed_v <= 0:
            sr = float(summary.get("success_rate") or 0.0)
            top_issue_summary = f"healthy run: {sr:.2f}% success ({success_v}/{total_v})"
        else:
            err_top = summary.get("error_top") if isinstance(summary, dict) else None
            first = (err_top[0] if isinstance(err_top, list) and err_top and isinstance(err_top[0], dict) else {})
            st = str(first.get("step_type") or "UNKNOWN")
            cnt = int(first.get("count") or failed_v)
            fr = float(summary.get("failure_rate") or 0.0)
            top_issue_summary = f"top failure: {st} x{cnt} (failure_rate={fr:.2f}%)"

    return {
        "ok": True,
        "task_id": task_id,
        "attempt": attempt_v,
        "step_type": step_type_v,
        "step_ok": ok_v,
        "from_ts": from_ts_v,
        "to_ts": to_ts_v,
        "window_ms": window_ms,
        "error_top_n": top_n_v,
        "success_top_n": succ_top_n_v,
        "health_config": health_config_resp,
        "config_version": config_version,
        "summary_generated_at_ms": summary_generated_at_ms,
        "summary_signature_algo": _SUMMARY_SIGNATURE_ALGO,
        "summary_id_salted": summary_id_salted,
        "summary_salt_hint": summary_salt_hint,
        "summary_id": summary_id,
        "summary_fingerprint": summary_fingerprint,
        "summary_fingerprint_algo": _SUMMARY_FINGERPRINT_ALGO,
        "top_issue_summary": top_issue_summary,
        "summary": summary,
    }


@router.get("/api/agent/tasks/{task_id}/artifacts")
def api_agent_task_artifacts(request: Request, task_id: str) -> dict:
    store = _get_store(request)
    fn = getattr(store, "get_agent_task_artifacts", None)
    fn_task = getattr(store, "get_agent_task", None)
    if callable(fn):
        art = fn(task_id)
        if art is not None:
            t = fn_task(task_id) if callable(fn_task) else None
            fn_steps = getattr(store, "list_agent_task_steps", None)
            steps = (fn_steps(task_id, limit=500, offset=0, cursor=None, attempt=None, order="asc", from_ts=None, to_ts=None) if callable(fn_steps) else [])
            return {"ok": True, "task_id": task_id, "artifacts": art or {"changed_files": [], "step_results": [], "test_result": None, "summary": ""}, "last_verify": (t.get("last_verify") if isinstance(t, dict) else None), "steps": steps}
    with _AGENT_TASKS_LOCK:
        task = _AGENT_TASKS.get(task_id)
        if not isinstance(task, dict):
            raise _task_not_found(task_id)
        return {
            "ok": True,
            "task_id": task_id,
            "artifacts": task.get("artifacts") or {"changed_files": [], "step_results": [], "test_result": None, "summary": ""},
            "last_verify": task.get("last_verify"),
            "steps": [],
        }


@router.post("/api/agent/tasks/{task_id}/cancel")
def api_agent_task_cancel(request: Request, task_id: str) -> dict:
    with _AGENT_TASKS_LOCK:
        task = _AGENT_TASKS.get(task_id)
        if not isinstance(task, dict):
            raise _task_not_found(task_id)
        if task.get("status") in {"succeeded", "failed", "cancelled"}:
            return {"ok": True, "task_id": task_id, "status": task.get("status")}
        task["status"] = "cancelled"
        task["current_step"] = "CANCELLED"
        task["stop_reason"] = "cancelled"
        task["updated_at_ms"] = _now_ms()
        task["finished_at_ms"] = _now_ms()
        _task_log(task, "task_cancelled", _get_store(request))
        _persist_task(_get_store(request), task)
        return {"ok": True, **_task_public(task)}


class DeleteMemoriesIn(BaseModel):
    memory_ids: list[int] = Field(min_length=1, max_length=50)


class MemoryWriteToggleIn(BaseModel):
    enabled: bool


class RenameConversationIn(BaseModel):
    title: str = Field(min_length=1, max_length=80)


@router.get("/api/conversations/{conversation_id}/memories")
def api_conversation_memories(request: Request, conversation_id: str, limit: int = 50) -> dict:
    store = _get_store(request)
    return {
        "ok": True,
        "conversation_id": conversation_id,
        "memory_write_enabled": store.get_memory_write_enabled(conversation_id),
        "items": store.list_memories(conversation_id=conversation_id, limit=limit),
    }


@router.post("/api/conversations/{conversation_id}/memories/delete")
def api_conversation_memories_delete(request: Request, conversation_id: str, body: DeleteMemoriesIn) -> dict:
    store = _get_store(request)
    deleted = store.delete_memories(conversation_id, body.memory_ids)
    return {"ok": True, "conversation_id": conversation_id, "deleted": int(deleted)}


@router.post("/api/conversations/{conversation_id}/memory_write")
def api_conversation_memory_write_toggle(request: Request, conversation_id: str, body: MemoryWriteToggleIn) -> dict:
    store = _get_store(request)
    store.set_memory_write_enabled(conversation_id, body.enabled)
    return {"ok": True, "conversation_id": conversation_id, "memory_write_enabled": bool(body.enabled)}


@router.post("/api/conversations/{conversation_id}/rename")
def api_conversation_rename(request: Request, conversation_id: str, body: RenameConversationIn) -> dict:
    store = _get_store(request)
    store.rename_conversation(conversation_id, body.title)
    return {"ok": True}


@router.post("/api/conversations/{conversation_id}/archive")
def api_conversation_archive(request: Request, conversation_id: str) -> dict:
    store = _get_store(request)
    store.archive_conversation(conversation_id)
    return {"ok": True}


@router.post("/api/conversations/{conversation_id}/purge")
def api_conversation_purge(request: Request, conversation_id: str) -> dict:
    store = _get_store(request)
    store.purge_conversation(conversation_id)
    return {"ok": True}


@router.post("/api/conversations/purge_all")
def api_conversation_purge_all(request: Request) -> dict:
    store = _get_store(request)
    store.purge_all_conversations()
    return {"ok": True}


@router.get("/health")
def health() -> dict:
    return {"ok": True}


def _get_settings(req: Request) -> Settings:
    settings = getattr(req.app.state, "settings", None)
    if settings is None:
        raise RuntimeError("settings_not_initialized")
    return settings


def _get_store(req: Request) -> Storage:
    store = getattr(req.app.state, "store", None)
    if store is None:
        raise RuntimeError("store_not_initialized")
    return store


def _get_tracer(req: Request) -> Tracer:
    tracer = getattr(req.app.state, "tracer", None)
    if tracer is None:
        raise RuntimeError("tracer_not_initialized")
    return tracer


def _get_tool_executor(req: Request) -> ToolExecutor:
    ex = getattr(req.app.state, "tool_executor", None)
    if ex is None:
        raise RuntimeError("tool_executor_not_initialized")
    return ex


def _get_tools_spec(req: Request) -> dict:
    spec = getattr(req.app.state, "tools_spec", None)
    if spec is None:
        raise RuntimeError("tools_spec_not_initialized")
    return spec


def _streaming_response(*, input_text: str, conversation_id: str | None, user_id: str | None, attachments: list[AttachmentIn] | None, request: Request) -> StreamingResponse:
    settings = _get_settings(request)
    store = _get_store(request)
    tracer = _get_tracer(request)

    trace_id = uuid.uuid4().hex
    conv_id = conversation_id or uuid.uuid4().hex

    attachment_meta, attachment_error = _validate_attachments(attachments)
    if attachment_error is not None:
        def gen_bad() -> Iterator[bytes]:
            yield sse_bytes(StreamEvent(trace_id=trace_id, type="error", payload=attachment_error))
            yield sse_bytes(StreamEvent(trace_id=trace_id, type="done", payload={"stop_reason": "model_error"}))

        return StreamingResponse(
            gen_bad(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
        )

    req = ChatRequest(
        trace_id=trace_id,
        conversation_id=conv_id,
        input_text=input_text,
        user_id=user_id,
        metadata={"attachments": attachment_meta},
    )

    executor = _get_tool_executor(request)
    tools_spec = _get_tools_spec(request)

    def gen() -> Iterator[bytes]:
        for ev in run_chat_stream(settings=settings, store=store, tracer=tracer, req=req, tool_executor=executor, tools_spec=tools_spec):
            yield sse_bytes(ev)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/chat/stream_get")
def chat_stream_get(
    request: Request,
    input_text: str = Query(min_length=1, max_length=20000),
    conversation_id: str | None = None,
    user_id: str | None = None,
) -> StreamingResponse:
    return _streaming_response(input_text=input_text, conversation_id=conversation_id, user_id=user_id, attachments=None, request=request)


@router.post("/chat/stream")
def chat_stream(request: Request, body: ChatIn) -> StreamingResponse:
    return _streaming_response(
        input_text=body.input_text,
        conversation_id=body.conversation_id,
        user_id=body.user_id,
        attachments=body.attachments,
        request=request,
    )


@router.get("/chat/plain", response_class=HTMLResponse)
def chat_plain(
    request: Request,
    input_text: str = Query(min_length=1, max_length=8000),
    conversation_id: str | None = None,
) -> HTMLResponse:
    settings = _get_settings(request)
    store = _get_store(request)
    tracer = _get_tracer(request)

    trace_id = uuid.uuid4().hex
    conv_id = conversation_id or uuid.uuid4().hex
    req = ChatRequest(trace_id=trace_id, conversation_id=conv_id, input_text=input_text, user_id=None)

    chunks: list[str] = []
    error_msg: str | None = None

    executor = _get_tool_executor(request)
    tools_spec = _get_tools_spec(request)

    for ev in run_chat_stream(settings=settings, store=store, tracer=tracer, req=req, tool_executor=executor, tools_spec=tools_spec):
        if ev.type == "token":
            delta = (ev.payload or {}).get("delta")
            if isinstance(delta, str):
                chunks.append(delta)
        elif ev.type == "error":
            m = (ev.payload or {}).get("message")
            if isinstance(m, str):
                error_msg = m

    answer = "".join(chunks).strip()
    if not answer:
        answer = error_msg or "(no output)"

    html = f"""<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>agunt v0 - result</title>
</head>
<body>
  <div style=\"max-width: 820px; margin: 24px auto; font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial;\">
    <a href=\"/\">← 返回</a>
    <h3>结果</h3>
    <div style="margin: 6px 0; color: #666; font-size: 12px;">trace_id: {trace_id} | conversation_id: {conv_id}</div>
    <pre style=\"margin-top:12px; padding:12px; background:#111; color:#0f0; white-space:pre-wrap;\">{answer}</pre>
  </div>
</body>
</html>"""

    return HTMLResponse(content=html)