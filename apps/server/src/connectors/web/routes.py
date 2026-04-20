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
from typing import Any, Iterator

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from config import Settings
from connectors.web.auth import require_api_key
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


class AgentTaskCreateIn(BaseModel):
    goal: str = Field(min_length=1, max_length=2000)
    scope_paths: list[str] = Field(default_factory=list, max_length=50)
    forbidden_paths: list[str] = Field(default_factory=list, max_length=50)
    acceptance_cmd: str = Field(min_length=1, max_length=1000)
    max_retry: int = Field(default=3, ge=1, le=5)
    dry_run: bool = False
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
        fn2(str(task.get("task_id") or ""), task.get("artifacts") or {"changed_files": [], "test_result": None, "summary": ""})


def _normalize_task_error_code(code: object) -> str:
    c = str(code or "").strip().upper()
    if not c:
        return "TASK_RUNTIME_ERROR"
    mapping = {
        "COMMAND_EMPTY": "COMMAND_EMPTY",
        "COMMAND_NOT_ALLOWED": "COMMAND_NOT_ALLOWED",
        "COMMAND_TIMEOUT": "COMMAND_TIMEOUT",
        "COMMAND_FAILED": "COMMAND_FAILED",
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


def _run_acceptance_command(settings: Settings, command: str) -> dict[str, Any]:
    parts = shlex.split(command)
    if not parts:
        return {"ok": False, "error_code": "COMMAND_EMPTY", "stdout": "", "stderr": "empty command", "exit_code": -1}
    allowed_bins = {"python", "python3", "pip", "git"}
    if parts[0] not in allowed_bins:
        return {"ok": False, "error_code": "COMMAND_NOT_ALLOWED", "stdout": "", "stderr": f"bin_not_allowed:{parts[0]}", "exit_code": -1}
    cwd = str(Path(str(settings.project_root)).resolve())
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
            _persist_task(store, task)

        verify = _run_acceptance_command(settings, cmd)

        with _AGENT_TASKS_LOCK:
            task = _AGENT_TASKS.get(task_id)
            if not isinstance(task, dict) or task.get("status") == "cancelled":
                return
            task["last_verify"] = verify
            task["updated_at_ms"] = _now_ms()
            artifacts = task.setdefault("artifacts", {"changed_files": [], "test_result": None, "summary": ""})
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
    task = {
        "task_id": task_id,
        "status": "queued",
        "goal": body.goal,
        "scope_paths": list(body.scope_paths),
        "forbidden_paths": list(body.forbidden_paths),
        "acceptance_cmd": body.acceptance_cmd,
        "max_retry": int(body.max_retry),
        "attempt": 0,
        "dry_run": bool(body.dry_run),
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
        "artifacts": {"changed_files": [], "test_result": None, "summary": ""},
    }
    _task_log(task, "task_created", _get_store(request))
    with _AGENT_TASKS_LOCK:
        _AGENT_TASKS[task_id] = task

    store = _get_store(request)
    _persist_task(store, task)

    if body.auto_start:
        settings = _get_settings(request)
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
            task["artifacts"] = (fn_art(task.get("task_id")) if callable(fn_art) else None) or {"changed_files": [], "test_result": None, "summary": ""}
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
            task["artifacts"] = (fn_art(task_id) if callable(fn_art) else None) or {"changed_files": [], "test_result": None, "summary": ""}
            fn_logs = getattr(store, "list_agent_task_logs", None)
            if callable(fn_logs):
                task["logs"] = fn_logs(task_id, limit=500)
            return {"ok": True, **_task_public(task)}
    with _AGENT_TASKS_LOCK:
        task = _AGENT_TASKS.get(task_id)
        if not isinstance(task, dict):
            raise _task_not_found(task_id)
        return {"ok": True, **_task_public(task)}


@router.get("/api/agent/tasks/{task_id}/artifacts")
def api_agent_task_artifacts(request: Request, task_id: str) -> dict:
    store = _get_store(request)
    fn = getattr(store, "get_agent_task_artifacts", None)
    fn_task = getattr(store, "get_agent_task", None)
    if callable(fn):
        art = fn(task_id)
        if art is not None:
            t = fn_task(task_id) if callable(fn_task) else None
            return {"ok": True, "task_id": task_id, "artifacts": art, "last_verify": (t.get("last_verify") if isinstance(t, dict) else None)}
    with _AGENT_TASKS_LOCK:
        task = _AGENT_TASKS.get(task_id)
        if not isinstance(task, dict):
            raise _task_not_found(task_id)
        return {
            "ok": True,
            "task_id": task_id,
            "artifacts": task.get("artifacts") or {"changed_files": [], "test_result": None, "summary": ""},
            "last_verify": task.get("last_verify"),
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
