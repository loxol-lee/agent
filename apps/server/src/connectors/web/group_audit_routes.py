from __future__ import annotations

from fastapi import APIRouter
from typing import Any, Callable
import hashlib
import json

from .group_common import build_group_router

router = build_group_router(tag="group-audit")


def parse_group_refs_json(refs_raw: Any) -> dict[str, Any]:
    if isinstance(refs_raw, str) and refs_raw.strip():
        try:
            v = json.loads(refs_raw)
            if isinstance(v, dict):
                return v
        except Exception:
            return {}
    return {}


def build_group_trace_map_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    trace_ids: set[str] = set()
    request_ids: set[str] = set()
    mapped_rows = 0
    for it in rows:
        refs = parse_group_refs_json(it.get("refs_json"))
        tid = str(refs.get("trace_id") or "").strip()
        rid = str(refs.get("request_id") or "").strip()
        if tid:
            trace_ids.add(tid)
        if rid:
            request_ids.add(rid)
        if tid or rid:
            mapped_rows += 1
    return {"thread_messages": len(rows), "trace_rows": mapped_rows, "trace_ids": len(trace_ids), "request_ids": len(request_ids)}


def build_group_trace_map_items(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    items: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for m in rows:
        refs = parse_group_refs_json(m.get("refs_json"))
        trace_id = str(refs.get("trace_id") or "").strip()
        request_id = str(refs.get("request_id") or "").strip()
        call_id = str(refs.get("call_id") or "").strip()
        if not trace_id and not request_id:
            continue
        row = {
            "round_no": int(m.get("round_no") or 0),
            "agent_id": str(m.get("agent_id") or ""),
            "role": str(m.get("role") or ""),
            "trace_id": trace_id,
            "request_id": request_id,
            "call_id": call_id,
            "created_at_ms": int(m.get("created_at_ms") or 0),
        }
        key = (row["trace_id"], row["request_id"], row["agent_id"], str(row["round_no"]))
        if key in seen:
            continue
        seen.add(key)
        items.append(row)
    trace_ids = {str(x.get("trace_id") or "") for x in items if str(x.get("trace_id") or "")}
    request_ids = {str(x.get("request_id") or "") for x in items if str(x.get("request_id") or "")}
    counts = {"thread_messages": len(rows), "trace_rows": len(items), "trace_ids": len(trace_ids), "request_ids": len(request_ids)}
    return items, counts


def build_group_audit_closure_checks(completeness: dict[str, Any], audit_counts: dict[str, Any], trace_counts: dict[str, Any]) -> dict[str, bool]:
    checks = {
        "export_ready": bool(completeness.get("export_ready")),
        "ids_complete": bool(completeness.get("ids_complete")),
        "thread_non_empty": int(audit_counts.get("thread") or 0) > 0,
        "decisions_non_empty": int(audit_counts.get("decisions") or 0) > 0,
        "rounds_non_empty": int(audit_counts.get("rounds") or 0) > 0,
        "trace_mapped": int(trace_counts.get("trace_rows") or 0) > 0,
        "trace_id_present": int(trace_counts.get("trace_ids") or 0) > 0,
        "request_id_present": int(trace_counts.get("request_ids") or 0) > 0,
    }
    checks["audit_closure_ready"] = all(bool(v) for v in checks.values())
    return checks


def handle_group_task_trace_map(*, task_id: str, limit: int, store: Any, get_task_or_raise: Callable[[Any, str], dict]) -> dict:
    fn_thread = getattr(store, "list_group_agent_messages", None)
    if not callable(fn_thread):
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    task = get_task_or_raise(store, task_id)
    rows = fn_thread(task_id, limit=max(1, min(int(limit), 5000)), cursor=None, order="asc", role=None) or []
    items, counts = build_group_trace_map_items(rows)
    return {"ok": True, "task_id": task_id, "status": str(task.get("status") or ""), "phase": str(task.get("phase") or ""), "counts": counts, "items": items}


def handle_group_task_audit(*, task_id: str, thread_limit: int, decisions_limit: int, rounds_limit: int, store: Any, get_task_or_raise: Callable[[Any, str], dict], get_artifacts: Callable[[Any, str], dict], public_task: Callable[[dict], dict]) -> dict:
    task = get_task_or_raise(store, task_id)
    fn_thread = getattr(store, "list_group_agent_messages", None)
    fn_decisions = getattr(store, "list_group_decisions", None)
    fn_rounds = getattr(store, "list_group_task_rounds", None)
    thread_items = (fn_thread(task_id, limit=max(1, min(int(thread_limit), 5000)), cursor=None, order="asc", role=None) if callable(fn_thread) else []) or []
    decision_items = (fn_decisions(task_id, limit=max(1, min(int(decisions_limit), 2000)), cursor=None, order="asc") if callable(fn_decisions) else []) or []
    rounds_items = (fn_rounds(task_id, limit=max(1, min(int(rounds_limit), 5000)), cursor=None, order="asc") if callable(fn_rounds) else []) or []
    artifacts = get_artifacts(store, task_id)
    payload = {"thread": thread_items, "rounds": rounds_items, "decisions": decision_items, "artifacts": artifacts}
    audit_hash = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return {
        "ok": True,
        "task": public_task(task),
        "counts": {"thread": len(thread_items), "rounds": len(rounds_items), "decisions": len(decision_items), "changed_files": len(list(artifacts.get("changed_files") or []))},
        "audit_hash": audit_hash,
        "audit_hash_algo": "sha256-stable-json-v1",
    }


def handle_group_task_export(*, task_id: str, thread_limit: int, decisions_limit: int, rounds_limit: int, store: Any, get_task_or_raise: Callable[[Any, str], dict], get_artifacts: Callable[[Any, str], dict], public_task: Callable[[dict], dict]) -> dict:
    task = get_task_or_raise(store, task_id)
    task_pub = public_task(task)
    audit = {"task_id": str(task.get("task_id") or ""), "request_id": str(task.get("request_id") or ""), "trace_id": str(task.get("trace_id") or "")}
    fn_thread = getattr(store, "list_group_agent_messages", None)
    fn_decisions = getattr(store, "list_group_decisions", None)
    fn_rounds = getattr(store, "list_group_task_rounds", None)
    thread_items = (fn_thread(task_id, limit=max(1, min(int(thread_limit), 5000)), cursor=None, order="asc") if callable(fn_thread) else []) or []
    decision_items = (fn_decisions(task_id, limit=max(1, min(int(decisions_limit), 2000))) if callable(fn_decisions) else []) or []
    rounds_items = (fn_rounds(task_id, limit=max(1, min(int(rounds_limit), 5000))) if callable(fn_rounds) else []) or []
    artifacts = get_artifacts(store, task_id)
    annotated_rounds = [{**dict(it), "task_id": str(dict(it).get("task_id") or audit["task_id"]), "request_id": str(dict(it).get("request_id") or audit["request_id"]), "trace_id": str(dict(it).get("trace_id") or audit["trace_id"])} for it in rounds_items]
    annotated_thread = [{**dict(it), "task_id": str(dict(it).get("task_id") or audit["task_id"]), "request_id": str(dict(it).get("request_id") or audit["request_id"]), "trace_id": str(dict(it).get("trace_id") or audit["trace_id"])} for it in thread_items]
    annotated_decisions = [{**dict(it), "task_id": str(dict(it).get("task_id") or audit["task_id"]), "request_id": str(dict(it).get("request_id") or audit["request_id"]), "trace_id": str(dict(it).get("trace_id") or audit["trace_id"])} for it in decision_items]
    trace_map_summary = build_group_trace_map_summary(annotated_thread)
    tr = str(artifacts.get("test_result") or "").strip().lower()
    qa_gate_pass = tr in {"qa_passed", "passed", "ok", "success", "green"}
    phase_set = {str(x.get("phase") or "") for x in rounds_items if isinstance(x, dict)}
    phase_history_complete = bool({"discussion", "execution", "qa_verifying"}.issubset(phase_set))
    rounds_audit_linked = all(bool(str(x.get("task_id") or "") and str(x.get("request_id") or "") and str(x.get("trace_id") or "")) for x in annotated_rounds) if annotated_rounds else False
    completeness = {"ids_complete": bool(audit["task_id"] and audit["request_id"] and audit["trace_id"]), "thread_non_empty": len(annotated_thread) > 0, "decisions_non_empty": len(annotated_decisions) > 0, "rounds_non_empty": len(annotated_rounds) > 0, "rounds_audit_linked": rounds_audit_linked, "phase_history_complete": phase_history_complete, "artifacts_present": isinstance(artifacts, dict), "qa_gate_pass": qa_gate_pass}
    completeness["export_ready"] = all(bool(v) for v in completeness.values())
    export_payload = {"task": task_pub, "audit": audit, "trace_map_summary": trace_map_summary, "thread": {"items": annotated_thread, "total": len(annotated_thread)}, "rounds": {"items": annotated_rounds, "total": len(annotated_rounds)}, "decisions": {"items": annotated_decisions, "total": len(annotated_decisions)}, "artifacts": artifacts, "completeness": completeness}
    export_signature = hashlib.sha256(json.dumps(export_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return {"ok": True, **export_payload, "export_signature": export_signature, "export_signature_algo": "sha256-stable-json-v1"}


def handle_group_task_audit_closure(*, task_id: str, export_out: dict, audit_out: dict, trace_out: dict) -> dict:
    completeness = dict(export_out.get("completeness") or {})
    trace_counts = dict(trace_out.get("counts") or {})
    audit_counts = dict(audit_out.get("counts") or {})
    checks = build_group_audit_closure_checks(completeness, audit_counts, trace_counts)
    return {
        "ok": True,
        "task_id": task_id,
        "audit_hash": str(audit_out.get("audit_hash") or ""),
        "export_signature": str(export_out.get("export_signature") or ""),
        "checks": checks,
        "counts": {"audit": audit_counts, "trace_map": trace_counts},
    }


def register_group_audit_routes(main_router: APIRouter) -> None:
    main_router.include_router(router)


__all__ = [
    "router",
    "register_group_audit_routes",
    "parse_group_refs_json",
    "build_group_trace_map_summary",
    "build_group_trace_map_items",
    "build_group_audit_closure_checks",
    "handle_group_task_trace_map",
    "handle_group_task_audit",
    "handle_group_task_export",
    "handle_group_task_audit_closure",
]
