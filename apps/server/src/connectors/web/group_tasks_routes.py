from __future__ import annotations

from fastapi import APIRouter, HTTPException
from typing import Any, Callable
import json

from .group_common import build_group_router

router = build_group_router(tag="group-tasks")


def get_group_task_or_raise(store: Any, task_id: str, *, not_found: Callable[[str], HTTPException]) -> dict[str, Any]:
    fn_get = getattr(store, "get_group_task", None)
    if not callable(fn_get):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    task = fn_get(task_id)
    if not isinstance(task, dict):
        raise not_found(task_id)
    return task


def list_group_tasks_or_raise(
    store: Any,
    *,
    limit: int,
    status: str | None,
    phase: str | None,
    owner_id: str | None,
) -> list[dict[str, Any]]:
    fn_list = getattr(store, "list_group_tasks", None)
    if not callable(fn_list):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    return fn_list(
        limit=max(1, min(int(limit), 200)),
        status=(str(status).strip() if status is not None else None),
        phase=(str(phase).strip() if phase is not None else None),
        owner_id=(str(owner_id).strip() if owner_id is not None else None),
        cursor_updated_at_ms=None,
    ) or []


def get_group_artifacts_or_default(store: Any, task_id: str) -> dict[str, Any]:
    fn_get = getattr(store, "get_group_artifacts", None)
    artifacts = (fn_get(task_id) if callable(fn_get) else None) or {}
    return {
        "changed_files": [str(x) for x in list(artifacts.get("changed_files") or [])],
        "test_result": artifacts.get("test_result"),
        "summary": str(artifacts.get("summary") or ""),
    }


def handle_group_task_execution_submit(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], is_closed: Callable[[dict], bool], normalize_role: Callable[[str], str], assert_can_submit_execution: Callable[[str], None], phase_execution: str, phase_qa_verifying: str, status_running: str, err_task_closed: str, err_exec_phase_invalid: str, public_task: Callable[[dict], dict], now_ms: Callable[[], int], put_task: Callable[[Any, dict], None], write_round: Callable[[Any, str, str, str, str], None]) -> dict:
    task = get_task_or_raise(store, task_id)
    if is_closed(task):
        raise HTTPException(status_code=409, detail={"code": err_task_closed})
    try:
        actor_role = normalize_role(str(getattr(body, "actor_role", "") or ""))
        assert_can_submit_execution(actor_role)
    except ValueError as e:
        code = str(e)
        if code == "ROLE_PERMISSION_DENIED":
            raise HTTPException(status_code=403, detail={"code": code, "actor_role": str(getattr(body, "actor_role", "") or "")})
        raise HTTPException(status_code=400, detail={"code": code})
    current_phase = str(task.get("phase") or "")
    if current_phase != phase_execution:
        raise HTTPException(status_code=409, detail={"code": err_exec_phase_invalid, "current_phase": current_phase, "required_phase": phase_execution})
    now = now_ms()
    task["phase"] = phase_qa_verifying
    task["status"] = status_running
    task["stop_reason"] = None
    task["error_code"] = None
    task["updated_at_ms"] = now
    task["finished_at_ms"] = None
    put_task(store, task)
    write_round(store, task_id, phase_qa_verifying, actor_role, "execution_submitted")
    changed_files = [str(x).strip() for x in list(getattr(body, "changed_files", []) or []) if str(x).strip()]
    summary = str(getattr(body, "summary", "") or "").strip()[:4000]
    fn_get_art = getattr(store, "get_group_artifacts", None)
    fn_upsert_art = getattr(store, "upsert_group_artifacts", None)
    if callable(fn_upsert_art):
        cur = (fn_get_art(task_id) if callable(fn_get_art) else None) or {"changed_files": [], "test_result": None, "summary": ""}
        old_files = [str(x) for x in list(cur.get("changed_files") or [])]
        merged_files = old_files + [x for x in changed_files if x not in old_files]
        note = f"[execution_submit] by {actor_role}" + (f": {summary}" if summary else "")
        old_summary = str(cur.get("summary") or "")
        merged_summary = (old_summary + "\n" + note).strip() if old_summary else note
        fn_upsert_art(task_id, {"changed_files": merged_files, "test_result": "execution_submitted", "summary": merged_summary})
    fn_msg = getattr(store, "insert_group_agent_message", None)
    if callable(fn_msg):
        msg = "execution_submitted" + (f": {summary}" if summary else "")
        refs_json = json.dumps({"changed_files": changed_files}, ensure_ascii=False)
        fn_msg(task_id, 3, actor_role, actor_role, msg, refs_json)
    return {"ok": True, "execution": {"submitted_by": actor_role, "changed_files": changed_files, "summary": summary, "at_ms": now}, **public_task(task)}


def handle_group_task_qa_review(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], normalize_role: Callable[[str], str], assert_can_qa_review: Callable[[str], None], phase_qa_verifying: str, phase_decision: str, phase_execution: str, status_from_phase: Callable[[str], str], public_task: Callable[[dict], dict], now_ms: Callable[[], int], put_task: Callable[[Any, dict], None], write_round: Callable[[Any, str, str, str, str], None], err_qa_phase_invalid: str) -> dict:
    task = get_task_or_raise(store, task_id)
    try:
        actor_role = normalize_role(str(getattr(body, "actor_role", "") or ""))
        assert_can_qa_review(actor_role)
    except ValueError as e:
        code = str(e)
        if code == "ROLE_PERMISSION_DENIED":
            raise HTTPException(status_code=403, detail={"code": code, "actor_role": str(getattr(body, "actor_role", "") or "")})
        raise HTTPException(status_code=400, detail={"code": code})
    current_phase = str(task.get("phase") or "")
    if current_phase != phase_qa_verifying:
        raise HTTPException(status_code=409, detail={"code": err_qa_phase_invalid, "current_phase": current_phase, "required_phase": phase_qa_verifying})
    passed = bool(getattr(body, "passed", False))
    reason = str(getattr(body, "reason", "") or "").strip()[:1000]
    now = now_ms()
    task["phase"] = (phase_decision if passed else phase_execution)
    task["status"] = status_from_phase(str(task.get("phase") or ""))
    task["stop_reason"] = ("qa_passed" if passed else "rework_requested")
    task["error_code"] = (None if passed else "QA_REJECTED")
    task["updated_at_ms"] = now
    task["finished_at_ms"] = None
    put_task(store, task)
    write_round(store, task_id, str(task.get("phase") or ""), actor_role, ("qa_passed" if passed else "qa_rejected"))
    fn_msg = getattr(store, "insert_group_agent_message", None)
    if callable(fn_msg):
        msg = ("qa_passed" if passed else "qa_rejected") + ((f": {reason}") if reason else "")
        fn_msg(task_id, 10001, actor_role, actor_role, msg, "{}")
    fn_get_art = getattr(store, "get_group_artifacts", None)
    fn_upsert_art = getattr(store, "upsert_group_artifacts", None)
    if callable(fn_upsert_art):
        cur = (fn_get_art(task_id) if callable(fn_get_art) else None) or {"changed_files": [], "test_result": None, "summary": ""}
        old_summary = str(cur.get("summary") or "")
        qa_note = f"[qa_review] {'passed' if passed else 'rejected'} by {actor_role}" + (f": {reason}" if reason else "")
        summary = (old_summary + "\n" + qa_note).strip() if old_summary else qa_note
        fn_upsert_art(task_id, {"changed_files": list(cur.get("changed_files") or []), "test_result": ("qa_passed" if passed else "qa_rejected"), "summary": summary})
    return {"ok": True, "qa_review": {"passed": passed, "reason": reason, "reviewed_by": actor_role, "at_ms": now}, **public_task(task)}


def handle_group_task_cancel(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], normalize_role: Callable[[str], str], assert_can_cancel: Callable[[str], None], assert_owner: Callable[[dict, str], None], status_done: str, status_cancelled: str, public_task: Callable[[dict], dict], now_ms: Callable[[], int], put_task: Callable[[Any, dict], None], write_round: Callable[[Any, str, str, str, str], None], decision_type_cancel: str) -> dict:
    task = get_task_or_raise(store, task_id)
    try:
        actor_role = normalize_role(str(getattr(body, "actor_role", "") or ""))
        assert_can_cancel(actor_role)
        assert_owner(task, str(getattr(body, "owner_id", "") or ""))
    except ValueError as e:
        code = str(e)
        if code in {"ROLE_PERMISSION_DENIED", "GROUP_OWNER_MISMATCH"}:
            raise HTTPException(status_code=403, detail={"code": code, "actor_role": str(getattr(body, "actor_role", "") or ""), "owner_id": str(getattr(body, "owner_id", "") or "")})
        raise HTTPException(status_code=400, detail={"code": code})
    cur_status = str(task.get("status") or "")
    if cur_status in {status_done, status_cancelled}:
        return {"ok": True, **public_task(task)}
    now = now_ms()
    reason = str(getattr(body, "reason", "") or "").strip()[:1000]
    task["status"] = status_cancelled
    task["stop_reason"] = "cancelled"
    task["error_code"] = "TASK_CANCELLED"
    task["updated_at_ms"] = now
    task["finished_at_ms"] = now
    put_task(store, task)
    write_round(store, task_id, str(task.get("phase") or ""), actor_role, "cancelled")
    fn_msg = getattr(store, "insert_group_agent_message", None)
    if callable(fn_msg):
        msg = "cancelled" + (f": {reason}" if reason else "")
        fn_msg(task_id, 10002, actor_role, actor_role, msg, "{}")
    fn_decision = getattr(store, "insert_group_decision", None)
    if callable(fn_decision):
        fn_decision(task_id, decision_type_cancel, None, (reason or "cancelled_by_owner"), actor_role)
    return {"ok": True, **public_task(task)}


def handle_group_task_thread_message(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], normalize_role: Callable[[str], str], assert_can_post_thread: Callable[[str], None], status_done: str, status_cancelled: str, err_task_closed: str, phase_planning: str, phase_discussion: str, status_created: str, now_ms: Callable[[], int], put_task: Callable[[Any, dict], None], write_round: Callable[[Any, str, str, str, str], None], public_task: Callable[[dict], dict]) -> dict:
    fn_msg = getattr(store, "insert_group_agent_message", None)
    if not callable(fn_msg):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    task = get_task_or_raise(store, task_id)
    if str(task.get("status") or "") in {status_done, status_cancelled}:
        raise HTTPException(status_code=409, detail={"code": err_task_closed})
    try:
        actor_role = normalize_role(str(getattr(body, "actor_role", "") or ""))
        assert_can_post_thread(actor_role)
    except ValueError as e:
        code = str(e)
        if code == "ROLE_PERMISSION_DENIED":
            raise HTTPException(status_code=403, detail={"code": code, "actor_role": str(getattr(body, "actor_role", "") or "")})
        raise HTTPException(status_code=400, detail={"code": code})
    phase = str(task.get("phase") or phase_planning)
    if phase == phase_planning:
        task["phase"] = phase_discussion
        task["status"] = status_created
        task["updated_at_ms"] = now_ms()
        put_task(store, task)
        write_round(store, task_id, phase_discussion, actor_role, "thread_first_message")
    round_no_map = {phase_planning: 1, phase_discussion: 2, "execution": 3, "qa_verifying": 4, "decision": 5}
    round_no = int(round_no_map.get(str(task.get("phase") or ""), 2))
    refs = dict(getattr(body, "refs", {}) or {})
    content = str(getattr(body, "content", "") or "").strip()[:4000]
    fn_msg(task_id, round_no, actor_role, actor_role, content, json.dumps(refs, ensure_ascii=False))
    return {"ok": True, "message": {"task_id": task_id, "round_no": round_no, "agent_id": actor_role, "role": actor_role, "content": content, "refs": refs}, **public_task(task)}


def handle_group_task_artifacts_update(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], get_artifacts_default: Callable[[Any, str], dict], is_closed: Callable[[dict], bool], normalize_role: Callable[[str], str], assert_can_upsert_artifacts: Callable[[str], None], err_task_closed: str, round_no_from_phase: Callable[[str], int]) -> dict:
    fn_upsert = getattr(store, "upsert_group_artifacts", None)
    if not callable(fn_upsert):
        raise HTTPException(status_code=500, detail={"code": "GROUP_TASK_STORAGE_NOT_SUPPORTED"})
    task = get_task_or_raise(store, task_id)
    if is_closed(task):
        raise HTTPException(status_code=409, detail={"code": err_task_closed})
    try:
        actor_role = normalize_role(str(getattr(body, "actor_role", "") or ""))
        assert_can_upsert_artifacts(actor_role)
    except ValueError as e:
        code = str(e)
        if code == "ROLE_PERMISSION_DENIED":
            raise HTTPException(status_code=403, detail={"code": code, "actor_role": str(getattr(body, "actor_role", "") or "")})
        raise HTTPException(status_code=400, detail={"code": code})
    cur = get_artifacts_default(store, task_id)
    old_files = [str(x) for x in list(cur.get("changed_files") or [])]
    new_files = [str(x).strip() for x in list(getattr(body, "changed_files", []) or []) if str(x).strip()]
    merged_files = old_files + [x for x in new_files if x not in old_files]
    old_summary = str(cur.get("summary") or "")
    append_summary = str(getattr(body, "summary", "") or "").strip()[:8000]
    merged_summary = (old_summary + "\n" + append_summary).strip() if old_summary and append_summary else (append_summary or old_summary)
    test_result = (getattr(body, "test_result", None) if getattr(body, "test_result", None) is not None else cur.get("test_result"))
    fn_upsert(task_id, {"changed_files": merged_files, "test_result": test_result, "summary": merged_summary})
    fn_msg = getattr(store, "insert_group_agent_message", None)
    if callable(fn_msg):
        refs_json = json.dumps({"changed_files_delta": new_files, "test_result": test_result}, ensure_ascii=False)
        fn_msg(task_id, round_no_from_phase(str(task.get("phase") or "")), actor_role, actor_role, "artifacts_updated", refs_json)
    return {"ok": True, "task_id": task_id, "artifacts": {"changed_files": merged_files, "test_result": test_result, "summary": merged_summary}}


def handle_group_task_approve(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], get_artifacts: Callable[[Any, str], dict], is_closed: Callable[[dict], bool], normalize_role: Callable[[str], str], assert_can_approve: Callable[[str], None], assert_owner: Callable[[dict, str], None], quality_gate_ci: Callable[[str, dict], dict], phase_decision: str, status_done: str, err_task_closed: str, err_approve_phase_invalid: str, err_quality_gate_blocked: str, public_task: Callable[[dict], dict], now_ms: Callable[[], int], put_task: Callable[[Any, dict], None], write_round: Callable[[Any, str, str, str, str], None], decision_type_approve: str) -> dict:
    task = get_task_or_raise(store, task_id)
    if is_closed(task):
        raise HTTPException(status_code=409, detail={"code": err_task_closed})
    try:
        actor_role = normalize_role(str(getattr(body, "actor_role", "") or ""))
        assert_can_approve(actor_role)
    except ValueError as e:
        code = str(e)
        if code == "ROLE_PERMISSION_DENIED":
            raise HTTPException(status_code=403, detail={"code": code, "actor_role": str(getattr(body, "actor_role", "") or "")})
        raise HTTPException(status_code=400, detail={"code": code})
    try:
        assert_owner(task, str(getattr(body, "owner_id", "") or ""))
    except ValueError as e:
        raise HTTPException(status_code=403, detail={"code": str(e), "owner_id": str(getattr(body, "owner_id", "") or "")})
    current_phase = str(task.get("phase") or "")
    if current_phase != phase_decision:
        raise HTTPException(status_code=409, detail={"code": err_approve_phase_invalid, "current_phase": current_phase, "required_phase": phase_decision})
    artifacts = get_artifacts(store, task_id)
    qg = quality_gate_ci(task_id, artifacts)
    if bool(qg.get("blocked")):
        raise HTTPException(status_code=409, detail={"code": err_quality_gate_blocked, "quality_gate_ci": qg})
    owner_id = str(getattr(body, "owner_id", "") or task.get("owner_id") or "owner")
    winner_plan_id = (str(getattr(body, "winner_plan_id", "")).strip() if getattr(body, "winner_plan_id", None) is not None else None)
    reason = str(getattr(body, "reason", "") or "").strip()[:2000]
    now = now_ms()
    task["status"] = status_done
    task["phase"] = phase_decision
    task["stop_reason"] = "approved"
    task["error_code"] = None
    task["updated_at_ms"] = now
    task["finished_at_ms"] = now
    put_task(store, task)
    write_round(store, task_id, phase_decision, actor_role, (reason or "approved"))
    fn_msg = getattr(store, "insert_group_agent_message", None)
    if callable(fn_msg):
        refs_json = json.dumps({"winner_plan_id": winner_plan_id}, ensure_ascii=False)
        msg = "approved" + (f": {reason}" if reason else "")
        fn_msg(task_id, 9999, owner_id, "owner", msg, refs_json)
    fn_decision = getattr(store, "insert_group_decision", None)
    if callable(fn_decision):
        fn_decision(task_id, decision_type_approve, winner_plan_id, (reason or "approved_by_owner"), owner_id)
    return {"ok": True, **public_task(task)}


def handle_group_task_rerun(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], is_closed: Callable[[dict], bool], normalize_phase: Callable[[str], str], normalize_role: Callable[[str], str], assert_can_rerun: Callable[[str, str], None], assert_transition: Callable[[str, str], None], phase_planning: str, err_task_closed: str, err_transition_not_allowed: str, err_phase_not_allowed: str, status_from_phase: Callable[[str], str], public_task: Callable[[dict], dict], now_ms: Callable[[], int], put_task: Callable[[Any, dict], None], write_round: Callable[[Any, str, str, str, str], None]) -> dict:
    task = get_task_or_raise(store, task_id)
    if is_closed(task):
        raise HTTPException(status_code=409, detail={"code": err_task_closed})
    try:
        phase = normalize_phase(str(getattr(body, "phase", "") or ""))
        actor_role = normalize_role(str(getattr(body, "actor_role", "") or ""))
        assert_can_rerun(actor_role, phase)
    except ValueError as e:
        code = str(e)
        if code == "ROLE_PERMISSION_DENIED":
            raise HTTPException(status_code=403, detail={"code": code, "actor_role": str(getattr(body, "actor_role", "") or ""), "next_phase": str(getattr(body, "phase", "") or "")})
        raise HTTPException(status_code=400, detail={"code": code})
    current_phase = str(task.get("phase") or phase_planning)
    try:
        assert_transition(current_phase, phase)
    except ValueError as e:
        if str(e) == err_transition_not_allowed:
            raise HTTPException(status_code=409, detail={"code": err_transition_not_allowed, "current_phase": current_phase, "next_phase": phase})
        raise HTTPException(status_code=400, detail={"code": err_phase_not_allowed})
    now = now_ms()
    task["phase"] = phase
    task["status"] = status_from_phase(phase)
    task["stop_reason"] = None
    task["error_code"] = None
    task["updated_at_ms"] = now
    task["finished_at_ms"] = None
    put_task(store, task)
    write_round(store, task_id, phase, actor_role, "rerun")
    fn_msg = getattr(store, "insert_group_agent_message", None)
    if callable(fn_msg):
        fn_msg(task_id, 10000, actor_role, actor_role, f"rerun:{phase}", "{}")
    return {"ok": True, **public_task(task)}


def register_group_tasks_routes(main_router: APIRouter) -> None:
    main_router.include_router(router)


__all__ = [
    "router",
    "register_group_tasks_routes",
    "get_group_task_or_raise",
    "list_group_tasks_or_raise",
    "get_group_artifacts_or_default",
    "handle_group_task_execution_submit",
    "handle_group_task_qa_review",
    "handle_group_task_cancel",
    "handle_group_task_thread_message",
    "handle_group_task_artifacts_update",
    "handle_group_task_approve",
    "handle_group_task_rerun",
]
