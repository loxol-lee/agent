from __future__ import annotations

from fastapi import APIRouter, HTTPException
from typing import Any, Callable

from .group_common import build_group_router

router = build_group_router(tag="group-prechecks")


def parse_group_task_ids(task_ids: str, *, limit: int = 200) -> list[str]:
    return [x.strip() for x in str(task_ids or "").split(",") if x.strip()][: max(1, min(int(limit), 1000))]


def collect_group_precheck_items(task_ids: list[str], fetcher: Callable[[str], dict]) -> tuple[list[dict], list[dict]]:
    items: list[dict] = []
    errors: list[dict] = []
    for tid in task_ids:
        try:
            items.append(fetcher(tid))
        except HTTPException as e:
            code = "HTTP_ERROR"
            if isinstance(e.detail, dict):
                code = str(e.detail.get("code") or code)
            errors.append({"task_id": tid, "code": code, "status_code": int(e.status_code)})
    return items, errors


def build_group_task_can_response(task_id: str, actor_role: str, prechecks_out: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": True,
        "task_id": task_id,
        "actor_role": str(prechecks_out.get("actor_role") or actor_role),
        "phase": str(prechecks_out.get("phase") or ""),
        "closed": bool(prechecks_out.get("closed")),
        "can": dict(prechecks_out.get("can") or {}),
    }


def handle_group_task_approve_precheck(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], normalize_role: Callable[[str], str], assert_can_approve: Callable[[str], None], assert_owner: Callable[[dict, str], None], is_closed: Callable[[dict], bool], get_artifacts: Callable[[Any, str], dict], quality_gate_ci: Callable[[str, dict], dict], phase_decision: str, err_task_closed: str, err_approve_phase_invalid: str, err_quality_gate_blocked: str) -> dict:
    task = get_task_or_raise(store, task_id)
    blockers: list[str] = []
    actor_role = str(getattr(body, "actor_role", "") or "")
    try:
        actor_role = normalize_role(actor_role)
        assert_can_approve(actor_role)
    except ValueError as e:
        blockers.append(str(e))
    try:
        assert_owner(task, str(getattr(body, "owner_id", "") or ""))
    except ValueError as e:
        blockers.append(str(e))
    if is_closed(task):
        blockers.append(err_task_closed)
    phase = str(task.get("phase") or "")
    if phase != phase_decision:
        blockers.append(err_approve_phase_invalid)
    artifacts = get_artifacts(store, task_id)
    qg = quality_gate_ci(task_id, artifacts)
    if bool(qg.get("blocked")):
        blockers.append(err_quality_gate_blocked)
    return {"ok": True, "task_id": task_id, "can_approve": len(blockers) == 0, "actor_role": actor_role, "phase": phase, "blockers": blockers, "quality_gate_ci": qg}


def handle_group_task_rerun_precheck(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], normalize_phase: Callable[[str], str], normalize_role: Callable[[str], str], assert_can_rerun: Callable[[str, str], None], assert_transition: Callable[[str, str], None], is_closed: Callable[[dict], bool], phase_planning: str, err_task_closed: str) -> dict:
    task = get_task_or_raise(store, task_id)
    blockers: list[str] = []
    phase = str(getattr(body, "phase", "") or "")
    actor_role = str(getattr(body, "actor_role", "") or "")
    try:
        phase = normalize_phase(phase)
        actor_role = normalize_role(actor_role)
        assert_can_rerun(actor_role, phase)
    except ValueError as e:
        blockers.append(str(e))
    current_phase = str(task.get("phase") or phase_planning)
    try:
        assert_transition(current_phase, phase if phase else current_phase)
    except ValueError as e:
        blockers.append(str(e))
    if is_closed(task):
        blockers.append(err_task_closed)
    return {"ok": True, "task_id": task_id, "can_rerun": len(blockers) == 0, "actor_role": actor_role, "current_phase": current_phase, "next_phase": phase, "blockers": blockers}


def handle_group_task_execution_submit_precheck(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], normalize_role: Callable[[str], str], assert_can_submit_execution: Callable[[str], None], is_closed: Callable[[dict], bool], phase_execution: str, err_execution_phase_invalid: str, err_task_closed: str) -> dict:
    task = get_task_or_raise(store, task_id)
    blockers: list[str] = []
    actor_role = str(getattr(body, "actor_role", "") or "")
    try:
        actor_role = normalize_role(actor_role)
        assert_can_submit_execution(actor_role)
    except ValueError as e:
        blockers.append(str(e))
    current_phase = str(task.get("phase") or "")
    if current_phase != phase_execution:
        blockers.append(err_execution_phase_invalid)
    if is_closed(task):
        blockers.append(err_task_closed)
    return {"ok": True, "task_id": task_id, "can_execution_submit": len(blockers) == 0, "actor_role": actor_role, "current_phase": current_phase, "required_phase": phase_execution, "blockers": blockers}


def handle_group_task_qa_review_precheck(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], normalize_role: Callable[[str], str], assert_can_qa_review: Callable[[str], None], is_closed: Callable[[dict], bool], phase_qa_verifying: str, err_qa_phase_invalid: str, err_task_closed: str) -> dict:
    task = get_task_or_raise(store, task_id)
    blockers: list[str] = []
    actor_role = str(getattr(body, "actor_role", "") or "")
    try:
        actor_role = normalize_role(actor_role)
        assert_can_qa_review(actor_role)
    except ValueError as e:
        blockers.append(str(e))
    current_phase = str(task.get("phase") or "")
    if current_phase != phase_qa_verifying:
        blockers.append(err_qa_phase_invalid)
    if is_closed(task):
        blockers.append(err_task_closed)
    return {"ok": True, "task_id": task_id, "can_qa_review": len(blockers) == 0, "actor_role": actor_role, "current_phase": current_phase, "required_phase": phase_qa_verifying, "blockers": blockers}


def handle_group_task_thread_message_precheck(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], normalize_role: Callable[[str], str], assert_can_post_thread: Callable[[str], None], is_closed: Callable[[dict], bool], err_task_closed: str) -> dict:
    task = get_task_or_raise(store, task_id)
    blockers: list[str] = []
    actor_role = str(getattr(body, "actor_role", "") or "")
    try:
        actor_role = normalize_role(actor_role)
        assert_can_post_thread(actor_role)
    except ValueError as e:
        blockers.append(str(e))
    if is_closed(task):
        blockers.append(err_task_closed)
    return {"ok": True, "task_id": task_id, "can_post_thread_message": len(blockers) == 0, "actor_role": actor_role, "phase": str(task.get("phase") or ""), "status": str(task.get("status") or ""), "blockers": blockers}


def handle_group_task_artifacts_update_precheck(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], normalize_role: Callable[[str], str], assert_can_upsert_artifacts: Callable[[str], None], is_closed: Callable[[dict], bool], err_task_closed: str) -> dict:
    task = get_task_or_raise(store, task_id)
    blockers: list[str] = []
    actor_role = str(getattr(body, "actor_role", "") or "")
    try:
        actor_role = normalize_role(actor_role)
        assert_can_upsert_artifacts(actor_role)
    except ValueError as e:
        blockers.append(str(e))
    if is_closed(task):
        blockers.append(err_task_closed)
    return {"ok": True, "task_id": task_id, "can_update_artifacts": len(blockers) == 0, "actor_role": actor_role, "phase": str(task.get("phase") or ""), "status": str(task.get("status") or ""), "blockers": blockers}


def handle_group_task_cancel_precheck(*, task_id: str, body: Any, store: Any, get_task_or_raise: Callable[[Any, str], dict], normalize_role: Callable[[str], str], assert_can_cancel: Callable[[str], None], assert_owner: Callable[[dict, str], None], is_closed: Callable[[dict], bool], err_task_closed: str) -> dict:
    task = get_task_or_raise(store, task_id)
    blockers: list[str] = []
    actor_role = str(getattr(body, "actor_role", "") or "")
    try:
        actor_role = normalize_role(actor_role)
        assert_can_cancel(actor_role)
    except ValueError as e:
        blockers.append(str(e))
    try:
        assert_owner(task, str(getattr(body, "owner_id", "") or ""))
    except ValueError as e:
        blockers.append(str(e))
    if is_closed(task):
        blockers.append(err_task_closed)
    return {"ok": True, "task_id": task_id, "can_cancel": len(blockers) == 0, "actor_role": actor_role, "status": str(task.get("status") or ""), "phase": str(task.get("phase") or ""), "blockers": blockers}


def handle_group_task_prechecks(*, task_id: str, actor_role: str, owner_id: str, next_phase: str | None, store: Any, get_task_or_raise: Callable[[Any, str], dict], is_closed: Callable[[dict], bool], normalize_role: Callable[[str], str], normalize_phase: Callable[[str], str], assert_can_approve: Callable[[str], None], assert_owner: Callable[[dict, str], None], assert_can_rerun: Callable[[str, str], None], assert_transition: Callable[[str, str], None], assert_can_submit_execution: Callable[[str], None], assert_can_qa_review: Callable[[str], None], assert_can_post_thread: Callable[[str], None], assert_can_upsert_artifacts: Callable[[str], None], assert_can_cancel: Callable[[str], None], get_artifacts: Callable[[Any, str], dict], quality_gate_ci: Callable[[str, dict], dict], phase_decision: str, phase_execution: str, phase_qa_verifying: str, phase_planning: str, err_task_closed: str, err_approve_phase_invalid: str, err_qa_phase_invalid: str, err_quality_gate_blocked: str, err_exec_phase_invalid: str) -> dict:
    task = get_task_or_raise(store, task_id)
    role = str(actor_role or "owner")
    phase = str(task.get("phase") or "")
    closed = is_closed(task)
    blockers: dict[str, list[str]] = {"approve": [], "rerun": [], "execution_submit": [], "qa_review": [], "thread_message": [], "artifacts_update": [], "cancel": []}
    try:
        r = normalize_role(role)
    except ValueError as e:
        code = str(e)
        for k in blockers.keys(): blockers[k].append(code)
        return {"ok": True, "task_id": task_id, "actor_role": role, "owner_id": owner_id, "phase": phase, "closed": closed, "can": {k: False for k in blockers.keys()}, "blockers": blockers}
    if closed:
        for k in blockers.keys(): blockers[k].append(err_task_closed)
    try: assert_can_approve(r)
    except ValueError as e: blockers["approve"].append(str(e))
    try: assert_owner(task, owner_id)
    except ValueError as e: blockers["approve"].append(str(e)); blockers["cancel"].append(str(e))
    if phase != phase_decision: blockers["approve"].append(err_approve_phase_invalid)
    p = next_phase or phase_execution
    try:
        p = normalize_phase(p)
        assert_can_rerun(r, p)
        assert_transition(str(task.get("phase") or phase_planning), p)
    except ValueError as e: blockers["rerun"].append(str(e))
    if phase != phase_execution: blockers["execution_submit"].append(err_exec_phase_invalid)
    try: assert_can_submit_execution(r)
    except ValueError as e: blockers["execution_submit"].append(str(e))
    if phase != phase_qa_verifying: blockers["qa_review"].append(err_qa_phase_invalid)
    try: assert_can_qa_review(r)
    except ValueError as e: blockers["qa_review"].append(str(e))
    try: assert_can_post_thread(r)
    except ValueError as e: blockers["thread_message"].append(str(e))
    try: assert_can_upsert_artifacts(r)
    except ValueError as e: blockers["artifacts_update"].append(str(e))
    try: assert_can_cancel(r)
    except ValueError as e: blockers["cancel"].append(str(e))
    artifacts = get_artifacts(store, task_id)
    qg = quality_gate_ci(task_id, artifacts)
    if bool(qg.get("blocked")): blockers["approve"].append(err_quality_gate_blocked)
    can = {k: (len(v) == 0) for k, v in blockers.items()}
    return {"ok": True, "task_id": task_id, "actor_role": r, "owner_id": owner_id, "phase": phase, "closed": closed, "next_phase": p, "can": can, "blockers": blockers, "quality_gate_ci": qg}


def handle_group_tasks_prechecks_batch(*, task_ids: str, actor_role: str, owner_id: str, next_phase: str | None, fetch_prechecks: Callable[[str, str, str, str | None], dict]) -> dict:
    ids = parse_group_task_ids(task_ids, limit=200)
    items, errors = collect_group_precheck_items(ids, lambda tid: fetch_prechecks(tid, actor_role, owner_id, next_phase))
    return {"ok": True, "total": len(items), "errors": errors, "items": items}


def handle_group_task_can(*, task_id: str, actor_role: str, owner_id: str, next_phase: str | None, fetch_prechecks: Callable[[str, str, str, str | None], dict]) -> dict:
    out = fetch_prechecks(task_id, actor_role, owner_id, next_phase)
    return build_group_task_can_response(task_id, actor_role, out)


def handle_group_tasks_can_batch(*, task_ids: str, actor_role: str, owner_id: str, next_phase: str | None, fetch_can: Callable[[str, str, str, str | None], dict]) -> dict:
    ids = parse_group_task_ids(task_ids, limit=200)
    items, errors = collect_group_precheck_items(ids, lambda tid: fetch_can(tid, actor_role, owner_id, next_phase))
    return {"ok": True, "total": len(items), "errors": errors, "items": items}


def register_group_prechecks_routes(main_router: APIRouter) -> None:
    main_router.include_router(router)


__all__ = [
    "router",
    "register_group_prechecks_routes",
    "parse_group_task_ids",
    "collect_group_precheck_items",
    "build_group_task_can_response",
    "handle_group_task_prechecks",
    "handle_group_tasks_prechecks_batch",
    "handle_group_task_can",
    "handle_group_tasks_can_batch",
    "handle_group_task_approve_precheck",
    "handle_group_task_rerun_precheck",
    "handle_group_task_execution_submit_precheck",
    "handle_group_task_qa_review_precheck",
    "handle_group_task_thread_message_precheck",
    "handle_group_task_artifacts_update_precheck",
    "handle_group_task_cancel_precheck",
]
