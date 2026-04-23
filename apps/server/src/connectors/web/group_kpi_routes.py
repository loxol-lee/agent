from __future__ import annotations

from fastapi import APIRouter, HTTPException
from typing import Any, Callable

from .group_common import build_group_router

router = build_group_router(tag="group-kpi")


def build_group_task_kpi_response(
    *,
    task_id: str,
    owner_id: str,
    phase: str,
    status: str,
    thread_messages_count: int,
    rounds_count: int,
    decisions_count: int,
    changed_files_count: int,
    approve_ready: bool,
    quality_gate_pass: bool,
) -> dict:
    return {
        "ok": True,
        "task_id": task_id,
        "owner_id": owner_id,
        "phase": phase,
        "status": status,
        "kpi": {
            "thread_messages_count": int(thread_messages_count),
            "rounds_count": int(rounds_count),
            "decisions_count": int(decisions_count),
            "changed_files_count": int(changed_files_count),
            "approve_ready": bool(approve_ready),
            "quality_gate_pass": bool(quality_gate_pass),
        },
    }


def collect_group_kpi_items(task_ids: list[str], owner_filter: str | None, fetcher: Callable[[str], dict]) -> tuple[list[dict], list[dict]]:
    items: list[dict] = []
    errors: list[dict] = []
    owner_v = (str(owner_filter).strip() if owner_filter is not None else None)
    for tid in task_ids:
        try:
            out = fetcher(tid)
            if owner_v is not None and str(out.get("owner_id") or "") != owner_v:
                continue
            items.append(out)
        except HTTPException as e:
            code = "HTTP_ERROR"
            if isinstance(e.detail, dict):
                code = str(e.detail.get("code") or code)
            errors.append({"task_id": tid, "code": code, "status_code": int(e.status_code)})
    return items, errors


def sort_group_kpi_items(items: list[dict], sort_by: str, top: int) -> tuple[list[dict], str, int]:
    allowed = {"changed_files_count", "thread_messages_count", "rounds_count", "decisions_count"}
    sort_v = (str(sort_by or "changed_files_count").strip() or "changed_files_count")
    if sort_v not in allowed:
        raise HTTPException(status_code=400, detail={"code": "GROUP_KPI_SORT_BY_INVALID", "sort_by": sort_v})
    out = list(items)
    out.sort(key=lambda x: int((x.get("kpi") or {}).get(sort_v) or 0), reverse=True)
    top_v = max(1, min(int(top), 200))
    return out[:top_v], sort_v, top_v


def build_group_kpi_distribution(items: list[dict]) -> dict:
    dist = {"changed_files": {"0": 0, "1_5": 0, "6_20": 0, "21_plus": 0}, "rounds": {"0": 0, "1_2": 0, "3_5": 0, "6_plus": 0}, "decisions": {"0": 0, "1_2": 0, "3_5": 0, "6_plus": 0}}
    for out in items:
        k = dict(out.get("kpi") or {})
        cf, rd, dc = int(k.get("changed_files_count") or 0), int(k.get("rounds_count") or 0), int(k.get("decisions_count") or 0)
        dist["changed_files"]["0" if cf <= 0 else "1_5" if cf <= 5 else "6_20" if cf <= 20 else "21_plus"] += 1
        dist["rounds"]["0" if rd <= 0 else "1_2" if rd <= 2 else "3_5" if rd <= 5 else "6_plus"] += 1
        dist["decisions"]["0" if dc <= 0 else "1_2" if dc <= 2 else "3_5" if dc <= 5 else "6_plus"] += 1
    return dist


def build_group_kpi_alerts(items: list[dict], limit: int) -> tuple[list[dict], int]:
    alerts: list[dict[str, Any]] = []
    for out in items:
        k = dict(out.get("kpi") or {})
        reasons: list[str] = []
        if not bool(k.get("quality_gate_pass")): reasons.append("GROUP_QUALITY_GATE_BLOCKED")
        if not bool(k.get("approve_ready")): reasons.append("GROUP_APPROVE_NOT_READY")
        if int(k.get("changed_files_count") or 0) <= 0: reasons.append("GROUP_CHANGED_FILES_EMPTY")
        if not reasons: continue
        severity = (3 if "GROUP_QUALITY_GATE_BLOCKED" in reasons else 0) + (2 if "GROUP_APPROVE_NOT_READY" in reasons else 0) + (1 if "GROUP_CHANGED_FILES_EMPTY" in reasons else 0)
        alerts.append({"task_id": str(out.get("task_id") or ""), "owner_id": str(out.get("owner_id") or ""), "phase": str(out.get("phase") or ""), "status": str(out.get("status") or ""), "severity": severity, "reasons": reasons, "kpi": k})
    alerts.sort(key=lambda x: (-int(x.get("severity") or 0), str(x.get("task_id") or "")))
    limit_v = max(1, min(int(limit), 200))
    return alerts[:limit_v], limit_v


def build_group_kpi_overview(items: list[dict]) -> dict:
    total = len(items)
    qg_pass = sum(1 for x in items if bool((x.get("kpi") or {}).get("quality_gate_pass")))
    ready = sum(1 for x in items if bool((x.get("kpi") or {}).get("approve_ready")))
    changed = sum(int((x.get("kpi") or {}).get("changed_files_count") or 0) for x in items)
    return {"total_tasks": total, "quality_gate_pass_tasks": qg_pass, "approve_ready_tasks": ready, "quality_gate_pass_rate": (float(qg_pass) / float(total)) if total > 0 else 0.0, "approve_ready_rate": (float(ready) / float(total)) if total > 0 else 0.0, "avg_changed_files_count": (float(changed) / float(total)) if total > 0 else 0.0}


def build_group_kpi_owners(items: list[dict], top: int, min_tasks: int) -> tuple[list[dict], int, int]:
    owners: dict[str, dict[str, int]] = {}
    for out in items:
        owner = str(out.get("owner_id") or "") or "unknown"
        k = dict(out.get("kpi") or {})
        agg = owners.get(owner) or {"tasks": 0, "quality_gate_pass_tasks": 0, "approve_ready_tasks": 0, "sum_changed_files": 0}
        agg["tasks"] += 1
        if bool(k.get("quality_gate_pass")): agg["quality_gate_pass_tasks"] += 1
        if bool(k.get("approve_ready")): agg["approve_ready_tasks"] += 1
        agg["sum_changed_files"] += int(k.get("changed_files_count") or 0)
        owners[owner] = agg
    min_v = max(1, min(int(min_tasks), 200))
    rows: list[dict] = []
    for owner_id, agg in owners.items():
        tasks = int(agg.get("tasks") or 0)
        if tasks < min_v: continue
        qg, ready, changed = int(agg.get("quality_gate_pass_tasks") or 0), int(agg.get("approve_ready_tasks") or 0), int(agg.get("sum_changed_files") or 0)
        rows.append({"owner_id": owner_id, "tasks": tasks, "quality_gate_pass_tasks": qg, "approve_ready_tasks": ready, "quality_gate_pass_rate": (float(qg) / float(tasks)) if tasks > 0 else 0.0, "approve_ready_rate": (float(ready) / float(tasks)) if tasks > 0 else 0.0, "avg_changed_files_count": (float(changed) / float(tasks)) if tasks > 0 else 0.0})
    rows.sort(key=lambda x: (-int(x.get("tasks") or 0), -float(x.get("quality_gate_pass_rate") or 0.0), str(x.get("owner_id") or "")))
    top_v = max(1, min(int(top), 200))
    return rows[:top_v], top_v, min_v


def handle_group_task_kpi(*, task_id: str, store: Any, get_task_or_raise: Callable[[Any, str], dict], get_artifacts: Callable[[Any, str], dict], quality_gate_ci: Callable[[str, dict], dict], is_task_closed: Callable[[dict], bool]) -> dict:
    task = get_task_or_raise(store, task_id)
    artifacts = get_artifacts(store, task_id)
    fn_thread = getattr(store, "list_group_agent_messages", None)
    fn_rounds = getattr(store, "list_group_task_rounds", None)
    fn_decisions = getattr(store, "list_group_decisions", None)
    thread = (fn_thread(task_id, limit=200, cursor=None, order="desc", role=None) if callable(fn_thread) else []) or []
    rounds = (fn_rounds(task_id, limit=200, cursor=None, order="desc") if callable(fn_rounds) else []) or []
    decisions = (fn_decisions(task_id, limit=200, cursor=None, order="desc") if callable(fn_decisions) else []) or []
    qg = quality_gate_ci(task_id, artifacts)
    ready = bool((str(task.get("phase") or "") == "decision") and (not is_task_closed(task)) and (not bool(qg.get("blocked"))))
    return build_group_task_kpi_response(task_id=task_id, owner_id=str(task.get("owner_id") or ""), phase=str(task.get("phase") or ""), status=str(task.get("status") or ""), thread_messages_count=len(thread), rounds_count=len(rounds), decisions_count=len(decisions), changed_files_count=len(list(artifacts.get("changed_files") or [])), approve_ready=ready, quality_gate_pass=bool(qg.get("pass")))


def handle_group_tasks_kpi_batch(task_ids: list[str], owner_id: str | None, fetch_kpi: Callable[[str], dict]) -> dict:
    ids = [str(x).strip() for x in list(task_ids or []) if str(x).strip()][:200]
    owner_v = (str(owner_id).strip() if owner_id is not None else None)
    items, errors = collect_group_kpi_items(ids, owner_v, fetch_kpi)
    return {"ok": True, "total": len(items), "errors": errors, "items": items, "owner_id_filter": owner_v}


def handle_group_tasks_kpi_batch_get(task_ids: str, owner_id: str | None, fetch_kpi: Callable[[str], dict]) -> dict:
    ids = [x.strip() for x in str(task_ids or "").split(",") if x.strip()][:200]
    return handle_group_tasks_kpi_batch(ids, owner_id, fetch_kpi)


def handle_group_tasks_kpi_leaderboard(*, top: int, sort_by: str, owner_id: str | None, status: str | None, phase: str | None, list_tasks: Callable[..., list[dict]], fetch_kpi: Callable[[str], dict]) -> dict:
    rows = list_tasks(limit=200, status=status, phase=phase, owner_id=owner_id)
    ids = [str(r.get("task_id") or "") for r in rows if str(r.get("task_id") or "")]
    items, _ = collect_group_kpi_items(ids, None, fetch_kpi)
    out, sort_v, top_v = sort_group_kpi_items(items, sort_by, top)
    return {"ok": True, "sort_by": sort_v, "top": top_v, "filters": {"owner_id": (str(owner_id).strip() if owner_id is not None else None), "status": (str(status).strip() if status is not None else None), "phase": (str(phase).strip() if phase is not None else None)}, "total": len(out), "items": out}


def handle_group_tasks_kpi_distribution(*, owner_id: str | None, status: str | None, phase: str | None, list_tasks: Callable[..., list[dict]], fetch_kpi: Callable[[str], dict]) -> dict:
    rows = list_tasks(limit=200, status=status, phase=phase, owner_id=owner_id)
    ids = [str(r.get("task_id") or "") for r in rows if str(r.get("task_id") or "")]
    items, _ = collect_group_kpi_items(ids, None, fetch_kpi)
    return {"ok": True, "filters": {"owner_id": (str(owner_id).strip() if owner_id is not None else None), "status": (str(status).strip() if status is not None else None), "phase": (str(phase).strip() if phase is not None else None)}, "distribution": build_group_kpi_distribution(items)}


def handle_group_tasks_kpi_alerts(*, limit: int, owner_id: str | None, status: str | None, phase: str | None, list_tasks: Callable[..., list[dict]], fetch_kpi: Callable[[str], dict]) -> dict:
    rows = list_tasks(limit=200, status=status, phase=phase, owner_id=owner_id)
    ids = [str(r.get("task_id") or "") for r in rows if str(r.get("task_id") or "")]
    items, _ = collect_group_kpi_items(ids, None, fetch_kpi)
    out_items, limit_v = build_group_kpi_alerts(items, limit)
    return {"ok": True, "limit": limit_v, "total": len(out_items), "filters": {"owner_id": (str(owner_id).strip() if owner_id is not None else None), "status": (str(status).strip() if status is not None else None), "phase": (str(phase).strip() if phase is not None else None)}, "items": out_items}


def handle_group_tasks_kpi_overview(*, owner_id: str | None, status: str | None, phase: str | None, list_tasks: Callable[..., list[dict]], fetch_kpi: Callable[[str], dict]) -> dict:
    rows = list_tasks(limit=200, status=status, phase=phase, owner_id=owner_id)
    ids = [str(r.get("task_id") or "") for r in rows if str(r.get("task_id") or "")]
    items, _ = collect_group_kpi_items(ids, None, fetch_kpi)
    return {"ok": True, "filters": {"owner_id": (str(owner_id).strip() if owner_id is not None else None), "status": (str(status).strip() if status is not None else None), "phase": (str(phase).strip() if phase is not None else None)}, "overview": build_group_kpi_overview(items)}


def handle_group_tasks_kpi_owners(*, top: int, min_tasks: int, status: str | None, phase: str | None, list_tasks: Callable[..., list[dict]], fetch_kpi: Callable[[str], dict]) -> dict:
    rows = list_tasks(limit=200, status=status, phase=phase, owner_id=None)
    ids = [str(r.get("task_id") or "") for r in rows if str(r.get("task_id") or "")]
    items, _ = collect_group_kpi_items(ids, None, fetch_kpi)
    out, top_v, min_v = build_group_kpi_owners(items, top=top, min_tasks=min_tasks)
    return {"ok": True, "top": top_v, "min_tasks": min_v, "filters": {"status": (str(status).strip() if status is not None else None), "phase": (str(phase).strip() if phase is not None else None)}, "total": len(out), "items": out}


def register_group_kpi_routes(main_router: APIRouter) -> None:
    main_router.include_router(router)


__all__ = [
    "router",
    "register_group_kpi_routes",
    "build_group_task_kpi_response",
    "collect_group_kpi_items",
    "sort_group_kpi_items",
    "build_group_kpi_distribution",
    "build_group_kpi_alerts",
    "build_group_kpi_overview",
    "build_group_kpi_owners",
    "handle_group_task_kpi",
    "handle_group_tasks_kpi_batch",
    "handle_group_tasks_kpi_batch_get",
    "handle_group_tasks_kpi_leaderboard",
    "handle_group_tasks_kpi_distribution",
    "handle_group_tasks_kpi_alerts",
    "handle_group_tasks_kpi_overview",
    "handle_group_tasks_kpi_owners",
]
