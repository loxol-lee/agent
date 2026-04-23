from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from connectors.web import routes
from config import Settings
from core.contracts.observability import Trace
from core.infra.storage.sqlite_store import SQLiteStore


def _build_request(store: SQLiteStore, settings: Settings | None = None) -> Request:
    app = FastAPI()
    app.state.store = store
    if settings is not None:
        app.state.settings = settings
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [],
        "query_string": b"",
        "client": ("127.0.0.1", 12345),
        "app": app,
    }
    return Request(scope)


def _create_legacy_agent_tasks_table(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE agent_tasks (
          task_id TEXT PRIMARY KEY,
          goal TEXT NOT NULL,
          status TEXT NOT NULL,
          current_step TEXT NOT NULL,
          attempt INTEGER NOT NULL DEFAULT 0,
          max_retry INTEGER NOT NULL,
          acceptance_cmd TEXT NOT NULL,
          working_dir TEXT NOT NULL,
          dry_run INTEGER NOT NULL DEFAULT 0,
          rollback_on_action_failure INTEGER NOT NULL DEFAULT 1,
          request_id TEXT NOT NULL,
          trace_id TEXT NOT NULL,
          stop_reason TEXT,
          error_code TEXT,
          scope_paths_json TEXT NOT NULL,
          forbidden_paths_json TEXT NOT NULL,
          actions_json TEXT NOT NULL,
          last_verify_json TEXT,
          logs_json TEXT,
          created_at_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL,
          finished_at_ms INTEGER
        )
        """
    )
    conn.commit()
    conn.close()


def _legacy_task_payload(task_id: str, actor_role: str, request_id: str, trace_id: str, ts: int, goal: str) -> dict[str, object]:
    return {
        "task_id": task_id,
        "goal": goal,
        "status": "queued",
        "current_step": "QUEUED",
        "attempt": 0,
        "max_retry": 1,
        "acceptance_cmd": "python3 -m unittest -v test_quality_gate_contract.py",
        "working_dir": "apps/server/src",
        "dry_run": False,
        "rollback_on_action_failure": True,
        "actor_role": actor_role,
        "request_id": request_id,
        "trace_id": trace_id,
        "scope_paths": ["apps/server/src"],
        "forbidden_paths": [],
        "actions": [],
        "logs": [],
        "created_at_ms": ts,
        "updated_at_ms": ts,
    }


def _build_legacy_store(schema_path: Path, db_name: str) -> tuple[SQLiteStore, Path, tempfile.TemporaryDirectory]:
    tmp = tempfile.TemporaryDirectory()
    db_path = Path(tmp.name) / db_name
    _create_legacy_agent_tasks_table(db_path)
    store = SQLiteStore(str(db_path), str(schema_path))
    return store, db_path, tmp


class QualityGateContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.src_dir = Path(__file__).resolve().parent
        self.schema_path = self.src_dir / "core" / "infra" / "storage" / "schema.sql"
        db_path = Path(self._tmp.name) / "test.sqlite3"
        self.store = SQLiteStore(str(db_path), str(self.schema_path))
        self.settings = Settings(
            sqlite_path=str(db_path),
            siliconflow_base_url="http://example.com",
            siliconflow_api_key=None,
            siliconflow_model="test",
            project_root=str(self.src_dir.parents[2]),
        )
        self.req = _build_request(self.store, self.settings)

        self.conversation_id = "c1"
        self.trace_id = "t1"

        self.store.insert_trace(
            Trace(
                trace_id=self.trace_id,
                conversation_id=self.conversation_id,
                started_at_ms=1,
                finished_at_ms=2,
                status="ok",
                stop_reason="completed",
                error_message=None,
            )
        )
        self.store.insert_message(self.conversation_id, self.trace_id, "user", "hi")
        self.store.insert_message(self.conversation_id, self.trace_id, "assistant", "ok")
        self.store.insert_replay_event(
            self.trace_id,
            1,
            "model_done",
            json.dumps({"request_id": self.trace_id, "stop_reason": "completed"}, ensure_ascii=False),
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _wait_terminal_task(self, task_id: str, rounds: int = 50, sleep_s: float = 0.02) -> dict[str, object]:
        import time as _t

        poll_interval = max(0.005, float(sleep_s))
        timeout_s = max(float(rounds) * poll_interval, 15.0)
        deadline = _t.monotonic() + timeout_s
        last: dict[str, object] | None = None

        while _t.monotonic() < deadline:
            cur = routes.api_agent_task_get(request=self.req, task_id=task_id)
            last = cur
            if cur.get("status") in {"succeeded", "failed", "cancelled"}:
                return cur
            _t.sleep(poll_interval)

        last_status = (last or {}).get("status")
        raise AssertionError(f"task did not finish in time: {task_id}, last_status={last_status}, timeout_s={timeout_s:.2f}")

    def _task_body(self, goal: str, **overrides: object) -> routes.AgentTaskCreateIn:
        payload: dict[str, object] = {
            "scope_paths": ["apps/server/src"],
            "forbidden_paths": [],
            "acceptance_cmd": "git status",
            "working_dir": "apps/server/src",
            "max_retry": 1,
            "dry_run": False,
            "auto_start": False,
        }
        payload.update(overrides)
        return routes.AgentTaskCreateIn(goal=goal, **payload)

    def _advance_group_task_to_decision(self, task_id: str) -> None:
        for phase in ("discussion", "execution", "qa_verifying", "decision"):
            out = routes.api_group_task_rerun(
                request=self.req,
                task_id=task_id,
                body=routes.GroupTaskRerunIn(phase=phase),
            )
            self.assertTrue(out.get("ok"))
            self.assertEqual(out.get("phase"), phase)

    def test_group_task_create_and_get(self) -> None:
        created = routes.api_group_task_create(
            request=self.req,
            body=routes.GroupTaskCreateIn(goal="v2 create/get smoke", owner_id="owner"),
        )
        self.assertTrue(created.get("ok"))
        self.assertEqual(created.get("status"), "created")
        self.assertEqual(created.get("phase"), "planning")
        task_id = str(created.get("task_id"))
        self.assertTrue(task_id.startswith("gtask_"))

        got = routes.api_group_task_get(request=self.req, task_id=task_id)
        self.assertTrue(got.get("ok"))
        self.assertEqual(got.get("task_id"), task_id)
        self.assertEqual(got.get("goal"), "v2 create/get smoke")
        self.assertEqual(got.get("owner_id"), "owner")

    def test_group_task_get_not_found(self) -> None:
        with self.assertRaises(HTTPException) as e:
            routes.api_group_task_get(request=self.req, task_id="gtask_not_exists")
        self.assertEqual(e.exception.status_code, 404)
        self.assertEqual((e.exception.detail or {}).get("code"), "GROUP_TASK_NOT_FOUND")

    def test_group_task_thread_and_artifacts_defaults(self) -> None:
        created = routes.api_group_task_create(
            request=self.req,
            body=routes.GroupTaskCreateIn(goal="v2 thread/artifacts", owner_id="owner"),
        )
        task_id = str(created.get("task_id"))

        thread = routes.api_group_task_thread(request=self.req, task_id=task_id)
        self.assertTrue(thread.get("ok"))
        items = thread.get("items") or []
        self.assertGreaterEqual(len(items), 1)

        artifacts = routes.api_group_task_artifacts(request=self.req, task_id=task_id)
        self.assertTrue(artifacts.get("ok"))
        art = artifacts.get("artifacts") or {}
        self.assertEqual(art.get("changed_files"), [])
        self.assertEqual(art.get("summary"), "")

    def test_group_task_thread_not_found(self) -> None:
        with self.assertRaises(HTTPException) as e:
            routes.api_group_task_thread(request=self.req, task_id="gtask_not_exists")
        self.assertEqual(e.exception.status_code, 404)
        self.assertEqual((e.exception.detail or {}).get("code"), "GROUP_TASK_NOT_FOUND")

    def test_group_task_thread_paging(self) -> None:
        created = routes.api_group_task_create(
            request=self.req,
            body=routes.GroupTaskCreateIn(goal="v2 thread paging", owner_id="owner"),
        )
        task_id = str(created.get("task_id"))
        self.store.insert_group_agent_message(task_id, 1, "pm", "pm", "m1", "{}")
        self.store.insert_group_agent_message(task_id, 1, "rd", "rd", "m2", "{}")
        self.store.insert_group_agent_message(task_id, 1, "qa", "qa", "m3", "{}")

        p1 = routes.api_group_task_thread(request=self.req, task_id=task_id, limit=2, order="asc")
        self.assertTrue(p1.get("ok"))
        items1 = p1.get("items") or []
        self.assertEqual(len(items1), 2)
        paging1 = p1.get("paging") or {}
        c1 = paging1.get("next_cursor")
        self.assertIsInstance(c1, int)

        p2 = routes.api_group_task_thread(request=self.req, task_id=task_id, limit=2, cursor=int(c1), order="asc")
        self.assertTrue(p2.get("ok"))
        items2 = p2.get("items") or []
        self.assertGreaterEqual(len(items2), 1)

    def test_group_task_approve_and_rerun(self) -> None:
        created = routes.api_group_task_create(
            request=self.req,
            body=routes.GroupTaskCreateIn(goal="v2 approve/rerun", owner_id="owner"),
        )
        task_id = str(created.get("task_id"))
        self._advance_group_task_to_decision(task_id)
        self.store.upsert_group_artifacts(task_id, {"changed_files": ["apps/server/src/a.py"], "test_result": "qa_passed", "summary": "ok"})

        approved = routes.api_group_task_approve(
            request=self.req,
            task_id=task_id,
            body=routes.GroupTaskApproveIn(owner_id="owner"),
        )
        self.assertTrue(approved.get("ok"))
        self.assertEqual(approved.get("status"), "done")
        self.assertEqual(approved.get("phase"), "decision")
        self.assertEqual(approved.get("stop_reason"), "approved")

        with self.assertRaises(HTTPException) as e2:
            routes.api_group_task_rerun(
                request=self.req,
                task_id=task_id,
                body=routes.GroupTaskRerunIn(phase="execution"),
            )
        self.assertEqual(e2.exception.status_code, 409)
        self.assertEqual((e2.exception.detail or {}).get("code"), "GROUP_TASK_CLOSED")

    def test_group_task_rerun_invalid_phase(self) -> None:
        created = routes.api_group_task_create(
            request=self.req,
            body=routes.GroupTaskCreateIn(goal="v2 rerun invalid phase", owner_id="owner"),
        )
        task_id = str(created.get("task_id"))
        with self.assertRaises(HTTPException) as e:
            routes.api_group_task_rerun(request=self.req, task_id=task_id, body=routes.GroupTaskRerunIn(phase="bad_phase"))
        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual((e.exception.detail or {}).get("code"), "GROUP_PHASE_NOT_ALLOWED")

    def test_group_task_rerun_transition_denied(self) -> None:
        created = routes.api_group_task_create(
            request=self.req,
            body=routes.GroupTaskCreateIn(goal="v2 rerun transition denied", owner_id="owner"),
        )
        task_id = str(created.get("task_id"))
        with self.assertRaises(HTTPException) as e:
            routes.api_group_task_rerun(request=self.req, task_id=task_id, body=routes.GroupTaskRerunIn(phase="execution"))
        self.assertEqual(e.exception.status_code, 409)
        self.assertEqual((e.exception.detail or {}).get("code"), "GROUP_TRANSITION_NOT_ALLOWED")

    def test_group_task_approve_requires_decision_phase(self) -> None:
        created = routes.api_group_task_create(
            request=self.req,
            body=routes.GroupTaskCreateIn(goal="v2 approve requires decision", owner_id="owner"),
        )
        task_id = str(created.get("task_id"))
        with self.assertRaises(HTTPException) as e:
            routes.api_group_task_approve(request=self.req, task_id=task_id, body=routes.GroupTaskApproveIn(owner_id="owner"))
        self.assertEqual(e.exception.status_code, 409)
        self.assertEqual((e.exception.detail or {}).get("code"), "GROUP_APPROVE_PHASE_INVALID")

    def test_group_task_approve_owner_only(self) -> None:
        created = routes.api_group_task_create(
            request=self.req,
            body=routes.GroupTaskCreateIn(goal="v2 approve owner only", owner_id="owner"),
        )
        task_id = str(created.get("task_id"))
        self._advance_group_task_to_decision(task_id)
        with self.assertRaises(HTTPException) as e:
            routes.api_group_task_approve(
                request=self.req,
                task_id=task_id,
                body=routes.GroupTaskApproveIn(owner_id="owner", actor_role="qa"),
            )
        self.assertEqual(e.exception.status_code, 403)
        self.assertEqual((e.exception.detail or {}).get("code"), "ROLE_PERMISSION_DENIED")

    def test_group_task_rerun_role_matrix(self) -> None:
        created = routes.api_group_task_create(
            request=self.req,
            body=routes.GroupTaskCreateIn(goal="v2 rerun role matrix", owner_id="owner"),
        )
        task_id = str(created.get("task_id"))

        _ = routes.api_group_task_rerun(
            request=self.req,
            task_id=task_id,
            body=routes.GroupTaskRerunIn(phase="discussion", actor_role="owner"),
        )
        rd_ok = routes.api_group_task_rerun(
            request=self.req,
            task_id=task_id,
            body=routes.GroupTaskRerunIn(phase="execution", actor_role="rd"),
        )
        self.assertTrue(rd_ok.get("ok"))

        with self.assertRaises(HTTPException) as e2:
            routes.api_group_task_rerun(
                request=self.req,
                task_id=task_id,
                body=routes.GroupTaskRerunIn(phase="execution", actor_role="qa"),
            )
        self.assertEqual(e2.exception.status_code, 403)
        self.assertEqual((e2.exception.detail or {}).get("code"), "ROLE_PERMISSION_DENIED")

    def test_group_task_rerun_discussion_maps_to_created_status(self) -> None:
        created = routes.api_group_task_create(
            request=self.req,
            body=routes.GroupTaskCreateIn(goal="v2 rerun discussion", owner_id="owner"),
        )
        task_id = str(created.get("task_id"))

        self._advance_group_task_to_decision(task_id)
        self.store.upsert_group_artifacts(task_id, {"changed_files": ["apps/server/src/a.py"], "test_result": "qa_passed", "summary": "ok"})
        _ = routes.api_group_task_approve(
            request=self.req,
            task_id=task_id,
            body=routes.GroupTaskApproveIn(owner_id="owner"),
        )

        with self.assertRaises(HTTPException) as e2:
            routes.api_group_task_rerun(
                request=self.req,
                task_id=task_id,
                body=routes.GroupTaskRerunIn(phase="discussion"),
            )
        self.assertEqual(e2.exception.status_code, 409)
        self.assertEqual((e2.exception.detail or {}).get("code"), "GROUP_TASK_CLOSED")

    def test_group_tasks_list_with_stats(self) -> None:
        c1 = routes.api_group_task_create(request=self.req, body=routes.GroupTaskCreateIn(goal="v2 list #1", owner_id="owner"))
        c2 = routes.api_group_task_create(request=self.req, body=routes.GroupTaskCreateIn(goal="v2 list #2", owner_id="owner"))
        _ = c1, c2

        out = routes.api_group_tasks(request=self.req, limit=50)
        self.assertTrue(out.get("ok"))
        items = out.get("items") or []
        self.assertGreaterEqual(len(items), 2)
        self.assertEqual(out.get("total"), len(items))
        stats = out.get("stats") or {}
        by_status = stats.get("by_status") or {}
        self.assertGreaterEqual(int(by_status.get("created") or 0), 2)

    def test_group_task_approve_writes_decision(self) -> None:
        created = routes.api_group_task_create(request=self.req, body=routes.GroupTaskCreateIn(goal="v2 decision", owner_id="owner"))
        task_id = str(created.get("task_id"))
        self._advance_group_task_to_decision(task_id)
        self.store.upsert_group_artifacts(task_id, {"changed_files": ["apps/server/src/a.py"], "test_result": "qa_passed", "summary": "ok"})
        _ = routes.api_group_task_approve(request=self.req, task_id=task_id, body=routes.GroupTaskApproveIn(owner_id="owner"))

        decisions = routes.api_group_task_decisions(request=self.req, task_id=task_id, limit=20)
        self.assertTrue(decisions.get("ok"))
        items = decisions.get("items") or []
        self.assertGreaterEqual(len(items), 1)
        last = items[-1]
        self.assertEqual(last.get("decision_type"), "approve")
        self.assertEqual(last.get("approved_by"), "owner")

    def test_group_task_trace_map_and_audit_closure(self) -> None:
        created = routes.api_group_task_create(request=self.req, body=routes.GroupTaskCreateIn(goal="v2 trace closure", owner_id="owner"))
        task_id = str(created.get("task_id"))
        self._advance_group_task_to_decision(task_id)
        self.store.upsert_group_artifacts(task_id, {"changed_files": ["apps/server/src/a.py"], "test_result": "qa_passed", "summary": "ok"})
        self.store.insert_group_agent_message(task_id, 4, "rd", "rd", "trace row", json.dumps({"trace_id": "tr_g_1", "request_id": "rq_g_1", "call_id": "c1"}, ensure_ascii=False))
        _ = routes.api_group_task_approve(request=self.req, task_id=task_id, body=routes.GroupTaskApproveIn(owner_id="owner"))

        tm = routes.api_group_task_trace_map(request=self.req, task_id=task_id)
        self.assertTrue(tm.get("ok"))
        self.assertGreaterEqual(int((tm.get("counts") or {}).get("trace_rows") or 0), 1)

        ac = routes.api_group_task_audit_closure(request=self.req, task_id=task_id)
        self.assertTrue(ac.get("ok"))
        checks = ac.get("checks") or {}
        self.assertTrue(bool(checks.get("trace_mapped")))
        self.assertTrue(bool(checks.get("audit_closure_ready")))

    def test_group_task_kpi_dashboard_endpoints(self) -> None:
        c1 = routes.api_group_task_create(request=self.req, body=routes.GroupTaskCreateIn(goal="kpi #1", owner_id="owner"))
        c2 = routes.api_group_task_create(request=self.req, body=routes.GroupTaskCreateIn(goal="kpi #2", owner_id="owner2"))
        t1, t2 = str(c1.get("task_id")), str(c2.get("task_id"))
        self._advance_group_task_to_decision(t1)
        self.store.insert_group_agent_message(t1, 4, "qa", "qa", "kpi", "{}")
        self.store.insert_group_decision(t1, "approve", "p1", "ok", "owner")
        self.store.upsert_group_artifacts(t1, {"changed_files": ["apps/server/src/a.py", "apps/server/src/b.py"], "test_result": "qa_passed", "summary": "ok"})

        k1 = routes.api_group_task_kpi(request=self.req, task_id=t1)
        self.assertTrue(k1.get("ok"))
        self.assertEqual(k1.get("owner_id"), "owner")

        pb = routes.api_group_tasks_kpi_batch(request=self.req, body=routes.GroupTasksBatchKpiIn(task_ids=[t1, t2], owner_id="owner"))
        gb = routes.api_group_tasks_kpi_batch_get(request=self.req, task_ids=f"{t1},{t2}", owner_id="owner")
        self.assertEqual(pb.get("owner_id_filter"), "owner")
        self.assertEqual(gb.get("owner_id_filter"), "owner")
        self.assertGreaterEqual(int(pb.get("total") or 0), 1)

        lb = routes.api_group_tasks_kpi_leaderboard(request=self.req, top=5, sort_by="changed_files_count")
        ds = routes.api_group_tasks_kpi_distribution(request=self.req)
        al = routes.api_group_tasks_kpi_alerts(request=self.req, limit=20)
        ov = routes.api_group_tasks_kpi_overview(request=self.req)
        ow = routes.api_group_tasks_kpi_owners(request=self.req, top=10, min_tasks=1)
        self.assertTrue(lb.get("ok"))
        self.assertTrue(ds.get("ok"))
        self.assertTrue(al.get("ok"))
        self.assertTrue(ov.get("ok"))
        self.assertTrue(ow.get("ok"))

        with self.assertRaises(HTTPException) as e:
            routes.api_group_tasks_kpi_leaderboard(request=self.req, sort_by="bad")
        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual((e.exception.detail or {}).get("code"), "GROUP_KPI_SORT_BY_INVALID")

    def test_group_task_prechecks_and_can_endpoints(self) -> None:
        created = routes.api_group_task_create(request=self.req, body=routes.GroupTaskCreateIn(goal="v2 prechecks/can", owner_id="owner"))
        task_id = str(created.get("task_id"))

        p0 = routes.api_group_task_prechecks(request=self.req, task_id=task_id, actor_role="owner", owner_id="owner", next_phase="execution")
        self.assertTrue(p0.get("ok"))
        self.assertIn("can", p0)
        self.assertIn("blockers", p0)

        c0 = routes.api_group_task_can(request=self.req, task_id=task_id, actor_role="owner", owner_id="owner", next_phase="execution")
        self.assertTrue(c0.get("ok"))
        self.assertIn("can", c0)

        pb = routes.api_group_tasks_prechecks_batch(
            request=self.req,
            task_ids=f"{task_id},gtask_not_exists",
            actor_role="owner",
            owner_id="owner",
            next_phase="execution",
        )
        self.assertTrue(pb.get("ok"))
        self.assertGreaterEqual(int(pb.get("total") or 0), 1)
        self.assertGreaterEqual(len(pb.get("errors") or []), 1)

        cb = routes.api_group_tasks_can_batch(
            request=self.req,
            task_ids=f"{task_id},gtask_not_exists",
            actor_role="owner",
            owner_id="owner",
            next_phase="execution",
        )
        self.assertTrue(cb.get("ok"))
        self.assertGreaterEqual(int(cb.get("total") or 0), 1)
        self.assertGreaterEqual(len(cb.get("errors") or []), 1)

    def test_group_task_export_contains_trace_map_summary(self) -> None:
        created = routes.api_group_task_create(request=self.req, body=routes.GroupTaskCreateIn(goal="v2 export trace summary", owner_id="owner"))
        task_id = str(created.get("task_id"))
        self._advance_group_task_to_decision(task_id)
        self.store.upsert_group_artifacts(task_id, {"changed_files": ["apps/server/src/a.py"], "test_result": "qa_passed", "summary": "ok"})
        self.store.insert_group_agent_message(task_id, 4, "rd", "rd", "trace row", json.dumps({"trace_id": "tr_g_2", "request_id": "rq_g_2", "call_id": "c2"}, ensure_ascii=False))
        _ = routes.api_group_task_approve(request=self.req, task_id=task_id, body=routes.GroupTaskApproveIn(owner_id="owner"))

        out = routes.api_group_task_export(request=self.req, task_id=task_id)
        self.assertTrue(out.get("ok"))
        summary = out.get("trace_map_summary") or {}
        self.assertGreaterEqual(int(summary.get("trace_rows") or 0), 1)
        self.assertGreaterEqual(int(summary.get("trace_ids") or 0), 1)
        self.assertGreaterEqual(int(summary.get("request_ids") or 0), 1)

    def test_export_contains_contract_fields(self) -> None:
        out = routes.api_conversation_export(
            request=self.req,
            conversation_id=self.conversation_id,
            quality_gate_mode="off",
            expected_contract_fingerprint=None,
        )
        self.assertTrue(out["ok"])
        self.assertIn("contract_fingerprint", out)
        self.assertIn("quality_gate_ci", out)
        self.assertIn("contract_drift", out)
        self.assertFalse(out["contract_drift"])
        self.assertIn("contract_drift", out["quality_gate_ci"])

    def test_export_blocks_on_contract_drift_even_when_gate_off(self) -> None:
        with self.assertRaises(HTTPException) as e:
            routes.api_conversation_export(
                request=self.req,
                conversation_id=self.conversation_id,
                quality_gate_mode="off",
                expected_contract_fingerprint="deadbeefdeadbeef",
            )
        self.assertEqual(e.exception.status_code, 409)
        self.assertEqual(e.exception.detail["code"], "CONTRACT_DRIFT_BLOCKED")
        self.assertTrue(e.exception.detail["contract_drift"])

    def test_quality_gate_blocks_on_contract_drift(self) -> None:
        with self.assertRaises(HTTPException) as e:
            routes.api_conversation_quality_gate(
                request=self.req,
                conversation_id=self.conversation_id,
                quality_gate_mode="fail",
                expected_contract_fingerprint="deadbeefdeadbeef",
                include_details=False,
            )
        self.assertEqual(e.exception.status_code, 409)
        self.assertEqual(e.exception.detail["code"], "CONTRACT_DRIFT_BLOCKED")
        self.assertTrue(e.exception.detail["contract_drift"])
        self.assertIn("quality_gate_ci", e.exception.detail)

    def test_export_security_checks_accumulate(self) -> None:
        tid = "t2"
        self.store.insert_trace(
            Trace(
                trace_id=tid,
                conversation_id=self.conversation_id,
                started_at_ms=10,
                finished_at_ms=20,
                status="ok",
                stop_reason="completed",
                error_message=None,
            )
        )
        self.store.insert_message(self.conversation_id, tid, "user", "u2")
        self.store.insert_message(self.conversation_id, tid, "assistant", "a2")
        self.store.insert_replay_event(
            tid,
            1,
            "request_meta",
            json.dumps({"attachments": [{"mime_type": "application/x-msdownload", "size_bytes": 10}]}, ensure_ascii=False),
        )
        self.store.insert_replay_event(
            tid,
            2,
            "model_done",
            json.dumps({"request_id": tid, "stop_reason": "completed"}, ensure_ascii=False),
        )
        self.store.insert_replay_event(tid, 3, "tool_result", json.dumps({"call_id": "c1"}, ensure_ascii=False))
        self.store.insert_replay_event(tid, 4, "tool_result", json.dumps({"call_id": "c2"}, ensure_ascii=False))
        self.store.insert_tool_run(
            trace_id=tid,
            conversation_id=self.conversation_id,
            call_id="c1",
            tool_name="echo",
            allowed=False,
            risk="low",
            side_effect=False,
            args_json="{}",
            result_json="{}",
            ok=False,
            error_code=None,
            error_message=None,
            started_at_ms=11,
            finished_at_ms=12,
        )
        self.store.insert_tool_run(
            trace_id=tid,
            conversation_id=self.conversation_id,
            call_id="c2",
            tool_name="remember",
            allowed=True,
            risk="low",
            side_effect=True,
            args_json="{}",
            result_json=json.dumps({"content": {"skipped": True, "reason": "memory_write_disabled"}}, ensure_ascii=False),
            ok=True,
            error_code=None,
            error_message=None,
            started_at_ms=13,
            finished_at_ms=14,
        )

        out = routes.api_conversation_export(
            request=self.req,
            conversation_id=self.conversation_id,
            quality_gate_mode="off",
            expected_contract_fingerprint=None,
        )
        sec = out["quality_gate"]["security_checks"]
        self.assertGreaterEqual(sec["attachment_policy_violation_count"], 1)
        self.assertEqual(sec["tool_not_allowed_without_error_count"], 1)
        self.assertEqual(sec["silent_tool_failure_count"], 1)
        self.assertEqual(sec["remember_write_block_count"], 1)
        rules = out["quality_gate"]["rules"]
        sec_rule = next(r for r in rules if r.get("id") == "security_integrity")
        self.assertEqual(sec_rule.get("status"), "fail")

    def test_quality_gate_blocks_on_security_integrity(self) -> None:
        tid = "t3"
        self.store.insert_trace(
            Trace(
                trace_id=tid,
                conversation_id=self.conversation_id,
                started_at_ms=30,
                finished_at_ms=40,
                status="ok",
                stop_reason="completed",
                error_message=None,
            )
        )
        self.store.insert_message(self.conversation_id, tid, "user", "u3")
        self.store.insert_message(self.conversation_id, tid, "assistant", "a3")
        self.store.insert_replay_event(
            tid,
            1,
            "model_done",
            json.dumps({"request_id": tid, "stop_reason": "completed"}, ensure_ascii=False),
        )
        self.store.insert_replay_event(tid, 2, "tool_result", json.dumps({"call_id": "c3"}, ensure_ascii=False))
        self.store.insert_tool_run(
            trace_id=tid,
            conversation_id=self.conversation_id,
            call_id="c3",
            tool_name="echo",
            allowed=False,
            risk="low",
            side_effect=False,
            args_json="{}",
            result_json="{}",
            ok=False,
            error_code=None,
            error_message=None,
            started_at_ms=31,
            finished_at_ms=32,
        )

        with self.assertRaises(HTTPException) as e:
            routes.api_conversation_quality_gate(
                request=self.req,
                conversation_id=self.conversation_id,
                quality_gate_mode="fail",
                expected_contract_fingerprint=None,
                include_details=False,
            )
        self.assertEqual(e.exception.status_code, 409)
        self.assertEqual(e.exception.detail["code"], "QUALITY_GATE_BLOCKED")

    def test_export_audit_closure_fails_on_tool_runs_unmapped(self) -> None:
        tid = "t4"
        self.store.insert_trace(
            Trace(
                trace_id=tid,
                conversation_id=self.conversation_id,
                started_at_ms=50,
                finished_at_ms=60,
                status="ok",
                stop_reason="completed",
                error_message=None,
            )
        )
        self.store.insert_message(self.conversation_id, tid, "user", "u4")
        self.store.insert_message(self.conversation_id, tid, "assistant", "a4")
        self.store.insert_replay_event(
            tid,
            1,
            "model_done",
            json.dumps({"request_id": tid, "stop_reason": "completed"}, ensure_ascii=False),
        )
        self.store.insert_tool_run(
            trace_id=tid,
            conversation_id=self.conversation_id,
            call_id="c4",
            tool_name="echo",
            allowed=True,
            risk="low",
            side_effect=False,
            args_json="{}",
            result_json="{}",
            ok=True,
            error_code=None,
            error_message=None,
            started_at_ms=51,
            finished_at_ms=52,
        )

        out = routes.api_conversation_export(
            request=self.req,
            conversation_id=self.conversation_id,
            quality_gate_mode="off",
            expected_contract_fingerprint=None,
        )
        audit = out["quality_gate"]["audit_checks"]
        self.assertGreaterEqual(audit["tool_runs_unmapped"], 1)
        rule = next(r for r in out["quality_gate"]["rules"] if r.get("id") == "audit_closure")
        self.assertEqual(rule.get("status"), "fail")

    def test_export_audit_closure_fails_on_replay_seq_gap(self) -> None:
        tid = "t5"
        self.store.insert_trace(
            Trace(
                trace_id=tid,
                conversation_id=self.conversation_id,
                started_at_ms=70,
                finished_at_ms=80,
                status="ok",
                stop_reason="completed",
                error_message=None,
            )
        )
        self.store.insert_message(self.conversation_id, tid, "user", "u5")
        self.store.insert_message(self.conversation_id, tid, "assistant", "a5")
        self.store.insert_replay_event(
            tid,
            1,
            "model_done",
            json.dumps({"request_id": tid, "stop_reason": "completed"}, ensure_ascii=False),
        )
        self.store.insert_replay_event(tid, 3, "tool_result", json.dumps({"call_id": "c5"}, ensure_ascii=False))
        self.store.insert_tool_run(
            trace_id=tid,
            conversation_id=self.conversation_id,
            call_id="c5",
            tool_name="echo",
            allowed=True,
            risk="low",
            side_effect=False,
            args_json="{}",
            result_json="{}",
            ok=True,
            error_code=None,
            error_message=None,
            started_at_ms=71,
            finished_at_ms=72,
        )

        out = routes.api_conversation_export(
            request=self.req,
            conversation_id=self.conversation_id,
            quality_gate_mode="off",
            expected_contract_fingerprint=None,
        )
        audit = out["quality_gate"]["audit_checks"]
        self.assertGreaterEqual(audit["replay_seq_gap_count"], 1)
        rule = next(r for r in out["quality_gate"]["rules"] if r.get("id") == "audit_closure")
        self.assertEqual(rule.get("status"), "fail")

    def test_agent_task_rejects_invalid_working_dir(self) -> None:
        with self.assertRaises(HTTPException) as e:
            routes.api_agent_task_create(
                request=self.req,
                body=self._task_body("g", working_dir="/tmp"),
            )
        self.assertEqual(e.exception.status_code, 400)

    def test_agent_task_create_blocked_when_exec_disabled(self) -> None:
        object.__setattr__(self.settings, "agent_exec_enabled", False)
        req = _build_request(self.store, self.settings)
        with self.assertRaises(HTTPException) as e:
            routes.api_agent_task_create(
                request=req,
                body=self._task_body("disabled"),
            )
        self.assertEqual(e.exception.status_code, 503)
        self.assertEqual((e.exception.detail or {}).get("code"), "AGENT_EXEC_DISABLED")

    def test_agent_task_reviewer_cannot_submit_write_actions(self) -> None:
        with self.assertRaises(HTTPException) as e:
            routes.api_agent_task_create(
                request=self.req,
                body=self._task_body(
                    "reviewer write denied",
                    actions=[{"type": "write_file", "params": {"file_path": "apps/server/src/.tmp_nope.txt", "content": "x"}}],
                    actor_role="reviewer_agent",
                ),
            )
        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual((e.exception.detail or {}).get("code"), "ROLE_PERMISSION_DENIED")

    def test_agent_task_reviewer_can_submit_read_actions(self) -> None:
        out = routes.api_agent_task_create(
            request=self.req,
            body=self._task_body(
                "reviewer read allowed",
                actions=[{"type": "read_file", "params": {"file_path": "apps/server/src/test_quality_gate_contract.py"}}],
                actor_role="reviewer_agent",
            ),
        )
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("actor_role"), "reviewer_agent")
        self.assertEqual(out.get("actions_count"), 1)
        tid = str(out.get("task_id"))
        got = routes.api_agent_task_get(request=self.req, task_id=tid)
        self.assertEqual(got.get("actor_role"), "reviewer_agent")

    def test_agent_task_owner_cannot_submit_write_actions(self) -> None:
        with self.assertRaises(HTTPException) as e:
            routes.api_agent_task_create(
                request=self.req,
                body=self._task_body(
                    "owner write denied",
                    actions=[{"type": "search_replace", "params": {"file_path": "apps/server/src/test_quality_gate_contract.py", "old_str": "x", "new_str": "y"}}],
                    actor_role="owner",
                ),
            )
        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual((e.exception.detail or {}).get("code"), "ROLE_PERMISSION_DENIED")

    def test_agent_task_owner_can_submit_read_actions(self) -> None:
        out = routes.api_agent_task_create(
            request=self.req,
            body=self._task_body(
                "owner read allowed",
                actions=[{"type": "read_file", "params": {"file_path": "apps/server/src/test_quality_gate_contract.py"}}],
                actor_role="owner",
            ),
        )
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("actor_role"), "owner")
        self.assertEqual(out.get("actions_count"), 1)

    def test_agent_task_actor_role_update_persisted(self) -> None:
        out = routes.api_agent_task_create(
            request=self.req,
            body=self._task_body(
                "actor role persist",
                actions=[{"type": "read_file", "params": {"file_path": "apps/server/src/test_quality_gate_contract.py"}}],
                actor_role="reviewer_agent",
            ),
        )
        tid = str(out.get("task_id"))
        self.store.update_agent_task(tid, {"actor_role": "owner"})
        got = routes.api_agent_task_get(request=self.req, task_id=tid)
        self.assertEqual(got.get("actor_role"), "owner")

    def test_agent_tasks_list_contains_actor_role(self) -> None:
        out = routes.api_agent_task_create(
            request=self.req,
            body=self._task_body(
                "list has actor role",
                actions=[{"type": "read_file", "params": {"file_path": "apps/server/src/test_quality_gate_contract.py"}}],
                actor_role="reviewer_agent",
            ),
        )
        tid = str(out.get("task_id"))
        rows = (routes.api_agent_tasks(request=self.req, limit=50).get("items") or [])
        hit = next((x for x in rows if str(x.get("task_id")) == tid), None)
        self.assertIsNotNone(hit)
        self.assertEqual((hit or {}).get("actor_role"), "reviewer_agent")

    def test_sqlite_legacy_agent_tasks_auto_migrates_actor_role(self) -> None:
        store, db_path, tmp = _build_legacy_store(self.schema_path, "legacy.sqlite3")
        try:
            check_conn = sqlite3.connect(str(db_path))
            try:
                cols = [r[1] for r in check_conn.execute("PRAGMA table_info(agent_tasks)").fetchall()]
            finally:
                check_conn.close()
            self.assertIn("actor_role", cols)

            store.insert_agent_task(_legacy_task_payload("legacy_t1", "reviewer_agent", "r1", "tr1", 1, "legacy migrate"))
            got = store.get_agent_task("legacy_t1") or {}
            self.assertEqual(got.get("actor_role"), "reviewer_agent")
        finally:
            tmp.cleanup()

    def test_sqlite_legacy_agent_tasks_list_api_contains_actor_role(self) -> None:
        store, _, tmp = _build_legacy_store(self.schema_path, "legacy-list.sqlite3")
        try:
            req = _build_request(store, self.settings)
            store.insert_agent_task(_legacy_task_payload("legacy_t2", "owner", "r2", "tr2", 2, "legacy list"))
            rows = (routes.api_agent_tasks(request=req, limit=20).get("items") or [])
            hit = next((x for x in rows if str(x.get("task_id")) == "legacy_t2"), None)
            self.assertIsNotNone(hit)
            self.assertEqual((hit or {}).get("actor_role"), "owner")
        finally:
            tmp.cleanup()

    def test_agent_task_dry_run_finishes(self) -> None:
        out = routes.api_agent_task_create(
            request=self.req,
            body=self._task_body("dry run", dry_run=True, auto_start=True),
        )
        tid = out.get("task_id")
        self.assertIsInstance(tid, str)
        cur = self._wait_terminal_task(str(tid), rounds=30, sleep_s=0.02)
        self.assertEqual(cur.get("status"), "succeeded")
        self.assertEqual(cur.get("test_result"), "passed")

    def test_agent_task_actions_write_file_success(self) -> None:
        target = "apps/server/src/.tmp_action_write_ok.txt"
        out = routes.api_agent_task_create(
            request=self.req,
            body=self._task_body(
                "action write",
                actions=[{"type": "write_file", "params": {"file_path": target, "content": "hello_action"}}],
                auto_start=True,
            ),
        )
        tid = str(out.get("task_id"))
        cur = self._wait_terminal_task(tid, rounds=40, sleep_s=0.03)
        self.assertEqual(cur.get("status"), "succeeded")
        art = routes.api_agent_task_artifacts(request=self.req, task_id=tid)
        files = (art.get("artifacts") or {}).get("changed_files") or []
        self.assertTrue(any(".tmp_action_write_ok.txt" in str(p) for p in files))

    def test_agent_task_actions_forbidden_path_fails(self) -> None:
        out = routes.api_agent_task_create(
            request=self.req,
            body=self._task_body(
                "action forbidden",
                forbidden_paths=["apps/server/src/blocked_area"],
                actions=[{"type": "write_file", "params": {"file_path": "apps/server/src/blocked_area/a.txt", "content": "x"}}],
                auto_start=True,
            ),
        )
        tid = str(out.get("task_id"))
        cur = self._wait_terminal_task(tid, rounds=40, sleep_s=0.03)
        self.assertEqual(cur.get("status"), "failed")
        self.assertEqual(cur.get("error_code"), "PATH_NOT_ALLOWED")

    def test_agent_task_actions_search_replace_success(self) -> None:
        target = "apps/server/src/.tmp_action_replace_ok.txt"
        target_abs = Path(self.settings.project_root) / target
        target_abs.parent.mkdir(parents=True, exist_ok=True)
        target_abs.write_text("hello old world", encoding="utf-8")

        out = routes.api_agent_task_create(
            request=self.req,
            body=self._task_body(
                "replace text",
                actions=[{"type": "search_replace", "params": {"file_path": target, "old_str": "old", "new_str": "new"}}],
                auto_start=False,
            ),
        )
        task_id = str(out.get("task_id"))
        routes._run_agent_task(task_id, self.settings, self.store)
        cur = routes.api_agent_task_get(request=self.req, task_id=task_id)
        self.assertEqual(cur.get("status"), "succeeded")
        self.assertIn("hello new world", target_abs.read_text(encoding="utf-8"))

    def test_agent_task_actions_failure_rollback(self) -> None:
        target = "apps/server/src/.tmp_action_rollback.txt"
        target_abs = Path(self.settings.project_root) / target
        target_abs.parent.mkdir(parents=True, exist_ok=True)
        target_abs.write_text("origin", encoding="utf-8")

        out = routes.api_agent_task_create(
            request=self.req,
            body=self._task_body(
                "rollback on action failed",
                actions=[
                    {"type": "write_file", "params": {"file_path": target, "content": "changed"}},
                    {"type": "search_replace", "params": {"file_path": target, "old_str": "not-exists", "new_str": "x"}},
                ],
                rollback_on_action_failure=True,
                auto_start=False,
            ),
        )
        tid = str(out.get("task_id"))
        routes._run_agent_task(tid, self.settings, self.store)
        cur = routes.api_agent_task_get(request=self.req, task_id=tid)
        self.assertEqual(cur.get("status"), "failed")
        self.assertEqual(cur.get("error_code"), "SEARCH_TARGET_NOT_FOUND")

        self.assertEqual(target_abs.read_text(encoding="utf-8"), "origin")
        steps = routes.api_agent_task_steps(request=self.req, task_id=tid, limit=200)
        items = steps.get("items") or []
        self.assertTrue(any(str(it.get("step_type")) == "ACTION_SEARCH_REPLACE" and (not bool(it.get("ok"))) for it in items))

    def test_agent_task_steps_endpoint(self) -> None:
        out = routes.api_agent_task_create(
            request=self.req,
            body=self._task_body(
                "steps endpoint",
                actions=[{"type": "write_file", "params": {"file_path": "apps/server/src/.tmp_steps_action.txt", "content": "ok"}}],
                rollback_on_action_failure=True,
                auto_start=True,
            ),
        )
        tid = str(out.get("task_id"))
        self._wait_terminal_task(tid, rounds=50, sleep_s=0.02)

        steps = routes.api_agent_task_steps(request=self.req, task_id=tid, limit=200)
        self.assertTrue(steps.get("ok"))
        items = steps.get("items") or []
        self.assertGreaterEqual(len(items), 2)
        types = {str(it.get("step_type")) for it in items}
        self.assertIn("PLAN", types)
        self.assertIn("VERIFY", types)
        self.assertIn("ACTION_WRITE_FILE", types)

    def test_agent_task_command_allowlist_blocks_non_whitelisted(self) -> None:
        out = routes.api_agent_task_create(
            request=self.req,
            body=self._task_body("command allowlist", acceptance_cmd="python3 -c 'print(1)'", auto_start=True),
        )
        tid = str(out.get("task_id"))
        cur = self._wait_terminal_task(tid, rounds=50, sleep_s=0.02)
        self.assertEqual(cur.get("status"), "failed")
        self.assertEqual(cur.get("error_code"), "COMMAND_NOT_ALLOWED")
        lv = cur.get("last_verify") or {}
        self.assertEqual(lv.get("error_code"), "COMMAND_NOT_ALLOWED")

    def test_agent_task_actions_protected_env_file_fails(self) -> None:
        out = routes.api_agent_task_create(
            request=self.req,
            body=self._task_body(
                "protected file",
                actions=[{"type": "write_file", "params": {"file_path": "apps/server/src/.env.local", "content": "X=1"}}],
                auto_start=True,
            ),
        )
        tid = str(out.get("task_id"))
        cur = self._wait_terminal_task(tid, rounds=50, sleep_s=0.02)
        self.assertEqual(cur.get("status"), "failed")
        self.assertEqual(cur.get("error_code"), "FILE_PROTECTED")

    def test_agent_task_actions_protected_env_search_replace_fails(self) -> None:
        protected_rel = "apps/server/src/.env.local"
        protected_abs = Path(self.settings.project_root) / protected_rel
        protected_abs.parent.mkdir(parents=True, exist_ok=True)
        protected_abs.write_text("SECRET_KEY=abc", encoding="utf-8")

        out = routes.api_agent_task_create(
            request=self.req,
            body=self._task_body(
                "protected replace",
                actions=[{"type": "search_replace", "params": {"file_path": protected_rel, "old_str": "abc", "new_str": "xyz"}}],
                auto_start=True,
            ),
        )
        tid = str(out.get("task_id"))
        cur = self._wait_terminal_task(tid, rounds=50, sleep_s=0.02)
        self.assertEqual(cur.get("status"), "failed")
        self.assertEqual(cur.get("error_code"), "FILE_PROTECTED")

        self.assertIn("SECRET_KEY=abc", protected_abs.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()