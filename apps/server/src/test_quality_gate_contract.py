from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from connectors.web import routes
from core.contracts.observability import Trace
from core.infra.storage.sqlite_store import SQLiteStore


def _build_request(store: SQLiteStore) -> Request:
    app = FastAPI()
    app.state.store = store
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


class QualityGateContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        src_dir = Path(__file__).resolve().parent
        schema_path = src_dir / "core" / "infra" / "storage" / "schema.sql"
        db_path = Path(self._tmp.name) / "test.sqlite3"
        self.store = SQLiteStore(str(db_path), str(schema_path))
        self.req = _build_request(self.store)

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


if __name__ == "__main__":
    unittest.main()