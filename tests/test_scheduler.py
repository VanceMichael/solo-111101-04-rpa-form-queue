import json
import tempfile
import unittest
from pathlib import Path

from collector_04.portal import LocalForm
from collector_04.scheduler import (
    CONFIRMED,
    MANUAL,
    NEEDS_INPUT,
    PREPARED,
    SUBMIT_UNVERIFIED,
    SUBMITTING,
    Scheduler,
    StateStore,
    canonical_digest,
    run_scenario,
    status_projection,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures"


def write_manifest(directory, options, name="case", run_id=None):
    manifest = {
        "name": name,
        "run_id": run_id or f"run-{name}",
        "sources": ["/tasks"],
        "options": options,
        "fixture_root": str(FIXTURES),
        "portal_database": str(Path(directory) / "portal.sqlite3"),
    }
    path = Path(directory) / "scenario.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False))
    return path


def job(result, job_id):
    return next(item for item in result["jobs"] if item["id"] == job_id)


class SchedulerTests(unittest.TestCase):
    def test_timeout_commit_recovers_via_lookup_without_resubmit(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = write_manifest(directory, {"form_outcome": "commit_then_timeout",
                                                  "rule_version": 1, "resume": True})
            database = Path(directory) / "state.sqlite3"
            first = run_scenario(manifest, database)
            self.assertEqual(job(first, "D1")["state"], CONFIRMED)
            self.assertEqual(first["receipts"][0]["via"], "lookup")
            self.assertEqual(first["form_submissions"], 1)
            # 重复执行不得重发：替身库行数不变，回执不变
            second = run_scenario(manifest, database)
            self.assertEqual(second["form_submissions"], 1)
            self.assertEqual(second["receipts"], first["receipts"])
            self.assertEqual(LocalForm(Path(directory) / "portal.sqlite3").count(), 1)

    def test_validation_failure_is_separate_and_does_not_block_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = write_manifest(directory, {"form_outcome": "commit_then_timeout",
                                                  "rule_version": 1, "resume": True})
            result = run_scenario(manifest, Path(directory) / "state.sqlite3")
            d1, d2 = job(result, "D1"), job(result, "D2")
            self.assertEqual(d1["state"], CONFIRMED)  # D2 校验失败不阻塞 D1
            self.assertEqual(d2["state"], NEEDS_INPUT)
            self.assertEqual(d2["reason"], "missing_required_fields")
            self.assertEqual(d2["missing"], ["supplier"])
            # 校验失败只有提交前证据，绝不进入提交流程
            self.assertEqual(d2["evidence"]["post_submit"], [])
            self.assertEqual(d2["evidence"]["pre_submit"][0]["kind"], "validation_failed")

    def test_hidden_receipt_goes_manual_and_never_resubmits(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = write_manifest(directory, {"form_outcome": "commit_then_timeout",
                                                  "rule_version": 1, "hide_receipt": True,
                                                  "resume": True})
            database = Path(directory) / "state.sqlite3"
            first = run_scenario(manifest, database)
            d1 = job(first, "D1")
            self.assertEqual(d1["state"], MANUAL)
            self.assertEqual(d1["reason"], "unverifiable_commit")
            self.assertEqual(first["handoffs"][0]["reason"], "unverifiable_commit")
            self.assertEqual(first["form_submissions"], 1)
            second = run_scenario(manifest, database)
            self.assertEqual(second["form_submissions"], 1)  # 人工接管后不得重发
            self.assertEqual(len(second["handoffs"]), 1)  # 接管单不重复
            self.assertEqual(job(second, "D1")["state"], MANUAL)

    def test_pause_prepares_without_submit_then_continues(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = write_manifest(directory, {"form_outcome": "success",
                                                  "rule_version": 1,
                                                  "pause_before_submit": True})
            database = Path(directory) / "state.sqlite3"
            paused = run_scenario(manifest, database)
            self.assertEqual(job(paused, "D1")["state"], PREPARED)
            self.assertEqual(job(paused, "D1")["reason"], "paused_before_submit")
            self.assertEqual(paused["form_submissions"], 0)
            # 解除暂停后继续：同一状态库上确认，不重复准备证据
            continued = run_scenario(manifest, database, pause_override=False)
            self.assertEqual(job(continued, "D1")["state"], CONFIRMED)
            self.assertEqual(continued["form_submissions"], 1)
            prepared_events = [e for e in job(continued, "D1")["evidence"]["pre_submit"]
                               if e["kind"] == "prepared"]
            self.assertEqual(len(prepared_events), 1)

    def test_rule_change_keeps_confirmed_and_revalidates_unsubmitted(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = write_manifest(directory, {"form_outcome": "success", "rule_version": 1,
                                                  "resume": True, "next_rule_version": 2})
            database = Path(directory) / "state.sqlite3"
            first = run_scenario(manifest, database)
            d1, d2 = job(first, "D1"), job(first, "D2")
            self.assertEqual(d1["state"], CONFIRMED)
            self.assertEqual(d1["rule_version"], 1)  # 已确认记录保持确认时的规则版本
            self.assertEqual(first["form_submissions"], 1)
            # 未提交任务按新规则重新校验，旧证据保留
            self.assertEqual(d2["state"], NEEDS_INPUT)
            self.assertEqual(d2["rule_version"], 2)
            self.assertEqual(d2["missing"], ["supplier", "tax_id"])
            versions = [e["rule_version"] for e in d2["evidence"]["pre_submit"]
                        if e["kind"] == "validation_failed"]
            self.assertEqual(versions, [1, 2])
            # 重跑不得把已确认任务按新规则重录
            second = run_scenario(manifest, database)
            self.assertEqual(second["form_submissions"], 1)
            self.assertEqual(job(second, "D1")["receipt"], d1["receipt"])
            self.assertEqual(second["rule_version"], 2)

    def test_unverified_job_is_never_resubmitted_across_reruns(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = write_manifest(directory, {"form_outcome": "commit_then_timeout",
                                                  "rule_version": 1, "resume": True})
            database = Path(directory) / "state.sqlite3"
            for _ in range(3):
                result = run_scenario(manifest, database)
                self.assertEqual(result["form_submissions"], 1)
                self.assertEqual(job(result, "D1")["state"], CONFIRMED)

    def test_ambiguous_lookup_goes_manual(self):
        with tempfile.TemporaryDirectory() as directory:
            portal = LocalForm(Path(directory) / "portal.sqlite3")
            fields = {"supplier": "S1", "amount": "12.50"}
            portal.submit("D1", fields)
            portal.submit("D1", fields)  # 历史上被重复录入，读回两条同内容回执
            store = StateStore(Path(directory) / "state.sqlite3")
            store.initialize()
            store.register_task("D1", "run-case")
            store.update_job("D1", state=SUBMIT_UNVERIFIED, reason="commit_then_timeout",
                             digest=canonical_digest(fields))
            Scheduler(store, portal).recover_unverified()
            job_row = store.get_job("D1")
            self.assertEqual(job_row["state"], MANUAL)
            self.assertEqual(job_row["reason"], "ambiguous_commit")
            self.assertEqual(portal.count(), 2)  # 恢复过程不重发

    def test_crash_after_commit_recovers_via_lookup_not_resubmit(self):
        with tempfile.TemporaryDirectory() as directory:
            portal = LocalForm(Path(directory) / "portal.sqlite3", outcome="commit_then_timeout")
            fields = {"supplier": "S1", "amount": "12.50"}
            with self.assertRaises(TimeoutError):
                portal.submit("D1", fields)  # 已落库但进程“崩溃”，状态停在 submitting
            store = StateStore(Path(directory) / "state.sqlite3")
            store.initialize()
            store.register_task("D1", "run-case")
            store.update_job("D1", state=SUBMITTING, digest=canonical_digest(fields))
            Scheduler(store, portal).recover_unverified()
            self.assertEqual(store.get_job("D1")["state"], CONFIRMED)
            self.assertEqual(portal.count(), 1)

    def test_crash_before_commit_is_conservative_manual(self):
        with tempfile.TemporaryDirectory() as directory:
            portal = LocalForm(Path(directory) / "portal.sqlite3")
            store = StateStore(Path(directory) / "state.sqlite3")
            store.initialize()
            store.register_task("D1", "run-case")
            store.update_job("D1", state=SUBMITTING, digest=canonical_digest({"supplier": "S1"}))
            Scheduler(store, portal).recover_unverified()
            # 读回为空无法区分“未提交”与“已提交但不可见”：转人工而不是重发
            self.assertEqual(store.get_job("D1")["state"], MANUAL)
            self.assertEqual(store.get_job("D1")["reason"], "unverifiable_commit")
            self.assertEqual(portal.count(), 0)

    def test_status_projection_reads_persisted_state(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = write_manifest(directory, {"form_outcome": "success", "rule_version": 1})
            database = Path(directory) / "state.sqlite3"
            run_scenario(manifest, database)
            status = status_projection(database)
            self.assertEqual(job(status, "D1")["state"], CONFIRMED)
            self.assertEqual(status["form_submissions"], 1)


if __name__ == "__main__":
    unittest.main()
