"""独立进程恢复测试：每个进程都是全新解释器，验证重启后的恢复与防重。

值班同事可对照 portal.sqlite3 的实际提交行数判断是否发生重录。
"""

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures"


def run_cli(*args):
    return subprocess.run(
        [sys.executable, "-m", "collector_04.cli", *args],
        cwd=ROOT, text=True, capture_output=True, timeout=30,
    )


def portal_rows(portal_database):
    with sqlite3.connect(portal_database) as connection:
        return connection.execute("SELECT count(*) FROM submissions").fetchone()[0]


class ProcessRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        work = Path(self.directory.name)
        self.database = work / "state.sqlite3"
        self.portal = work / "portal.sqlite3"
        self.output = work / "result.json"

    def tearDown(self):
        self.directory.cleanup()

    def write_manifest(self, name, options):
        manifest = {
            "name": name,
            "run_id": f"run-{name}",
            "sources": ["/tasks"],
            "options": options,
            "fixture_root": str(FIXTURES),
            "portal_database": str(self.portal),
        }
        path = Path(self.directory.name) / f"{name}.json"
        path.write_text(json.dumps(manifest, ensure_ascii=False))
        return path

    def run_scenario(self, manifest, *extra):
        completed = run_cli("--run", str(manifest), "--database", str(self.database),
                            "--output", str(self.output), *extra)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return json.loads(self.output.read_text())

    def job(self, result, job_id):
        return next(item for item in result["jobs"] if item["id"] == job_id)

    def test_confirmation_lost_recovers_in_fresh_process(self):
        manifest = self.write_manifest("confirmation_lost", {
            "form_outcome": "commit_then_timeout", "rule_version": 1, "resume": True})
        first = self.run_scenario(manifest)
        self.assertEqual(self.job(first, "D1")["state"], "confirmed")
        self.assertEqual(self.job(first, "D2")["state"], "needs_input")
        self.assertEqual(portal_rows(self.portal), 1)
        # 第二个独立进程重复同一输入：不得重录，投影保持一致
        second = self.run_scenario(manifest)
        self.assertEqual(self.job(second, "D1")["state"], "confirmed")
        self.assertEqual(second["form_submissions"], 1)
        self.assertEqual(portal_rows(self.portal), 1)
        self.assertEqual(second["receipts"], first["receipts"])

    def test_pause_then_continue_with_flag_override(self):
        manifest = self.write_manifest("pause", {
            "form_outcome": "success", "rule_version": 1, "pause_before_submit": True})
        paused = self.run_scenario(manifest)
        self.assertEqual(self.job(paused, "D1")["state"], "prepared")
        self.assertEqual(paused["form_submissions"], 0)
        self.assertFalse(self.portal.exists() and portal_rows(self.portal))
        # 值班同事解除暂停后继续：同一状态库，新进程完成提交
        continued = self.run_scenario(manifest, "--no-pause-before-submit")
        self.assertEqual(self.job(continued, "D1")["state"], "confirmed")
        self.assertEqual(portal_rows(self.portal), 1)
        # 查询最终处理清单
        status = run_cli("--database", str(self.database), "--status")
        self.assertEqual(status.returncode, 0, status.stderr)
        listing = json.loads(status.stdout)
        self.assertEqual(self.job(listing, "D1")["state"], "confirmed")
        self.assertEqual(listing["form_submissions"], 1)

    def test_manual_handoff_survives_restart_without_resubmit(self):
        manifest = self.write_manifest("manual", {
            "form_outcome": "commit_then_timeout", "rule_version": 1,
            "hide_receipt": True, "resume": True})
        first = self.run_scenario(manifest)
        self.assertEqual(self.job(first, "D1")["state"], "manual")
        self.assertEqual(self.job(first, "D1")["reason"], "unverifiable_commit")
        self.assertEqual(portal_rows(self.portal), 1)
        second = self.run_scenario(manifest)
        self.assertEqual(self.job(second, "D1")["state"], "manual")
        self.assertEqual(len(second["handoffs"]), 1)
        self.assertEqual(portal_rows(self.portal), 1)  # 重启后绝不重发

    def test_rule_change_does_not_reenter_confirmed_across_processes(self):
        manifest = self.write_manifest("new_rules", {
            "form_outcome": "success", "rule_version": 1,
            "resume": True, "next_rule_version": 2})
        first = self.run_scenario(manifest)
        self.assertEqual(self.job(first, "D1")["state"], "confirmed")
        self.assertEqual(self.job(first, "D1")["rule_version"], 1)
        second = self.run_scenario(manifest)
        self.assertEqual(portal_rows(self.portal), 1)
        self.assertEqual(self.job(second, "D1")["receipt"], self.job(first, "D1")["receipt"])
        self.assertEqual(self.job(second, "D2")["missing"], ["supplier", "tax_id"])


if __name__ == "__main__":
    unittest.main()
