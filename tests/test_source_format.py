import tempfile
import unittest
from pathlib import Path
from collector_04.portal import LocalForm


class FormProtocolTests(unittest.TestCase):
    def test_commit_survives_timeout_and_new_client(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "portal.sqlite3"
            form = LocalForm(database, "commit_then_timeout")
            with self.assertRaises(TimeoutError):
                form.submit("A", {"supplier": "S1", "amount": "10.00"})
            recovered = LocalForm(database)
            self.assertEqual(recovered.count(), 1)
            self.assertEqual(recovered.lookup("A")[0]["fields"]["amount"], "10.00")

    def test_server_does_not_guarantee_idempotency(self):
        with tempfile.TemporaryDirectory() as directory:
            form = LocalForm(Path(directory) / "portal.sqlite3")
            for attempt in range(2):
                form.submit("A", {"supplier": "S1", "amount": "10.00"})
            self.assertEqual(form.count(), 2)

    def test_rule_change_is_visible_before_submit(self):
        with tempfile.TemporaryDirectory() as directory:
            form = LocalForm(Path(directory) / "portal.sqlite3")
            result = form.submit("A", {"supplier": "S1", "amount": "10.00"}, version=2)
            self.assertEqual(result, {"status": "invalid", "fields": ["tax_id"]})
            self.assertEqual(form.count(), 0)

    def test_hidden_receipts_do_not_mean_absent_submissions(self):
        with tempfile.TemporaryDirectory() as directory:
            form = LocalForm(Path(directory) / "portal.sqlite3", hide_receipt=True)
            form.submit("A", {"supplier": "S1", "amount": "10.00"})
            self.assertEqual(form.lookup("A"), [])
            self.assertEqual(form.count(), 1)
