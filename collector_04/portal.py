import json
import sqlite3
from hashlib import sha256


class LocalForm:
    def __init__(self, database, outcome="success", hide_receipt=False):
        self.database = str(database)
        self.outcome = outcome
        self.hide_receipt = hide_receipt
        with self.connect() as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS submissions(receipt INTEGER PRIMARY KEY, task TEXT NOT NULL, digest TEXT NOT NULL, fields TEXT NOT NULL)")

    def connect(self):
        return sqlite3.connect(self.database)

    def read_form(self, version):
        return {"version": version, "required": ["supplier", "amount"] + (["tax_id"] if version >= 2 else [])}

    def submit(self, task, fields, version=1):
        missing = [key for key in self.read_form(version)["required"] if not fields.get(key)]
        if missing:
            return {"status": "invalid", "fields": missing}
        canonical = json.dumps(fields, ensure_ascii=False, sort_keys=True)
        digest = sha256(canonical.encode()).hexdigest()
        with self.connect() as connection:
            cursor = connection.execute("INSERT INTO submissions(task,digest,fields) VALUES(?,?,?)", (task, digest, canonical))
            receipt = cursor.lastrowid
        if self.outcome == "commit_then_timeout":
            raise TimeoutError("confirmation not observed")
        return {"status": "saved", "receipt": receipt, "digest": digest}

    def lookup(self, task):
        if self.hide_receipt:
            return []
        with self.connect() as connection:
            return [{"receipt": row[0], "digest": row[1], "fields": json.loads(row[2])} for row in connection.execute("SELECT receipt,digest,fields FROM submissions WHERE task=? ORDER BY receipt", (task,))]

    def count(self):
        with self.connect() as connection:
            return connection.execute("SELECT count(*) FROM submissions").fetchone()[0]
