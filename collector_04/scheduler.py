"""申报录入调度器：可恢复阶段、提交前后证据与人工接管。

状态机（持久化在 --database 中，进程重启后仍在）：

    received -> prepared -> submitting -> confirmed
                 |             |
                 v             v
            needs_input   submit_unverified --(lookup 恢复)--> confirmed
                                                          +--> manual

* 字段校验失败（needs_input）发生在提交前，与提交结果不确定
  （submitting / submit_unverified）严格分开，互不转换。
* 提交结果不确定的任务绝不盲目重发：恢复时只能 lookup 读回核对，
  找不到唯一同内容回执就交人工（manual），禁止再次 submit。
* 已确认（confirmed）与人工接管（manual）是终态，规则版本变化
  也不会把它们重新录入；未提交任务按新规则重新校验并保留旧证据。
"""

import json
import sqlite3
from hashlib import sha256
from pathlib import Path

from .adapter import decode
from .portal import LocalForm
from .records import utc_now
from .sources import FixtureTransport

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    run_id TEXT,
    state TEXT NOT NULL,
    reason TEXT,
    rule_version INTEGER,
    fields TEXT NOT NULL DEFAULT '{}',
    missing TEXT NOT NULL DEFAULT '[]',
    digest TEXT,
    receipt INTEGER,
    confirmed_via TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS job_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    run_id TEXT,
    phase TEXT NOT NULL,
    kind TEXT NOT NULL,
    at TEXT NOT NULL,
    data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS handoffs (
    job_id TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    at TEXT NOT NULL
);
"""

# 执行阶段
RECEIVED = "received"
PREPARED = "prepared"
SUBMITTING = "submitting"
SUBMIT_UNVERIFIED = "submit_unverified"
CONFIRMED = "confirmed"
NEEDS_INPUT = "needs_input"
MANUAL = "manual"

# 主流程不得再触碰的阶段：终态 + 提交结果未确认（只能走 lookup 恢复）
SKIP_STATES = (CONFIRMED, MANUAL, SUBMITTING, SUBMIT_UNVERIFIED)
# 提交后结果未确认的阶段：恢复时只准读回核对，禁止重发
UNVERIFIED_STATES = (SUBMITTING, SUBMIT_UNVERIFIED)
# 未提交阶段：规则版本变化时需要重新校验
UNSUBMITTED_STATES = (RECEIVED, PREPARED, NEEDS_INPUT)

# 人工接管原因
REASON_UNVERIFIABLE = "unverifiable_commit"
REASON_AMBIGUOUS = "ambiguous_commit"

TASK_META_KEYS = ("id", "rule_version")


def canonical_digest(fields):
    """与 portal.LocalForm 完全一致的字段摘要，用于恢复时核对回执内容。"""
    canonical = json.dumps(fields, ensure_ascii=False, sort_keys=True)
    return sha256(canonical.encode()).hexdigest()


def form_fields(task):
    """从申报任务中取出要录入表单的字段（剔除调度元信息）。"""
    return {key: value for key, value in task.items() if key not in TASK_META_KEYS}


class StateStore:
    """调度状态库：与 portal.sqlite3 是两个独立数据库，不伪造跨库事务。"""

    def __init__(self, path):
        self.path = str(path)

    def connect(self):
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def initialize(self):
        with self.connect() as connection:
            connection.executescript(SCHEMA)

    def get_meta(self, key, default=None):
        with self.connect() as connection:
            row = connection.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return row["value"] if row else default

    def set_meta(self, key, value):
        with self.connect() as connection:
            connection.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )

    def set_meta_if_absent(self, key, value):
        with self.connect() as connection:
            connection.execute("INSERT OR IGNORE INTO meta(key,value) VALUES(?,?)", (key, str(value)))

    def current_rule_version(self):
        return int(self.get_meta("rule_version", "1"))

    def register_task(self, job_id, run_id):
        with self.connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO jobs(id,run_id,state,updated_at) VALUES(?,?,?,?)",
                (job_id, run_id, RECEIVED, utc_now()),
            )

    def get_job(self, job_id):
        with self.connect() as connection:
            return connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()

    def all_jobs(self):
        with self.connect() as connection:
            return connection.execute("SELECT * FROM jobs ORDER BY id").fetchall()

    def jobs_in_states(self, states):
        marks = ",".join("?" for _ in states)
        with self.connect() as connection:
            return connection.execute(f"SELECT * FROM jobs WHERE state IN ({marks}) ORDER BY id", tuple(states)).fetchall()

    def update_job(self, job_id, **columns):
        columns["updated_at"] = utc_now()
        clause = ", ".join(f"{key}=?" for key in columns)
        with self.connect() as connection:
            connection.execute(f"UPDATE jobs SET {clause} WHERE id=?", (*columns.values(), job_id))

    def add_event(self, job_id, run_id, phase, kind, data):
        payload = {"kind": kind, "at": utc_now(), **data}
        with self.connect() as connection:
            connection.execute(
                "INSERT INTO job_events(job_id,run_id,phase,kind,at,data) VALUES(?,?,?,?,?,?)",
                (job_id, run_id, phase, kind, payload["at"], json.dumps(payload, ensure_ascii=False, sort_keys=True)),
            )

    def events_for(self, job_id):
        with self.connect() as connection:
            return connection.execute("SELECT * FROM job_events WHERE job_id=? ORDER BY id", (job_id,)).fetchall()

    def add_handoff(self, job_id, reason, detail):
        with self.connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO handoffs(job_id,reason,detail,at) VALUES(?,?,?,?)",
                (job_id, reason, detail, utc_now()),
            )

    def open_handoffs(self):
        with self.connect() as connection:
            return connection.execute("SELECT * FROM handoffs ORDER BY job_id").fetchall()


class Scheduler:
    """单次执行的调度对象。

    恢复（resume）必须新建 Scheduler 实例：新对象只依赖状态库中的
    持久化阶段与证据，绝不在内存对象上原地重试提交。
    """

    def __init__(self, store, portal, run_id="run", pause_before_submit=False):
        self.store = store
        self.portal = portal
        self.run_id = run_id
        self.pause_before_submit = pause_before_submit

    # ---- 主流程 ----

    def process_tasks(self, tasks):
        for task in tasks:
            job_id = str(task.get("id", "")).strip()
            if not job_id:
                raise ValueError("来源任务缺少 id")
            self.store.register_task(job_id, self.run_id)
            try:
                self.process_job(job_id, form_fields(task))
            except Exception as error:  # 单条申报异常不阻塞整批
                self.store.add_event(job_id, self.run_id, "state", "error", {"error": str(error)})

    def process_job(self, job_id, fields):
        job = self.store.get_job(job_id)
        if job["state"] in SKIP_STATES:
            return  # 已确认/人工接管/提交结果未确认：主流程绝不重发
        version = self.store.current_rule_version()
        required = list(self.portal.read_form(version)["required"])
        missing = [key for key in required if not fields.get(key)]
        if missing:
            # 字段校验失败：提交前分流，不影响其他任务
            self._mark_needs_input(job_id, version, fields, required, missing, trigger="validate")
            return
        digest = canonical_digest(fields)
        self._mark_prepared(job_id, version, fields, required, digest, trigger="validate")
        if self.pause_before_submit:
            return  # 暂停：只准备不提交
        self._submit(job_id, version, fields, digest)

    def _submit(self, job_id, version, fields, digest):
        # 先落“提交中”标记：即使进程在表单保存后崩溃，恢复时也走
        # lookup 核对而不是盲目重发。
        self.store.update_job(job_id, state=SUBMITTING, reason=None)
        self.store.add_event(job_id, self.run_id, "post_submit", "submit_attempt",
                             {"rule_version": version, "digest": digest})
        try:
            result = self.portal.submit(job_id, fields, version=version)
        except TimeoutError as error:
            # 服务端可能已保存而客户端未收到回执：结果不确定
            self.store.update_job(job_id, state=SUBMIT_UNVERIFIED, reason="commit_then_timeout")
            self.store.add_event(job_id, self.run_id, "post_submit", "submit_timeout",
                                 {"error": str(error), "digest": digest})
            return
        if result.get("status") == "invalid":
            # 表单端校验失败仍是字段问题，与提交不确定分开处理
            missing = list(result.get("fields", []))
            required = list(self.portal.read_form(version)["required"])
            self._mark_needs_input(job_id, version, fields, required, missing, trigger="form_invalid")
            return
        self._mark_confirmed(job_id, result["receipt"], result["digest"], via="submit")

    # ---- 规则版本变化 ----

    def apply_rule_change(self, next_version):
        current = self.store.current_rule_version()
        if next_version <= current:
            return
        self.store.set_meta("rule_version", str(next_version))
        for job in self.store.jobs_in_states(UNSUBMITTED_STATES):
            # 已确认/人工/未确认提交的任务不按新规则重新录入
            fields = json.loads(job["fields"])
            required = list(self.portal.read_form(next_version)["required"])
            missing = [key for key in required if not fields.get(key)]
            if missing:
                self._mark_needs_input(job["id"], next_version, fields, required, missing,
                                       trigger=f"rule_change:{current}->{next_version}")
            else:
                self._mark_prepared(job["id"], next_version, fields, required,
                                    canonical_digest(fields),
                                    trigger=f"rule_change:{current}->{next_version}")

    # ---- 恢复（必须用新建的执行对象调用） ----

    def recover_unverified(self):
        for job in self.store.jobs_in_states(UNVERIFIED_STATES):
            receipts = self.portal.lookup(job["id"])
            matches = [item for item in receipts if item["digest"] == job["digest"]]
            self.store.add_event(job["id"], self.run_id, "post_submit", "lookup",
                                 {"receipts": len(receipts), "matches": len(matches)})
            if len(matches) == 1:
                self._mark_confirmed(job["id"], matches[0]["receipt"], job["digest"], via="lookup")
            elif not matches:
                self._mark_manual(job["id"], REASON_UNVERIFIABLE,
                                  "提交后未观察到回执且读回无法验证，禁止重发，转人工核对")
            else:
                self._mark_manual(job["id"], REASON_AMBIGUOUS,
                                  f"读回 {len(matches)} 条同内容回执，无法判定唯一回执，转人工核对")

    # ---- 阶段迁移（全部留证据） ----

    def _mark_needs_input(self, job_id, version, fields, required, missing, trigger):
        job = self.store.get_job(job_id)
        missing = list(missing)
        if (job and job["state"] == NEEDS_INPUT and job["rule_version"] == version
                and json.loads(job["missing"]) == missing):
            return  # 校验结论未变，不重复留证
        self.store.update_job(job_id, state=NEEDS_INPUT, reason="missing_required_fields",
                              rule_version=version,
                              fields=json.dumps(fields, ensure_ascii=False, sort_keys=True),
                              missing=json.dumps(missing, ensure_ascii=False))
        self.store.add_event(job_id, self.run_id, "pre_submit", "validation_failed",
                             {"rule_version": version, "required": list(required),
                              "missing": missing, "fields": fields, "trigger": trigger})

    def _mark_prepared(self, job_id, version, fields, required, digest, trigger):
        job = self.store.get_job(job_id)
        reason = "paused_before_submit" if self.pause_before_submit else None
        if job and job["state"] == PREPARED and job["rule_version"] == version and job["digest"] == digest:
            return  # 已准备且内容与规则未变，不重复留证
        self.store.update_job(job_id, state=PREPARED, reason=reason,
                              rule_version=version,
                              fields=json.dumps(fields, ensure_ascii=False, sort_keys=True),
                              missing="[]", digest=digest)
        self.store.add_event(job_id, self.run_id, "pre_submit", "prepared",
                             {"rule_version": version, "required": list(required),
                              "fields": fields, "digest": digest, "trigger": trigger})

    def _mark_confirmed(self, job_id, receipt, digest, via):
        self.store.update_job(job_id, state=CONFIRMED, reason=None,
                              receipt=receipt, digest=digest, confirmed_via=via)
        self.store.add_event(job_id, self.run_id, "post_submit", "confirmed",
                             {"receipt": receipt, "digest": digest, "via": via})

    def _mark_manual(self, job_id, reason, detail):
        self.store.update_job(job_id, state=MANUAL, reason=reason)
        self.store.add_event(job_id, self.run_id, "post_submit", "handoff",
                             {"reason": reason, "detail": detail})
        self.store.add_handoff(job_id, reason, detail)


# ---- 场景执行与投影 ----


def load_manifest(scenario_path):
    manifest = json.loads(Path(scenario_path).read_text())
    if not isinstance(manifest, dict) or not manifest.get("sources"):
        raise ValueError("场景文件缺少 sources")
    return manifest


def load_tasks(manifest):
    root = Path(manifest.get("fixture_root") or ".")  # 相对当前工作目录解析
    transport = FixtureTransport(root)
    tasks = []
    for route in manifest["sources"]:
        response = transport.request(route)
        if response.status != 200:
            raise ValueError(f"source status {response.status}")
        tasks.extend(decode(response.body))
    return tasks


def run_scenario(scenario_path, database, pause_override=None, resume_override=None, portal_override=None):
    manifest = load_manifest(scenario_path)
    options = manifest.get("options") or {}
    pause = bool(options.get("pause_before_submit")) if pause_override is None else pause_override
    resume = bool(options.get("resume")) if resume_override is None else resume_override
    portal_database = portal_override or manifest.get("portal_database") or "portal.sqlite3"
    portal = LocalForm(portal_database,
                       outcome=options.get("form_outcome", "success"),
                       hide_receipt=bool(options.get("hide_receipt", False)))
    store = StateStore(database)
    store.initialize()
    store.set_meta_if_absent("rule_version", str(int(options.get("rule_version", 1))))
    store.set_meta("portal_database", str(portal_database))
    if manifest.get("name"):
        store.set_meta("scenario", str(manifest["name"]))
    run_id = str(manifest.get("run_id") or "run")
    store.set_meta("run_id", run_id)

    scheduler = Scheduler(store, portal, run_id=run_id, pause_before_submit=pause)
    scheduler.process_tasks(load_tasks(manifest))

    next_version = options.get("next_rule_version")
    if next_version is not None:
        scheduler.apply_rule_change(int(next_version))

    if resume:
        # 恢复必须新建执行对象：从状态库重新加载，不在内存对象上重试
        Scheduler(store, portal, run_id=run_id, pause_before_submit=pause).recover_unverified()

    return projection(store, portal, scenario=manifest.get("name"), run_id=run_id)


def status_projection(database, portal_override=None):
    store = StateStore(database)
    store.initialize()
    portal_database = portal_override or store.get_meta("portal_database")
    if not portal_database:
        raise ValueError("状态库中没有 portal 路径记录，请先用 --run 执行或用 --portal-database 指定")
    portal = LocalForm(portal_database)
    return projection(store, portal, scenario=store.get_meta("scenario"), run_id=store.get_meta("run_id"))


def projection(store, portal, scenario=None, run_id=None):
    jobs = []
    for job in store.all_jobs():
        events = store.events_for(job["id"])
        jobs.append({
            "id": job["id"],
            "state": job["state"],
            "reason": job["reason"],
            "rule_version": job["rule_version"],
            "fields": json.loads(job["fields"]),
            "missing": json.loads(job["missing"]),
            "receipt": job["receipt"],
            "evidence": {
                "pre_submit": [json.loads(event["data"]) for event in events if event["phase"] == "pre_submit"],
                "post_submit": [json.loads(event["data"]) for event in events if event["phase"] == "post_submit"],
            },
        })
    receipts = [
        {"id": job["id"], "receipt": job["receipt"], "digest": job["digest"], "via": job["confirmed_via"]}
        for job in jobs_confirmed(store)
    ]
    handoffs = [
        {"id": row["job_id"], "reason": row["reason"], "detail": row["detail"], "at": row["at"]}
        for row in store.open_handoffs()
    ]
    return {
        "scenario": scenario,
        "run_id": run_id,
        "rule_version": store.current_rule_version(),
        "jobs": jobs,
        "form_submissions": portal.count(),
        "receipts": receipts,
        "handoffs": handoffs,
    }


def jobs_confirmed(store):
    return store.jobs_in_states((CONFIRMED,))
