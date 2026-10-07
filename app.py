"""临床试验分层区组随机分配与盲法服务。

号源账模型：
  中央池(stratum_plans 计划数) -> 中心号段预占(segment_reservations)
       -> 受试者发号(participants/allocations) -> 药品备货发药(drug_shipments/drug_kits)
号段按失效时刻过期，未发完的号退回中央池重算；排队请求(queue)按 FIFO 接回。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB = BASE_DIR / "randomization.db"
MAX_ARM_LENGTH = 40
DEFAULT_TTL_MINUTES = 60
MAX_TTL_MINUTES = 24 * 60


class BusinessError(Exception):
    def __init__(self, message, status=400, code="bad_request", extra=None):
        super().__init__(message)
        self.message, self.status, self.code = message, status, code
        self.extra = extra or {}


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class RandomizationStore:
    def __init__(self, db_path=DEFAULT_DB):
        self.db_path = str(db_path)
        self._lock = threading.Lock()

    def connect(self):
        conn = sqlite3.connect(self.db_path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def init_schema(self):
        with self._lock, self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users(
                    id TEXT PRIMARY KEY, name TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('site','coordinator','monitor')),
                    site_id TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1))
                );
                CREATE TABLE IF NOT EXISTS trials(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
                    protocol_version TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft'
                        CHECK(status IN ('draft','running','stopped')),
                    arms_json TEXT NOT NULL, strata_factors_json TEXT NOT NULL,
                    block_size INTEGER NOT NULL CHECK(block_size >= 2),
                    seed TEXT NOT NULL, created_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL, started_at TEXT
                );
                CREATE TABLE IF NOT EXISTS strata(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    stratum_key TEXT NOT NULL, factors_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    UNIQUE(trial_id,stratum_key)
                );
                CREATE TABLE IF NOT EXISTS stratum_plans(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    stratum_id INTEGER NOT NULL REFERENCES strata(id),
                    planned_count INTEGER NOT NULL CHECK(planned_count >= 0),
                    updated_by TEXT NOT NULL REFERENCES users(id), updated_at TEXT NOT NULL,
                    UNIQUE(trial_id,stratum_id)
                );
                CREATE TABLE IF NOT EXISTS allocations(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    stratum_id INTEGER NOT NULL REFERENCES strata(id),
                    sequence INTEGER NOT NULL, block_no INTEGER NOT NULL,
                    arm TEXT NOT NULL, used_by INTEGER, used_at TEXT,
                    reservation_id INTEGER,
                    UNIQUE(stratum_id,sequence)
                );
                CREATE TABLE IF NOT EXISTS segment_reservations(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    stratum_id INTEGER NOT NULL REFERENCES strata(id),
                    site_id TEXT NOT NULL, requested_by TEXT NOT NULL REFERENCES users(id),
                    status TEXT NOT NULL DEFAULT 'active'
                        CHECK(status IN ('active','expired','fulfilled','released')),
                    size INTEGER NOT NULL CHECK(size > 0),
                    first_sequence INTEGER NOT NULL, last_sequence INTEGER NOT NULL,
                    expires_at TEXT NOT NULL, created_at TEXT NOT NULL,
                    client_token TEXT,
                    UNIQUE(trial_id,site_id,stratum_id,client_token)
                );
                CREATE TABLE IF NOT EXISTS reservation_queue(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    stratum_id INTEGER NOT NULL REFERENCES strata(id),
                    site_id TEXT NOT NULL, requested_by TEXT NOT NULL REFERENCES users(id),
                    size INTEGER NOT NULL CHECK(size > 0),
                    ttl_minutes INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued'
                        CHECK(status IN ('queued','fulfilled','cancelled')),
                    fulfilled_reservation_id INTEGER,
                    client_token TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS drug_shipments(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    reservation_id INTEGER NOT NULL UNIQUE REFERENCES segment_reservations(id),
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    stratum_id INTEGER NOT NULL REFERENCES strata(id),
                    site_id TEXT NOT NULL, created_by TEXT NOT NULL REFERENCES users(id),
                    kit_count INTEGER NOT NULL CHECK(kit_count > 0),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS drug_kits(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shipment_id INTEGER NOT NULL REFERENCES drug_shipments(id),
                    allocation_id INTEGER NOT NULL UNIQUE REFERENCES allocations(id),
                    kit_label TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL DEFAULT 'available'
                        CHECK(status IN ('available','dispensed','released')),
                    dispensed_at TEXT, dispensed_participant_id INTEGER,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS participants(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    site_id TEXT NOT NULL, external_id TEXT NOT NULL,
                    stratum_id INTEGER NOT NULL REFERENCES strata(id),
                    allocation_id INTEGER NOT NULL UNIQUE REFERENCES allocations(id),
                    reservation_id INTEGER REFERENCES segment_reservations(id),
                    allocation_code TEXT NOT NULL UNIQUE, status TEXT NOT NULL DEFAULT 'enrolled'
                        CHECK(status IN ('enrolled','withdrawn','completed')),
                    enrolled_by TEXT NOT NULL REFERENCES users(id), created_at TEXT NOT NULL,
                    UNIQUE(trial_id,external_id)
                );
                CREATE TABLE IF NOT EXISTS unblinding_requests(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    participant_id INTEGER NOT NULL REFERENCES participants(id),
                    requester_id TEXT NOT NULL REFERENCES users(id), reason TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','approved','rejected')),
                    first_approver TEXT REFERENCES users(id), second_approver TEXT REFERENCES users(id),
                    decided_at TEXT, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, trial_id INTEGER REFERENCES trials(id),
                    actor_id TEXT NOT NULL REFERENCES users(id), action TEXT NOT NULL,
                    detail TEXT NOT NULL, created_at TEXT NOT NULL
                );
                """
            )
            self._migrate(conn)
            conn.executescript(
                """
                CREATE INDEX IF NOT EXISTS idx_allocations_pool
                    ON allocations(stratum_id, reservation_id, used_by, sequence);
                CREATE INDEX IF NOT EXISTS idx_reservations_lookup
                    ON segment_reservations(trial_id, stratum_id, site_id, status);
                """
            )
            conn.execute(
                "INSERT OR IGNORE INTO users(id,name,role,site_id) VALUES('system','系统定时任务','coordinator','SYSTEM')"
            )

    @staticmethod
    def _migrate(conn):
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(allocations)")}
        if "reservation_id" not in cols:
            conn.execute("ALTER TABLE allocations ADD COLUMN reservation_id INTEGER")
        pcols = {r["name"] for r in conn.execute("PRAGMA table_info(participants)")}
        if "reservation_id" not in pcols:
            conn.execute("ALTER TABLE participants ADD COLUMN reservation_id INTEGER")

    def seed(self):
        self.init_schema()
        with self.connect() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO users(id,name,role,site_id) VALUES(?,?,?,?)",
                [
                    ("site1", "中心一协调员", "site", "S001"),
                    ("site2", "中心二协调员", "site", "S002"),
                    ("coord", "项目协调员", "coordinator", "CENTER"),
                    ("monitor1", "独立监查员甲", "monitor", "CENTER"),
                    ("monitor2", "独立监查员乙", "monitor", "CENTER"),
                ],
            )

    # ---------- helpers ----------
    def _user(self, conn, user_id, roles=None):
        if not user_id:
            raise BusinessError("缺少 X-User-Id", 401, "authentication_required")
        if user_id == "system":
            raise BusinessError("系统账号不可用于接口调用", 403, "forbidden")
        user = conn.execute("SELECT * FROM users WHERE id=? AND active=1", (user_id,)).fetchone()
        if not user:
            raise BusinessError("用户不存在或已停用", 401, "unknown_user")
        if roles and user["role"] not in roles:
            raise BusinessError("当前角色无权执行此操作", 403, "forbidden")
        return user

    def _trial(self, conn, trial_id):
        row = conn.execute("SELECT * FROM trials WHERE id=?", (trial_id,)).fetchone()
        if not row:
            raise BusinessError("试验不存在", 404, "not_found")
        return row

    def _audit(self, conn, trial_id, actor, action, detail):
        conn.execute(
            "INSERT INTO audit_log(trial_id,actor_id,action,detail,created_at) VALUES(?,?,?,?,?)",
            (trial_id, actor, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), now()),
        )

    def _stratum_by_factors(self, conn, trial, factors, for_update=False):
        """按分层因素定位全局层（与中心无关，中央池共享）。兼容旧的带 site 前缀键。"""
        expected = json.loads(trial["strata_factors_json"])
        if not isinstance(factors, dict) or set(factors) != set(expected):
            raise BusinessError(f"必须提供分层因素: {', '.join(expected)}", 422, "invalid_factors")
        normalized = {k: str(factors[k]).strip() for k in sorted(expected)}
        if any(not v for v in normalized.values()):
            raise BusinessError("分层因素值不能为空", 422, "invalid_factors")
        key = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        row = conn.execute(
            "SELECT * FROM strata WHERE trial_id=? AND stratum_key=?", (trial["id"], key)
        ).fetchone()
        if row:
            return row
        # 兼容旧库：曾经以 site 前缀入库的层
        legacy = conn.execute(
            "SELECT * FROM strata WHERE trial_id=? AND factors_json=?",
            (trial["id"], json.dumps(normalized, ensure_ascii=False, sort_keys=True)),
        ).fetchone()
        if legacy:
            return legacy
        cur = conn.execute(
            "INSERT INTO strata(trial_id,stratum_key,factors_json,created_at) VALUES(?,?,?,?)",
            (trial["id"], key, json.dumps(normalized, ensure_ascii=False, sort_keys=True), now()),
        )
        return conn.execute("SELECT * FROM strata WHERE id=?", (cur.lastrowid,)).fetchone()

    def _ensure_blocks(self, conn, trial, stratum, target_count):
        """确定性地补齐随机表，直到序号达到 target_count；末个区组按计划截断。"""
        arms = json.loads(trial["arms_json"])
        max_seq = conn.execute(
            "SELECT COALESCE(MAX(sequence),0) FROM allocations WHERE stratum_id=?", (stratum["id"],)
        ).fetchone()[0]
        block_no = conn.execute(
            "SELECT COALESCE(MAX(block_no),0) FROM allocations WHERE stratum_id=?", (stratum["id"],)
        ).fetchone()[0]
        while max_seq < target_count and block_no < 10000:
            block_no += 1
            rng = random.Random(f"{trial['seed']}:{stratum['stratum_key']}:{block_no}")
            plan = arms * (trial["block_size"] // len(arms))
            rng.shuffle(plan)
            for arm in plan:
                if max_seq >= target_count:
                    break
                max_seq += 1
                conn.execute(
                    "INSERT INTO allocations(trial_id,stratum_id,sequence,block_no,arm) VALUES(?,?,?,?,?)",
                    (trial["id"], stratum["id"], max_seq, block_no, arm),
                )

    def _free_allocations(self, conn, stratum_id):
        return conn.execute(
            """SELECT * FROM allocations WHERE stratum_id=? AND used_by IS NULL AND reservation_id IS NULL
               ORDER BY sequence""",
            (stratum_id,),
        ).fetchall()

    @staticmethod
    def _free_runs(free_rows):
        """把空闲号切成连续段，返回 [(rows,...)]。"""
        runs, current, last_seq = [], [], None
        for row in free_rows:
            if last_seq is not None and row["sequence"] != last_seq + 1:
                runs.append(current); current = []
            current.append(row); last_seq = row["sequence"]
        if current:
            runs.append(current)
        return runs

    def _pool_counts(self, conn, stratum_id):
        """中央池账：已发 / 有效占用 / 空闲（已生成）。"""
        issued = conn.execute(
            "SELECT COUNT(*) FROM allocations WHERE stratum_id=? AND used_by IS NOT NULL", (stratum_id,)
        ).fetchone()[0]
        held = conn.execute(
            """SELECT COUNT(*) FROM allocations a JOIN segment_reservations r ON a.reservation_id=r.id
               WHERE a.stratum_id=? AND r.status='active' AND a.used_by IS NULL""",
            (stratum_id,),
        ).fetchone()[0]
        free = conn.execute(
            """SELECT COUNT(*) FROM allocations
               WHERE stratum_id=? AND used_by IS NULL AND reservation_id IS NULL""",
            (stratum_id,),
        ).fetchone()[0]
        return {"issued": issued, "held": held, "free": free}

    def _active_conflicts(self, conn, stratum_id):
        return conn.execute(
            """SELECT id, site_id, size, first_sequence, last_sequence, expires_at
               FROM segment_reservations WHERE stratum_id=? AND status='active' ORDER BY first_sequence""",
            (stratum_id,),
        ).fetchall()

    # ---------- trial/protocol (existing) ----------
    def create_trial(self, user_id, name, protocol_version, arms, strata_factors, block_size, seed):
        name = name.strip()
        if len(name) < 3 or not protocol_version.strip() or len(seed.strip()) < 8:
            raise BusinessError("试验名称、方案版本和至少 8 位随机种子不能为空", 422, "invalid_trial")
        if not isinstance(arms, list) or len(arms) < 2:
            raise BusinessError("至少需要两个试验组", 422, "invalid_arms")
        arms = [str(a).strip() for a in arms]
        if any(not a or len(a) > MAX_ARM_LENGTH for a in arms) or len(set(arms)) != len(arms):
            raise BusinessError("试验组名称必须非空、唯一且不过长", 422, "invalid_arms")
        if not isinstance(strata_factors, list) or any(not str(x).strip() for x in strata_factors) or len({str(x).strip() for x in strata_factors}) != len(strata_factors):
            raise BusinessError("分层因素必须是非空且不重复的数组", 422, "invalid_strata")
        if isinstance(block_size, bool) or not isinstance(block_size, int) or block_size < len(arms) or block_size % len(arms) != 0:
            raise BusinessError("区组长度必须为分组数的正整数倍", 422, "invalid_block_size")
        with self.connect() as conn:
            self._user(conn, user_id, {"coordinator"})
            try:
                cur = conn.execute(
                    """INSERT INTO trials(name,protocol_version,arms_json,strata_factors_json,block_size,seed,created_by,created_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (name, protocol_version.strip(), json.dumps(arms), json.dumps([str(x).strip() for x in strata_factors]), block_size, seed.strip(), user_id, now()),
                )
            except sqlite3.IntegrityError:
                raise BusinessError("试验名称已存在", 409, "trial_exists")
            trial_id = cur.lastrowid
            self._audit(conn, trial_id, user_id, "trial.create", {"protocol_version": protocol_version, "arms": len(arms), "block_size": block_size})
            return {"id": trial_id, "name": name, "status": "draft", "arms": arms, "strata_factors": strata_factors, "block_size": block_size}

    def update_protocol(self, user_id, trial_id, protocol_version, arms=None, strata_factors=None, block_size=None, seed=None):
        with self.connect() as conn:
            self._user(conn, user_id, {"coordinator"})
            trial = self._trial(conn, trial_id)
            enrolled = conn.execute("SELECT COUNT(*) FROM participants WHERE trial_id=?", (trial_id,)).fetchone()[0]
            if enrolled or trial["status"] != "draft":
                raise BusinessError("入组开始后不能修改随机方案", 409, "protocol_locked")
            new_arms = arms if arms is not None else json.loads(trial["arms_json"])
            new_strata = strata_factors if strata_factors is not None else json.loads(trial["strata_factors_json"])
            new_block = block_size if block_size is not None else trial["block_size"]
            new_seed = str(seed) if seed is not None else trial["seed"]
            self._validate_protocol(new_arms, new_strata, new_block, new_seed)
            conn.execute(
                """UPDATE trials SET protocol_version=?,arms_json=?,strata_factors_json=?,block_size=?,seed=? WHERE id=?""",
                (protocol_version.strip(), json.dumps(new_arms), json.dumps(new_strata), new_block, new_seed, trial_id),
            )
            self._audit(conn, trial_id, user_id, "protocol.update", {"protocol_version": protocol_version})
            return {"id": trial_id, "protocol_version": protocol_version, "arms": new_arms, "block_size": new_block}

    @staticmethod
    def _validate_protocol(arms, strata_factors, block_size, seed):
        if not isinstance(arms, list) or len(arms) < 2 or len(set(arms)) != len(arms):
            raise BusinessError("试验组配置无效", 422, "invalid_arms")
        if not isinstance(strata_factors, list) or not strata_factors or len(set(strata_factors)) != len(strata_factors):
            raise BusinessError("分层因素配置无效", 422, "invalid_strata")
        if isinstance(block_size, bool) or not isinstance(block_size, int) or block_size < len(arms) or block_size % len(arms):
            raise BusinessError("区组长度无效", 422, "invalid_block_size")
        if len(str(seed)) < 8:
            raise BusinessError("随机种子至少 8 位", 422, "invalid_seed")

    def start_trial(self, user_id, trial_id):
        with self.connect() as conn:
            self._user(conn, user_id, {"coordinator"})
            trial = self._trial(conn, trial_id)
            if trial["status"] != "draft":
                raise BusinessError("只有草稿试验可以开始", 409, "invalid_status")
            conn.execute("UPDATE trials SET status='running',started_at=? WHERE id=?", (now(), trial_id))
            self._audit(conn, trial_id, user_id, "trial.start", {})
            return {"id": trial_id, "status": "running"}

    # ---------- 分层计划 ----------
    def set_plan(self, user_id, trial_id, strata_plans):
        """协调员按分层写入计划人数（草稿/运行中均可，但不得低于已占用量）。"""
        if not isinstance(strata_plans, list) or not strata_plans:
            raise BusinessError("strata_plans 必须是非空数组", 422, "invalid_plan")
        normalized = []
        seen = set()
        for item in strata_plans:
            if not isinstance(item, dict) or "factors" not in item or "planned_count" not in item:
                raise BusinessError("每层需提供 factors 与 planned_count", 422, "invalid_plan")
            count = item["planned_count"]
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise BusinessError("计划人数必须是非负整数", 422, "invalid_plan")
            fkey = json.dumps(item["factors"], sort_keys=True)
            if fkey in seen:
                raise BusinessError("分层因素组合重复", 422, "invalid_plan")
            seen.add(fkey)
            normalized.append((item["factors"], count))
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._user(conn, user_id, {"coordinator"})
                trial = self._trial(conn, trial_id)
                results = []
                for factors, count in normalized:
                    stratum = self._stratum_by_factors(conn, trial, factors)
                    existing = conn.execute(
                        "SELECT planned_count FROM stratum_plans WHERE trial_id=? AND stratum_id=?",
                        (trial_id, stratum["id"]),
                    ).fetchone()
                    if existing:
                        committed = conn.execute(
                            """SELECT COUNT(*) FROM allocations WHERE stratum_id=? AND
                               (used_by IS NOT NULL OR reservation_id IS NOT NULL)""",
                            (stratum["id"],),
                        ).fetchone()[0]
                        if count < committed:
                            raise BusinessError(
                                f"计划人数 {count} 不能低于已发号与有效预占合计 {committed}",
                                422, "plan_below_committed",
                            )
                        conn.execute(
                            "UPDATE stratum_plans SET planned_count=?,updated_by=?,updated_at=? WHERE trial_id=? AND stratum_id=?",
                            (count, user_id, now(), trial_id, stratum["id"]),
                        )
                        # 下调计划：裁掉超出新计划且尚未占用的号
                        conn.execute(
                            """DELETE FROM allocations WHERE stratum_id=? AND sequence>?
                               AND used_by IS NULL AND reservation_id IS NULL""",
                            (stratum["id"], count),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO stratum_plans(trial_id,stratum_id,planned_count,updated_by,updated_at) VALUES(?,?,?,?,?)",
                            (trial_id, stratum["id"], count, user_id, now()),
                        )
                    if count:
                        self._ensure_blocks(conn, trial, stratum, count)
                    results.append({"stratum_id": stratum["id"], "factors": json.loads(stratum["factors_json"]), "planned_count": count})
                self._audit(conn, trial_id, user_id, "plan.set", {"strata": [{"factors": f, "planned_count": c} for f, c in normalized]})
                return {"trial_id": trial_id, "strata_plans": results}
            except Exception:
                conn.rollback()
                raise

    # ---------- 号段预占 / 排队 / 过期 ----------
    def _create_shipment(self, conn, reservation, allocations, actor):
        cur = conn.execute(
            """INSERT INTO drug_shipments(reservation_id,trial_id,stratum_id,site_id,created_by,kit_count,created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (reservation["id"], reservation["trial_id"], reservation["stratum_id"],
             reservation["site_id"], actor, reservation["size"], now()),
        )
        shipment_id = cur.lastrowid
        kits = []
        for alloc in allocations:
            label = f"KIT-{reservation['trial_id']}-{reservation['stratum_id']}-R{reservation['id']}-{alloc['sequence']:05d}"
            # 号退回再备货：复用该号上一次 released 的药盒行，重新激活
            old = conn.execute(
                "SELECT * FROM drug_kits WHERE allocation_id=?", (alloc["id"],)
            ).fetchone()
            if old:
                conn.execute(
                    """UPDATE drug_kits SET shipment_id=?,kit_label=?,status='available',
                           dispensed_at=NULL,dispensed_participant_id=NULL,created_at=? WHERE id=?""",
                    (shipment_id, label, now(), old["id"]),
                )
            else:
                conn.execute(
                    """INSERT INTO drug_kits(shipment_id,allocation_id,kit_label,created_at)
                       VALUES(?,?,?,?)""",
                    (shipment_id, alloc["id"], label, now()),
                )
            kits.append({"kit_label": label, "allocation_sequence": alloc["sequence"]})
        self._audit(conn, reservation["trial_id"], actor, "drug.ship",
                    {"shipment_id": shipment_id, "reservation_id": reservation["id"],
                     "site_id": reservation["site_id"], "kit_count": reservation["size"]})
        return shipment_id, kits

    def _fulfill(self, conn, trial, stratum, site_id, actor, size, ttl_minutes,
                 client_token=None, queue_id=None):
        """从中央池取连续 size 个空闲号落预占并同步备货；不够连续一段则返回 None。"""
        run = None
        for candidate in self._free_runs(self._free_allocations(conn, stratum["id"])):
            if len(candidate) >= size:
                run = candidate[:size]
                break
        if run is None:
            return None
        rows = run
        first_seq, last_seq = rows[0]["sequence"], rows[-1]["sequence"]
        expires_at = (datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)).isoformat(timespec="seconds")
        cur = conn.execute(
            """INSERT INTO segment_reservations(trial_id,stratum_id,site_id,requested_by,status,size,
                  first_sequence,last_sequence,expires_at,created_at,client_token)
               VALUES(?,?,?,?,'active',?,?,?,?,?,?)""",
            (trial["id"], stratum["id"], site_id, actor, size, first_seq, last_seq, expires_at, now(), client_token),
        )
        reservation_id = cur.lastrowid
        for r in rows:
            conn.execute("UPDATE allocations SET reservation_id=? WHERE id=?", (reservation_id, r["id"]))
        reservation = conn.execute("SELECT * FROM segment_reservations WHERE id=?", (reservation_id,)).fetchone()
        shipment_id, kits = self._create_shipment(conn, reservation, rows, actor)
        if queue_id is not None:
            conn.execute(
                "UPDATE reservation_queue SET status='fulfilled',fulfilled_reservation_id=? WHERE id=?",
                (reservation_id, queue_id),
            )
        self._audit(conn, trial["id"], actor, "segment.reserve",
                    {"reservation_id": reservation_id, "queue_id": queue_id, "site_id": site_id,
                     "stratum_id": stratum["id"], "size": size,
                     "range": [first_seq, last_seq], "expires_at": expires_at})
        return self._reservation_view(conn, reservation, shipment_id=shipment_id, kits=kits)

    @staticmethod
    def _reservation_view(conn, reservation, shipment_id=None, kits=None):
        view = {
            "id": reservation["id"], "trial_id": reservation["trial_id"],
            "stratum_id": reservation["stratum_id"], "site_id": reservation["site_id"],
            "status": reservation["status"], "size": reservation["size"],
            "range": [reservation["first_sequence"], reservation["last_sequence"]],
            "expires_at": reservation["expires_at"], "created_at": reservation["created_at"],
        }
        if shipment_id is not None:
            view["shipment_id"] = shipment_id
            view["kits"] = kits
        return view

    def _drain_queue(self, conn, trial):
        """号回收后按 FIFO 尝试满足排队申请；不够连续数量的留在队首。"""
        fulfilled = []
        queued = conn.execute(
            "SELECT * FROM reservation_queue WHERE status='queued' ORDER BY id"
        ).fetchall()
        for item in queued:
            stratum = conn.execute("SELECT * FROM strata WHERE id=?", (item["stratum_id"],)).fetchone()
            free_rows = self._free_allocations(conn, item["stratum_id"])
            longest = len(self._free_runs(free_rows)[0]) if free_rows else 0
            if longest < item["size"]:
                break  # FIFO：队首都放不下，后面的更不动
            view = self._fulfill(conn, trial, stratum, item["site_id"], item["requested_by"],
                                 item["size"], item["ttl_minutes"],
                                 client_token=item["client_token"], queue_id=item["id"])
            if view is None:
                break
            fulfilled.append(view)
        return fulfilled

    def _expire_due(self, conn, trial):
        """把到期未发完的号段退回中央池，并触发排队重算。返回过期结果列表。"""
        moment = datetime.now(timezone.utc)
        due = conn.execute(
            "SELECT * FROM segment_reservations WHERE status='active' AND expires_at <= ? ORDER BY id",
            (moment.isoformat(timespec="seconds"),),
        ).fetchall()
        results = []
        for r in due:
            used = conn.execute(
                "SELECT COUNT(*) FROM allocations WHERE reservation_id=? AND used_by IS NOT NULL", (r["id"],)
            ).fetchone()[0]
            # 未发完的号：解绑退回中央池；对应未发药盒置 released
            conn.execute(
                """UPDATE drug_kits SET status='released' WHERE id IN (
                       SELECT k.id FROM drug_kits k JOIN allocations a ON k.allocation_id=a.id
                       WHERE a.reservation_id=? AND a.used_by IS NULL)""",
                (r["id"],),
            )
            conn.execute(
                "UPDATE allocations SET reservation_id=NULL WHERE reservation_id=? AND used_by IS NULL",
                (r["id"],),
            )
            new_status = "fulfilled" if used == r["size"] else "expired"
            conn.execute("UPDATE segment_reservations SET status=? WHERE id=?", (new_status, r["id"]))
            self._audit(conn, r["trial_id"], "system", "segment.expire",
                        {"reservation_id": r["id"], "site_id": r["site_id"],
                         "issued": used, "returned": r["size"] - used, "status": new_status})
            results.append({"reservation_id": r["id"], "site_id": r["site_id"],
                            "issued": used, "returned": r["size"] - used, "status": new_status})
        fulfilled_from_queue = self._drain_queue(conn, trial) if due else []
        return results, fulfilled_from_queue

    def reserve_segment(self, user_id, trial_id, factors, size, ttl_minutes=None,
                        site_id=None, client_token=None):
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise BusinessError("申请号数必须是正整数", 422, "invalid_size")
        ttl = ttl_minutes if ttl_minutes is not None else DEFAULT_TTL_MINUTES
        if isinstance(ttl, bool) or not isinstance(ttl, int) or not (1 <= ttl <= MAX_TTL_MINUTES):
            raise BusinessError(f"失效时刻（分钟）须在 1~{MAX_TTL_MINUTES} 之间", 422, "invalid_ttl")
        token = client_token.strip() if isinstance(client_token, str) and client_token.strip() else None
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")  # 先落库者拿到写锁
                actor = self._user(conn, user_id, {"site", "coordinator"})
                # 越权：中心用户只能给自己中心申请
                if actor["role"] == "site":
                    target_site = actor["site_id"]
                    if site_id and site_id != target_site:
                        raise BusinessError("越权申请：不能为其他中心预占号段", 403, "cross_site_forbidden")
                else:
                    if not site_id or not str(site_id).strip():
                        raise BusinessError("协调员代申请必须指定 site_id", 422, "site_required")
                    target_site = str(site_id).strip()
                trial = self._trial(conn, trial_id)
                if trial["status"] != "running":
                    raise BusinessError("试验尚未开始或已经停止", 409, "trial_not_running")
                self._expire_due(conn, trial)
                stratum = self._stratum_by_factors(conn, trial, factors)
                plan = conn.execute(
                    "SELECT * FROM stratum_plans WHERE trial_id=? AND stratum_id=?",
                    (trial_id, stratum["id"]),
                ).fetchone()
                if not plan:
                    raise BusinessError("该分层尚未下达计划人数，无法预占号段", 409, "plan_missing")
                self._ensure_blocks(conn, trial, stratum, plan["planned_count"])

                # 幂等：同一 token 直接回放，写盘失败重试不会留下半条预占
                if token:
                    dup = conn.execute(
                        "SELECT * FROM segment_reservations WHERE trial_id=? AND site_id=? AND stratum_id=? AND client_token=?",
                        (trial_id, target_site, stratum["id"], token),
                    ).fetchone()
                    if dup:
                        conn.commit()
                        if dup["status"] == "active":
                            return {"result": "reserved", "reservation": self._reservation_view(conn, dup)}
                        # 已过期/发完：幂等回放当时的终态
                        return {"result": dup["status"], "reservation": self._reservation_view(conn, dup)}
                    qdup = conn.execute(
                        """SELECT * FROM reservation_queue WHERE trial_id=? AND site_id=? AND stratum_id=?
                           AND client_token=? ORDER BY id DESC LIMIT 1""",
                        (trial_id, target_site, stratum["id"], token),
                    ).fetchone()
                    if qdup:
                        conn.commit()
                        if qdup["status"] == "queued":
                            position = conn.execute(
                                "SELECT COUNT(*) FROM reservation_queue WHERE stratum_id=? AND status='queued' AND id<=?",
                                (stratum["id"], qdup["id"]),
                            ).fetchone()[0]
                            raise BusinessError(
                                f"分层计划人数已写满，申请已排队（前方 {position - 1} 个）",
                                409, "plan_full_queued",
                                {"queued": True, "queue_id": qdup["id"], "position": position,
                                 "remaining": 0, "conflicts": [
                                     {"reservation_id": c["id"], "site_id": c["site_id"], "size": c["size"],
                                      "range": [c["first_sequence"], c["last_sequence"]], "expires_at": c["expires_at"]}
                                     for c in self._active_conflicts(conn, stratum["id"])],
                                 "pool": {"planned": plan["planned_count"], **self._pool_counts(conn, stratum["id"])}},
                            )
                        dup = conn.execute("SELECT * FROM segment_reservations WHERE id=?",
                                           (qdup["fulfilled_reservation_id"],)).fetchone()
                        return {"result": "reserved", "reservation": self._reservation_view(conn, dup)}

                counts = self._pool_counts(conn, stratum["id"])
                free_rows = self._free_allocations(conn, stratum["id"])
                runs = self._free_runs(free_rows)
                longest = len(runs[0]) if runs else 0
                conflicts = [
                    {"reservation_id": c["id"], "site_id": c["site_id"], "size": c["size"],
                     "range": [c["first_sequence"], c["last_sequence"]], "expires_at": c["expires_at"]}
                    for c in self._active_conflicts(conn, stratum["id"])
                ]
                conflict_pool = {"planned": plan["planned_count"], **counts,
                                 "longest_contiguous": longest}
                if longest >= size:
                    view = self._fulfill(conn, trial, stratum, target_site, user_id, size, ttl, client_token=token)
                    conn.commit()
                    return {"result": "reserved", "reservation": view,
                            "pool": {"planned": plan["planned_count"], **counts}}

                # 剩余不足：计划写满（没有任何空闲）则排队，否则报冲突让调用方看剩余连续号重试
                if counts["free"] == 0 and counts["issued"] + counts["held"] >= plan["planned_count"]:
                    cur = conn.execute(
                        """INSERT INTO reservation_queue(trial_id,stratum_id,site_id,requested_by,size,ttl_minutes,client_token,created_at)
                           VALUES(?,?,?,?,?,?,?,?)""",
                        (trial_id, stratum["id"], target_site, user_id, size, ttl, token, now()),
                    )
                    self._audit(conn, trial_id, user_id, "segment.queue",
                                {"queue_id": cur.lastrowid, "site_id": target_site,
                                 "stratum_id": stratum["id"], "size": size})
                    position = conn.execute(
                        "SELECT COUNT(*) FROM reservation_queue WHERE stratum_id=? AND status='queued' AND id<=?",
                        (stratum["id"], cur.lastrowid),
                    ).fetchone()[0]
                    conn.commit()  # 队列记录先落库，再以 409 告知排队结果
                    raise BusinessError(
                        f"分层计划人数已写满，申请已排队（前方 {position - 1} 个）",
                        409, "plan_full_queued",
                        {"queued": True, "queue_id": cur.lastrowid, "position": position,
                         "remaining": 0, "conflicts": conflicts, "pool": conflict_pool},
                    )
                raise BusinessError(
                    f"中央池剩余 {counts['free']} 个号（最长连续 {longest} 个），不足申请的 {size} 个",
                    409, "segment_conflict",
                    {"remaining": longest, "remaining_total": counts["free"], "requested": size,
                     "conflicts": conflicts, "pool": conflict_pool},
                )
            except BusinessError:
                conn.rollback()
                raise
            except Exception:
                conn.rollback()
                raise

    def list_reservations(self, user_id, trial_id, site_id=None):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            self._trial(conn, trial_id)
            sql = "SELECT * FROM segment_reservations WHERE trial_id=?"
            params = [trial_id]
            if actor["role"] == "site":
                sql += " AND site_id=?"; params.append(actor["site_id"])
            elif site_id:
                sql += " AND site_id=?"; params.append(site_id)
            sql += " ORDER BY id"
            rows = conn.execute(sql, params).fetchall()
            queue_sql = "SELECT * FROM reservation_queue WHERE trial_id=?"
            qparams = [trial_id]
            if actor["role"] == "site":
                queue_sql += " AND site_id=?"; qparams.append(actor["site_id"])
            elif site_id:
                queue_sql += " AND site_id=?"; qparams.append(site_id)
            queue = [dict(q) for q in conn.execute(queue_sql + " ORDER BY id", qparams).fetchall()]
            return {"items": [self._reservation_view(conn, r) for r in rows], "queue": queue}

    def sweep_expired(self, user_id, trial_id):
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
                trial = self._trial(conn, trial_id)
                expired, fulfilled = self._expire_due(conn, trial)
                conn.commit()
                return {"expired": expired, "fulfilled_from_queue": fulfilled}
            except Exception:
                conn.rollback()
                raise

    def release_segment(self, user_id, reservation_id):
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                actor = self._user(conn, user_id, {"site", "coordinator"})
                r = conn.execute("SELECT * FROM segment_reservations WHERE id=?", (reservation_id,)).fetchone()
                if not r:
                    raise BusinessError("号段不存在", 404, "not_found")
                if actor["role"] == "site" and r["site_id"] != actor["site_id"]:
                    raise BusinessError("不能释放其他中心的号段", 403, "cross_site_forbidden")
                if r["status"] != "active":
                    raise BusinessError("号段已不是有效状态", 409, "invalid_status")
                trial = self._trial(conn, r["trial_id"])
                used = conn.execute(
                    "SELECT COUNT(*) FROM allocations WHERE reservation_id=? AND used_by IS NOT NULL", (r["id"],)
                ).fetchone()[0]
                conn.execute(
                    """UPDATE drug_kits SET status='released' WHERE id IN (
                           SELECT k.id FROM drug_kits k JOIN allocations a ON k.allocation_id=a.id
                           WHERE a.reservation_id=? AND a.used_by IS NULL)""",
                    (r["id"],),
                )
                conn.execute(
                    "UPDATE allocations SET reservation_id=NULL WHERE reservation_id=? AND used_by IS NULL", (r["id"],)
                )
                conn.execute(
                    "UPDATE segment_reservations SET status=? WHERE id=?",
                    ("fulfilled" if used == r["size"] else "released", r["id"]),
                )
                self._audit(conn, r["trial_id"], user_id, "segment.release",
                            {"reservation_id": r["id"], "returned": r["size"] - used})
                fulfilled = self._drain_queue(conn, trial)
                conn.commit()
                return {"reservation_id": r["id"], "returned": r["size"] - used,
                        "fulfilled_from_queue": fulfilled}
            except Exception:
                conn.rollback()
                raise

    # ---------- 入组发号 ----------
    def _next_legacy_allocation(self, conn, trial, stratum):
        """无计划层（旧模式）：按区组从中央序列直接取号。"""
        arms = json.loads(trial["arms_json"])
        block_no = conn.execute(
            "SELECT COALESCE(MAX(block_no),0) FROM allocations WHERE stratum_id=?", (stratum["id"],)
        ).fetchone()[0]
        for _ in range(100):
            count = conn.execute(
                "SELECT COUNT(*) FROM allocations WHERE stratum_id=? AND block_no=?", (stratum["id"], block_no + 1)
            ).fetchone()[0]
            if count == 0:
                block_no += 1
                rng = random.Random(f"{trial['seed']}:{stratum['stratum_key']}:{block_no}")
                plan = arms * (trial["block_size"] // len(arms))
                rng.shuffle(plan)
                start = conn.execute(
                    "SELECT COALESCE(MAX(sequence),0) FROM allocations WHERE stratum_id=?", (stratum["id"],)
                ).fetchone()[0]
                for offset, arm in enumerate(plan, 1):
                    conn.execute(
                        "INSERT INTO allocations(trial_id,stratum_id,sequence,block_no,arm) VALUES(?,?,?,?,?)",
                        (trial["id"], stratum["id"], start + offset, block_no, arm),
                    )
            free = conn.execute(
                "SELECT * FROM allocations WHERE stratum_id=? AND used_by IS NULL AND reservation_id IS NULL ORDER BY sequence LIMIT 1",
                (stratum["id"],),
            ).fetchone()
            if free:
                return free, None
        raise BusinessError("随机分配表已耗尽，请由统计人员扩展方案", 409, "allocation_exhausted")

    def enroll(self, user_id, trial_id, external_id, factors):
        external_id = str(external_id).strip()
        if not external_id:
            raise BusinessError("外部受试者编号不能为空", 422, "invalid_external_id")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                actor = self._user(conn, user_id, {"site"})
                trial = self._trial(conn, trial_id)
                if trial["status"] != "running":
                    raise BusinessError("试验尚未开始或已经停止", 409, "trial_not_running")
                existing = conn.execute(
                    "SELECT * FROM participants WHERE trial_id=? AND external_id=?", (trial_id, external_id)
                ).fetchone()
                if existing:
                    if existing["site_id"] != actor["site_id"]:
                        raise BusinessError("不能在当前中心查看其他中心的受试者", 403, "site_isolation")
                    conn.commit()
                    return self._blinded_participant(conn, existing, actor, allow_arm=False, idempotent=True)

                self._expire_due(conn, trial)
                stratum = self._stratum_by_factors(conn, trial, factors)
                plan = conn.execute(
                    "SELECT * FROM stratum_plans WHERE trial_id=? AND stratum_id=?",
                    (trial_id, stratum["id"]),
                ).fetchone()

                reservation = None
                if plan is not None:
                    # 从本中心当前有效号段里按序取号；自动挑一个还有空号的
                    reservation = conn.execute(
                        """SELECT * FROM segment_reservations
                           WHERE trial_id=? AND stratum_id=? AND site_id=? AND status='active'
                             AND EXISTS (SELECT 1 FROM allocations a
                                         WHERE a.reservation_id=segment_reservations.id AND a.used_by IS NULL)
                           ORDER BY first_sequence LIMIT 1""",
                        (trial_id, stratum["id"], actor["site_id"]),
                    ).fetchone()
                    if reservation is None:
                        raise BusinessError(
                            "本中心该分层没有有效号段，请先申请号段预占",
                            409, "reservation_required",
                            {"pool": self._pool_counts(conn, stratum["id"])},
                        )
                    allocation = conn.execute(
                        """SELECT * FROM allocations WHERE reservation_id=? AND used_by IS NULL
                           ORDER BY sequence LIMIT 1""",
                        (reservation["id"],),
                    ).fetchone()
                else:
                    allocation, reservation = self._next_legacy_allocation(conn, trial, stratum)

                allocation_code = hashlib.sha256(f"{trial_id}:{external_id}".encode()).hexdigest()[:12].upper()
                cur = conn.execute(
                    """INSERT INTO participants(trial_id,site_id,external_id,stratum_id,allocation_id,
                          reservation_id,allocation_code,enrolled_by,created_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (trial_id, actor["site_id"], external_id, stratum["id"], allocation["id"],
                     reservation["id"] if reservation else None, allocation_code, user_id, now()),
                )
                participant_id = cur.lastrowid
                conn.execute("UPDATE allocations SET used_by=?,used_at=? WHERE id=?",
                             (participant_id, now(), allocation["id"]))
                kit_info = None
                if reservation is not None:
                    kit = conn.execute(
                        "SELECT * FROM drug_kits WHERE allocation_id=? AND status='available'",
                        (allocation["id"],),
                    ).fetchone()
                    if kit:
                        conn.execute(
                            "UPDATE drug_kits SET status='dispensed',dispensed_at=?,dispensed_participant_id=? WHERE id=?",
                            (now(), participant_id, kit["id"]),
                        )
                        kit_info = {"kit_label": kit["kit_label"], "shipment_id": kit["shipment_id"]}
                self._audit(conn, trial_id, user_id, "participant.enroll",
                            {"participant_id": participant_id, "external_id": external_id,
                             "allocation_id": allocation["id"], "sequence": allocation["sequence"],
                             "reservation_id": reservation["id"] if reservation else None,
                             "site_id": actor["site_id"], "kit": kit_info})
                participant = conn.execute("SELECT * FROM participants WHERE id=?", (participant_id,)).fetchone()
                conn.commit()
                result = self._blinded_participant(conn, participant, actor, allow_arm=False, idempotent=False)
                result["sequence"] = allocation["sequence"]
                if kit_info:
                    result["kit_label"] = kit_info["kit_label"]
                return result
            except sqlite3.IntegrityError as exc:
                conn.rollback()
                if "participants.trial_id, participants.external_id" in str(exc):
                    with self.connect() as retry:
                        row = retry.execute("SELECT * FROM participants WHERE trial_id=? AND external_id=?", (trial_id, external_id)).fetchone()
                        if row and row["site_id"] == actor["site_id"]:
                            return self._blinded_participant(retry, row, actor, False, True)
                raise BusinessError("并发入组冲突，请重新提交", 409, "enrollment_conflict")
            except BusinessError:
                conn.rollback()
                raise
            except Exception:
                conn.rollback()
                raise

    def _blinded_participant(self, conn, participant, viewer, allow_arm=False, idempotent=False):
        result = {
            "id": participant["id"], "trial_id": participant["trial_id"],
            "external_id": participant["external_id"], "site_id": participant["site_id"],
            "allocation_code": participant["allocation_code"], "status": participant["status"],
            "created_at": participant["created_at"], "idempotent": idempotent,
        }
        if allow_arm:
            result["arm"] = conn.execute("SELECT arm FROM allocations WHERE id=?", (participant["allocation_id"],)).fetchone()["arm"]
        return result

    def list_participants(self, user_id, trial_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            self._trial(conn, trial_id)
            if actor["role"] == "site":
                rows = conn.execute("SELECT * FROM participants WHERE trial_id=? AND site_id=? ORDER BY id", (trial_id, actor["site_id"])).fetchall()
            else:
                rows = conn.execute("SELECT * FROM participants WHERE trial_id=? ORDER BY id", (trial_id,)).fetchall()
            return [self._blinded_participant(conn, row, actor) for row in rows]

    def get_participant(self, user_id, participant_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            row = conn.execute("SELECT * FROM participants WHERE id=?", (participant_id,)).fetchone()
            if not row:
                raise BusinessError("受试者不存在", 404, "not_found")
            if actor["role"] == "site" and row["site_id"] != actor["site_id"]:
                raise BusinessError("只能查看本中心受试者", 403, "site_isolation")
            approved = conn.execute(
                "SELECT 1 FROM unblinding_requests WHERE participant_id=? AND status='approved'", (participant_id,)
            ).fetchone() is not None
            return self._blinded_participant(conn, row, actor, allow_arm=approved)

    # ---------- 揭盲（既有） ----------
    def request_unblinding(self, user_id, participant_id, reason):
        if len(reason.strip()) < 8:
            raise BusinessError("揭盲原因至少 8 字", 422, "reason_required")
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator"})
            participant = conn.execute("SELECT * FROM participants WHERE id=?", (participant_id,)).fetchone()
            if not participant:
                raise BusinessError("受试者不存在", 404, "not_found")
            if actor["role"] == "site" and participant["site_id"] != actor["site_id"]:
                raise BusinessError("不能申请其他中心的揭盲", 403, "site_isolation")
            open_request = conn.execute(
                "SELECT id FROM unblinding_requests WHERE participant_id=? AND status='pending'", (participant_id,)
            ).fetchone()
            if open_request:
                raise BusinessError("该受试者已有待审批的揭盲申请", 409, "request_exists")
            cur = conn.execute(
                "INSERT INTO unblinding_requests(participant_id,requester_id,reason,created_at) VALUES(?,?,?,?)",
                (participant_id, user_id, reason.strip(), now()),
            )
            self._audit(conn, participant["trial_id"], user_id, "unblinding.request", {"request_id": cur.lastrowid, "participant_id": participant_id})
            return {"id": cur.lastrowid, "status": "pending"}

    def approve_unblinding(self, user_id, request_id):
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._user(conn, user_id, {"monitor", "coordinator"})
                request = conn.execute("SELECT * FROM unblinding_requests WHERE id=?", (request_id,)).fetchone()
                if not request:
                    raise BusinessError("揭盲申请不存在", 404, "not_found")
                if request["status"] != "pending":
                    raise BusinessError("揭盲申请已经完成", 409, "already_decided")
                if request["first_approver"] is None:
                    conn.execute("UPDATE unblinding_requests SET first_approver=? WHERE id=?", (user_id, request_id))
                    participant = conn.execute("SELECT * FROM participants WHERE id=?", (request["participant_id"],)).fetchone()
                    self._audit(conn, participant["trial_id"], user_id, "unblinding.approve.first", {"request_id": request_id})
                    return {"id": request_id, "status": "pending", "first_approver": user_id, "second_approval_required": True}
                if request["first_approver"] == user_id:
                    raise BusinessError("两次揭盲审批必须由不同人员完成", 409, "distinct_approver_required")
                conn.execute(
                    "UPDATE unblinding_requests SET second_approver=?,status='approved',decided_at=? WHERE id=?",
                    (user_id, now(), request_id),
                )
                participant = conn.execute("SELECT * FROM participants WHERE id=?", (request["participant_id"],)).fetchone()
                arm = conn.execute("SELECT arm FROM allocations WHERE id=?", (participant["allocation_id"],)).fetchone()["arm"]
                self._audit(conn, participant["trial_id"], user_id, "unblinding.approve.second", {"request_id": request_id, "participant_id": participant["id"]})
                return {"id": request_id, "status": "approved", "first_approver": request["first_approver"], "second_approver": user_id, "arm": arm}
            except Exception:
                conn.rollback()
                raise

    # ---------- 号源账 / 审计对账 ----------
    def ledger(self, user_id, trial_id):
        """审计员拿预占、发号、药品三本记录对账。"""
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            trial = self._trial(conn, trial_id)
            site_scope = actor["site_id"] if actor["role"] == "site" else None
            strata_rows = conn.execute("SELECT * FROM strata WHERE trial_id=? ORDER BY id", (trial_id,)).fetchall()
            strata_ledger = []
            balanced = True
            for s in strata_rows:
                plan = conn.execute(
                    "SELECT * FROM stratum_plans WHERE trial_id=? AND stratum_id=?", (trial_id, s["id"])
                ).fetchone()
                generated = conn.execute(
                    "SELECT COUNT(*) FROM allocations WHERE stratum_id=?", (s["id"],)
                ).fetchone()[0]
                counts = self._pool_counts(conn, s["id"])
                checks = []
                if plan is not None:
                    # 账平：已发 + 有效占用 + 空闲 + 尚未生成 = 计划
                    not_generated = plan["planned_count"] - generated
                    row_balanced = counts["issued"] + counts["held"] + counts["free"] + not_generated == plan["planned_count"]
                    checks.append({"name": "pool_matches_plan", "ok": row_balanced,
                                   "detail": {"issued": counts["issued"], "held": counts["held"],
                                              "free": counts["free"], "not_generated": not_generated,
                                              "planned": plan["planned_count"]}})
                    balanced = balanced and row_balanced
                else:
                    not_generated = None
                strata_ledger.append({
                    "stratum_id": s["id"], "factors": json.loads(s["factors_json"]),
                    "planned_count": plan["planned_count"] if plan else None,
                    "generated": generated, "not_generated": not_generated, **counts,
                })

            res_sql = "SELECT * FROM segment_reservations WHERE trial_id=?"
            res_params = [trial_id]
            if site_scope:
                res_sql += " AND site_id=?"; res_params.append(site_scope)
            reservations = []
            for r in conn.execute(res_sql + " ORDER BY id", res_params).fetchall():
                issued = conn.execute(
                    "SELECT COUNT(*) FROM allocations WHERE reservation_id=? AND used_by IS NOT NULL", (r["id"],)
                ).fetchone()[0]
                shipment = conn.execute("SELECT * FROM drug_shipments WHERE reservation_id=?", (r["id"],)).fetchone()
                kit_stats = None
                if shipment:
                    kit_stats = {
                        "shipment_id": shipment["id"], "kit_count": shipment["kit_count"],
                        "dispensed": conn.execute("SELECT COUNT(*) FROM drug_kits WHERE shipment_id=? AND status='dispensed'", (shipment["id"],)).fetchone()[0],
                        "available": conn.execute("SELECT COUNT(*) FROM drug_kits WHERE shipment_id=? AND status='available'", (shipment["id"],)).fetchone()[0],
                        "released": conn.execute("SELECT COUNT(*) FROM drug_kits WHERE shipment_id=? AND status='released'", (shipment["id"],)).fetchone()[0],
                    }
                    # 药品账平：备货数 == 号段大小；已发药 == 已发号
                    ok = (shipment["kit_count"] == r["size"]
                          and kit_stats["dispensed"] == issued
                          and kit_stats["dispensed"] + kit_stats["available"] + kit_stats["released"] == shipment["kit_count"])
                    checks.append({"name": f"shipment_{shipment['id']}_matches_segment", "ok": ok,
                                   "detail": {"segment_size": r["size"], "issued_numbers": issued, **kit_stats}})
                    balanced = balanced and ok
                reservations.append({
                    "id": r["id"], "stratum_id": r["stratum_id"], "site_id": r["site_id"],
                    "status": r["status"], "size": r["size"], "range": [r["first_sequence"], r["last_sequence"]],
                    "expires_at": r["expires_at"], "issued": issued, "shipment": kit_stats,
                })

            q_sql = "SELECT * FROM reservation_queue WHERE trial_id=?"
            q_params = [trial_id]
            if site_scope:
                q_sql += " AND site_id=?"; q_params.append(site_scope)
            queue = [dict(q) for q in conn.execute(q_sql + " ORDER BY id", q_params).fetchall()]

            ship_sql = """SELECT sh.*, s.factors_json FROM drug_shipments sh
                          JOIN strata s ON s.id=sh.stratum_id WHERE sh.trial_id=?"""
            ship_params = [trial_id]
            if site_scope:
                ship_sql += " AND sh.site_id=?"; ship_params.append(site_scope)
            shipments = []
            for sh in conn.execute(ship_sql + " ORDER BY sh.id", ship_params).fetchall():
                shipments.append({
                    "id": sh["id"], "reservation_id": sh["reservation_id"], "site_id": sh["site_id"],
                    "factors": json.loads(sh["factors_json"]), "kit_count": sh["kit_count"],
                    "created_at": sh["created_at"],
                })

            issue_sql = """SELECT p.id,p.site_id,p.external_id,p.allocation_code,p.reservation_id,
                                  a.sequence,a.arm,k.kit_label,k.status AS kit_status
                           FROM participants p JOIN allocations a ON a.id=p.allocation_id
                           LEFT JOIN drug_kits k ON k.allocation_id=a.id
                           WHERE p.trial_id=?"""
            issue_params = [trial_id]
            if site_scope:
                issue_sql += " AND p.site_id=?"; issue_params.append(site_scope)
            issues = []
            for row in conn.execute(issue_sql + " ORDER BY p.id", issue_params).fetchall():
                item = {"participant_id": row["id"], "site_id": row["site_id"], "external_id": row["external_id"],
                        "allocation_code": row["allocation_code"], "sequence": row["sequence"],
                        "reservation_id": row["reservation_id"],
                        "kit_label": row["kit_label"], "kit_status": row["kit_status"]}
                # 监查员（审计员）对账时可见分组；中心与协调员视图保持盲态
                if actor["role"] == "monitor":
                    item["arm"] = row["arm"]
                issues.append(item)

            audit = conn.execute("SELECT * FROM audit_log WHERE trial_id=? ORDER BY id", (trial_id,)).fetchall()
            return {
                "trial": {"id": trial["id"], "name": trial["name"],
                          "protocol_version": trial["protocol_version"], "status": trial["status"]},
                "viewer": {"id": actor["id"], "role": actor["role"], "site_scope": site_scope},
                "balanced": balanced, "checks": checks,
                "strata": strata_ledger, "reservations": reservations, "queue": queue,
                "shipments": shipments, "issues": issues,
                "audit": [dict(x) | {"detail": json.loads(x["detail"])} for x in audit],
            }

    def trial_summary(self, user_id, trial_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            trial = self._trial(conn, trial_id)
            where, params = "", [trial_id]
            if actor["role"] == "site":
                where, params = " AND site_id=?", [trial_id, actor["site_id"]]
            total = conn.execute(f"SELECT COUNT(*) FROM participants WHERE trial_id=?" + where, params).fetchone()[0]
            by_site = conn.execute(
                f"SELECT site_id,COUNT(*) AS count FROM participants WHERE trial_id=?" + where + " GROUP BY site_id", params
            ).fetchall()
            audit = conn.execute("SELECT * FROM audit_log WHERE trial_id=? ORDER BY id", (trial_id,)).fetchall()
            return {
                "trial": {"id": trial["id"], "name": trial["name"], "protocol_version": trial["protocol_version"], "status": trial["status"]},
                "participants_visible": total, "by_site": [dict(x) for x in by_site],
                "audit": [dict(x) | {"detail": json.loads(x["detail"])} for x in audit],
            }


class Handler(BaseHTTPRequestHandler):
    server_version = "Randomization/1.0"
    def _store(self): return self.server.store  # type: ignore[attr-defined]
    def _body(self):
        length = int(self.headers.get("Content-Length", "0"))
        try: data = json.loads(self.rfile.read(length) or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError): raise BusinessError("请求体必须是合法 JSON", 400, "invalid_json")
        if not isinstance(data, dict): raise BusinessError("JSON 顶层必须是对象", 422, "invalid_json")
        return data
    def _send(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status); self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def _send_error(self, exc):
        payload = {"error": {"code": exc.code, "message": exc.message}}
        if exc.extra:
            payload["error"]["details"] = exc.extra
        self._send(exc.status, payload)
    def _dispatch(self, method):
        path = urlparse(self.path).path.rstrip("/") or "/"; parts = [p for p in path.split("/") if p]
        user = self.headers.get("X-User-Id", ""); store = self._store()
        if method == "GET" and path == "/":
            body = (BASE_DIR / "web" / "index.html").read_bytes(); self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(body)))
            self.end_headers(); self.wfile.write(body); return
        if method == "GET" and path == "/health": return self._send(200, {"ok": True})
        if parts == ["api", "trials"] and method == "POST":
            d=self._body(); return self._send(201, store.create_trial(user,d.get("name",""),d.get("protocol_version",""),d.get("arms"),d.get("strata_factors"),d.get("block_size"),d.get("seed","")))
        if len(parts) >= 3 and parts[:2] == ["api", "trials"]:
            trial_id=int(parts[2])
            if len(parts)==4 and parts[3]=="protocol" and method=="POST":
                d=self._body(); return self._send(200, store.update_protocol(user,trial_id,d.get("protocol_version",""),d.get("arms"),d.get("strata_factors"),d.get("block_size"),d.get("seed")))
            if len(parts)==4 and parts[3]=="start" and method=="POST": return self._send(200, store.start_trial(user,trial_id))
            if len(parts)==4 and parts[3]=="participants" and method=="GET": return self._send(200, {"items": store.list_participants(user,trial_id)})
            if len(parts)==4 and parts[3]=="enroll" and method=="POST":
                d=self._body(); return self._send(201, store.enroll(user,trial_id,d.get("external_id",""),d.get("factors",{})))
            if len(parts)==4 and parts[3]=="summary" and method=="GET": return self._send(200, store.trial_summary(user,trial_id))
            if len(parts)==4 and parts[3]=="plans" and method=="POST":
                d=self._body(); return self._send(200, store.set_plan(user,trial_id,d.get("strata_plans")))
            if len(parts)==4 and parts[3]=="reservations" and method=="POST":
                d=self._body()
                return self._send(201, store.reserve_segment(
                    user, trial_id, d.get("factors", {}), d.get("size"),
                    d.get("ttl_minutes"), d.get("site_id"), d.get("client_token")))
            if len(parts)==4 and parts[3]=="reservations" and method=="GET":
                qs = urlparse(self.path).query
                site = None
                for kv in qs.split("&"):
                    if kv.startswith("site_id="):
                        from urllib.parse import unquote
                        site = unquote(kv.split("=",1)[1])
                return self._send(200, store.list_reservations(user, trial_id, site))
            if len(parts)==4 and parts[3]=="sweep-expired" and method=="POST":
                return self._send(200, store.sweep_expired(user, trial_id))
            if len(parts)==4 and parts[3]=="ledger" and method=="GET":
                return self._send(200, store.ledger(user, trial_id))
        if len(parts)==3 and parts[:2]==["api","participants"] and method=="GET": return self._send(200, store.get_participant(user,int(parts[2])))
        if len(parts)==4 and parts[:2]==["api","participants"] and parts[3]=="unblinding-requests" and method=="POST":
            d=self._body(); return self._send(201, store.request_unblinding(user,int(parts[2]),d.get("reason","")))
        if len(parts)==4 and parts[:2]==["api","unblinding-requests"] and parts[3]=="approve" and method=="POST":
            return self._send(200, store.approve_unblinding(user,int(parts[2])))
        if len(parts)==4 and parts[:2]==["api","reservations"] and parts[3]=="release" and method=="POST":
            d=self._body(); rid=d.get("reservation_id")
            if not isinstance(rid,int): raise BusinessError("需要整数 reservation_id",422,"invalid_reservation")
            return self._send(200, store.release_segment(user,rid))
        raise BusinessError("接口不存在",404,"not_found")
    def _handle(self, method):
        try: self._dispatch(method)
        except BusinessError as exc: self._send_error(exc)
        except (ValueError,TypeError): self._send(400,{"error":{"code":"invalid_path","message":"路径参数格式错误"}})
        except Exception as exc: self._send(500,{"error":{"code":"internal_error","message":str(exc)}})
    def do_GET(self): self._handle("GET")
    def do_POST(self): self._handle("POST")
    def log_message(self, fmt, *args): print(f"{self.address_string()} - {fmt % args}")


class RandomizationServer(ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self, address, store): self.store=store; super().__init__(address,Handler)


def main():
    parser=argparse.ArgumentParser(description="临床试验随机分配与盲法服务")
    parser.add_argument("--db",default=str(DEFAULT_DB)); parser.add_argument("--port",type=int,default=8104)
    parser.add_argument("--init",action="store_true"); parser.add_argument("--seed",action="store_true")
    args=parser.parse_args(); store=RandomizationStore(args.db); store.init_schema()
    if args.seed: store.seed()
    if args.init or args.seed: print(f"数据库已初始化: {args.db}"); return
    server=RandomizationServer(("127.0.0.1",args.port),store); print(f"随机化服务运行于 http://127.0.0.1:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__=="__main__": main()
