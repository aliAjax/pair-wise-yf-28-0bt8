import tempfile
import threading
import unittest
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app import BusinessError, RandomizationStore


def _future(days=2):
    return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat(timespec="seconds")


def _past(hours=1):
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat(timespec="seconds")


class RandomizationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = RandomizationStore(Path(self.tmp.name) / "test.db")
        self.store.seed()
        self.trial = self.store.create_trial(
            "coord", "多中心降压研究", "v1.0", ["A", "B"], ["risk"], 4, "seed-2026-001"
        )
        self.store.start_trial("coord", self.trial["id"])

    def tearDown(self):
        self.tmp.cleanup()

    def test_stratified_block_randomization_and_two_person_unblinding(self):
        participants = [
            self.store.enroll("site1", self.trial["id"], f"S001-{i:03d}", {"risk": "low"})
            for i in range(1, 5)
        ]
        self.assertNotIn("arm", participants[0])
        with self.store.connect() as conn:
            arms = [r["arm"] for r in conn.execute(
                "SELECT a.arm FROM allocations a JOIN participants p ON p.allocation_id=a.id WHERE p.trial_id=? ORDER BY p.id",
                (self.trial["id"],),
            ).fetchall()]
        self.assertEqual(Counter(arms), Counter({"A": 2, "B": 2}))
        request = self.store.request_unblinding("site1", participants[0]["id"], "受试者发生严重不良事件需要紧急处理")
        first = self.store.approve_unblinding("monitor1", request["id"])
        self.assertEqual(first["status"], "pending")
        with self.assertRaises(BusinessError) as ctx:
            self.store.approve_unblinding("monitor1", request["id"])
        self.assertEqual(ctx.exception.code, "distinct_approver_required")
        second = self.store.approve_unblinding("monitor2", request["id"])
        self.assertEqual(second["status"], "approved")
        self.assertIn(second["arm"], {"A", "B"})

    def test_idempotent_enrollment_site_isolation_and_protocol_lock(self):
        first = self.store.enroll("site1", self.trial["id"], "S001-001", {"risk": "high"})
        again = self.store.enroll("site1", self.trial["id"], "S001-001", {"risk": "high"})
        self.assertEqual(first["id"], again["id"])
        self.assertTrue(again["idempotent"])
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM participants").fetchone()[0], 1)
        with self.assertRaises(BusinessError) as ctx:
            self.store.get_participant("site2", first["id"])
        self.assertEqual(ctx.exception.code, "site_isolation")
        with self.assertRaises(BusinessError) as ctx:
            self.store.update_protocol("coord", self.trial["id"], "v2")
        self.assertEqual(ctx.exception.code, "protocol_locked")

    def _small_trial(self, planned=12):
        trial = self.store.create_trial(
            "coord", "号源小试验", "v1", ["A", "B"], ["risk"], 4, "seed-2026-002", stratum_planned=planned
        )
        self.store.start_trial("coord", trial["id"])
        return trial

    def test_reserve_range_conflict_shows_remaining_and_conflict_id(self):
        trial = self._small_trial()
        first = self.store.reserve_range("site1", trial["id"], {"risk": "low"}, 6, _future(), seq_start=1)
        self.assertEqual(first["status"], "active")
        self.assertEqual((first["seq_start"], first["seq_end"]), (1, 6))
        with self.assertRaises(BusinessError) as ctx:
            self.store.reserve_range("site1", trial["id"], {"risk": "low"}, 6, _future(), seq_start=1)
        self.assertEqual(ctx.exception.code, "range_conflict")
        self.assertEqual(ctx.exception.extra["conflict_reservation_id"], first["reservation_id"])
        self.assertEqual(ctx.exception.extra["held"]["site_id"], "S001")
        self.assertTrue(any(r["seq_start"] == 7 for r in ctx.exception.extra["remaining"]))

    def test_concurrent_same_stratum_first_commit_wins(self):
        trial = self._small_trial()
        results = {}

        def worker(name):
            try:
                r = self.store.reserve_range("site1", trial["id"], {"risk": "low"}, 6, _future(), seq_start=1)
                results[name] = ("ok", r["reservation_id"], r["seq_start"], r["seq_end"])
            except BusinessError as exc:
                results[name] = ("err", exc.code, exc.extra.get("conflict_reservation_id"))

        threads = [threading.Thread(target=worker, args=(n,)) for n in ("A", "B")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        oks = [v for v in results.values() if v[0] == "ok"]
        errs = [v for v in results.values() if v[0] == "err"]
        self.assertEqual(len(oks), 1)
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs[0][1], "range_conflict")
        self.assertEqual(errs[0][2], oks[0][1])

    def test_cross_site_reservation_and_stock_rejected(self):
        trial = self._small_trial()
        other = self.store.reserve_range("site2", trial["id"], {"risk": "low"}, 4, _future())
        with self.assertRaises(BusinessError) as ctx:
            self.store.stock_drugs("site1", trial["id"], other["reservation_id"], ["P1", "P2"])
        self.assertEqual(ctx.exception.code, "site_isolation")
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM drug_records").fetchone()[0], 0)

    def test_plan_full_then_queue(self):
        trial = self._small_trial(planned=8)
        r1 = self.store.reserve_range("site1", trial["id"], {"risk": "low"}, 4, _future())
        r2 = self.store.reserve_range("site1", trial["id"], {"risk": "low"}, 4, _future())
        self.assertEqual((r1["seq_start"], r2["seq_end"]), (1, 8))
        queued = self.store.reserve_range("site1", trial["id"], {"risk": "low"}, 1, _future())
        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["queue_no"], 1)
        with self.store.connect() as conn:
            row = conn.execute("SELECT status FROM reservations WHERE id=?", (queued["reservation_id"],)).fetchone()
        self.assertEqual(row["status"], "queued")

    def test_expiry_returns_numbers_and_fulfills_queue(self):
        trial = self._small_trial(planned=12)
        r1 = self.store.reserve_range("site1", trial["id"], {"risk": "low"}, 6, _future())
        r2 = self.store.reserve_range("site1", trial["id"], {"risk": "low"}, 6, _future())
        queued = self.store.reserve_range("site1", trial["id"], {"risk": "low"}, 1, _future())
        self.assertEqual(queued["status"], "queued")
        self.store.stock_drugs("site1", trial["id"], r2["reservation_id"], [f"P{i}" for i in range(7, 13)])
        with self.store.connect() as conn:
            conn.execute("UPDATE reservations SET expires_at=? WHERE id=?", (_past(), r2["reservation_id"]))
        # 触发过期清算
        self.store.availability("site1", trial["id"], {"risk": "low"})
        with self.store.connect() as conn:
            r2row = conn.execute("SELECT status FROM reservations WHERE id=?", (r2["reservation_id"],)).fetchone()
            qrow = conn.execute("SELECT status, seq_start, seq_end FROM reservations WHERE id=?", (queued["reservation_id"],)).fetchone()
            returned = conn.execute("SELECT COUNT(*) FROM drug_records WHERE reservation_id=? AND status='returned'", (r2["reservation_id"],)).fetchone()[0]
        self.assertEqual(r2row["status"], "expired")
        self.assertEqual(qrow["status"], "active")
        self.assertGreaterEqual(qrow["seq_start"], 7)
        self.assertEqual(returned, 6)

    def test_failed_write_leaves_no_half_record_and_retry_succeeds(self):
        trial = self._small_trial()
        original_audit = self.store._audit

        def faulty(conn, trial_id, actor, action, detail):
            if action == "reservation.create":
                raise RuntimeError("模拟写盘失败")
            return original_audit(conn, trial_id, actor, action, detail)

        self.store._audit = faulty
        with self.assertRaises(RuntimeError):
            self.store.reserve_range("site1", trial["id"], {"risk": "low"}, 5, _future())
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM reservations").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM allocations WHERE reservation_id IS NOT NULL").fetchone()[0], 0)
        self.store._audit = original_audit
        done = self.store.reserve_range("site1", trial["id"], {"risk": "low"}, 5, _future())
        self.assertEqual(done["status"], "active")
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM reservations").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM allocations WHERE reservation_id IS NOT NULL").fetchone()[0], 5)

    def test_enroll_consumes_reserved_numbers_and_dispenses_drugs(self):
        trial = self._small_trial()
        r = self.store.reserve_range("site1", trial["id"], {"risk": "low"}, 4, _future())
        self.store.stock_drugs("site1", trial["id"], r["reservation_id"], ["P1", "P2", "P3", "P4"])
        p = self.store.enroll("site1", trial["id"], "S001-001", {"risk": "low"})
        with self.store.connect() as conn:
            alloc = conn.execute(
                "SELECT a.reservation_id FROM allocations a JOIN participants p ON p.allocation_id=a.id WHERE p.id=?",
                (p["id"],),
            ).fetchone()
            drug = conn.execute(
                "SELECT d.status FROM drug_records d JOIN participants p ON p.allocation_id=d.allocation_id WHERE p.id=?",
                (p["id"],),
            ).fetchone()
        self.assertEqual(alloc["reservation_id"], r["reservation_id"])
        self.assertEqual(drug["status"], "dispensed")

    def test_reconciliation_flags_issued_without_stock(self):
        trial = self._small_trial()
        self.store.enroll("site1", trial["id"], "S001-001", {"risk": "low"})
        report = self.store.reconciliation("coord", trial["id"])
        self.assertTrue(any(d["type"] == "issued_without_stock" for d in report["discrepancies"]))


if __name__ == "__main__":
    unittest.main()
