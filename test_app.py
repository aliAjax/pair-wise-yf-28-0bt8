import tempfile
import threading
import unittest
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app import BusinessError, RandomizationStore


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


class NumberLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = RandomizationStore(Path(self.tmp.name) / "test.db")
        self.store.seed()
        self.trial = self.store.create_trial(
            "coord", "号源账研究", "v1.0", ["A", "B"], ["risk"], 4, "seed-ledger-01"
        )
        self.store.start_trial("coord", self.trial["id"])
        # 两层各计划 8 人，中央池共 16 个号
        self.store.set_plan("coord", self.trial["id"], [
            {"factors": {"risk": "low"}, "planned_count": 8},
            {"factors": {"risk": "high"}, "planned_count": 8},
        ])

    def tearDown(self):
        self.tmp.cleanup()

    def stratum_id(self, key):
        with self.store.connect() as conn:
            return conn.execute(
                "SELECT id FROM strata WHERE trial_id=? AND stratum_key=?", (self.trial["id"], key)
            ).fetchone()["id"]

    def test_plan_generates_exact_pool_and_reserve_stocks_drugs(self):
        low = self.stratum_id('{"risk":"low"}')
        with self.store.connect() as conn:
            n = conn.execute("SELECT COUNT(*) FROM allocations WHERE stratum_id=?", (low,)).fetchone()[0]
        self.assertEqual(n, 8)  # 计划写多少，池里就是多少，末区组截断
        res = self.store.reserve_segment("site1", self.trial["id"], {"risk": "low"}, 4, 120,
                                         client_token="tok-site1-low")
        self.assertEqual(res["result"], "reserved")
        self.assertEqual(res["reservation"]["range"], [1, 4])
        self.assertEqual(res["reservation"]["site_id"], "S001")
        self.assertEqual(len(res["reservation"]["kits"]), 4)
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM drug_kits").fetchone()[0], 4)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM drug_shipments").fetchone()[0], 1)
            free = conn.execute(
                "SELECT COUNT(*) FROM allocations WHERE stratum_id=? AND used_by IS NULL AND reservation_id IS NULL",
                (low,),
            ).fetchone()[0]
        self.assertEqual(free, 4)

    def test_concurrent_same_stratum_first_wins_second_sees_remaining_and_conflicts(self):
        barrier = threading.Barrier(2)
        outcomes = {}

        def worker(user, size, token, key):
            barrier.wait()
            try:
                outcomes[key] = ("ok", self.store.reserve_segment(
                    user, self.trial["id"], {"risk": "low"}, size, 120, client_token=token))
            except BusinessError as exc:
                outcomes[key] = ("err", exc)

        # 池里 8 个；两人同时各抢 6，先落库的拿 6，后到的只剩连续 2 个
        t1 = threading.Thread(target=worker, args=("site1", 6, "c-tok-1", "a"))
        t2 = threading.Thread(target=worker, args=("site2", 6, "c-tok-2", "b"))
        t1.start(); t2.start(); t1.join(); t2.join()

        ok = {k: v for k, v in outcomes.items() if v[0] == "ok"}
        err = {k: v for k, v in outcomes.items() if v[0] == "err"}
        self.assertEqual(len(ok), 1)
        self.assertEqual(len(err), 1)
        winner = list(ok.values())[0][1]
        loser = list(err.values())[0][1]
        self.assertEqual(winner["reservation"]["size"], 6)
        self.assertEqual(loser.code, "segment_conflict")
        self.assertEqual(loser.extra["remaining"], 2)
        self.assertEqual(loser.extra["pool"]["free"], 2)
        self.assertEqual(len(loser.extra["conflicts"]), 1)
        self.assertEqual(loser.extra["conflicts"][0]["size"], 6)

        with self.store.connect() as conn:
            active = conn.execute(
                "SELECT COUNT(*) FROM segment_reservations WHERE status='active'"
            ).fetchone()[0]
            self.assertEqual(active, 1)

    def test_plan_full_queues_and_expiry_recycles_fifo(self):
        # site1 占满 low 层全部 8 个号
        self.store.reserve_segment("site1", self.trial["id"], {"risk": "low"}, 8, 60,
                                   client_token="full")
        # site2 再申请 -> 计划写满，排队
        with self.assertRaises(BusinessError) as ctx:
            self.store.reserve_segment("site2", self.trial["id"], {"risk": "low"}, 3, 60,
                                       client_token="wait-3")
        self.assertEqual(ctx.exception.code, "plan_full_queued")
        self.assertEqual(ctx.exception.extra["remaining"], 0)
        queue_id = ctx.exception.extra["queue_id"]
        # 排第二个：5
        with self.assertRaises(BusinessError) as ctx2:
            self.store.reserve_segment("site2", self.trial["id"], {"risk": "low"}, 5, 60,
                                       client_token="wait-5")
        self.assertEqual(ctx2.exception.extra["position"], 2)

        # site1 先给 2 个受试者发号（发出去的号过期后照旧保留）
        for i in (1, 2):
            self.store.enroll("site1", self.trial["id"], f"S001-E{i}", {"risk": "low"})

        # 让号段过期（直接改库写入失效时刻），再触发清扫
        with self.store.connect() as conn:
            conn.execute("UPDATE segment_reservations SET expires_at=? WHERE client_token='full'",
                         ((datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(timespec="seconds"),))
        swept = self.store.sweep_expired("coord", self.trial["id"])
        self.assertEqual(swept["expired"][0]["issued"], 2)
        self.assertEqual(swept["expired"][0]["returned"], 6)

        with self.store.connect() as conn:
            # 已发的两个号仍挂在原受试者名下，分组不变
            rows = conn.execute(
                """SELECT p.external_id,a.sequence,a.arm FROM participants p
                   JOIN allocations a ON a.id=p.allocation_id ORDER BY p.id"""
            ).fetchall()
        self.assertEqual([r["external_id"] for r in rows], ["S001-E1", "S001-E2"])
        self.assertTrue(all(r["arm"] in ("A", "B") for r in rows))
        issued_sequences = {r["sequence"] for r in rows}

        # FIFO：回收 6 个连续号，先满足排队的 3 个；5 个的仍排队（只剩连续 3 个）
        self.assertEqual(len(swept["fulfilled_from_queue"]), 1)
        got = swept["fulfilled_from_queue"][0]
        self.assertEqual(got["site_id"], "S002")
        self.assertEqual(got["size"], 3)
        # 新号段不能碰已发号
        self.assertNotIn(got["range"][0], issued_sequences)
        self.assertNotIn(got["range"][1], issued_sequences)

        with self.store.connect() as conn:
            q = conn.execute("SELECT * FROM reservation_queue WHERE id=?", (queue_id,)).fetchone()
            self.assertEqual(q["status"], "fulfilled")
            still = conn.execute(
                "SELECT COUNT(*) FROM reservation_queue WHERE status='queued'"
            ).fetchone()[0]
            self.assertEqual(still, 1)
            # 退回的药盒：3 个随新号段重新激活备货，另 3 个保持 released
            released = conn.execute("SELECT COUNT(*) FROM drug_kits WHERE status='released'").fetchone()[0]
            self.assertEqual(released, 3)
            available = conn.execute("SELECT COUNT(*) FROM drug_kits WHERE status='available'").fetchone()[0]
            self.assertEqual(available, 3)
            # 再备货是一笔新的发货单
            shipments = conn.execute("SELECT COUNT(*) FROM drug_shipments").fetchone()[0]
            self.assertEqual(shipments, 2)

    def test_cross_center_application_rejected(self):
        # 中心用户借 site_id 申请别的中心
        with self.assertRaises(BusinessError) as ctx:
            self.store.reserve_segment("site1", self.trial["id"], {"risk": "low"}, 2, 60,
                                       site_id="S002", client_token="evil")
        self.assertEqual(ctx.exception.code, "cross_site_forbidden")
        self.assertEqual(ctx.exception.status, 403)
        # 协调员申请必须带 site_id
        with self.assertRaises(BusinessError) as ctx2:
            self.store.reserve_segment("coord", self.trial["id"], {"risk": "low"}, 2, 60)
        self.assertEqual(ctx2.exception.code, "site_required")
        # 协调员代申请合法
        ok = self.store.reserve_segment("coord", self.trial["id"], {"risk": "high"}, 2, 60,
                                        site_id="S002", client_token="coord-proxy")
        self.assertEqual(ok["reservation"]["site_id"], "S002")

    def test_write_failure_leaves_no_half_reservation_and_token_retries(self):
        calls = {"n": 0}
        original = RandomizationStore._create_shipment

        def flaky(self, conn, reservation, allocations, actor):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("disk full")
            return original(self, conn, reservation, allocations, actor)

        RandomizationStore._create_shipment = flaky
        try:
            with self.assertRaises(RuntimeError):
                self.store.reserve_segment("site1", self.trial["id"], {"risk": "low"}, 4, 60,
                                           client_token="retry-me")
        finally:
            RandomizationStore._create_shipment = original

        # 失败后不留半条：无预占、无备货、无药盒、号全部空闲
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM segment_reservations").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM drug_shipments").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM drug_kits").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM allocations WHERE reservation_id IS NOT NULL").fetchone()[0], 0)

        # 同一 token 重试成功
        ok = self.store.reserve_segment("site1", self.trial["id"], {"risk": "low"}, 4, 60,
                                        client_token="retry-me")
        self.assertEqual(ok["reservation"]["size"], 4)
        # 再重放同一 token：幂等，不重复占号
        replay = self.store.reserve_segment("site1", self.trial["id"], {"risk": "low"}, 4, 60,
                                            client_token="retry-me")
        self.assertEqual(replay["reservation"]["id"], ok["reservation"]["id"])
        with self.store.connect() as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segment_reservations WHERE client_token='retry-me'").fetchone()[0], 1)

    def test_enroll_consumes_own_segment_and_dispenses_kit(self):
        # 没有有效号段不能入组
        with self.assertRaises(BusinessError) as ctx:
            self.store.enroll("site2", self.trial["id"], "S002-X1", {"risk": "low"})
        self.assertEqual(ctx.exception.code, "reservation_required")

        self.store.reserve_segment("site1", self.trial["id"], {"risk": "low"}, 3, 60,
                                   client_token="s1-low")
        p = self.store.enroll("site1", self.trial["id"], "S001-E1", {"risk": "low"})
        self.assertEqual(p["sequence"], 1)
        self.assertTrue(p["kit_label"].startswith("KIT-"))
        with self.store.connect() as conn:
            kit = conn.execute("SELECT k.*, a.sequence FROM drug_kits k JOIN allocations a ON a.id=k.allocation_id WHERE k.dispensed_participant_id=?",
                               (p["id"],)).fetchone()
            self.assertEqual(kit["status"], "dispensed")
            self.assertEqual(kit["sequence"], 1)
        # site2 不能用 site1 的号段
        with self.assertRaises(BusinessError) as ctx:
            self.store.enroll("site2", self.trial["id"], "S002-E1", {"risk": "low"})
        self.assertEqual(ctx.exception.code, "reservation_required")

    def test_auditor_ledger_balances_and_keeps_blinding(self):
        self.store.reserve_segment("site1", self.trial["id"], {"risk": "low"}, 4, 60,
                                   client_token="l1")
        self.store.reserve_segment("site1", self.trial["id"], {"risk": "high"}, 2, 60,
                                   client_token="h1")
        self.store.enroll("site1", self.trial["id"], "S001-A", {"risk": "low"})
        self.store.enroll("site1", self.trial["id"], "S001-B", {"risk": "high"})

        ledger = self.store.ledger("monitor1", self.trial["id"])
        self.assertTrue(ledger["balanced"], ledger["checks"])
        self.assertTrue(all(c["ok"] for c in ledger["checks"]))
        # 审计员可见预占、发号、药品三类记录
        self.assertEqual(len(ledger["reservations"]), 2)
        self.assertEqual(len(ledger["issues"]), 2)
        self.assertEqual(len(ledger["shipments"]), 2)
        # 监查员对账能看到分组，中心视图仍盲
        self.assertIn("arm", ledger["issues"][0])
        site_ledger = self.store.ledger("site2", self.trial["id"])
        self.assertEqual(site_ledger["reservations"], [])
        self.assertEqual(site_ledger["issues"], [])
        site1_ledger = self.store.ledger("site1", self.trial["id"])
        self.assertNotIn("arm", site1_ledger["issues"][0])

        # 模拟一条账被手工改坏：balanced 翻 False
        with self.store.connect() as conn:
            conn.execute("UPDATE drug_kits SET status='dispensed' WHERE status='available' LIMIT 1")
        broken = self.store.ledger("monitor1", self.trial["id"])
        self.assertFalse(broken["balanced"])


    def test_plan_shrink_and_grow_keeps_ledger_balanced(self):
        low = self.stratum_id('{"risk":"low"}')
        # 8 -> 4：超出的 4 个空闲号被裁掉
        self.store.set_plan("coord", self.trial["id"], [
            {"factors": {"risk": "low"}, "planned_count": 4},
            {"factors": {"risk": "high"}, "planned_count": 8},
        ])
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM allocations WHERE stratum_id=?", (low,)).fetchone()[0], 4)
        # 不能降到低于已占用
        self.store.reserve_segment("site1", self.trial["id"], {"risk": "low"}, 3, 60, client_token="hold")
        with self.assertRaises(BusinessError) as ctx:
            self.store.set_plan("coord", self.trial["id"], [
                {"factors": {"risk": "low"}, "planned_count": 2},
                {"factors": {"risk": "high"}, "planned_count": 8},
            ])
        self.assertEqual(ctx.exception.code, "plan_below_committed")
        # 4 -> 8：确定性扩回
        self.store.set_plan("coord", self.trial["id"], [
            {"factors": {"risk": "low"}, "planned_count": 8},
            {"factors": {"risk": "high"}, "planned_count": 8},
        ])
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM allocations WHERE stratum_id=?", (low,)).fetchone()[0], 8)
        ledger = self.store.ledger("monitor1", self.trial["id"])
        self.assertTrue(ledger["balanced"], ledger["checks"])


if __name__ == "__main__":
    unittest.main()
