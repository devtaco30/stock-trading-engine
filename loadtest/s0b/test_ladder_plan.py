import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import ladder_plan as lp


def kinds(steps):
    return [(s["kind"], s["rung"]) for s in steps]


class PlanTest(unittest.TestCase):
    def test_단계별_쌓기량은_이전_단계와의_차이이고_종목당_계좌_수를_곱한다(self):
        # 계좌 100개, 계좌당 미체결 3 -> 9 -> 30 (종목 3개라 종목당 1 -> 3 -> 10)
        plan = lp.plan(100, [3, 9, 30], accept_probes={})
        prefill = [s["per_stock"] for s in plan["steps"] if s["kind"] == "PREFILL"]
        self.assertEqual(prefill, [100 * 1, 100 * 2, 100 * 7])

    def test_시드_보유는_마지막_단계_쌓기량에_접수_검증량과_여유를_더한_값이다(self):
        plan = lp.plan(100, [3, 9, 30], accept_probes={2: 2})   # 마지막 단계에서 계좌당 종목당 2건 더 접수
        self.assertEqual(plan["hold"], 10 + 2 + lp.HOLD_MARGIN)

    def test_거부용_수량은_그_단계의_가용분보다_1_크다(self):
        plan = lp.plan(100, [3, 9, 30], accept_probes={2: 2})
        hold = plan["hold"]
        quantities = [s["quantity"] for s in plan["steps"] if s["kind"] == "REJECT"]
        self.assertEqual(quantities, [hold - 1 + 1, hold - 3 + 1, hold - 10 + 1])

    def test_거부_구간의_보낼_양은_계좌_수의_배수이고_하나_이상이다(self):
        plan = lp.plan(30, [3, 9, 30], accept_probes={}, rate_estimate=lambda n, c: 1000.0, probe_seconds=15)
        for s in plan["steps"]:
            if s["kind"] == "REJECT":
                self.assertGreaterEqual(s["per_stock"], 30)
                self.assertEqual(s["per_stock"] % 30, 0)

    def test_거부_구간의_보낼_양은_속도_추정_곱_초에_맞추고_상한을_넘지_않는다(self):
        plan = lp.plan(10, [3, 9], accept_probes={}, rate_estimate=lambda n, c: 3000.0, probe_seconds=10)
        # 3000 x 10 = 3만 건 / 종목 3개 = 종목당 1만 건 (10의 배수)
        self.assertEqual([s["per_stock"] for s in plan["steps"] if s["kind"] == "REJECT"], [10000, 10000])
        capped = lp.plan(10, [3, 9], accept_probes={}, rate_estimate=lambda n, c: 1e9, probe_seconds=10, max_total_probe=300)
        self.assertEqual([s["per_stock"] for s in capped["steps"] if s["kind"] == "REJECT"], [100, 100])

    def test_중간_단계의_접수_검증은_다음_단계_쌓기량을_그만큼_줄여_명목_C를_맞춘다(self):
        plan = lp.plan(100, [3, 9, 30], accept_probes={1: 1})   # 두 번째 단계(C=9)에서 종목당 1건 접수 검증
        self.assertEqual(kinds(plan["steps"]), [
            ("PREFILL", 0), ("REJECT", 0), ("PREFILL", 1), ("REJECT", 1), ("ACCEPT", 1), ("PREFILL", 2), ("REJECT", 2)])
        prefill = [s["per_stock"] for s in plan["steps"] if s["kind"] == "PREFILL"]
        self.assertEqual(prefill, [100 * 1, 100 * 2, 100 * (10 - (3 + 1))])

    def test_거부_접수_순서와_접수_검증_수량은_1이다(self):
        plan = lp.plan(100, [3, 9, 30], accept_probes={2: 4})
        accept = [s for s in plan["steps"] if s["kind"] == "ACCEPT"]
        self.assertEqual(len(accept), 1)
        self.assertEqual(accept[0]["quantity"], 1)
        self.assertEqual(accept[0]["per_stock"], 100 * 4)

    def test_잘못된_입력은_거부한다(self):
        with self.assertRaises(ValueError):
            lp.plan(100, [4, 9], accept_probes={})            # 종목 수 3의 배수가 아님
        with self.assertRaises(ValueError):
            lp.plan(100, [9, 9], accept_probes={})            # 단계가 늘지 않음
        with self.assertRaises(ValueError):
            lp.plan(100, [3, 9, 30], accept_probes={0: 5})    # 접수 검증이 다음 단계 증가분(2)보다 큼
        with self.assertRaises(ValueError):
            lp.plan(0, [3, 9], accept_probes={})
        with self.assertRaises(ValueError):
            lp.plan(100, [3, 9], accept_probes={5: 1})        # 없는 단계


class FormatTest(unittest.TestCase):
    def test_TSV는_첫_줄이_HOLD이고_나머지는_종류_단계_양_수량_이름이다(self):
        plan = lp.plan(100, [3, 9], accept_probes={}, rate_estimate=lambda n, c: 300.0, probe_seconds=10)
        lines = lp.to_tsv(plan).splitlines()
        self.assertTrue(lines[0].startswith("HOLD\t"))
        self.assertEqual(lines[1].split("\t")[0:2], ["PREFILL", "0"])
        self.assertEqual(len(lines[1].split("\t")), 5)


if __name__ == "__main__":
    unittest.main()
