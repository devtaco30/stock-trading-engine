import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import ladder_plan as lp
import probe_rates as pr


def log_lines(intervals, t0=0.0):
    """intervals = [(접수 증분, 거부 증분), ...] 1초 간격 보고기 줄."""
    out, total = [], 0
    for i, (a, r) in enumerate(intervals):
        total += a
        ts = t0 + i + 1
        stamp = "2026-11-01T10:%02d:%06.3f+09:00" % (int(ts) // 60, ts % 60)
        out.append("%s  INFO 1 --- [etrics-reporter] c.f.s.a.w.l.MetricsAccountResultListener : metrics accepted=%d rejected=%d fill=0 dup=0 acceptedTotal=%d" % (stamp, a, r, total))
    return "\n".join(out) + "\n"


def step(kind, rung, per_stock, label="C=3"):
    return {"kind": kind, "rung": rung, "per_stock": per_stock, "quantity": 1, "label": label}


def write(path, text):
    with open(path, "w") as f:
        f.write(text)


class AnalyzeTest(unittest.TestCase):
    def analyze(self, intervals, steps):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "account.log")
            write(p, log_lines(intervals))
            return pr.analyze(pr.parse_intervals(p), {"hold": 9, "steps": steps})

    def test_쌓기_구간과_거부_구간을_기대_건수로_나누고_양_끝_구간은_뺀다(self):
        result = self.analyze([(1000, 0)] * 3 + [(0, 0)] * 2 + [(0, 5000)] * 6,
                              [step("PREFILL", 0, 1000), step("REJECT", 0, 10000)])
        prefill, reject = result
        self.assertEqual(prefill["observed"], 3000)
        self.assertTrue(prefill["count_ok"])
        self.assertEqual(reject["observed"], 30000)
        self.assertTrue(reject["count_ok"])
        self.assertEqual(reject["samples"], 4)
        self.assertAlmostEqual(reject["window_seconds"], 4.0, places=3)
        self.assertAlmostEqual(reject["rate"], 5000.0, places=1)

    def test_쌓기가_끝나는_구간에서_거부가_시작되면_그_구간은_양쪽_단계가_나눠_갖고_속도에서_뺀다(self):
        result = self.analyze([(1000, 0), (1000, 0), (1000, 5000)] + [(0, 5000)] * 5,
                              [step("PREFILL", 0, 1000), step("REJECT", 0, 10000)])
        prefill, reject = result
        self.assertEqual(prefill["observed"], 3000)
        self.assertEqual(reject["observed"], 30000)
        self.assertAlmostEqual(reject["rate"], 5000.0, places=1)
        self.assertEqual(reject["samples"], 4)    # 섞인 첫 구간과 마지막 구간을 뺀 4개

    def test_기대_건수와_관측_건수가_어긋나면_count_ok가_거짓이다(self):
        result = self.analyze([(1000, 0)] * 3 + [(0, 5000)] * 4,
                              [step("PREFILL", 0, 1000), step("REJECT", 0, 10000)])
        self.assertTrue(result[0]["count_ok"])
        self.assertFalse(result[1]["count_ok"])
        self.assertEqual(result[1]["observed"], 20000)

    def test_거부_구간_사이의_멈춤은_단계를_나누지_않는다(self):
        result = self.analyze([(1000, 0)] * 3 + [(0, 5000)] * 3 + [(0, 0)] + [(0, 5000)] * 3,
                              [step("PREFILL", 0, 1000), step("REJECT", 0, 10000)])
        self.assertTrue(result[1]["count_ok"])
        self.assertEqual(result[1]["observed"], 30000)

    def test_접수_검증_구간은_접수_증분으로_잰다(self):
        steps = [step("PREFILL", 0, 1000), step("REJECT", 0, 10000), step("ACCEPT", 0, 2000), step("PREFILL", 1, 1000)]
        result = self.analyze([(1000, 0)] * 3 + [(0, 5000)] * 6 + [(0, 0)] + [(2000, 0)] * 3 + [(0, 0)] + [(1000, 0)] * 3, steps)
        accept = result[2]
        self.assertEqual(accept["kind"], "ACCEPT")
        self.assertEqual(accept["observed"], 6000)
        self.assertAlmostEqual(accept["rate"], 2000.0, places=1)
        self.assertTrue(result[3]["count_ok"])

    def test_구간이_두_개_이하면_양_끝을_못_빼므로_전부_쓰고_표시한다(self):
        result = self.analyze([(1000, 0)] * 3 + [(0, 15000)] * 2, [step("PREFILL", 0, 1000), step("REJECT", 0, 10000)])
        reject = result[1]
        self.assertFalse(reject["edges_trimmed"])
        self.assertEqual(reject["samples"], 2)
        self.assertAlmostEqual(reject["rate"], 15000.0, places=1)


class PlanFileTest(unittest.TestCase):
    def test_계획_TSV를_읽어_단계_목록으로_돌려준다(self):
        plan = lp.plan(100, [3, 9], accept_probes={1: 2})
        parsed = pr.parse_plan(lp.to_tsv(plan))
        self.assertEqual(parsed["hold"], plan["hold"])
        self.assertEqual([(s["kind"], s["rung"], s["per_stock"]) for s in parsed["steps"]],
                         [(s["kind"], s["rung"], s["per_stock"]) for s in plan["steps"]])


class TableTest(unittest.TestCase):
    def make_run(self, root, name, accounts, intervals, steps):
        d = os.path.join(root, name)
        os.makedirs(os.path.join(d, "logs"))
        write(os.path.join(d, "params.txt"), "ACCTS=%d PREFILL_PER_STOCK=0 PER_STOCK=1 ORDER_LOG=off HOLD=1\n" % accounts)
        write(os.path.join(d, "logs", "account.log"), log_lines(intervals))
        write(os.path.join(d, "plan.tsv"), lp.to_tsv({"hold": 9, "steps": steps}))
        return d

    def test_표는_N별_행_C별_열로_거부_속도를_채우고_접수_대_거부_비를_적는다(self):
        steps = [step("PREFILL", 0, 1000, "C=3"), step("REJECT", 0, 10000, "C=3"), step("ACCEPT", 0, 2000, "C=3")]
        intervals = [(1000, 0)] * 3 + [(0, 5000)] * 6 + [(0, 0)] + [(2000, 0)] * 4
        with tempfile.TemporaryDirectory() as root:
            a = self.make_run(root, "a", 100, intervals, steps)
            text = pr.table([a])
        self.assertIn("N=100", text)
        self.assertIn("C=3", text)
        self.assertIn("5000", text)
        self.assertIn("접수/거부 0.40", text)


if __name__ == "__main__":
    unittest.main()
