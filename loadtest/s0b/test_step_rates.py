import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import step_rates as sr


def lines(points):
    out = []
    for ts, total in points:
        stamp = "2026-10-31T10:00:%06.3f+09:00" % ts
        out.append("%s  INFO 1 --- [etrics-reporter] c.f.s.a.w.l.MetricsAccountResultListener : metrics accepted=0 rejected=0 fill=0 dup=0 acceptedTotal=%d" % (stamp, total))
    return "\n".join(out) + "\n"


class ParseSeriesTest(unittest.TestCase):
    def test_보고기_줄에서_시각과_누적_접수_수를_읽고_다른_줄은_건너뛴다(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "account.log")
            text = lines([(1.0, 0), (2.0, 5000)]) + "2026-10-31T10:00:03.000+09:00  INFO 1 --- [x] c.A : account-engine ringRemaining=1\n"
            open(path, "w").write(text)
            series = sr.parse_series(path)
        self.assertEqual([a for _, a in series], [0, 5000])
        self.assertAlmostEqual(series[1][0] - series[0][0], 1.0, places=3)


class BinStatsTest(unittest.TestCase):
    def test_일정한_속도면_창_길이와_속도와_표본이_나온다(self):
        series = [(float(t), 10000 * t) for t in range(0, 31)]  # 계좌 100개: 계좌당 m = 100 * t
        stat = sr.bin_stats(series, 100, 20, 200)
        self.assertAlmostEqual(stat["window_seconds"], 1.8, places=3)
        self.assertAlmostEqual(stat["window_rate"], 10000.0, places=1)
        self.assertEqual(stat["samples"], 2)
        self.assertAlmostEqual(stat["sec_rate_median"], 10000.0, places=1)

    def test_속도가_바뀌어도_구간_경계를_보간해_창_속도를_구한다(self):
        series = [(0.0, 0), (1.0, 20000), (2.0, 30000), (3.0, 60000)]  # 계좌 100개: m = 0, 200, 300, 600
        stat = sr.bin_stats(series, 100, 20, 200)
        self.assertAlmostEqual(stat["window_seconds"], 0.9, places=3)
        self.assertAlmostEqual(stat["window_rate"], 20000.0, places=1)
        self.assertEqual(stat["samples"], 1)

    def test_끝까지_닿지_못한_구간은_없음으로_돌려준다(self):
        series = [(0.0, 0), (1.0, 10000)]  # 계좌 100개: 최대 m = 100
        self.assertIsNone(sr.bin_stats(series, 100, 20, 200))

    def test_첫_구간은_시작_전_0건_줄이_있어도_경계를_잡는다(self):
        series = [(0.0, 0), (0.5, 0), (1.5, 10000), (2.5, 20000)]
        stat = sr.bin_stats(series, 100, 20, 100)
        self.assertIsNotNone(stat)
        self.assertGreater(stat["window_seconds"], 0)


class TableTest(unittest.TestCase):
    def make_run(self, root, name, accounts, rate_per_sec, seconds):
        d = os.path.join(root, name)
        os.makedirs(os.path.join(d, "logs"))
        open(os.path.join(d, "params.txt"), "w").write("ACCTS=%d PREFILL_PER_STOCK=0 PER_STOCK=1 ORDER_LOG=off HOLD=1\n" % accounts)
        open(os.path.join(d, "logs", "account.log"), "w").write(lines([(float(t), rate_per_sec * t) for t in range(0, seconds + 1)]))
        return d

    def test_표는_N별_열과_m_구간별_행을_채우고_못_잰_칸은_안_쟀음이라고_쓴다(self):
        with tempfile.TemporaryDirectory() as root:
            a = self.make_run(root, "a", 100, 10000, 40)    # m 최대 4000
            b = self.make_run(root, "b", 10, 1000, 30)      # m 최대 3000, 두 실행 모두 [2000,20000)에 닿지 못함
            text = sr.table([a, b], bins=[(20, 200), (200, 2000), (2000, 20000)])
        self.assertIn("N=100", text)
        self.assertIn("N=10", text)
        self.assertIn("안 쟀음", text)
        self.assertIn("[20,200)", text)

    def test_같은_N의_여러_실행은_한_칸에_모은다(self):
        with tempfile.TemporaryDirectory() as root:
            a = self.make_run(root, "a", 100, 10000, 40)
            b = self.make_run(root, "b", 100, 20000, 40)
            text = sr.table([a, b], bins=[(20, 200)])
        self.assertEqual(text.count("N=100"), 1)
        self.assertIn("10000", text)
        self.assertIn("20000", text)


if __name__ == "__main__":
    unittest.main()
