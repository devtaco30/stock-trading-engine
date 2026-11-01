import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import step_rates as sr


def lines(points):
    out = []
    for ts, total in points:
        stamp = "2026-10-31T10:%02d:%06.3f+09:00" % (int(ts) // 60, ts % 60)
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


class ParseBinsTest(unittest.TestCase):
    def test_콜론과_쉼표로_구간_목록을_읽는다(self):
        self.assertEqual(sr.parse_bins("200:2000,2000:6000"), [(200, 2000), (2000, 6000)])

    def test_잘못된_구간은_거부한다(self):
        for bad in ("", "200", "200:", "a:b", "2000:200", "200:200", "200:2000,"):
            with self.assertRaises(ValueError, msg=bad):
                sr.parse_bins(bad)


class MainTest(unittest.TestCase):
    def make_run(self, root, name, accounts, rate_per_sec, seconds):
        d = os.path.join(root, name)
        os.makedirs(os.path.join(d, "logs"))
        open(os.path.join(d, "params.txt"), "w").write("ACCTS=%d PREFILL_PER_STOCK=0 PER_STOCK=1 ORDER_LOG=off HOLD=1\n" % accounts)
        open(os.path.join(d, "logs", "account.log"), "w").write(lines([(float(t), rate_per_sec * t) for t in range(0, seconds + 1)]))
        return d

    def test_구간_인자를_주면_그_구간으로_한_실행의_JSON을_낸다(self):
        with tempfile.TemporaryDirectory() as root:
            d = self.make_run(root, "a", 100, 10000, 80)
            result = json.loads(sr.main(["--bins", "200:2000,2000:6000", d]))
        self.assertEqual([(b["lo"], b["hi"]) for b in result["bins"]], [(200, 2000), (2000, 6000)])
        self.assertEqual(result["accounts"], 100)

    def test_구간_인자가_없으면_기본_구간을_쓴다(self):
        with tempfile.TemporaryDirectory() as root:
            d = self.make_run(root, "a", 100, 10000, 80)
            result = json.loads(sr.main([d]))
        self.assertEqual([(b["lo"], b["hi"]) for b in result["bins"]], sr.DEFAULT_BINS)


class TableTest(unittest.TestCase):
    def make_run(self, root, name, accounts, rate_per_sec, seconds):
        return MainTest.make_run(self, root, name, accounts, rate_per_sec, seconds)

    def test_표는_N별_행과_m_구간별_열을_채우고_못_잰_칸은_안_쟀음이라고_쓴다(self):
        with tempfile.TemporaryDirectory() as root:
            a = self.make_run(root, "a", 100, 10000, 40)    # m 최대 4000
            b = self.make_run(root, "b", 10, 1000, 30)      # m 최대 3000, 두 실행 모두 [2000,20000)에 닿지 못함
            text = sr.table([a, b], bins=[(20, 200), (200, 2000), (2000, 20000)])
        rows = text.splitlines()
        self.assertIn("[20,200)", rows[0])
        self.assertIn("[2000,20000)", rows[0])
        self.assertTrue(rows[1].startswith("N=10 "), rows[1])     # N 오름차순
        self.assertTrue(rows[2].startswith("N=100 "), rows[2])
        self.assertIn("안 쟀음", rows[1])
        self.assertIn("안 쟀음", rows[2])

    def test_같은_N의_여러_실행은_한_행에_모은다(self):
        with tempfile.TemporaryDirectory() as root:
            a = self.make_run(root, "a", 100, 10000, 40)
            b = self.make_run(root, "b", 100, 20000, 40)
            text = sr.table([a, b], bins=[(20, 200)])
        self.assertEqual(text.count("N=100"), 1)
        self.assertIn("10000", text)
        self.assertIn("20000", text)


class ByTotalTest(unittest.TestCase):
    def make_run(self, root, name, accounts, rate_per_sec, seconds):
        return MainTest.make_run(self, root, name, accounts, rate_per_sec, seconds)

    def test_전체_합_기준_구간은_계좌_수와_무관하게_전체_접수_수로_경계를_잡는다(self):
        with tempfile.TemporaryDirectory() as root:
            a = self.make_run(root, "a", 100, 10000, 40)   # 전체 합이 초당 10,000씩 늘어 20,000에서 60,000까지 4초
            text = sr.table([a], bins=[(20000, 60000)], by_total=True)
        self.assertIn("창 10000", text)
        self.assertIn("창 4.0초", text)
        self.assertIn("m 200~600", text)          # 같은 구간의 계좌당 미체결 범위(N=100)

    def test_전체_합_기준에서도_닿지_못한_칸은_안_쟀음이다(self):
        with tempfile.TemporaryDirectory() as root:
            a = self.make_run(root, "a", 100, 10000, 10)   # 전체 합 최대 100,000
            text = sr.table([a], bins=[(20000, 60000), (100000, 600000)], by_total=True)
        self.assertIn("안 쟀음", text)

    def test_N이_다른_실행도_같은_전체_합_구간에서_한_표에_놓인다(self):
        with tempfile.TemporaryDirectory() as root:
            a = self.make_run(root, "a", 30, 5000, 60)
            b = self.make_run(root, "b", 300, 20000, 60)
            text = sr.table([a, b], bins=[(40000, 120000)], by_total=True)
        rows = text.splitlines()
        self.assertTrue(rows[1].startswith("N=30 "), rows[1])
        self.assertTrue(rows[2].startswith("N=300 "), rows[2])
        self.assertIn("m 1333~4000", rows[1])
        self.assertIn("m 133~400", rows[2])


if __name__ == "__main__":
    unittest.main()
