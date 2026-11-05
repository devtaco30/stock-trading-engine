import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import balance_check as bc

BASE = "2026-11-04T10:%02d:%06.3f+09:00"


def metric_line(ts, accepted, rejected, fill, accepted_total):
    stamp = BASE % (int(ts) // 60, ts % 60)
    return ("%s  INFO 1 --- [etrics-reporter] c.f.s.a.w.l.MetricsAccountResultListener : "
            "metrics accepted=%d rejected=%d fill=%d dup=0 acceptedTotal=%d" % (stamp, accepted, rejected, fill, accepted_total))


def synthetic(n, c, rate, prefill_seconds, steady_seconds, fills_per_second=None, accepted_per_second=None, rejected_per_second=0):
    """쌓기 prefill_seconds초에 n*c건을 접수한 뒤 steady_seconds초 동안 초당 rate건을 접수하는 보고기 줄을 만든다."""
    fills_per_second = rate if fills_per_second is None else fills_per_second
    accepted_per_second = rate if accepted_per_second is None else accepted_per_second
    lines = []
    total = 0
    t = 10.0
    for _ in range(prefill_seconds):
        step = n * c // prefill_seconds
        total += step
        lines.append(metric_line(t, step, 0, 0, total))
        t += 1
    steady_start = t
    for _ in range(steady_seconds):
        total += accepted_per_second
        lines.append(metric_line(t, accepted_per_second, rejected_per_second, fills_per_second, total))
        t += 1
    return "\n".join(lines) + "\n", steady_start


def write(text):
    d = tempfile.mkdtemp()
    path = os.path.join(d, "account.log")
    open(path, "w").write(text)
    return path


def epoch(ts):
    import datetime
    return datetime.datetime.fromisoformat(BASE % (int(ts) // 60, ts % 60)).timestamp()


class ParseMetricsTest(unittest.TestCase):
    def test_보고기_줄에서_시각과_구간_접수_거부_체결과_누적_접수를_읽는다(self):
        path = write(metric_line(12.0, 5, 1, 4, 99) + "\n2026-11-04T10:00:13.000+09:00  INFO 1 --- [x] c.A : other\n")
        rows = bc.parse_metrics(path)
        self.assertEqual(rows, [(epoch(12.0), 5, 1, 4, 99)])


class AnalyzeTest(unittest.TestCase):
    N, C, R = 30, 60, 100

    def run_case(self, **kw):
        text, steady_start = synthetic(self.N, self.C, self.R, 5, 120, **kw)
        rows = bc.parse_metrics(write(text))
        return bc.analyze(rows, self.N, self.C, self.R, epoch(steady_start), epoch(steady_start) + 120, rejected_warn=0, heap_peak_ratio=None)

    def test_체결이_접수와_같은_속도면_미체결_합계가_N_곱하기_C로_일정하고_모든_기준을_통과한다(self):
        result = self.run_case()
        self.assertTrue(result["balanced"], result)
        self.assertAlmostEqual(result["final_open"], self.N * self.C, delta=1)
        self.assertLessEqual(abs(result["drift"]), 0.01 * self.N * self.C)
        self.assertGreaterEqual(result["accept_ratio"], 0.99)

    def test_체결이_모자라_미체결이_계속_늘면_기울기_기준에서_실패한다(self):
        result = self.run_case(fills_per_second=int(self.R * 0.9))
        self.assertFalse(result["balanced"])
        self.assertFalse(result["checks"]["slope"])

    def test_접수_속도가_목표의_99퍼센트_밑이면_속도_기준에서_실패한다(self):
        result = self.run_case(accepted_per_second=int(self.R * 0.9), fills_per_second=int(self.R * 0.9))
        self.assertFalse(result["checks"]["accept_ratio"])
        self.assertFalse(result["balanced"])

    def test_거부가_하나라도_있으면_실패한다(self):
        result = self.run_case(rejected_per_second=1)
        self.assertFalse(result["checks"]["no_rejects"])

    def test_다른_shard_계좌의_체결_fan_out_거부는_진짜_거부에서_빼고_따로_센다(self):
        text, steady_start = synthetic(self.N, self.C, self.R, 5, 120, rejected_per_second=3)
        rows = bc.parse_metrics(write(text))
        raw = sum(r[2] for r in rows)
        result = bc.analyze(rows, self.N, self.C, self.R, epoch(steady_start), epoch(steady_start) + 120, rejected_warn=0, heap_peak_ratio=None, not_owned=raw)
        self.assertEqual(result["rejected_total"], 0)
        self.assertEqual(result["not_owned_fanout"], raw)
        self.assertTrue(result["checks"]["no_rejects"])
        partly = bc.analyze(rows, self.N, self.C, self.R, epoch(steady_start), epoch(steady_start) + 120, rejected_warn=0, heap_peak_ratio=None, not_owned=raw - 1)
        self.assertEqual(partly["rejected_total"], 1)
        self.assertFalse(partly["checks"]["no_rejects"])

    def test_힙_사용_비율이_80퍼센트_이상이면_실패한다(self):
        text, steady_start = synthetic(self.N, self.C, self.R, 5, 120)
        rows = bc.parse_metrics(write(text))
        result = bc.analyze(rows, self.N, self.C, self.R, epoch(steady_start), epoch(steady_start) + 120, rejected_warn=0, heap_peak_ratio=0.81)
        self.assertFalse(result["checks"]["heap"])
        self.assertFalse(result["balanced"])


class HeapPeakTest(unittest.TestCase):
    def test_jstat_gc_출력에서_S0U_S1U_EU_OU_합의_최대를_바이트_기준_8GiB에_대한_비율로_낸다(self):
        header = "S0C S1C S0U S1U EC EU OC OU MC MU CCSC CCSU YGC YGCT FGC FGCT CGC CGCT GCT"
        row = lambda s0u, s1u, eu, ou: "0 0 %d %d 0 %d 0 %d 0 0 0 0 0 0 0 0 0 0 0" % (s0u, s1u, eu, ou)
        text = header + "\n" + row(0, 0, 1024, 1024) + "\n" + row(1024, 1024, 1024 * 1024, 1024) + "\n"
        d = tempfile.mkdtemp()
        path = os.path.join(d, "heap.jstat")
        open(path, "w").write(text)
        peak_bytes = (1024 + 1024 + 1024 * 1024 + 1024) * 1024
        self.assertAlmostEqual(bc.heap_peak_ratio(path, 8 * 1024 ** 3), peak_bytes / (8 * 1024 ** 3), places=9)



class MergeRowsTest(unittest.TestCase):
    def test_두_워커의_초별_값은_합치고_접수_합계는_마지막으로_본_값을_이어_더한다(self):
        a = [(100.2, 10, 0, 4, 10), (101.2, 20, 0, 8, 30), (102.2, 5, 1, 2, 35)]
        b = [(100.7, 6, 0, 2, 6), (102.7, 4, 0, 2, 10)]   # 101초 구간 보고가 빠졌다
        merged = bc.merge_rows([a, b])
        self.assertEqual([round(row[0]) for row in merged], [100, 101, 102])
        self.assertEqual(merged[0][1:], (16, 0, 6, 16))
        self.assertEqual(merged[1][1:], (20, 0, 8, 36))     # b의 접수 합계는 앞의 6을 이어서 쓴다
        self.assertEqual(merged[2][1:], (9, 1, 4, 45))

    def test_워커가_하나면_입력과_같다(self):
        a = [(100.2, 10, 0, 4, 10), (101.2, 20, 0, 8, 30)]
        merged = bc.merge_rows([a])
        self.assertEqual([row[1:] for row in merged], [row[1:] for row in a])


class OrderAndAnomalyTest(unittest.TestCase):
    def test_파일_순서를_그대로_쓰고_정렬하지_않는다(self):
        path = write("\n".join([metric_line(12.0, 5, 0, 4, 10), metric_line(11.0, 6, 0, 5, 16)]) + "\n")
        rows = bc.parse_metrics(path)
        self.assertEqual([r[4] for r in rows], [10, 16])

    def test_종료_직전_마지막_줄만_시각이_거꾸로면_그_줄은_마지막_보고로_쓰고_표시만_한다(self):
        rows = [(100.0, 5, 0, 4, 10), (101.0, 5, 0, 4, 20), (100.4, 1, 0, 0, 25)]
        status, notes = bc.classify_rows(rows)
        self.assertEqual(status, "final_flush_row")
        fixed = bc.monotone_rows(rows)
        self.assertEqual([r[0] for r in fixed], [100.0, 101.0, 101.0])
        self.assertEqual(fixed[-1][4], 25)

    def test_접수_합계가_줄어들면_무효이다(self):
        status, notes = bc.classify_rows([(100.0, 5, 0, 4, 10), (101.0, 5, 0, 4, 9)])
        self.assertEqual(status, "invalid")
        self.assertTrue(any("acceptedTotal" in n for n in notes))

    def test_중간에서_시각이_거꾸로_가면_무효이다(self):
        status, notes = bc.classify_rows([(100.0, 5, 0, 4, 10), (102.0, 5, 0, 4, 20), (101.0, 5, 0, 4, 30), (103.0, 5, 0, 4, 40)])
        self.assertEqual(status, "invalid")

    def test_정상_로그는_ok이다(self):
        self.assertEqual(bc.classify_rows([(100.0, 5, 0, 4, 10), (101.0, 5, 0, 4, 20)])[0], "ok")

    def test_같은_초에_찍힌_두_줄은_워커_안에서_더하고_접수_합계는_뒤_값을_쓴다(self):
        a = [(100.1, 10, 0, 4, 10), (100.9, 5, 0, 2, 15)]
        b = [(100.5, 6, 0, 2, 6)]
        merged = bc.merge_rows([a, b])
        self.assertEqual(merged[0][1:], (21, 0, 8, 21))


class NotOwnedRangeTest(unittest.TestCase):
    def test_워커_하나면_NOT_OWNED는_정확히_0이어야_한다(self):
        self.assertTrue(bc.not_owned_ok([0], trades=100))
        self.assertFalse(bc.not_owned_ok([1], trades=100))

    def test_워커_둘이면_서버마다_같은_수이고_총_체결의_2배_이하여야_한다(self):
        self.assertTrue(bc.not_owned_ok([54, 54], trades=100))
        self.assertTrue(bc.not_owned_ok([0, 0], trades=100))
        self.assertFalse(bc.not_owned_ok([54, 53], trades=100), "서버마다 수가 다르면 주문 오라우팅 가능성")
        self.assertFalse(bc.not_owned_ok([101, 101], trades=100), "교차 체결은 체결 수를 넘을 수 없다")


class FillCompleteTest(unittest.TestCase):
    def test_누계_체결이_총_체결의_2배면_완전이다(self):
        rows = [(100.0, 1, 0, 6, 10), (101.0, 1, 0, 4, 11)]
        self.assertTrue(bc.fills_complete(rows, trades=5))
        self.assertFalse(bc.fills_complete(rows, trades=6))

if __name__ == "__main__":
    unittest.main()
