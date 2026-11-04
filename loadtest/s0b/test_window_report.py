import datetime
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import window_report as wr

KST = datetime.timezone(datetime.timedelta(hours=9))
T0 = datetime.datetime(2026, 11, 4, 10, 0, 0, tzinfo=KST).timestamp()   # PACED_START 시각
N, C, RATE = 10, 100, 1000


def iso(ts):
    return datetime.datetime.fromtimestamp(ts, KST).isoformat(timespec="milliseconds")


def metric_line(ts, accepted, rejected, fill, total):
    return ("%s  INFO 1 --- [etrics-reporter] c.f.s.a.w.l.MetricsAccountResultListener : metrics accepted=%d rejected=%d fill=%d dup=0 acceptedTotal=%d"
            % (iso(ts), accepted, rejected, fill, total))


def make_dir(root, steady_seconds, per_second=None, rejected_at=None, warn_at=None, not_owned_at=None, gc_after=None, rss=None, lateness=None):
    """쌓기 5초(N x C건) 뒤 steady_seconds초 동안 초당 RATE건 접수·체결. per_second(sec)=(접수, 체결)로 초별 덮어쓰기."""
    os.makedirs(os.path.join(root, "logs"))
    with open(os.path.join(root, "params.txt"), "w") as f:
        f.write("ACCTS=%d C=%d RATE=%d TOTAL_TRADES=0\n" % (N, C, RATE))
    with open(os.path.join(root, "paced.out"), "w") as f:
        f.write("PACED_START %d\n" % int(T0 * 1000))
        for minute, (mx, over) in (lateness or {}).items():
            f.write("LATENESS_MINUTE minute=%d maxLatenessMs=%d over1sCount=%d\n" % (minute, mx, over))
        f.write("PACED_DONE sent=%d elapsedMs=%d maxLatenessMs=3 avgLatenessMs=0 over1sCount=0\n" % (steady_seconds * RATE, steady_seconds * 1000))
    lines = []
    total = 0
    for i in range(5):
        step = N * C // 5
        total += step
        lines.append(metric_line(T0 - 10 + i, step, 0, 0, total))
    for sec in range(steady_seconds):
        acc, fill = per_second(sec) if per_second else (RATE, RATE)
        total += acc
        rej = (rejected_at or {}).get(sec, 0)
        lines.append(metric_line(T0 + sec, acc, rej, fill, total))
        if warn_at and sec in warn_at:
            lines.append("%s  WARN 1 --- [ main] x : [계좌] 스냅샷 쓰기 큐가 가득 차 이번 회차 스킵: appliedSeq=1" % iso(T0 + sec + 0.5))
        if not_owned_at and sec in not_owned_at:
            for _ in range(not_owned_at[sec]):
                lines.append("%s  WARN 1 --- [ main] x : 담당 슬롯이 아닌 계좌로 이벤트가 왔습니다" % iso(T0 + sec + 0.6))
    with open(os.path.join(root, "logs", "account.log"), "w") as f:
        f.write("\n".join(lines) + "\n")
    if gc_after is not None:   # gc_after(sec) -> 직후 힙 MB, 10초마다 한 줄
        gc = []
        for sec in range(0, steady_seconds, 10):
            stamp = datetime.datetime.fromtimestamp(T0 + sec, KST).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+0900"
            gc.append("[%s][%ds] GC(%d) Pause Young (Normal) (G1 Evacuation Pause) 300M->%dM(516M) 1.686ms" % (stamp, sec, sec // 10, gc_after(sec)))
        with open(os.path.join(root, "logs", "gc-account.log"), "w") as f:
            f.write("\n".join(gc) + "\n")
    if rss is not None:        # rss(sec) -> account RSS KB, 5초마다
        with open(os.path.join(root, "cpu.log"), "w") as f:
            for sec in range(0, steady_seconds, 5):
                f.write("%d account 99 0:10.00 %d\n" % (int(T0 + sec), rss(sec)))
                f.write("%d matching 98 0:10.00 %d\n" % (int(T0 + sec), 3_000_000))


class WindowReportTest(unittest.TestCase):
    def report(self, **kw):
        with tempfile.TemporaryDirectory() as d:
            window = kw.pop("window", 60)
            span = kw.pop("span", 60)
            make_dir(d, **kw)
            return wr.build_report(d, window_seconds=window, trend_span=span)

    def test_정상이면_창마다_통과하고_창_수는_유지_시간을_창으로_나눈_몫이다(self):
        r = self.report(steady_seconds=300)
        self.assertEqual(5, len(r["windows"]))
        self.assertTrue(all(w["ok"] for w in r["windows"]), r["windows"])
        self.assertTrue(r["all_windows_ok"])
        self.assertAlmostEqual(1.0, r["windows"][2]["accept_ratio"], places=3)
        self.assertEqual(0, r["ignored_tail_seconds"])

    def test_창보다_짧은_꼬리는_판정하지_않고_초를_알려준다(self):
        r = self.report(steady_seconds=330)
        self.assertEqual(5, len(r["windows"]))
        self.assertEqual(30, r["ignored_tail_seconds"])

    def test_접수가_모자란_창만_접수_비율이_실패한다(self):
        r = self.report(steady_seconds=300, per_second=lambda s: (400, 400) if 120 <= s < 180 else (RATE, RATE))
        ok = [w["checks"]["accept_ratio"] for w in r["windows"]]
        self.assertEqual([True, True, False, True, True], ok)
        self.assertFalse(r["all_windows_ok"])

    def test_스냅샷_경고는_그_시각의_창에만_센다(self):
        r = self.report(steady_seconds=300, warn_at={190})
        self.assertEqual([0, 0, 0, 1, 0], [w["snapshot_warn"] for w in r["windows"]])
        self.assertFalse(r["windows"][3]["ok"])

    def test_거부는_NOT_OWNED를_빼고_센다(self):
        r = self.report(steady_seconds=300, rejected_at={70: 5}, not_owned_at={70: 3})
        self.assertEqual([0, 2, 0, 0, 0], [w["rejected"] for w in r["windows"]])
        self.assertFalse(r["windows"][1]["checks"]["no_rejects"])

    def test_NOT_OWNED만큼만_거부면_거부_0이다(self):
        r = self.report(steady_seconds=300, rejected_at={70: 3}, not_owned_at={70: 3})
        self.assertEqual(0, r["windows"][1]["rejected"])
        self.assertTrue(r["all_windows_ok"])

    def test_창_안에서_미체결이_목표의_1퍼센트를_넘게_흐르면_변화_검사가_실패한다(self):
        # 접수는 그대로 1000, 체결을 절반으로 줄이면 미체결이 초당 500씩 늘어 60초면 30,000(목표 1,000의 30배)
        r = self.report(steady_seconds=300, per_second=lambda s: (RATE, 500) if 60 <= s < 120 else (RATE, RATE))
        self.assertFalse(r["windows"][1]["checks"]["open_change"])
        self.assertTrue(r["windows"][0]["checks"]["open_change"])

    def test_GC_직후_힙이_계속_오르면_누적_의심이_켜지고_평평하면_꺼진다(self):
        up = self.report(steady_seconds=300, gc_after=lambda s: 100 + s // 2)     # 100 -> 249 MB
        self.assertTrue(up["trend"]["gc_after_suspect"])
        self.assertGreater(up["trend"]["gc_after_ratio"], 1.1)
        flat = self.report(steady_seconds=300, gc_after=lambda s: 100 + (s % 20) // 10)
        self.assertFalse(flat["trend"]["gc_after_suspect"])
        self.assertEqual(5, len([w for w in flat["windows"] if w["gc_after_mb_min"] is not None]))

    def test_RSS가_1_1배를_넘게_오르면_역할별로_의심이_켜진다(self):
        r = self.report(steady_seconds=300, rss=lambda s: 1_000_000 + s * 1000)   # 1.0GB -> 1.3GB
        self.assertTrue(r["trend"]["rss"]["account"]["suspect"])
        self.assertFalse(r["trend"]["rss"]["matching"]["suspect"])
        self.assertIn("rss_mb_max", r["windows"][0])

    def test_분_단위_발신_지연이_있으면_창별_최대와_1초_초과를_모은다(self):
        r = self.report(steady_seconds=300, lateness={0: (12, 0), 1: (40, 0), 2: (9, 0), 3: (2000, 4), 4: (5, 0)})
        self.assertEqual(12, r["windows"][0]["lateness_max_ms"])
        self.assertEqual(40, r["windows"][1]["lateness_max_ms"])
        self.assertEqual(4, r["windows"][3]["lateness_over_1s"])
        self.assertFalse(r["windows"][3]["checks"]["lateness"])
        self.assertTrue(r["windows"][0]["checks"]["lateness"])

    def test_분_단위_지연_줄이_없으면_지연_검사는_null이고_통과를_막지_않는다(self):
        r = self.report(steady_seconds=300)
        self.assertIsNone(r["windows"][0]["lateness_max_ms"])
        self.assertTrue(r["all_windows_ok"])

    def test_1분_추세_로그가_있으면_창별_남은_공간_최솟값과_스냅샷_갱신_경과_최댓값을_붙인다(self):
        with tempfile.TemporaryDirectory() as d:
            make_dir(d, steady_seconds=120)
            trend = os.path.join(d, "trend.log")
            with open(trend, "w") as f:
                f.write("START free_gb=90 ts=%d min_gb=20\n" % int(T0 - 60))
                for sec in (0, 60, 90):
                    f.write("ts=%d free_gb=%d acct_dir_mb=1 match_dir_mb=1 a0_snap_mtime=%d a0_snap_bytes=100 m_snap_mtime=%d m_snap_bytes=50\n"
                            % (T0 + sec, 80 - sec // 30, T0 + sec - (3 if sec != 90 else 40), T0 + sec - 1))
            r = wr.build_report(d, window_seconds=60, trend_span=30, trend_log=trend)
        self.assertEqual(80, r["windows"][0]["free_gb_min"])
        self.assertEqual(77, r["windows"][1]["free_gb_min"])
        self.assertEqual(3, r["windows"][0]["snapshot_age_max_s"])
        self.assertEqual(40, r["windows"][1]["snapshot_age_max_s"])


if __name__ == "__main__":
    unittest.main()
