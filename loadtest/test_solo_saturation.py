#!/usr/bin/env python3
"""solo_saturation.py(k6 단독 포화점 리허설 도구) 단위 테스트. 앱·Docker·높은 부하 없이 돈다.
더미 서버 테스트만 로컬 포트에 요청 몇 건을 보낸다.

실행: python3 -m unittest loadtest.test_solo_saturation -v   (저장소 루트에서)
"""
import csv
import http.client
import json
import os
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

import importlib.util

MODULE_PATH = Path(__file__).with_name("solo_saturation.py")
spec = importlib.util.spec_from_file_location("solo_saturation", MODULE_PATH)
ss = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ss)

SERVER_SOURCE = Path(__file__).with_name("DummyOrderServer.java")


class RatesTest(unittest.TestCase):
    def test_default_rates(self):
        self.assertEqual(ss.DEFAULT_RATES, [192, 500, 1000, 2000, 4000, 6000])

    def test_parse_rates_accepts_numbers_in_given_order(self):
        self.assertEqual(ss.parse_rates("192 500 8000"), [192, 500, 8000])

    def test_parse_rates_rejects_bad_input(self):
        for bad in ("", "abc", "192 -5", "0", "192 1.5"):
            with self.assertRaises(ValueError, msg=bad):
                ss.parse_rates(bad)

    def test_parse_rates_rejects_duplicates(self):
        with self.assertRaises(ValueError):
            ss.parse_rates("192 192")

    def test_n_for_rate_is_rate_times_seconds_capped_by_input_lines(self):
        self.assertEqual(ss.n_for_rate(192, 20, 100000), 3840)
        self.assertEqual(ss.n_for_rate(4000, 20, 100000), 80000)
        self.assertEqual(ss.n_for_rate(6000, 20, 100000), 100000)
        self.assertEqual(ss.n_for_rate(8000, 20, 100000), 100000)

    def test_estimate_seconds_sums_run_and_overheads(self):
        # 속도마다: 도착 시간(N/속도 올림) + 마무리 여유 5초(replay-e2e.js) + 시작 전 SETTLE + 정리 + 속도 사이 GAP
        seconds = ss.estimate_seconds([192, 6000], arrival_sec=20, input_lines=100000, settle_sec=5, teardown_sec=3, gap_sec=5,
                                      server_start_sec=3)
        run_192 = 20 + 5
        run_6000 = 17 + 5  # 100000 / 6000 = 16.7 → 17
        overhead = 5 + 3 + 5
        self.assertEqual(seconds, 3 + (run_192 + overhead) + (run_6000 + overhead))


class SummaryTest(unittest.TestCase):
    def summary(self, dropped=0, failed=0, reqs=3840, avg=0.4, p95=0.9, p99=1.4, mx=9.0):
        return {"metrics": {
            "dropped_iterations": {"count": dropped, "rate": 0},
            "http_req_failed": {"passes": failed, "fails": reqs - failed, "value": 0},
            "http_reqs": {"count": reqs, "rate": 190.0},
            "http_req_duration": {"avg": avg, "p(95)": p95, "p(99)": p99, "max": mx},
        }}

    def test_reads_flat_k6_export(self):
        facts = ss.read_summary(self.summary(dropped=7, failed=2))
        self.assertEqual((facts["dropped"], facts["http_failed"], facts["http_reqs"]), (7, 2, 3840))
        self.assertEqual((facts["lat_avg_ms"], facts["lat_p95_ms"], facts["lat_p99_ms"], facts["lat_max_ms"]), (0.4, 0.9, 1.4, 9.0))

    def test_reads_values_wrapped_export(self):
        wrapped = {"metrics": {name: {"values": value} for name, value in self.summary(dropped=1)["metrics"].items()}}
        self.assertEqual(ss.read_summary(wrapped)["dropped"], 1)

    def test_missing_summary_gives_none(self):
        self.assertIsNone(ss.read_summary(None))


class ArrivalRateTest(unittest.TestCase):
    def test_rate_from_server_timestamps(self):
        stats = {"count": 1001, "firstNanos": 0, "lastNanos": 5_000_000_000}
        self.assertAlmostEqual(ss.actual_arrival_rate(stats), 200.0)

    def test_rate_needs_two_requests(self):
        self.assertIsNone(ss.actual_arrival_rate({"count": 1, "firstNanos": 5, "lastNanos": 5}))
        self.assertIsNone(ss.actual_arrival_rate(None))


class CpuTest(unittest.TestCase):
    def test_parse_ps_time(self):
        self.assertAlmostEqual(ss.parse_ps_time("0:05.25"), 5.25)
        self.assertAlmostEqual(ss.parse_ps_time("1:02.50"), 62.5)
        self.assertAlmostEqual(ss.parse_ps_time("1:02:03"), 3723.0)
        self.assertIsNone(ss.parse_ps_time(""))
        self.assertIsNone(ss.parse_ps_time("abc"))

    def test_cpu_percent_is_delta_over_elapsed_one_core(self):
        self.assertAlmostEqual(ss.cpu_percent(10.0, 11.5, 1.0), 150.0)
        self.assertAlmostEqual(ss.cpu_percent(10.0, 10.2, 0.5), 40.0)

    def test_cpu_percent_ignores_bad_deltas(self):
        self.assertIsNone(ss.cpu_percent(None, 1.0, 1.0))
        self.assertIsNone(ss.cpu_percent(1.0, 2.0, 0.0))
        self.assertEqual(ss.cpu_percent(5.0, 4.0, 1.0), 0.0)  # 프로세스가 끝나 누적이 줄어든 경우

    def test_idle_from_total_busy(self):
        # 전체 사용 150%(1코어=100%)이고 코어가 10개면 사용 15%, idle 85%
        self.assertAlmostEqual(ss.idle_percent(150.0, 10), 85.0)
        self.assertEqual(ss.idle_percent(2000.0, 10), 0.0)

    def test_sampler_writes_header_and_rows_quickly(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "cpu.csv"
            started = time.monotonic()
            ss.run_sampler(out, interval=0.2, pids={"self": os.getpid()}, samples=4)
            elapsed = time.monotonic() - started
            rows = list(csv.reader(open(out, encoding="utf-8")))
        self.assertEqual(rows[0], ss.SAMPLE_HEADER)
        self.assertEqual(len(rows), 1 + 4)
        self.assertLess(elapsed, 3.0)  # 한 번 수집이 8~9초 걸리던 샘플러와 달리 간격에 가깝다


class SaturationJudgeTest(unittest.TestCase):
    """판정은 dropped_iterations, 실제 도착률의 목표 이탈(5% 이상)로만 한다. k6 CPU 는 기록만 한다(protocol-c 4-3절)."""

    def test_clean_rate_is_not_saturated(self):
        verdict = ss.judge_rate(dropped=0, http_failed=0, arrival=1000.0, target_rate=1000)
        self.assertEqual(verdict["reasons"], [])
        self.assertFalse(verdict["saturated"])

    def test_dropped_iterations_saturate(self):
        verdict = ss.judge_rate(dropped=1, http_failed=0, arrival=1000.0, target_rate=1000)
        self.assertEqual(verdict["reasons"], ["k6_dropped"])
        self.assertTrue(verdict["saturated"])

    def test_arrival_off_target_by_5_percent_or_more_saturates(self):
        self.assertEqual(ss.judge_rate(0, 0, 950.0, 1000)["reasons"], ["arrival_off"])   # 정확히 5% 낮음
        self.assertEqual(ss.judge_rate(0, 0, 1050.0, 1000)["reasons"], ["arrival_off"])  # 정확히 5% 높음
        self.assertEqual(ss.judge_rate(0, 0, 600.0, 1000)["reasons"], ["arrival_off"])

    def test_arrival_within_5_percent_is_fine(self):
        self.assertEqual(ss.judge_rate(0, 0, 950.1, 1000)["reasons"], [])
        self.assertEqual(ss.judge_rate(0, 0, 1049.9, 1000)["reasons"], [])
        self.assertEqual(ss.judge_rate(0, 0, 6001.0, 6000)["reasons"], [])

    def test_missing_arrival_is_skipped(self):
        self.assertEqual(ss.judge_rate(0, 0, None, 1000)["reasons"], [])

    def test_both_reasons_listed_in_order(self):
        self.assertEqual(ss.judge_rate(3, 0, 500.0, 1000)["reasons"], ["k6_dropped", "arrival_off"])

    def test_k6_cpu_is_not_a_saturation_reason(self):
        # 시그니처에 CPU 가 없다. build_row 가 CPU 값이 아무리 높아도 판정에 넘기지 않는 것은 TableTest 에서 확인한다
        import inspect
        self.assertNotIn("cpu", " ".join(inspect.signature(ss.judge_rate).parameters).lower())

    def test_http_failure_is_reported_as_rehearsal_anomaly_not_saturation(self):
        verdict = ss.judge_rate(dropped=0, http_failed=4, arrival=1000.0, target_rate=1000)
        self.assertFalse(verdict["saturated"])
        self.assertEqual(verdict["anomalies"], ["http_failed"])


class TableTest(unittest.TestCase):
    def row(self, rate, saturated, reasons, dropped=0, failed=0, arrival=None, cpu=(10.0, 20.0), idle=80.0):
        return {"rate": rate, "n": rate * 20, "dropped": dropped, "http_failed": failed, "arrival": arrival or float(rate),
                "k6_cpu_mean": cpu[0], "k6_cpu_max": cpu[1], "idle_min": idle, "lat_avg_ms": 0.4, "lat_p95_ms": 0.9,
                "lat_p99_ms": 1.4, "lat_max_ms": 9.0, "saturated": saturated, "reasons": reasons, "anomalies": []}

    def test_table_has_one_row_per_rate_and_first_saturated_rate(self):
        rows = [self.row(192, False, []), self.row(1000, False, []), self.row(4000, True, ["arrival_off"], arrival=3700.0, cpu=(55.0, 80.0)),
                self.row(6000, True, ["k6_dropped", "arrival_off"], dropped=300)]
        text = ss.build_table(rows)
        for rate in (192, 1000, 4000, 6000):
            self.assertIn(f"| {rate} |", text)
        self.assertIn("포화한 첫 속도: 4000건/초", text)
        self.assertIn("k6_dropped, arrival_off", text)

    def test_table_when_nothing_saturates(self):
        text = ss.build_table([self.row(192, False, []), self.row(500, False, [])])
        self.assertIn("시험한 속도에서 포화 없음", text)

    def test_table_does_not_hardcode_quoting_ratio(self):
        text = ss.build_table([self.row(192, False, []), self.row(4000, True, ["arrival_off"])])
        self.assertIn("인용 비율은 리허설 데이터를 본 뒤 선언 문서에 정한다", text)
        for forbidden in ("까지만 인용", "% 이하", "신뢰구간"):
            self.assertNotIn(forbidden, text)

    def test_table_header_columns(self):
        text = ss.build_table([self.row(192, False, [])])
        header = next(line for line in text.splitlines() if line.startswith("| 속도"))
        for column in ("dropped", "http 실패", "실제 도착률", "k6 CPU", "전체 idle", "지연", "판정"):
            self.assertIn(column, header)

    def test_empty_rows(self):
        self.assertIn("결과가 없다", ss.build_table([]))

    def test_build_row_from_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = Path(tmp)
            (run / "k6-1000.json").write_text(json.dumps({"metrics": {
                "dropped_iterations": {"count": 0}, "http_req_failed": {"passes": 0, "fails": 100}, "http_reqs": {"count": 100},
                "http_req_duration": {"avg": 0.3, "p(95)": 0.8, "p(99)": 1.2, "max": 5.0}}}))
            (run / "server-1000.json").write_text(json.dumps({"count": 1001, "firstNanos": 0, "lastNanos": 1_000_000_000}))
            with open(run / "cpu-1000.csv", "w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(ss.SAMPLE_HEADER)
                writer.writerow([1.0, "", "", "", ""])
                writer.writerow([2.0, 30.0, 5.0, 100.0, 90.0])
                writer.writerow([3.0, 50.0, 5.0, 120.0, 88.0])
                writer.writerow([4.0, 45.0, 5.0, 130.0, 87.0])
            row = ss.build_row(run, 1000, 20000)
        self.assertEqual(row["rate"], 1000)
        self.assertAlmostEqual(row["arrival"], 1000.0)
        self.assertEqual(row["reasons"], [])  # CPU 가 40% 이상이어도 포화 이유가 아니다(기록만)
        self.assertFalse(row["saturated"])
        self.assertEqual(row["k6_cpu_max"], 50.0)
        self.assertEqual(row["idle_min"], 87.0)

    def test_build_row_marks_arrival_far_from_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = Path(tmp)
            (run / "k6-1000.json").write_text(json.dumps({"metrics": {
                "http_req_failed": {"passes": 0, "fails": 100}, "http_reqs": {"count": 100},
                "http_req_duration": {"avg": 0.3, "p(95)": 0.8, "p(99)": 1.2, "max": 5.0}}}))
            (run / "server-1000.json").write_text(json.dumps({"count": 801, "firstNanos": 0, "lastNanos": 1_000_000_000}))
            row = ss.build_row(run, 1000, 20000)
        self.assertEqual(row["reasons"], ["arrival_off"])
        self.assertEqual(row["dropped"], 0)  # dropped_iterations 키가 없으면 0

    def test_table_note_says_cpu_is_recorded_only(self):
        text = ss.build_table([self.row(192, False, [])])
        self.assertIn("k6 CPU는 기록만 하고 포화 판정에 쓰지 않는다", text)
        self.assertNotIn("k6_cpu", text)

    def test_build_row_without_files_is_marked_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            row = ss.build_row(Path(tmp), 500, 10000)
        self.assertTrue(row["missing"])


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class DummyServerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = free_port()
        cls.process = subprocess.Popen(["java", "-XX:+UseSerialGC", str(SERVER_SOURCE), str(cls.port)],
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", cls.port), timeout=0.3):
                    break
            except OSError:
                time.sleep(0.2)
        else:
            cls.process.kill()
            raise RuntimeError("더미 서버가 20초 안에 열리지 않았다")

    @classmethod
    def tearDownClass(cls):
        cls.process.terminate()
        cls.process.wait(timeout=10)

    def connection(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        self.addCleanup(conn.close)
        return conn

    def test_replies_202_immediately_over_keep_alive_and_counts(self):
        conn = self.connection()
        conn.request("POST", "/__reset")
        conn.getresponse().read()
        body = json.dumps({"accountId": 2001, "stockCode": "A000001", "orderType": "LIMIT", "price": 100, "quantity": 1,
                           "requestId": "x"})
        started = time.monotonic()
        for index in range(20):
            path = "/api/v1/orders/buy" if index % 2 == 0 else "/api/v2/orders/sell"
            conn.request("POST", path, body=body, headers={"Authorization": "Bearer t", "Content-Type": "application/json"})
            response = conn.getresponse()
            response.read()
            self.assertEqual(response.status, 202)
        self.assertLess(time.monotonic() - started, 2.0)
        conn.request("GET", "/__stats")
        stats = json.loads(conn.getresponse().read())
        self.assertEqual(stats["count"], 20)
        self.assertGreater(stats["lastNanos"], stats["firstNanos"])

    def test_reset_clears_counter_and_health_answers(self):
        conn = self.connection()
        conn.request("GET", "/__health")
        health = conn.getresponse()
        health.read()
        self.assertEqual(health.status, 200)
        conn.request("POST", "/api/v1/orders/buy", body="{}")
        conn.getresponse().read()
        conn.request("POST", "/__reset")
        conn.getresponse().read()
        conn.request("GET", "/__stats")
        self.assertEqual(json.loads(conn.getresponse().read())["count"], 0)

    def test_unknown_path_is_404_and_not_counted(self):
        conn = self.connection()
        conn.request("POST", "/__reset")
        conn.getresponse().read()
        conn.request("POST", "/nothing")
        response = conn.getresponse()
        response.read()
        self.assertEqual(response.status, 404)
        conn.request("GET", "/__stats")
        self.assertEqual(json.loads(conn.getresponse().read())["count"], 0)


if __name__ == "__main__":
    unittest.main()
