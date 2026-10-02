#!/usr/bin/env python3
"""replay-e2e.js 의 X-Sched-At 헤더(구간 분해 측정의 G용) 테스트. 로컬 서버에 k6로 요청 몇 건만 보낸다(5건, 초당 50건).
k6 가 없으면 건너뛴다.

실행: python3 -m unittest loadtest.test_sched_header -v   (저장소 루트에서)
"""
import json
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

SCRIPT = Path(__file__).with_name("replay-e2e.js")


class CaptureHandler(BaseHTTPRequestHandler):
    seen = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        CaptureHandler.seen.append(dict(self.headers))
        self.send_response(202)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


@unittest.skipIf(shutil.which("k6") is None, "k6 가 없다")
class SchedHeaderTest(unittest.TestCase):
    def run_k6(self, env_extra):
        CaptureHandler.seen = []
        server = HTTPServer(("127.0.0.1", 0), CaptureHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                input_file = Path(tmp) / "in.jsonl"
                lines = [json.dumps({"side": "BUY", "accountId": 2001, "stockCode": "A000001", "price": 100, "quantity": 1,
                                     "requestId": f"t-{i}"}) for i in range(5)]
                input_file.write_text("\n".join(lines) + "\n")
                env = {"BASE": f"http://127.0.0.1:{server.server_address[1]}", "INPUT_FILE": str(input_file), "N": "5",
                       "RATE": "50", "PRE_ALLOCATED_VUS": "2", "MAX_VUS": "5", "PASS": "t", "PATH": "/usr/bin:/bin:/opt/homebrew/bin"}
                env.update(env_extra)
                started = time.time()
                subprocess.run(["k6", "run", "--quiet", str(SCRIPT)], env=env, capture_output=True, timeout=60, check=True)
                self.assertLess(time.time() - started, 30)
        finally:
            server.shutdown()
        return [{k.lower(): v for k, v in headers.items()} for headers in CaptureHandler.seen]

    def test_header_is_attached_with_micros_and_follows_the_schedule(self):
        seen = self.run_k6({"SCHED_HEADER": "1"})
        self.assertEqual(len(seen), 5)
        values = sorted(int(headers["x-sched-at"]) for headers in seen)
        now_micros = time.time() * 1e6
        self.assertLess(abs(now_micros - values[-1]), 60 * 1e6)  # epoch 마이크로초
        steps = [b - a for a, b in zip(values, values[1:])]
        for step in steps:
            self.assertAlmostEqual(step, 20000, delta=2)  # 초당 50건이면 20ms = 20,000µs 간격

    def test_header_is_absent_by_default(self):
        seen = self.run_k6({})
        self.assertEqual(len(seen), 5)
        for headers in seen:
            self.assertNotIn("x-sched-at", headers)


if __name__ == "__main__":
    unittest.main()
