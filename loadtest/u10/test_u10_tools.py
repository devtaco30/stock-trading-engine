#!/usr/bin/env python3
"""handover-steps.py, assert-moved.py 단위 테스트. 합성 맵 기록과 합성 로그만 쓴다(Kafka·프로세스 없음).

실행: python3 -m unittest loadtest/u10/test_u10_tools.py -v   (저장소 루트에서)
"""
import datetime
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).parent
POD0 = 'aeron:udp?endpoint=localhost:20020'
POD1 = 'aeron:udp?endpoint=localhost:20021'


BASE_MS = int(datetime.datetime.fromisoformat('2026-10-29T19:39:00+09:00').timestamp() * 1000)


def epoch(ms):
    return BASE_MS + ms


def log_line(second_ms, text):
    return '2026-10-29T19:39:%02d.%03d+09:00  INFO 1 --- [x] c.f.Test : %s\n' % (second_ms // 1000, second_ms % 1000, text)


def step_logs(slot, base_ms, complete=True):
    lines = [log_line(base_ms, '[호가창 이동] 요청 보냄: 이동 번호=%d 종목 slot=%d' % (slot, slot))]
    if complete:
        lines.append(log_line(base_ms + 5, '[호가창 이동] 응답 완성: 이동 번호=%d 종목 slot=%d 종목 수=1' % (slot, slot)))
        lines.append(log_line(base_ms + 30, '[호가창 이동] 적재 확정: 이동 번호=%d 종목 slot=%d' % (slot, slot)))
        lines.append(log_line(base_ms + 40, '[호가창 이동] 담당 켜짐: 종목 slot=%d' % slot))
    return lines


def run_script(script, *args):
    return subprocess.run([sys.executable, str(HERE / script), *map(str, args)], capture_output=True, text=True)


class HandoverStepsTest(unittest.TestCase):
    def run_steps(self, map_rows, log_lines):
        with tempfile.TemporaryDirectory() as tmp:
            map_path = Path(tmp) / 'map.txt'
            log_path = Path(tmp) / 'matching-1.log'
            map_path.write_text(''.join('\t'.join(map(str, row)) + '\n' for row in map_rows))
            log_path.write_text(''.join(log_lines))
            return run_script('handover-steps.py', map_path, log_path)

    def test_모든_slot이_모든_구간을_갖추면_경고_없이_성공한다(self):
        logs = step_logs(32, 1000) + step_logs(33, 1010)
        rows = [(32, epoch(1100), POD1), (33, epoch(1110), POD1)]
        result = self.run_steps(rows, logs)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertNotIn('경고', result.stdout)

    def test_구간을_계산하지_못한_slot이_있으면_경고를_내고_실패한다(self):
        logs = step_logs(32, 1000) + step_logs(33, 1010, complete=False)
        rows = [(32, epoch(1100), POD1), (33, epoch(1110), POD1)]
        result = self.run_steps(rows, logs)
        self.assertIn('경고', result.stdout)
        self.assertNotEqual(0, result.returncode, result.stdout)

    def test_맵_기록의_timestampType을_출력한다(self):
        logs = step_logs(32, 1000)
        rows = [(32, epoch(1100), POD1, 'LogAppendTime')]
        result = self.run_steps(rows, logs)
        self.assertIn('LogAppendTime', result.stdout)

    def test_timestampType이_없는_옛_형식_기록도_읽는다(self):
        logs = step_logs(32, 1000)
        rows = [(32, epoch(1100), POD1)]
        result = self.run_steps(rows, logs)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)


class AssertMovedTest(unittest.TestCase):
    def run_assert(self, rows, slot):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'map.txt'
            path.write_text(''.join('\t'.join(map(str, row)) + '\n' for row in rows))
            return run_script('assert-moved.py', path, slot, POD0, POD1)

    def test_pod0에서_pod1로_바뀐_slot은_통과한다(self):
        rows = [(32, 100, POD0), (32, 200, POD0 + ';moving'), (32, 300, POD1)]
        self.assertEqual(0, self.run_assert(rows, 32).returncode)

    def test_이동_표시_없이_pod0_값에서_pod1_값으로_바뀐_slot도_통과한다(self):
        rows = [(32, 100, POD0), (32, 300, POD1)]
        self.assertEqual(0, self.run_assert(rows, 32).returncode)

    def test_이동하지_않고_pod0에_남은_slot은_실패한다(self):
        rows = [(32, 100, POD0), (33, 100, POD0), (33, 200, POD0 + ';moving'), (33, 300, POD1)]
        result = self.run_assert(rows, 32)
        self.assertNotEqual(0, result.returncode, result.stdout)

    def test_처음부터_pod1이_맡은_slot은_실패한다(self):
        rows = [(32, 100, POD1)]
        self.assertNotEqual(0, self.run_assert(rows, 32).returncode)

    def test_맵에_없는_slot은_실패한다(self):
        rows = [(33, 100, POD0)]
        self.assertNotEqual(0, self.run_assert(rows, 32).returncode)


if __name__ == '__main__':
    unittest.main()
