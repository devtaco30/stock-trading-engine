#!/usr/bin/env python3
"""12-4절 단계 길이 하한과 H4 입력 크기 계산 시험. 숫자 값은 설계서 12-4절의 예시를 그대로 쓴다.
실행: python3 -I loadtest/test_step_plan.py   (저장소 루트에서)
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import step_plan as sp  # noqa: E402


class StepFloorTest(unittest.TestCase):
    def test_설계서_예시_초과분_2000이면_66_56초(self):
        # 12-4절: (1,024 + 65,536 + 1,024 + 65,536) ÷ 2,000 = 66.56 (Aeron 구간 제외)
        self.assertAlmostEqual(66.56, sp.step_floor_seconds([1024, 65536, 1024, 65536], 7000, 5000), places=2)

    def test_처리_속도_이하의_도착률이면_큐가_차지_않아_하한이_없다(self):
        self.assertIsNone(sp.step_floor_seconds([1024], 5000, 5000))
        self.assertIsNone(sp.step_floor_seconds([1024], 4000, 5000))

    def test_큐_용량이_음수나_빈_목록이면_예외(self):
        with self.assertRaises(ValueError):
            sp.step_floor_seconds([], 7000, 5000)
        with self.assertRaises(ValueError):
            sp.step_floor_seconds([-1], 7000, 5000)

    def test_큐_용량은_2의_거듭제곱으로_올려_잡는다(self):
        # 12-4절: api 큐 설정값이 2의 거듭제곱이 아니면 실제 칸 수가 더 크다.
        self.assertEqual(65536, sp.ring_capacity(50000))
        self.assertEqual(65536, sp.ring_capacity(65536))
        self.assertEqual(1024, sp.ring_capacity(1024))
        self.assertEqual(1, sp.ring_capacity(1))


class RequiredOrdersTest(unittest.TestCase):
    def test_가장_높은_속도_x_도착_시간_x_패스_수(self):
        # H4: step load test의 가장 높은 속도 × 도착 시간 × 패스 수
        self.assertEqual(5000 * 20 * 4, sp.required_orders(5000, 20, 4))

    def test_설계서_예시_초당_5000건이면_20초에_10만건이_바닥난다(self):
        # 12-4절: 입력 100,000건은 초당 5,000건에서 20초면 바닥난다.
        self.assertEqual(20.0, sp.input_exhausted_seconds(100_000, 5000))

    def test_올림한다(self):
        self.assertEqual(101, sp.required_orders(10.1, 10, 1))

    def test_양수가_아니면_예외(self):
        for args in [(0, 20, 4), (5000, 0, 4), (5000, 20, 0)]:
            with self.assertRaises(ValueError):
                sp.required_orders(*args)
        with self.assertRaises(ValueError):
            sp.input_exhausted_seconds(100_000, 0)


if __name__ == "__main__":
    unittest.main()
