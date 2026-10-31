#!/usr/bin/env python3
"""H6 구성별 표(결정과 무관한 열) 시험. 포화 여부 열은 한계 정의가 정해지지 않아 값이 없으면 「정할 조건」으로 둔다.
실행: python3 -I loadtest/test_config_table.py   (저장소 루트에서)
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config_table as ct  # noqa: E402


class AchievementTest(unittest.TestCase):
    def test_달성률은_완료_RPS_나누기_기준_RPS_곱하기_이론_상한(self):
        # 1×1의 완료 RPS 4000, 가장 바쁜 shard 비율 0.5(이론 상한 2.0배) → 상한 8000. 6000이면 0.75.
        self.assertAlmostEqual(0.75, ct.achievement(6000, 4000, 0.5), places=4)

    def test_비율_0_267이면_상한_3_75배(self):
        self.assertAlmostEqual(7500 / (2000 * (1 / 0.2667)), ct.achievement(7500, 2000, 0.2667), places=4)

    def test_완료_RPS가_0이면_None이_아니라_0이다(self):
        self.assertEqual(0.0, ct.achievement(0, 4000, 0.5))

    def test_입력이_없거나_0이면_None(self):
        self.assertIsNone(ct.achievement(6000, 4000, None))
        self.assertIsNone(ct.achievement(6000, 0, 0.5))
        self.assertIsNone(ct.achievement(None, 4000, 0.5))
        self.assertIsNone(ct.achievement(6000, 4000, 0))


class RenderTest(unittest.TestCase):
    def rows(self):
        return [
            {"config": "1×1", "target_rps": 4000, "completed_rps": 4000, "max_ratio": 1.0},
            {"config": "1×2", "target_rps": 8000, "completed_rps": 6000, "max_ratio": 0.5},
        ]

    def test_두_행이_나오고_열_순서가_설계서와_같다(self):
        table = ct.render(self.rows(), baseline_config="1×1")
        lines = table.splitlines()
        self.assertEqual("| 구성 | 목표 속도 | 실제 완료 RPS | 포화 여부 | 가장 바쁜 매칭 shard의 비율 | 이론 상한 대비 달성률 |", lines[0])
        self.assertEqual(4, len(lines))  # 머리 + 구분 + 두 행

    def test_달성률_열은_계산식과_같다(self):
        table = ct.render(self.rows(), baseline_config="1×1")
        self.assertIn("| 1×2 | 8000 | 6000 | 정할 조건 | 0.500 | 0.750 |", table)
        self.assertIn("| 1×1 | 4000 | 4000 | 정할 조건 | 1.000 | 1.000 |", table)

    def test_포화_여부가_주어지면_그_값을_쓴다(self):
        rows = self.rows()
        rows[1]["saturated"] = "예"
        self.assertIn("| 1×2 | 8000 | 6000 | 예 | 0.500 | 0.750 |", ct.render(rows, baseline_config="1×1"))

    def test_완료_RPS가_0이면_달성률_칸에_0_000을_찍는다(self):
        rows = self.rows()
        rows[1]["completed_rps"] = 0
        self.assertIn("| 1×2 | 8000 | 0 | 정할 조건 | 0.500 | 0.000 |", ct.render(rows, baseline_config="1×1"))

    def test_기준_구성이_없으면_달성률_칸은_비워_두고_이유를_알린다(self):
        table = ct.render(self.rows(), baseline_config="2×1")
        self.assertIn("| 1×2 | 8000 | 6000 | 정할 조건 | 0.500 | 기준 없음 |", table)


if __name__ == "__main__":
    unittest.main()
