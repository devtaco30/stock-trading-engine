#!/usr/bin/env python3
"""H3 종목→매칭 shard 부하 비율 계산 도구 시험. 앱·Kafka 없이 돈다.
실행: python3 -I loadtest/test_matching_load_ratio.py   (저장소 루트에서)
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import matching_load_ratio as m  # noqa: E402

def pick_stocks_with_distinct_slots(count):
    """해시가 서로 다른 종목 slot으로 가는 종목코드를 앞에서부터 count개 고른다(같은 slot의 종목은 주인이 같아 나눌 수 없다)."""
    picked, used = [], set()
    candidate = 0
    while len(picked) < count:
        code = f"A{candidate:06d}"
        slot = m.stock_slot(code, 64)
        if slot not in used:
            used.add(slot)
            picked.append(code)
        candidate += 1
    return picked


STOCKS_30 = pick_stocks_with_distinct_slots(30)


def write_orders(path, per_stock):
    with open(path, "w", encoding="utf-8") as f:
        for code, count in per_stock.items():
            for _ in range(count):
                f.write(json.dumps({"side": "BUY", "accountId": 1, "stockCode": code}) + "\n")


def write_map(path, slot_to_value):
    with open(path, "w", encoding="utf-8") as f:
        for slot, value in sorted(slot_to_value.items()):
            f.write(f"{slot}\t{value}\n")


class StockSlotTest(unittest.TestCase):
    # 기대값은 jshell로 core의 StockSlotHasher 식(String.hashCode → 64비트 확장 → fmix64 → floorMod 64)을 직접 돌려 얻었다.
    def test_java_해시와_같은_종목_slot(self):
        self.assertEqual(23, m.stock_slot("005930", 64))
        self.assertEqual(37, m.stock_slot("A900270", 64))
        self.assertEqual(38, m.stock_slot("A000020", 64))
        self.assertEqual(17, m.stock_slot("한글", 64))

    def test_slot_개수가_1_미만이면_예외(self):
        with self.assertRaises(ValueError):
            m.stock_slot("005930", 0)


class LoadMapTest(unittest.TestCase):
    def test_moving_표시는_주소에서_떼고_따로_센다(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "map.tsv"
            write_map(p, {0: "aeron:udp?endpoint=localhost:20020", 1: "aeron:udp?endpoint=localhost:20021;moving"})
            addresses, moving = m.load_slot_map(p)
            self.assertEqual("aeron:udp?endpoint=localhost:20021", addresses[1])
            self.assertEqual({1}, moving)


class ComputeTest(unittest.TestCase):
    def build(self, tmp, split):
        """30개 종목을 split(예: [8, 8, 7, 7])대로 주소 하나씩에 배정한 맵과 종목당 100건 입력을 만든다."""
        orders = Path(tmp) / "orders.jsonl"
        write_orders(orders, {code: 100 for code in STOCKS_30})
        addresses = [f"aeron:udp?endpoint=localhost:{20020 + i}" for i in range(len(split))]
        slot_to_address = {}
        index = 0
        for owner, size in enumerate(split):
            for code in STOCKS_30[index:index + size]:
                slot_to_address[m.stock_slot(code, 64)] = addresses[owner]
            index += size
        # 종목이 쓰지 않는 slot도 맵에는 주인이 있어야 하므로 첫 주소에 채운다.
        for slot in range(64):
            slot_to_address.setdefault(slot, addresses[0])
        slot_map = Path(tmp) / "map.tsv"
        write_map(slot_map, slot_to_address)
        return orders, slot_map, addresses

    def test_8_8_7_7이면_가장_바쁜_shard_비율이_0_267(self):
        # 서로 다른 종목이 같은 slot에 해시되면 이 배정 구성이 깨지므로 먼저 확인한다.
        self.assertEqual(30, len({m.stock_slot(c, 64) for c in STOCKS_30}), "시험 종목이 slot을 공유한다 — 종목 이름을 바꾼다")
        with tempfile.TemporaryDirectory() as tmp:
            orders, slot_map, addresses = self.build(tmp, [8, 8, 7, 7])
            result = m.compute(orders, slot_map, 64)
            self.assertEqual(3000, result["total_orders"])
            self.assertEqual([800, 800, 700, 700], [result["orders_by_address"][a] for a in addresses])
            self.assertAlmostEqual(0.2667, result["max_ratio"], places=4)
            self.assertAlmostEqual(3.75, result["theoretical_speedup"], places=2)
            self.assertEqual(0, result["orders_without_owner"])

    def test_15_15이면_상한_2배(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, slot_map, _ = self.build(tmp, [15, 15])
            result = m.compute(orders, slot_map, 64)
            self.assertAlmostEqual(0.5, result["max_ratio"], places=4)
            self.assertAlmostEqual(2.0, result["theoretical_speedup"], places=2)

    def test_맵에_없는_slot의_주문은_주인_없는_주문으로_센다(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders = Path(tmp) / "orders.jsonl"
            write_orders(orders, {"A000001": 10})
            slot_map = Path(tmp) / "map.tsv"
            write_map(slot_map, {0: "aeron:udp?endpoint=localhost:20020"})
            result = m.compute(orders, slot_map, 64)
            self.assertEqual(10, result["orders_without_owner"])
            self.assertEqual(10, result["total_orders"])
            self.assertEqual({}, result["orders_by_address"])
            self.assertIsNone(result["max_ratio"])

    def test_이동_중인_slot의_주문은_따로_센다(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders = Path(tmp) / "orders.jsonl"
            write_orders(orders, {"A000001": 10})
            slot = m.stock_slot("A000001", 64)
            slot_map = Path(tmp) / "map.tsv"
            write_map(slot_map, {slot: "aeron:udp?endpoint=localhost:20020;moving"})
            result = m.compute(orders, slot_map, 64)
            self.assertEqual(10, result["orders_in_moving_slots"])


class OwnershipCheckTest(unittest.TestCase):
    def test_주인_없는_주문이_있으면_문제를_돌려준다(self):
        self.assertIsNone(m.ownership_problem({"orders_without_owner": 0, "total_orders": 10}))
        problem = m.ownership_problem({"orders_without_owner": 3, "total_orders": 10})
        self.assertIn("3", problem)
        self.assertIn("낮게", problem)

    def test_CLI는_주인_없는_주문이_있으면_종료_코드_1(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders = Path(tmp) / "orders.jsonl"
            write_orders(orders, {"A000001": 10})
            slot_map = Path(tmp) / "map.tsv"
            write_map(slot_map, {0: "aeron:udp?endpoint=localhost:20020"})
            slot = m.stock_slot("A000001", 64)
            if slot == 0:
                self.skipTest("시험 종목이 0번 slot이라 주인 없는 경우를 만들 수 없다")
            self.assertEqual(1, m.main(["x", str(orders), str(slot_map)]))
            write_map(slot_map, {slot: "aeron:udp?endpoint=localhost:20020"})
            self.assertEqual(0, m.main(["x", str(orders), str(slot_map)]))


if __name__ == "__main__":
    unittest.main()
