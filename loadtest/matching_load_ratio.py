#!/usr/bin/env python3
"""H3 종목을 매칭 shard에 배정한 결과의 부하 비율 계산.

입력
  주문 파일(jsonl, 한 줄에 주문 하나, stockCode 필드 필요. generate-e2e-input.py 산출물과 같은 형식)
  종목 slot 맵 파일(한 줄에 `종목 slot 번호<탭>값`, 값은 `주소` 또는 `주소;moving`).
    맵 토픽 matching-shard-map의 레코드(키=종목 slot 번호, 값=주문 입구 주소)를 하네스가 이 형식으로 덤프한 것이다.
계산
  주문의 stockCode를 core의 StockSlotHasher와 같은 식으로 종목 slot에 해시하고, 맵이 가리키는 주소별로 주문 수를 센다.
  가장 바쁜 주소의 주문 비율(max_ratio)과 그 역수(theoretical_speedup, 1대 대비 이론 상한)를 낸다.

사용: python3 -I loadtest/matching_load_ratio.py <주문 jsonl> <맵 파일> [종목 slot 개수=64]
"""
import json
import sys
from pathlib import Path

MOVING_SUFFIX = ";moving"
MASK64 = (1 << 64) - 1


def java_string_hash(text):
    """java.lang.String.hashCode(): UTF-16 코드 유닛마다 h = 31*h + c, 32비트 부호 있는 값."""
    units = text.encode("utf-16-be")
    h = 0
    for i in range(0, len(units), 2):
        h = (31 * h + ((units[i] << 8) | units[i + 1])) & 0xFFFFFFFF
    return h - (1 << 32) if h >= (1 << 31) else h


def fmix64(k):
    k &= MASK64
    k ^= k >> 33
    k = (k * 0xFF51AFD7ED558CCD) & MASK64
    k ^= k >> 33
    k = (k * 0xC4CEB9FE1A85EC53) & MASK64
    k ^= k >> 33
    return k


def stock_slot(stock_code, slot_count):
    """core StockSlotHasher.slotFor와 같다: String.hashCode를 64비트로 부호 확장해 fmix64를 거친 값의 floorMod."""
    if slot_count < 1:
        raise ValueError(f"종목 slot 개수는 1 이상이어야 한다: {slot_count}")
    mixed = fmix64(java_string_hash(stock_code) & MASK64)
    signed = mixed - (1 << 64) if mixed >= (1 << 63) else mixed
    return signed % slot_count  # 파이썬 %는 부호와 무관하게 0 이상이라 Math.floorMod와 같다


def load_slot_map(path):
    """(종목 slot → 주소, 이동 중인 종목 slot 집합)."""
    addresses = {}
    moving = set()
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        slot_text, value = line.split("\t", 1)
        slot = int(slot_text)
        value = value.strip()
        if value.endswith(MOVING_SUFFIX):
            moving.add(slot)
            value = value[: -len(MOVING_SUFFIX)]
        addresses[slot] = value
    return addresses, moving


def compute(orders_path, map_path, slot_count=64):
    addresses, moving = load_slot_map(map_path)
    orders_by_address = {}
    total = 0
    without_owner = 0
    in_moving = 0
    with open(orders_path, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            total += 1
            slot = stock_slot(json.loads(line)["stockCode"], slot_count)
            address = addresses.get(slot)
            if address is None:
                without_owner += 1
                continue
            if slot in moving:
                in_moving += 1
            orders_by_address[address] = orders_by_address.get(address, 0) + 1
    owned = sum(orders_by_address.values())
    max_ratio = max(orders_by_address.values()) / total if orders_by_address and total else None
    return {
        "total_orders": total,
        "orders_by_address": orders_by_address,
        "orders_without_owner": without_owner,
        "orders_in_moving_slots": in_moving,
        "owned_orders": owned,
        "max_ratio": max_ratio,
        "theoretical_speedup": (1 / max_ratio) if max_ratio else None,
    }


def ownership_problem(result):
    """주인 없는 종목 slot의 주문이 있으면 읽는 사람이 놓치지 않게 문장으로 돌려준다. 없으면 None.
    이 주문들은 어느 shard의 몫으로도 세지 않으므로 max_ratio가 실제보다 낮게 나온다."""
    without_owner = result["orders_without_owner"]
    if without_owner == 0:
        return None
    return (f"주인 없는 종목 slot의 주문이 {without_owner}건(전체 {result['total_orders']}건)이다 — "
            "이 주문은 어느 shard 몫으로도 세지 않아 max_ratio가 실제보다 낮게 나온다")


def main(argv):
    if len(argv) not in (3, 4):
        print(__doc__, file=sys.stderr)
        return 2
    slot_count = int(argv[3]) if len(argv) == 4 else 64
    result = compute(argv[1], argv[2], slot_count)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    problem = ownership_problem(result)
    if problem:
        print(problem, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
