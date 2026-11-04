#!/usr/bin/env python3
"""매칭 shard가 N개일 때 종목 코드 count개를 shard마다 같은 수로 고른다.

입력은 기동 뒤의 「종목 slot -> 매칭 shard 주소」 표다. 두 형식을 받는다.
  - matching_assigned_map 출력: `slot<탭>주소`
  - U10Tool map-history 출력:   `slot<탭>CreateTime(ms)<탭>값`
같은 slot의 마지막 줄이 유효하고, 값이 null·빈 값·`;moving`으로 끝나면 배정이 끝난 것으로 세지 않는다
(loadtest/matching-plan.sh 의 matching_parse_assigned_map 과 같은 규칙).

규칙
  - 주소를 문자열 오름차순으로 세운다. shard 수 N은 서로 다른 주소 수다.
  - shard별 할당량: count // N, 나머지 count % N 개는 정렬 앞쪽 주소부터 1개씩 더 준다.
  - 코드 후보는 `%06d`로 000001부터 오름차순. 후보의 slot을 소유한 shard의 할당량이 남아 있고 그 slot을 아직 안 썼으면 고른다.
  - 출력 순서는 shard를 번갈아 놓는다(1번째 몫 shard0, shard1, ..., 2번째 몫 shard0, ...). 어느 앞부분을 잘라 써도 shard가 고르게 섞인다.
종목 slot -> 코드 계산은 core의 StockSlotHasher(String.hashCode를 64비트로 넓힌 뒤 fmix64, floorMod)와 같다.
"""
import argparse
import sys
from collections import namedtuple

Pick = namedtuple("Pick", "code slot address")

MASK64 = 0xFFFFFFFFFFFFFFFF
MAX_CODE = 999999


class SelectError(Exception):
    pass


def _java_string_hash(text):
    h = 0
    for ch in text:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    return h - (1 << 32) if h >= (1 << 31) else h


def _fmix64(k):
    k &= MASK64
    k ^= k >> 33
    k = (k * 0xFF51AFD7ED558CCD) & MASK64
    k ^= k >> 33
    k = (k * 0xC4CEB9FE1A85EC53) & MASK64
    k ^= k >> 33
    return k


def slot_of(code, slot_count=64):
    widened = _java_string_hash(code)  # Java의 long 확장: 부호 유지
    mixed = _fmix64(widened & MASK64)
    signed = mixed - (1 << 64) if mixed >= (1 << 63) else mixed
    return signed % slot_count  # Python 의 % 는 Math.floorMod 와 같다


def parse_map(text):
    last = {}
    records = []
    for line in text.splitlines():
        cols = line.split("\t")
        if len(cols) < 2 or not cols[0].strip().lstrip("-").isdigit():
            continue
        slot = int(cols[0])
        if len(cols) >= 3 and cols[1].strip().isdigit():
            records.append((slot, cols[2]))
        else:
            records.append((slot, cols[1]))
    for slot, value in records:
        last[slot] = value
    return {s: v for s, v in last.items() if v not in ("", "null") and not v.endswith(";moving")}


def quotas(count, shard_count):
    base, extra = divmod(count, shard_count)
    return [base + (1 if i < extra else 0) for i in range(shard_count)]


def select(slot_to_address, count, slot_count=64):
    if count < 1:
        raise SelectError("종목 수는 1 이상이어야 한다: %d" % count)
    if not slot_to_address:
        raise SelectError("배정된 종목 slot이 없다(표가 비었다)")
    beyond = sorted(s for s in slot_to_address if s < 0 or s >= slot_count)
    if beyond:
        raise SelectError("종목 slot 개수 %d 밖의 slot이 있다: %s" % (slot_count, beyond))
    missing = [s for s in range(slot_count) if s not in slot_to_address]
    if missing:
        raise SelectError("주인 없는 종목 slot이 있다(배정이 끝나지 않았다): %s" % missing)
    addrs = sorted(set(slot_to_address.values()))
    if count < len(addrs):
        raise SelectError("종목 수 %d 가 shard 수 %d 보다 적다" % (count, len(addrs)))
    quota = dict(zip(addrs, quotas(count, len(addrs))))
    taken = {a: [] for a in addrs}
    used_slots = set()
    for number in range(1, MAX_CODE + 1):
        if all(len(taken[a]) >= quota[a] for a in addrs):
            break
        code = "%06d" % number
        slot = slot_of(code, slot_count)
        owner = slot_to_address[slot]
        if len(taken[owner]) < quota[owner] and slot not in used_slots:
            taken[owner].append(Pick(code, slot, owner))
            used_slots.add(slot)
    short = {a: quota[a] - len(taken[a]) for a in addrs if len(taken[a]) < quota[a]}
    if short:
        raise SelectError("코드 000001~%06d 안에서 할당량을 채우지 못했다(부족한 수): %s" % (MAX_CODE, short))
    ordered = []
    for rank in range(max(quota.values())):
        for a in addrs:
            if rank < len(taken[a]):
                ordered.append(taken[a][rank])
    return ordered


def main(argv=None):
    parser = argparse.ArgumentParser(description="매칭 shard마다 같은 수로 종목 코드를 고른다")
    parser.add_argument("--count", type=int, required=True, help="고를 종목 수")
    parser.add_argument("--slot-count", type=int, default=64, help="종목 slot 개수(기본 64)")
    parser.add_argument("--map", help="slot->주소 표 파일(없으면 표준입력)")
    args = parser.parse_args(argv)
    text = open(args.map).read() if args.map else sys.stdin.read()
    try:
        picks = select(parse_map(text), args.count, args.slot_count)
    except SelectError as e:
        print("종목 선택 실패: %s" % e, file=sys.stderr)
        return 1
    print("STOCKS=" + " ".join(p.code for p in picks))
    for p in picks:
        print("SLOT %s=%d %s" % (p.code, p.slot, p.address))
    counts = {}
    for p in picks:
        counts[p.address] = counts.get(p.address, 0) + 1
    for a in sorted(counts):
        print("SHARD_STOCKS %s=%d" % (a, counts[a]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
