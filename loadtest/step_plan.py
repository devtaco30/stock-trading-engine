#!/usr/bin/env python3
"""한계 시험의 단계 길이 하한(설계서 12-4절)과 입력 크기(H4) 계산. 숫자 값(큐 용량, 속도, 시간)은 호출하는 쪽이 넣는다.

step_floor_seconds  병목 단계 상류 큐 용량의 합 ÷ (도착률 - 처리 가능한 속도). 이보다 짧은 단계는 큐가 차기 전에 끝나
                    포화가 큐 깊이 증가로만 보이고 503이나 마감 초과가 나타나지 않는다. Aeron 구간의 용량은 확인되지 않았으므로
                    호출하는 쪽이 넣지 않으면 이 값에 들어가지 않는다. 큐 깊이 증가율이 일정해지는 것을 확인하는 구간은 이 값에 더하지 않았다(길이 미측정).
required_orders     가장 높은 속도 × 도착 시간(초) × 패스 수. 올림한다.
input_exhausted_seconds  입력 주문 수가 그 속도에서 바닥나기까지의 초.

사용: python3 -I loadtest/step_plan.py floor <도착률> <처리 속도> <큐 용량>...
      python3 -I loadtest/step_plan.py orders <가장 높은 속도> <도착 시간(초)> <패스 수>
"""
import math
import sys


def ring_capacity(configured):
    """링버퍼·큐 용량은 2의 거듭제곱으로 올려 잡힌다(설계서 12-4절). 설정값이 2의 거듭제곱이 아니면 실제 칸 수가 더 크다."""
    if configured < 1:
        raise ValueError(f"용량은 1 이상이어야 한다: {configured}")
    return 1 << (configured - 1).bit_length()


def step_floor_seconds(queue_capacities, arrival_rate, processing_rate):
    """도착률이 처리 속도를 넘지 않으면 큐가 차지 않으므로 None."""
    if not queue_capacities or any(capacity < 0 for capacity in queue_capacities):
        raise ValueError("큐 용량은 비어 있지 않은 0 이상의 목록이어야 한다")
    excess = arrival_rate - processing_rate
    if excess <= 0:
        return None
    return sum(queue_capacities) / excess


def required_orders(max_rate, arrival_seconds, passes):
    if max_rate <= 0 or arrival_seconds <= 0 or passes <= 0:
        raise ValueError("속도, 도착 시간, 패스 수는 모두 양수여야 한다")
    return math.ceil(max_rate * arrival_seconds * passes)


def input_exhausted_seconds(total_orders, rate):
    if rate <= 0:
        raise ValueError("속도는 양수여야 한다")
    return total_orders / rate


def main(argv):
    if len(argv) >= 5 and argv[1] == "floor":
        floor = step_floor_seconds([float(v) for v in argv[4:]], float(argv[2]), float(argv[3]))
        print("큐가 차지 않는다(도착률이 처리 속도 이하)" if floor is None else f"{floor:.2f}초 (Aeron 구간 제외)")
        return 0
    if len(argv) == 5 and argv[1] == "orders":
        print(required_orders(float(argv[2]), float(argv[3]), float(argv[4])))
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
