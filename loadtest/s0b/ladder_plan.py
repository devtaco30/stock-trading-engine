#!/usr/bin/env python3
"""계좌당 미체결 C를 단계별로 올려 가며 각 단계에서 접수 속도를 재는 실행(ladder.sh)의 주문 계획을 계산한다.

한 실행은 계좌 N개와 종목 3개에서 시작한다. 단계 i마다:
  PREFILL  수량 1 매도를 더 접수시켜 계좌당 미체결을 C_i로 올린다(체결·취소가 없어 모두 남는다).
  REJECT   그 상태에서 「가용 보유보다 1 큰 수량」의 매도를 보낸다. 가용 보유(보유 - 예약)를 넘어 INSUFFICIENT_HOLDING으로 거부되고,
           거부 전에 그 계좌의 모든 매도 예약을 순회하므로(AccountState.reservedSellQuantity) 순회 비용은 같고 예약은 늘지 않는다.
           즉 C가 고정된 채 얼마든지 길게 잴 수 있다.
  ACCEPT   (선택) 같은 상태에서 수량 1 매도를 소량 접수시켜 접수 속도를 잰다. 거부 속도가 접수 속도의 대리값인지 확인하는 용도이고,
           접수된 만큼 C가 늘므로(드리프트) 다음 PREFILL이 그만큼 줄어 명목 C_{i+1}을 맞춘다.
시드 보유(HOLD, 계좌 하나의 종목 하나당)는 마지막 단계 예약 + 접수 검증량 + 여유다. 모든 수량은 「종목당 계좌당」 기준이고
단계의 per_stock은 그 종목 하나에 보낼 주문 수(= 계좌 수 x 계좌당 수)다.

사용: ladder_plan.py <계좌 수> <C 목록, 쉼표> [<접수 검증 단계:종목당 계좌당 건수, 쉼표>]
출력: TSV. 첫 줄 HOLD\t<n>, 이후 줄은 KIND\t단계\t종목당 보낼 수\t수량\t이름
"""
import math
import sys

STOCKS = 3
HOLD_MARGIN = 5
DEFAULT_PROBE_SECONDS = 15
DEFAULT_MAX_TOTAL_PROBE = 2_000_000


def default_rate_estimate(accounts, c):
    """거부 속도의 거친 추정(건/초). 기존 접수 속도 x C가 약 2.5천만~5.8천만이었던 것을 쓰고 위아래를 막는다. 틀리면 사후 창 길이로 확인한다."""
    return max(3000.0, min(150000.0, 2.0 * 4.0e7 / max(c, 1)))


def plan(accounts, ladder_c, accept_probes, probe_seconds=DEFAULT_PROBE_SECONDS, rate_estimate=default_rate_estimate,
         max_total_probe=DEFAULT_MAX_TOTAL_PROBE):
    if accounts < 1:
        raise ValueError("계좌 수는 1 이상이어야 한다")
    if not ladder_c or any(c % STOCKS for c in ladder_c):
        raise ValueError("C는 종목 수 %d의 배수여야 한다: %s" % (STOCKS, ladder_c))
    if any(b <= a for a, b in zip(ladder_c, ladder_c[1:])) or ladder_c[0] < STOCKS:
        raise ValueError("C 단계는 늘어나야 한다: %s" % ladder_c)
    for rung in accept_probes:
        if rung < 0 or rung >= len(ladder_c):
            raise ValueError("접수 검증 단계가 없다: %s" % rung)
    per_stock = [c // STOCKS for c in ladder_c]
    for rung, a in accept_probes.items():
        if a < 1:
            raise ValueError("접수 검증 건수는 1 이상이어야 한다")
        if rung + 1 < len(per_stock) and a > per_stock[rung + 1] - per_stock[rung]:
            raise ValueError("접수 검증 %d건이 다음 단계 증가분 %d보다 크다" % (a, per_stock[rung + 1] - per_stock[rung]))
    last = len(ladder_c) - 1
    hold = per_stock[last] + accept_probes.get(last, 0) + HOLD_MARGIN
    steps = []
    reserved = 0   # 지금까지 종목당 계좌당 쌓인 예약
    for rung, target in enumerate(per_stock):
        steps.append({"kind": "PREFILL", "rung": rung, "per_stock": accounts * (target - reserved), "quantity": 1,
                      "label": "C=%d" % ladder_c[rung]})
        reserved = target
        total = rate_estimate(accounts, ladder_c[rung]) * probe_seconds
        total = min(total, max_total_probe)
        per_stock_probe = accounts * max(1, math.ceil(total / (STOCKS * accounts)))
        steps.append({"kind": "REJECT", "rung": rung, "per_stock": per_stock_probe, "quantity": hold - reserved + 1,
                      "label": "C=%d" % ladder_c[rung]})
        if rung in accept_probes:
            a = accept_probes[rung]
            steps.append({"kind": "ACCEPT", "rung": rung, "per_stock": accounts * a, "quantity": 1, "label": "C=%d" % ladder_c[rung]})
            reserved += a
    return {"accounts": accounts, "ladder_c": list(ladder_c), "hold": hold, "steps": steps}


def to_tsv(plan_result):
    lines = ["HOLD\t%d" % plan_result["hold"]]
    for s in plan_result["steps"]:
        lines.append("%s\t%d\t%d\t%d\t%s" % (s["kind"], s["rung"], s["per_stock"], s["quantity"], s["label"]))
    return "\n".join(lines) + "\n"


def parse_accept(text):
    probes = {}
    if text:
        for part in text.split(","):
            rung, count = part.split(":")
            probes[int(rung)] = int(count)
    return probes


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(2)
    result = plan(int(sys.argv[1]), [int(x) for x in sys.argv[2].split(",")], parse_accept(sys.argv[3] if len(sys.argv) > 3 else ""))
    sys.stdout.write(to_tsv(result))
