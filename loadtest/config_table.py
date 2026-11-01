#!/usr/bin/env python3
"""H6 구성(계좌 워커 N × 매칭 shard M)별 표. 결정과 무관한 열만 계산한다.

열: 구성 | 목표 속도 | 실제 완료 RPS | 포화 여부 | 가장 바쁜 매칭 shard의 비율 | 이론 상한 대비 달성률
- 포화 여부는 한계를 어떻게 정의할지(완료 RPS 증가율이 얼마 아래이면 정체인가)가 정해지지 않아 값이 없으면 「정할 조건」으로 둔다.
- 달성률 = 완료 RPS ÷ (기준 구성의 완료 RPS × 이론 상한), 이론 상한 = 1 ÷ 가장 바쁜 shard의 비율(H3, matching_load_ratio.py).
  기준 구성의 완료 RPS를 어느 단계에서 읽을지는 한계 정의에 달려 있으므로 이 도구는 행에 적힌 값을 그대로 쓴다.

입력 행(dict): config, target_rps, completed_rps, max_ratio, saturated(선택)
"""
import json
import sys

HEADER = "| 구성 | 목표 속도 | 실제 완료 RPS | 포화 여부 | 가장 바쁜 매칭 shard의 비율 | 이론 상한 대비 달성률 |"
SEPARATOR = "|---|---|---|---|---|---|"
UNDECIDED = "정할 조건"


def achievement(completed_rps, baseline_rps, max_ratio):
    if completed_rps is None or not baseline_rps or not max_ratio or baseline_rps <= 0 or max_ratio <= 0:
        return None
    return completed_rps / (baseline_rps * (1 / max_ratio))


def render(rows, baseline_config):
    baseline = next((row for row in rows if row["config"] == baseline_config), None)
    lines = [HEADER, SEPARATOR]
    for row in rows:
        if baseline is None:
            cell = "기준 없음"
        else:
            value = achievement(row.get("completed_rps"), baseline.get("completed_rps"), row.get("max_ratio"))
            cell = "-" if value is None else f"{value:.3f}"
        ratio = row.get("max_ratio")
        lines.append("| {} | {} | {} | {} | {} | {} |".format(
            row["config"], row.get("target_rps", "-"), row.get("completed_rps", "-"),
            row.get("saturated") or UNDECIDED, "-" if ratio is None else f"{ratio:.3f}", cell))
    return "\n".join(lines)


def main(argv):
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as f:
        rows = json.load(f)
    print(render(rows, argv[2]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
