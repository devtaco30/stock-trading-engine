#!/usr/bin/env python3
"""계좌당 미체결 수(m) 구간별 접수 속도를 낸다.

계좌 N개에 같은 수로 주문이 퍼지고(주문 i는 계좌 i % N), 체결·취소가 없어 접수한 주문이 모두 미체결로 남는 실행(s0b run.sh PREFILL=0)에서,
계좌 워커 보고기의 누적 접수 수(acceptedTotal) 1초 시계열을 읽는다. 계좌당 미체결 m = acceptedTotal / N.

한 칸에 나오는 값:
- 창 속도: m이 구간 [lo, hi)을 지나는 동안의 평균 속도 = (hi - lo) * N / (경계를 지난 시각 차). 경계 시각은 인접한 두 보고 사이를 선형 보간한다.
- 1초 속도: 보고 사이 구간(약 1초) 중 중점의 m이 구간 안에 드는 것들의 속도. 표본 수, 중앙값, 최소, 최대.
- 구간 끝(hi)에 닿지 못한 실행은 「안 쟀음」이다.

사용:
  step_rates.py <결과 폴더>                     한 실행을 JSON으로
  step_rates.py --table <결과 폴더>...           여러 실행을 N별 열, m 구간별 행의 표로
"""
import datetime
import json
import os
import re
import statistics
import sys

DEFAULT_BINS = [(20, 200), (200, 2000), (2000, 20000)]


def parse_series(path):
    series = []
    for line in open(path, errors="replace"):
        if "metrics accepted=" not in line:
            continue
        m = re.search(r"acceptedTotal=(\d+)", line)
        if not m:
            continue
        ts = datetime.datetime.fromisoformat(line.split(" ")[0]).timestamp()
        series.append((ts, int(m.group(1))))
    series.sort()
    return series


def _crossing_time(series, accounts, target_m):
    """계좌당 미체결이 target_m에 처음 닿는 시각. 닿지 못하면 None."""
    target_total = target_m * accounts
    for (t0, a0), (t1, a1) in zip(series, series[1:]):
        if a0 < target_total <= a1 and a1 > a0:
            return t0 + (t1 - t0) * (target_total - a0) / (a1 - a0)
    return None


def bin_stats(series, accounts, lo, hi):
    t_lo = _crossing_time(series, accounts, lo)
    t_hi = _crossing_time(series, accounts, hi)
    if t_lo is None or t_hi is None or t_hi <= t_lo:
        return None
    secs = []
    for (t0, a0), (t1, a1) in zip(series, series[1:]):
        if t1 <= t0 or a1 <= a0:
            continue
        mid = (a0 + a1) / 2 / accounts
        if lo <= mid < hi:
            secs.append((a1 - a0) / (t1 - t0))
    result = {
        "lo": lo, "hi": hi,
        "window_seconds": round(t_hi - t_lo, 3),
        "window_rate": round((hi - lo) * accounts / (t_hi - t_lo), 1),
        "samples": len(secs),
    }
    if secs:
        result["sec_rate_median"] = round(statistics.median(secs), 1)
        result["sec_rate_min"] = round(min(secs), 1)
        result["sec_rate_max"] = round(max(secs), 1)
    return result


def read_run(result_dir):
    params = dict(kv.split("=") for kv in open(os.path.join(result_dir, "params.txt")).read().split())
    return int(params["ACCTS"]), parse_series(os.path.join(result_dir, "logs", "account.log"))


def report(result_dir, bins=DEFAULT_BINS):
    accounts, series = read_run(result_dir)
    return {"accounts": accounts, "bins": [bin_stats(series, accounts, lo, hi) or {"lo": lo, "hi": hi, "측정": "안 쟀음"} for lo, hi in bins]}


def table(result_dirs, bins=DEFAULT_BINS):
    by_n = {}
    for d in result_dirs:
        accounts, series = read_run(d)
        by_n.setdefault(accounts, []).append(series)
    ns = sorted(by_n, reverse=True)
    out = ["m 구간(계좌당 미체결) | " + " | ".join("N=%d" % n for n in ns)]
    for lo, hi in bins:
        cells = []
        for n in ns:
            stats = [bin_stats(s, n, lo, hi) for s in by_n[n]]
            got = [s for s in stats if s]
            if not got:
                cells.append("안 쟀음")
                continue
            windows = [g["window_rate"] for g in got]
            secs = [g["sec_rate_median"] for g in got if "sec_rate_median" in g]
            cells.append("창 %s 건/초 (창 %s초), 1초 중앙 %s (표본 %s개), 실행 %d/%d회"
                         % ("/".join("%.0f" % w for w in windows), "/".join("%.1f" % g["window_seconds"] for g in got),
                            "/".join("%.0f" % v for v in secs) if secs else "없음", "/".join(str(g["samples"]) for g in got),
                            len(got), len(stats)))
        out.append("[%d,%d) | " % (lo, hi) + " | ".join(cells))
    return "\n".join(out)


if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "--table":
        print(table(args[1:]))
    elif len(args) == 1:
        print(json.dumps(report(args[0]), ensure_ascii=False))
    else:
        print(__doc__)
        sys.exit(2)
