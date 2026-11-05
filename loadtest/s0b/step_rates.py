#!/usr/bin/env python3
"""계좌당 미체결 수(m) 구간별 접수 속도를 낸다.

계좌 N개에 같은 수로 주문이 퍼지고(주문 i는 계좌 i % N), 체결·취소가 없어 접수한 주문이 모두 미체결로 남는 실행(s0b run.sh PREFILL=0)에서,
계좌 워커 보고기의 누적 접수 수(acceptedTotal) 1초 시계열을 읽는다. 계좌당 미체결 m = acceptedTotal / N.

한 칸에 나오는 값:
- 창 속도: m이 구간 [lo, hi)을 지나는 동안의 평균 속도 = (hi - lo) * N / (경계를 지난 시각 차). 경계 시각은 인접한 두 보고 사이를 선형 보간한다.
- 1초 속도: 보고 사이 구간(약 1초) 중 중점의 m이 구간 안에 드는 것들의 속도. 표본 수, 중앙값, 최소, 최대.
- 구간 끝(hi)에 닿지 못한 실행은 「안 쟀음」이다.

사용:
  step_rates.py [--bins lo:hi,lo:hi] <결과 폴더>              한 실행을 JSON으로
  step_rates.py [--bins lo:hi,lo:hi] --table <결과 폴더>...   여러 실행을 N별 행, m 구간별 열의 표로
  --bins 기본값은 20:200,200:2000,2000:20000
  --by-total  구간을 계좌당 m이 아니라 계좌 전체 접수 수(전체 합)의 범위로 읽는다(--table에서만. 칸에 N별 m 범위를 같이 적는다)
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


def parse_bins(text):
    """「200:2000,2000:6000」 형태의 구간 목록. lo < hi인 양의 정수 쌍만 받는다."""
    bins = []
    for part in text.split(","):
        pieces = part.split(":")
        if len(pieces) != 2 or not pieces[0].isdigit() or not pieces[1].isdigit():
            raise ValueError("구간은 lo:hi 형태의 정수여야 한다: " + repr(part))
        lo, hi = int(pieces[0]), int(pieces[1])
        if lo >= hi:
            raise ValueError("구간은 lo < hi여야 한다: " + repr(part))
        bins.append((lo, hi))
    return bins


def table(result_dirs, bins=DEFAULT_BINS, by_total=False):
    """행 = N(오름차순), 열 = 구간. 칸 = 창 속도 / 창 길이 / 1초 중앙(표본 수) / 실행 횟수. 실행별 값은 / 로 잇는다.

    by_total이 거짓이면 구간은 계좌당 미체결 m의 범위이고, 참이면 계좌 전체 접수 수(전체 합)의 범위다.
    전체 합 기준에서는 같은 구간이라도 N마다 계좌당 m이 다르므로 칸에 그 m 범위(m lo/N~hi/N)를 함께 적는다.
    """
    by_n = {}
    for d in result_dirs:
        accounts, series = read_run(d)
        by_n.setdefault(accounts, []).append(series)
    label = "전체 합" if by_total else "m"
    out = ["N | " + " | ".join("%s [%d,%d)" % (label, lo, hi) for lo, hi in bins)]
    for n in sorted(by_n):
        cells = []
        for lo, hi in bins:
            divisor = 1 if by_total else n
            stats = [bin_stats(s, divisor, lo, hi) for s in by_n[n]]
            got = [g for g in stats if g]
            if not got:
                cells.append("안 쟀음")
                continue
            secs = [g["sec_rate_median"] for g in got if "sec_rate_median" in g]
            m_text = (" (m %d~%d)" % (lo // n, hi // n)) if by_total else ""
            cells.append("창 %s 건/초 (창 %s초)%s, 1초 중앙 %s (표본 %s개), 실행 %d/%d회"
                         % ("/".join("%.0f" % g["window_rate"] for g in got), "/".join("%.1f" % g["window_seconds"] for g in got), m_text,
                            "/".join("%.0f" % v for v in secs) if secs else "없음", "/".join(str(g["samples"]) for g in got),
                            len(got), len(stats)))
        out.append("N=%d | " % n + " | ".join(cells))
    return "\n".join(out)


def main(argv):
    args = list(argv)
    bins = DEFAULT_BINS
    if "--bins" in args:
        i = args.index("--bins")
        bins = parse_bins(args[i + 1])
        del args[i:i + 2]
    by_total = False
    if "--by-total" in args:
        by_total = True
        args.remove("--by-total")
    if args and args[0] == "--table":
        return table(args[1:], bins, by_total)
    if len(args) == 1:
        return json.dumps(report(args[0], bins), ensure_ascii=False)
    raise SystemExit(__doc__)


if __name__ == "__main__":
    print(main(sys.argv[1:]))
