#!/usr/bin/env python3
"""장시간 유지 시험(mix.sh) 결과를 창(기본 600초)별로 판정하고 누적 의심 지표를 요약한다. balance_check.py는 판정 구간 하나만 보므로 이 파일이 따로 창별 값을 낸다.

창마다(유지 구간 시작 PACED_START부터 window초씩, 마지막에 남는 window초 미만 꼬리는 판정하지 않고 초만 알린다):
  accept_ratio  창의 접수 속도 / 목표 R >= 0.99
  no_rejects    거부(NOT_OWNED 로그 줄 수를 뺀 값) 0건
  no_snapshot_warn  스냅샷 쓰기 큐 가득 경고 0건
  open_change   창 안 미체결 합계의 일차 기울기 x 창 길이의 절댓값 <= 목표 N x C의 1% (balance_check의 slope와 같은 기준을 창마다)
  lateness      paced.out의 LATENESS_MINUTE 줄이 있으면 그 창의 1초 초과 지연 0건(줄이 없으면 null이고 통과를 막지 않는다)
추세(첫 trend_span초 대비 마지막 trend_span초, 기본 1800초): GC 직후 힙(gc-account*.log의 「A M->B M」 줄의 B) 평균 비율과 프로세스별 RSS 평균 비율이 1.1을 넘으면 「누적 의심」 표시.
의심 표시는 통과 기준이 아니다(all_windows_ok에 들어가지 않는다). 값은 그대로 보고한다.

사용: window_report.py [--window 600] [--trend-span 1800] [--trend-log trend.log] <결과 폴더>
  결과 폴더에서 params.txt, paced.out, logs/account.log(account-1.log), logs/gc-account*.log, cpu.log를 읽는다.
"""
import datetime
import glob
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import balance_check as bc

SUSPECT_RATIO = 1.1
NOT_OWNED_TEXT = "담당 슬롯이 아닌 계좌로 이벤트가 왔습니다"
WARN_TEXT = "스냅샷 쓰기 큐가 가득 차"
GC_RE = re.compile(r"^\[(\S+)\]\[\S+\] GC\(\d+\) Pause .*? (\d+)M->(\d+)M\(")


def _line_times(path, needle):
    times = []
    with open(path, errors="replace") as f:
        for line in f:
            if needle in line:
                times.append(datetime.datetime.fromisoformat(line.split(" ")[0]).timestamp())
    return times


def _gc_after(paths):
    points = []
    for path in paths:
        with open(path, errors="replace") as f:
            for line in f:
                m = GC_RE.match(line)
                if m:
                    ts = datetime.datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
                    points.append((ts, int(m.group(3))))
    return points


def _rss_points(path):
    by_role = {}
    if not os.path.exists(path):
        return by_role
    with open(path, errors="replace") as f:
        for line in f:
            parts = line.split()
            if len(parts) == 5:
                by_role.setdefault(parts[1], []).append((float(parts[0]), int(parts[4]) / 1024.0))
    return by_role


def _lateness_minutes(paced_text):
    return {int(m.group(1)): (int(m.group(2)), int(m.group(3)))
            for m in re.finditer(r"LATENESS_MINUTE minute=(\d+) maxLatenessMs=(\d+) over1sCount=(\d+)", paced_text)}


def _parse_trend_log(path):
    samples = []
    with open(path, errors="replace") as f:
        for line in f:
            if line.startswith("ts="):
                samples.append(dict(p.split("=", 1) for p in line.split() if "=" in p))
    return samples


def _mean(values):
    return sum(values) / len(values) if values else None


def _ratio(first, last):
    return last / first if first and last is not None else None


def build_report(result_dir, window_seconds=600, trend_span=1800, trend_log=None):
    params = bc._read_params(result_dir)
    accounts, c, rate = int(params["ACCTS"]), int(params["C"]), int(params["RATE"])
    target = accounts * c
    start, done = bc._read_paced(result_dir)
    with open(os.path.join(result_dir, "paced.out"), encoding="utf-8", errors="replace") as f:
        paced_text = f.read()
    log_paths = [os.path.join(result_dir, "logs", "account.log")]
    second = os.path.join(result_dir, "logs", "account-1.log")
    if os.path.exists(second):
        log_paths.append(second)
    raw_rows = [bc.parse_metrics(p) for p in log_paths]
    reasons = []
    for path, rows in zip(log_paths, raw_rows):
        status, notes = bc.classify_rows(rows)
        if status == "invalid":
            reasons.append("%s: %s" % (os.path.basename(path), "; ".join(notes)))
    rows = bc.merge_rows([bc.monotone_rows(r) for r in raw_rows])
    series = bc.open_series(rows)
    warn_times = sorted(t for p in log_paths for t in _line_times(p, WARN_TEXT))
    owned_times = sorted(t for p in log_paths for t in _line_times(p, NOT_OWNED_TEXT))
    gc_points = _gc_after(sorted(glob.glob(os.path.join(result_dir, "logs", "gc-account*.log"))))
    rss_by_role = _rss_points(os.path.join(result_dir, "cpu.log"))
    lateness = _lateness_minutes(paced_text)
    trend_samples = _parse_trend_log(trend_log) if trend_log else []
    steady = int(done.group(2)) / 1000.0 if done else (rows[-1][0] - start if rows else 0)
    count = int(steady // window_seconds)
    windows = []
    for i in range(count):
        ws, we = start + i * window_seconds, start + (i + 1) * window_seconds
        in_rows = [r for r in rows if ws <= r[0] < we]
        before = [r for r in rows if r[0] < ws]
        upto = [r for r in rows if r[0] < we]
        first_total = before[-1][4] if before else (rows[0][4] if rows else 0)
        last_total = upto[-1][4] if upto else first_total
        accept_rate = (last_total - first_total) / float(window_seconds)
        owned = sum(1 for t in owned_times if ws <= t < we)
        rejected = sum(r[2] for r in in_rows) - owned
        warns = sum(1 for t in warn_times if ws <= t < we)
        points = [(t, v) for t, v in series if ws <= t < we]
        drift = bc._slope(points) * window_seconds
        gc_in = [v for t, v in gc_points if ws <= t < we]
        rss_max = {role: round(max(v for t, v in pts if ws <= t < we), 1) for role, pts in rss_by_role.items() if any(ws <= t < we for t, _ in pts)}
        minutes = [m for m in lateness if ws <= start + m * 60 < we]
        late_max = max((lateness[m][0] for m in minutes), default=None)
        late_over = sum(lateness[m][1] for m in minutes) if minutes else None
        checks = {
            "accept_ratio": accept_rate / rate >= bc.ACCEPT_RATIO_LIMIT,
            "no_rejects": rejected == 0,
            "no_snapshot_warn": warns == 0,
            "open_change": abs(drift) <= bc.SLOPE_LIMIT * target,
            "lateness": True if late_over is None else late_over == 0,
        }
        window = {"index": i, "start_offset_s": i * window_seconds, "seconds": window_seconds, "accept_rate": accept_rate, "accept_ratio": accept_rate / rate,
                  "rejected": rejected, "snapshot_warn": warns, "open_first": points[0][1] if points else None, "open_last": points[-1][1] if points else None,
                  "open_min": min((v for _, v in points), default=None), "open_max": max((v for _, v in points), default=None), "drift": drift,
                  "gc_after_mb_min": min(gc_in) if gc_in else None, "gc_after_mb_mean": _mean(gc_in), "rss_mb_max": rss_max,
                  "lateness_max_ms": late_max, "lateness_over_1s": late_over, "checks": checks, "ok": all(checks.values())}
        if trend_samples:
            in_trend = [s for s in trend_samples if ws <= float(s["ts"]) < we]
            window["free_gb_min"] = min((int(s["free_gb"]) for s in in_trend), default=None)
            ages = [float(s["ts"]) - float(s[k]) for s in in_trend for k in s if k.endswith("_snap_mtime")]
            window["snapshot_age_max_s"] = max(ages, default=None)
        windows.append(window)
    span_end = start + steady
    first_lo, first_hi = start, start + trend_span
    last_lo, last_hi = span_end - trend_span, span_end
    gc_first = _mean([v for t, v in gc_points if first_lo <= t < first_hi])
    gc_last = _mean([v for t, v in gc_points if last_lo <= t <= last_hi])
    gc_ratio = _ratio(gc_first, gc_last)
    rss_trend = {}
    for role, pts in rss_by_role.items():
        f = _mean([v for t, v in pts if first_lo <= t < first_hi])
        l = _mean([v for t, v in pts if last_lo <= t <= last_hi])
        r = _ratio(f, l)
        rss_trend[role] = {"first_mean_mb": f, "last_mean_mb": l, "ratio": r, "suspect": r is not None and r > SUSPECT_RATIO}
    return {
        "window_seconds": window_seconds, "windows": windows, "ignored_tail_seconds": int(round(steady - count * window_seconds)),
        "all_windows_ok": bool(windows) and all(w["ok"] for w in windows) and not reasons,
        "invalid_reasons": reasons,
        "trend": {"span_seconds": trend_span, "gc_after_mean_mb_first": gc_first, "gc_after_mean_mb_last": gc_last, "gc_after_ratio": gc_ratio,
                  "gc_after_suspect": gc_ratio is not None and gc_ratio > SUSPECT_RATIO, "rss": rss_trend,
                  "note": "의심 표시는 통과 기준이 아니다. 값은 그대로 읽는다."},
    }


def main(argv):
    window, span, trend_log = 600, 1800, None
    args = argv[1:]
    while args and args[0].startswith("--"):
        if args[0] == "--window":
            window = int(args[1])
        elif args[0] == "--trend-span":
            span = int(args[1])
        elif args[0] == "--trend-log":
            trend_log = args[1]
        else:
            sys.exit("알 수 없는 옵션: " + args[0])
        args = args[2:]
    if len(args) != 1:
        sys.exit(__doc__)
    print(json.dumps(build_report(args[0], window, span, trend_log), ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main(sys.argv)
