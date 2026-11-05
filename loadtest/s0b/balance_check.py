#!/usr/bin/env python3
"""체결 섞기 실행(mix.sh)에서 계좌당 미체결 C가 일정하게 유지됐는지 판정한다.

계좌 쪽 1초 보고기에는 접수 합계(acceptedTotal)와 체결 반영 수(fill), 거부 수(rejected)만 있고 C는 없다.
매수가 모두 체결되고 체결 1건을 계좌 두 곳(매수·매도)이 각각 반영하면(FILLS_PER_TRADE=2, 첫 실행에서 최종값으로 확인한다)
  미체결 합계 = 접수한 매도 수 - 체결 수 = acceptedTotal - 2 x 누적 fill / FILLS_PER_TRADE
이고, 유지 단계가 끝나면 N x C로 돌아와야 한다(final_open).

판정 기준(근거 없는 제안값, Jack이 바꿀 수 있음). 유지 단계 시작 SKIP초 뒤부터 발신이 끝날 때까지 구간에서:
  slope        미체결 합계의 일차 기울기 x 구간 길이의 절댓값 <= 목표 N x C의 1%
  accept_ratio 구간의 접수 속도 / 목표 주문 속도 R >= 0.99
  no_rejects   거부 0건
  no_snapshot_warn 스냅샷 쓰기 큐 가득 경고 0건
  heap         계좌 JVM 힙 사용 최대 / 최대 힙 < 80% (heap.jstat가 있을 때만)

사용: balance_check.py [--skip 30] [--heap-max-bytes 8589934592] <결과 폴더>
  결과 폴더에서 params.txt(ACCTS, C, RATE), paced.out(PACED_START, PACED_DONE), logs/account.log(계좌 워커가 둘이면 logs/account-1.log도 초별로 합산), heap.jstat(있으면, 둘째는 heap-w1.jstat)를 읽는다.
"""
import datetime
import json
import os
import re
import sys

SLOPE_LIMIT = 0.01
ACCEPT_RATIO_LIMIT = 0.99
HEAP_LIMIT = 0.80
FILLS_PER_TRADE = 2


def parse_metrics(path):
    rows = []
    for line in open(path, errors="replace"):
        if "metrics accepted=" not in line:
            continue
        m = re.search(r"metrics accepted=(\d+) rejected=(\d+) fill=(\d+) dup=\d+ acceptedTotal=(\d+)", line)
        if not m:
            continue
        ts = datetime.datetime.fromisoformat(line.split(" ")[0]).timestamp()
        rows.append((ts, int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))))
    return rows   # 정렬하지 않는다: 파일에 찍힌 순서가 보고기가 낸 순서이고, 시각만으로 다시 줄 세우면 종료 직전 마지막 줄이 중간으로 끼어든다.


def classify_rows(rows):
    """파일 순서의 보고 줄이 믿을 만한지 본다. ok, final_flush_row(마지막 줄만 시각이 거꾸로 — 종료 때 마지막 보고를 뒤늦게 찍은 줄), invalid 중 하나와 설명 목록."""
    notes = []
    reversals = []
    for i in range(1, len(rows)):
        if rows[i][4] < rows[i - 1][4]:
            notes.append("acceptedTotal이 줄었다: 줄 %d (%d -> %d)" % (i, rows[i - 1][4], rows[i][4]))
        if rows[i][0] < rows[i - 1][0]:
            reversals.append(i)
            notes.append("시각이 거꾸로 갔다: 줄 %d" % i)
    if any("acceptedTotal" in n for n in notes) or any(i != len(rows) - 1 for i in reversals):
        return "invalid", notes
    if reversals:
        return "final_flush_row", notes
    return "ok", notes


def monotone_rows(rows):
    """시각이 거꾸로 간 줄의 시각을 그때까지의 최댓값으로 올린다(값은 그대로, 파일 순서 유지)."""
    fixed = []
    latest = None
    for row in rows:
        ts = row[0] if latest is None else max(row[0], latest)
        latest = ts
        fixed.append((ts,) + tuple(row[1:]))
    return fixed


def fills_complete(rows, trades):
    """누계 체결 반영 수가 총 체결 수의 2배(체결 1건 = 매수·매도 두 쪽)와 같은지."""
    return sum(r[3] for r in rows) == FILLS_PER_TRADE * trades


def not_owned_ok(per_worker_counts, trades):
    """다른 shard 계좌의 체결 fan-out 거부(NOT_OWNED) 수가 정상 범위인지. 워커 1개면 0, 둘이면 서버마다 같은 수이고 총 체결 수 이하
    (교차 체결 1건이 서버마다 NOT_OWNED 1건씩이므로 서버마다 교차 체결 수와 같고, 교차 체결은 체결 수를 넘을 수 없다). 합이 총 체결 수의 2배를 넘지 않는 것과 같다."""
    if len(per_worker_counts) == 1:
        return per_worker_counts[0] == 0
    return len(set(per_worker_counts)) == 1 and sum(per_worker_counts) <= FILLS_PER_TRADE * trades


def merge_rows(rows_per_worker):
    """계좌 워커별 초 보고를 초 단위로 합친다. 접수·거부·체결은 더하고, 접수 합계는 워커마다 마지막으로 본 값을 이어서 더한다(그 초에 보고가 빠져도 줄지 않는다)."""
    if len(rows_per_worker) == 1:
        return list(rows_per_worker[0])
    buckets = {}
    for worker, rows in enumerate(rows_per_worker):
        for ts, accepted, rejected, fill, accepted_total in rows:
            slot = buckets.setdefault(int(ts // 1), {})
            if worker in slot:
                _ts, a, r, f, _total = slot[worker]
                slot[worker] = (ts, a + accepted, r + rejected, f + fill, accepted_total)
            else:
                slot[worker] = (ts, accepted, rejected, fill, accepted_total)
    merged = []
    last_total = [0] * len(rows_per_worker)
    for second in sorted(buckets):
        accepted = rejected = fill = 0
        for worker, (ts, a, r, f, total) in buckets[second].items():
            accepted += a
            rejected += r
            fill += f
            last_total[worker] = total
        merged.append((float(second), accepted, rejected, fill, sum(last_total)))
    return merged


def open_series(rows, fills_per_trade=FILLS_PER_TRADE):
    series = []
    fill_total = 0
    for ts, _accepted, _rejected, fill, accepted_total in rows:
        fill_total += fill
        series.append((ts, accepted_total - 2 * fill_total / fills_per_trade))
    return series


def _slope(points):
    n = len(points)
    if n < 2:
        return 0.0
    mean_t = sum(t for t, _ in points) / n
    mean_v = sum(v for _, v in points) / n
    denom = sum((t - mean_t) ** 2 for t, _ in points)
    if denom == 0:
        return 0.0
    return sum((t - mean_t) * (v - mean_v) for t, v in points) / denom


def analyze(rows, accounts, c, rate, window_start, window_end, rejected_warn=0, heap_peak_ratio=None, fills_per_trade=FILLS_PER_TRADE, not_owned=0):
    series = open_series(rows, fills_per_trade)
    window = [(t, v) for t, v in series if window_start <= t <= window_end]
    target = accounts * c
    drift = _slope(window) * (window_end - window_start)
    before = [r for r in rows if r[0] < window_start]
    inside = [r for r in rows if r[0] <= window_end]
    first = before[-1] if before else rows[0]
    last = inside[-1] if inside else rows[-1]
    accept_rate = (last[4] - first[4]) / (last[0] - first[0]) if last[0] > first[0] else 0.0
    rejected_raw = sum(r[2] for r in rows)
    rejected_total = rejected_raw - not_owned   # 다른 shard 계좌의 체결 fan-out(NOT_OWNED)은 정상 경로라 뺀다
    checks = {
        "slope": abs(drift) <= SLOPE_LIMIT * target,
        "accept_ratio": accept_rate / rate >= ACCEPT_RATIO_LIMIT,
        "no_rejects": rejected_total == 0,
        "no_snapshot_warn": rejected_warn == 0,
        "heap": heap_peak_ratio is None or heap_peak_ratio < HEAP_LIMIT,
    }
    return {
        "target_open": target,
        "window_points": len(window),
        "window_seconds": window_end - window_start,
        "drift": drift,
        "accept_rate": accept_rate,
        "accept_ratio": accept_rate / rate,
        "rejected_total": rejected_total,
        "rejected_raw": rejected_raw,
        "not_owned_fanout": not_owned,
        "snapshot_warn": rejected_warn,
        "heap_peak_ratio": heap_peak_ratio,
        "final_open": series[-1][1] if series else None,
        "checks": checks,
        "balanced": all(checks.values()),
    }


def heap_peak_ratio(path, max_bytes):
    peak = 0
    names = None
    for line in open(path, errors="replace"):
        parts = line.split()
        if not parts:
            continue
        if parts[0] == "S0C":
            names = parts
            continue
        if names is None or len(parts) < len(names):
            continue
        try:
            used_kb = sum(float(parts[names.index(k)]) for k in ("S0U", "S1U", "EU", "OU"))
        except ValueError:
            continue
        peak = max(peak, used_kb * 1024)
    return peak / max_bytes


def _read_params(result_dir):
    text = open(os.path.join(result_dir, "params.txt"), encoding="utf-8").read()
    return {k: v for k, v in (kv.split("=", 1) for kv in text.split() if "=" in kv)}


def _read_paced(result_dir):
    text = open(os.path.join(result_dir, "paced.out"), encoding="utf-8", errors="replace").read()
    start = int(re.search(r"PACED_START (\d+)", text).group(1)) / 1000.0
    done = re.search(r"PACED_DONE sent=(\d+) elapsedMs=(\d+) maxLatenessMs=(\d+) avgLatenessMs=(\d+) over1sCount=(\d+)", text)
    return start, done


def report(result_dir, skip=30, heap_max_bytes=8 * 1024 ** 3):
    params = _read_params(result_dir)
    start, done = _read_paced(result_dir)
    window_end = start + int(done.group(2)) / 1000.0 if done else start
    log_paths = [os.path.join(result_dir, "logs", "account.log")]
    second_log = os.path.join(result_dir, "logs", "account-1.log")
    if os.path.exists(second_log):
        log_paths.append(second_log)
    raw_rows = [parse_metrics(path) for path in log_paths]
    reasons = []
    for path, rows in zip(log_paths, raw_rows):
        status, notes = classify_rows(rows)
        if status == "invalid":
            reasons.append("%s: %s" % (os.path.basename(path), "; ".join(notes)))
    rows = merge_rows([monotone_rows(r) for r in raw_rows])
    warn = sum(sum(1 for line in open(path, errors="replace") if "스냅샷 쓰기 큐가 가득 차" in line) for path in log_paths)
    heap_paths = [path for path in (os.path.join(result_dir, "heap.jstat"), os.path.join(result_dir, "heap-w1.jstat")) if os.path.exists(path)]
    ratio = max(heap_peak_ratio(path, heap_max_bytes) for path in heap_paths) if heap_paths else None
    per_worker_not_owned = [sum(1 for line in open(path, errors="replace") if "담당 슬롯이 아닌 계좌로 이벤트가 왔습니다" in line) for path in log_paths]
    not_owned = sum(per_worker_not_owned)
    result = analyze(rows, int(params["ACCTS"]), int(params["C"]), int(params["RATE"]), start + skip, window_end, warn, ratio, not_owned=not_owned)
    trades = int(params.get("TOTAL_TRADES", "0"))
    result["not_owned_per_worker"] = per_worker_not_owned
    if len(log_paths) > 1 and trades:
        # 교차 체결 1건은 두 워커에 한 번씩 NOT_OWNED를 낸다.
        result["cross_shard_trades"] = not_owned // 2
        result["cross_shard_ratio"] = (not_owned / 2) / trades
    if trades:
        result["checks"]["not_owned_range"] = not_owned_ok(per_worker_not_owned, trades)
        result["fills_complete"] = fills_complete(rows, trades)
        if not result["fills_complete"]:
            reasons.append("체결 반영이 끝나지 않았다: 누계 fill %d != 2 x 총 체결 %d" % (sum(r[3] for r in rows), trades))
    first_ts = rows[0][0] if rows else None
    last_ts = rows[-1][0] if rows else None
    if first_ts is None or start + skip < first_ts or window_end > last_ts + 2 or result["window_points"] < 0.5 * result["window_seconds"]:
        reasons.append("판정 구간이 보고 줄 범위를 벗어나거나 줄이 너무 적다(구간 줄 %d, 구간 %.0f초)" % (result["window_points"], result["window_seconds"]))
    result["status"] = "invalid" if reasons else "ok"
    result["invalid_reasons"] = reasons
    result["balanced"] = all(result["checks"].values()) and not reasons
    if done:
        result["sender"] = {"sent": int(done.group(1)), "elapsed_ms": int(done.group(2)), "max_lateness_ms": int(done.group(3)),
                            "avg_lateness_ms": int(done.group(4)), "over_1s_count": int(done.group(5))}
    return result


def main(argv):
    skip = 30
    heap_max = 8 * 1024 ** 3
    args = argv[1:]
    while args and args[0].startswith("--"):
        if args[0] == "--skip":
            skip = int(args[1])
        elif args[0] == "--heap-max-bytes":
            heap_max = int(args[1])
        else:
            sys.exit("알 수 없는 옵션: " + args[0])
        args = args[2:]
    if len(args) != 1:
        sys.exit(__doc__)
    print(json.dumps(report(args[0], skip, heap_max), ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main(sys.argv)
