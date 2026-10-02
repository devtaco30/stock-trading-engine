#!/usr/bin/env python3
"""k6 단독 포화점 리허설(설계서 6-3a, measure-protocol-c.md U7b)의 계산·표·샘플러. 앱 없이 더미 서버(DummyOrderServer.java)에
k6를 붙여, 속도마다 k6가 예정 속도를 지키는지 본다. 이 파일은 부하를 걸지 않는다(걸리는 쪽은 saturate-k6.sh).

명령:
  solo_saturation.py plan   --rates "192 500" --arrival-sec 20 --input-lines 100000   속도별 N 과 예상 소요 시간
  solo_saturation.py sample --out cpu.csv --pid k6=123 --pid server=456 [--interval 1]  1초 간격 CPU 샘플러(종료 신호까지)
  solo_saturation.py table  <결과 디렉터리> --rates "192 500" --arrival-sec 20 --input-lines 100000 [--out table.md]

포화 판정은 k6 dropped_iterations 1 이상, 또는 더미 서버가 센 실제 도착률이 목표 속도에서 5% 이상 벗어난 경우다.
k6 CPU는 표에 기록만 하고 판정에 쓰지 않는다(protocol-c 4-3절: 기능 문제가 없으면 CPU 값만으로 포화라 부르지 않는다).
도착률 5% 이탈은 선언 문서에 기준이 없어 새로 정한 임의값이다.
「k6 단독 포화점의 몇 %까지만 인용한다」는 비율은 리허설 뒤에 선언 문서가 정하므로 여기서 계산하지 않는다(표만 출력한다).
"""
import argparse
import csv
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

DEFAULT_RATES = [192, 500, 1000, 2000, 4000, 6000]
ARRIVAL_DEVIATION_LIMIT = 0.05   # 실제 도착률이 목표 속도에서 이 비율 이상 벗어나면 포화 (임의값, 선언 문서에 기준 없음)
SAMPLE_HEADER = ["ts", "k6_cpu", "server_cpu", "total_busy_pct", "idle_pct"]


# ---------------------------------------------------------------- 계획

def parse_rates(text):
    parts = str(text).split()
    if not parts:
        raise ValueError("속도가 비어 있다")
    rates = []
    for part in parts:
        if not part.isdigit() or int(part) <= 0:
            raise ValueError(f"속도는 양의 정수여야 한다: {part!r}")
        rates.append(int(part))
    if len(set(rates)) != len(rates):
        raise ValueError("속도가 겹친다")
    return rates


def n_for_rate(rate, arrival_sec, input_lines):
    return min(rate * arrival_sec, input_lines)


def run_seconds(rate, arrival_sec, input_lines):
    """replay-e2e.js 의 duration = ceil(N / 속도) + 5."""
    n = n_for_rate(rate, arrival_sec, input_lines)
    return -(-n // rate) + 5


def estimate_seconds(rates, arrival_sec, input_lines, settle_sec, teardown_sec, gap_sec, server_start_sec):
    total = server_start_sec
    for rate in rates:
        total += run_seconds(rate, arrival_sec, input_lines) + settle_sec + teardown_sec + gap_sec
    return total


# ---------------------------------------------------------------- k6 요약, 도착률

def read_summary(summary):
    if summary is None:
        return None

    def values(name):
        metric = summary.get("metrics", {}).get(name) or {}
        return metric.get("values", metric)

    duration = values("http_req_duration")
    return {
        "dropped": int(values("dropped_iterations").get("count", 0)),
        "http_failed": int(values("http_req_failed").get("passes", 0)),
        "http_reqs": int(values("http_reqs").get("count", 0)),
        "lat_avg_ms": duration.get("avg"),
        "lat_p95_ms": duration.get("p(95)"),
        "lat_p99_ms": duration.get("p(99)"),
        "lat_max_ms": duration.get("max"),
    }


def actual_arrival_rate(stats):
    """더미 서버가 기록한 첫·마지막 요청 시각으로 계산한 실제 도착률(건/초). 두 건 미만이면 None."""
    if not stats or stats.get("count", 0) < 2:
        return None
    span = (stats["lastNanos"] - stats["firstNanos"]) / 1e9
    if span <= 0:
        return None
    return (stats["count"] - 1) / span


# ---------------------------------------------------------------- CPU 샘플러

def parse_ps_time(text):
    """ps 의 누적 CPU 시간("M:SS.cc" 또는 "H:MM:SS")을 초로."""
    text = text.strip()
    if not text:
        return None
    seconds = 0.0
    try:
        for part in text.split(":"):
            seconds = seconds * 60 + float(part)
    except ValueError:
        return None
    return seconds


def cpu_percent(previous_seconds, current_seconds, elapsed):
    """직전 누적 시간과의 차이를 경과 시간으로 나눈 1코어 대비 %. 비교할 값이 없으면 None, 누적이 줄었으면 0."""
    if previous_seconds is None or current_seconds is None or elapsed <= 0:
        return None
    return max(0.0, (current_seconds - previous_seconds) / elapsed * 100.0)


def idle_percent(total_busy_pct, ncpu):
    return max(0.0, 100.0 - total_busy_pct / ncpu)


def process_cpu_seconds(pid):
    result = subprocess.run(["ps", "-o", "time=", "-p", str(pid)], capture_output=True, text=True)
    return parse_ps_time(result.stdout)


def total_cpu_seconds():
    """모든 프로세스의 누적 CPU 시간 합. 끝난 프로세스는 빠지므로 전체 사용률은 근사값이다."""
    result = subprocess.run(["ps", "-A", "-o", "time="], capture_output=True, text=True)
    total = 0.0
    for line in result.stdout.splitlines():
        seconds = parse_ps_time(line)
        if seconds is not None:
            total += seconds
    return total


def run_sampler(out_path, interval, pids, samples=None):
    """interval 초 간격으로 k6(라벨 k6)와 서버(라벨 server)의 CPU, 전체 사용률과 idle 을 CSV 에 쓴다.
    샘플 하나는 ps 두 번이라 수십 ms 안에 끝난다. samples 가 없으면 종료 신호까지 돈다."""
    ncpu = os.cpu_count() or 1
    stop = {"flag": False}

    def request_stop(*_):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    previous = {"time": None, "total": None, "k6": None, "server": None}
    written = 0
    with open(out_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(SAMPLE_HEADER)
        handle.flush()
        next_tick = time.monotonic()
        while not stop["flag"] and (samples is None or written < samples):
            now = time.monotonic()
            total = total_cpu_seconds()
            k6 = process_cpu_seconds(pids["k6"]) if "k6" in pids else None
            server = process_cpu_seconds(pids["server"]) if "server" in pids else None
            row = [f"{time.time():.3f}", "", "", "", ""]
            if previous["time"] is not None:
                elapsed = now - previous["time"]
                busy = cpu_percent(previous["total"], total, elapsed)
                k6_cpu = cpu_percent(previous["k6"], k6, elapsed)
                server_cpu = cpu_percent(previous["server"], server, elapsed)
                row = [row[0], _fmt(k6_cpu), _fmt(server_cpu), _fmt(busy),
                       _fmt(idle_percent(busy, ncpu)) if busy is not None else ""]
            writer.writerow(row)
            handle.flush()
            written += 1
            previous = {"time": now, "total": total, "k6": k6, "server": server}
            next_tick += interval
            delay = next_tick - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            else:
                next_tick = time.monotonic()


def _fmt(value):
    return "" if value is None else f"{value:.1f}"


# ---------------------------------------------------------------- 포화 판정과 표

def judge_rate(dropped, http_failed, arrival, target_rate):
    """포화 이유 목록. k6 CPU 는 입력이 아니다(기록만). http 실패는 포화가 아니라 리허설 이상으로 따로 둔다."""
    reasons = []
    if dropped >= 1:
        reasons.append("k6_dropped")
    if arrival is not None and abs(arrival - target_rate) / target_rate >= ARRIVAL_DEVIATION_LIMIT:
        reasons.append("arrival_off")
    anomalies = ["http_failed"] if http_failed >= 1 else []
    return {"reasons": reasons, "saturated": bool(reasons), "anomalies": anomalies}


def _read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _float(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def build_row(run_dir, rate, n):
    run_dir = Path(run_dir)
    facts = read_summary(_read_json(run_dir / f"k6-{rate}.json"))
    if facts is None:
        return {"rate": rate, "n": n, "missing": True}
    stats = _read_json(run_dir / f"server-{rate}.json")
    cpu_rows = []
    try:
        with open(run_dir / f"cpu-{rate}.csv", newline="", encoding="utf-8") as handle:
            cpu_rows = list(csv.DictReader(handle))
    except OSError:
        pass
    k6_samples = [_float(row.get("k6_cpu")) for row in cpu_rows]
    idle_samples = [value for value in (_float(row.get("idle_pct")) for row in cpu_rows) if value is not None]
    present = [value for value in k6_samples if value is not None]
    arrival = actual_arrival_rate(stats)
    verdict = judge_rate(facts["dropped"], facts["http_failed"], arrival, rate)
    return {
        "rate": rate, "n": n, "missing": False,
        "dropped": facts["dropped"], "http_failed": facts["http_failed"], "arrival": arrival,
        "k6_cpu_mean": sum(present) / len(present) if present else None,
        "k6_cpu_max": max(present) if present else None,
        "idle_min": min(idle_samples) if idle_samples else None,
        "lat_avg_ms": facts["lat_avg_ms"], "lat_p95_ms": facts["lat_p95_ms"], "lat_p99_ms": facts["lat_p99_ms"],
        "lat_max_ms": facts["lat_max_ms"], **verdict,
    }


def _cell(value, digits=1):
    return "—" if value is None else f"{value:.{digits}f}"


def build_table(rows):
    lines = ["# k6 단독 포화점 리허설", ""]
    if not rows:
        return "\n".join(lines + ["결과가 없다."]) + "\n"
    header = ["속도(건/초)", "N", "dropped", "http 실패", "실제 도착률(건/초)", "k6 CPU 평균/최대(%)", "전체 idle 최솟값(%)",
              "지연 평균/p95/p99/최대(ms)", "판정"]
    lines.append("| 속도 | " + " | ".join(header[1:]) + " |")
    lines.append("|" + "|".join("---" for _ in header) + "|")
    first_saturated = None
    for row in rows:
        if row.get("missing"):
            lines.append(f"| {row['rate']} | {row['n']} | — | — | — | — | — | — | 입력 누락 |")
            continue
        judged = "포화: " + ", ".join(row["reasons"]) if row["saturated"] else "정상"
        if row["anomalies"]:
            judged += f" (리허설 이상: {', '.join(row['anomalies'])})"
        latency = "/".join(_cell(row[key], 2) for key in ("lat_avg_ms", "lat_p95_ms", "lat_p99_ms", "lat_max_ms"))
        lines.append(f"| {row['rate']} | {row['n']} | {row['dropped']} | {row['http_failed']} | {_cell(row['arrival'], 0)} | "
                     f"{_cell(row['k6_cpu_mean'])}/{_cell(row['k6_cpu_max'])} | {_cell(row['idle_min'])} | {latency} | {judged} |")
        if row["saturated"] and first_saturated is None:
            first_saturated = row["rate"]
    lines.append("")
    if first_saturated is None:
        lines.append("포화한 첫 속도: 없음 (시험한 속도에서 포화 없음)")
    else:
        lines.append(f"포화한 첫 속도: {first_saturated}건/초")
    lines += ["",
              "- 판정 신호: k6 dropped_iterations 1 이상(k6_dropped), 실제 도착률이 목표 속도에서 5% 이상 벗어남(arrival_off). 5%는 선언 문서에 기준이 없어 새로 정한 임의값이다.",
              "- k6 CPU는 기록만 하고 포화 판정에 쓰지 않는다(protocol-c 4-3절).",
              "- 실제 도착률은 더미 서버가 기록한 첫·마지막 요청 시각으로 계산했다. 전체 idle은 ps 누적 시간 합의 차이로 구한 근사값이다.",
              "- 인용 비율은 리허설 데이터를 본 뒤 선언 문서에 정한다. 이 표는 포화점만 보여 준다."]
    return "\n".join(lines) + "\n"


def build_table_for_dir(run_dir, rates, arrival_sec, input_lines):
    rows = [build_row(run_dir, rate, n_for_rate(rate, arrival_sec, input_lines)) for rate in rates]
    return build_table(rows)


# ---------------------------------------------------------------- CLI

def main(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan")
    plan.add_argument("--rates", default=" ".join(str(r) for r in DEFAULT_RATES))
    plan.add_argument("--arrival-sec", type=int, default=20)
    plan.add_argument("--input-lines", type=int, default=100000)
    plan.add_argument("--settle-sec", type=int, default=5)
    plan.add_argument("--teardown-sec", type=int, default=3)
    plan.add_argument("--gap-sec", type=int, default=5)
    plan.add_argument("--server-start-sec", type=int, default=3)

    sample = sub.add_parser("sample")
    sample.add_argument("--out", required=True)
    sample.add_argument("--interval", type=float, default=1.0)
    sample.add_argument("--pid", action="append", default=[], help="라벨=PID (k6, server)")

    table = sub.add_parser("table")
    table.add_argument("run_dir")
    table.add_argument("--rates", default=" ".join(str(r) for r in DEFAULT_RATES))
    table.add_argument("--arrival-sec", type=int, default=20)
    table.add_argument("--input-lines", type=int, default=100000)
    table.add_argument("--out")

    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            rates = parse_rates(args.rates)
            for rate in rates:
                n = n_for_rate(rate, args.arrival_sec, args.input_lines)
                print(f"{rate}/초 N={n} (k6 {run_seconds(rate, args.arrival_sec, args.input_lines)}초)")
            seconds = estimate_seconds(rates, args.arrival_sec, args.input_lines, args.settle_sec, args.teardown_sec,
                                       args.gap_sec, args.server_start_sec)
            print(f"예상 소요 약 {seconds}초 ({seconds / 60:.1f}분)")
            return 0
        if args.command == "sample":
            pids = {}
            for item in args.pid:
                label, _, value = item.partition("=")
                pids[label] = int(value)
            run_sampler(args.out, args.interval, pids)
            return 0
        text = build_table_for_dir(args.run_dir, parse_rates(args.rates), args.arrival_sec, args.input_lines)
        print(text)
        if args.out:
            Path(args.out).write_text(text, encoding="utf-8")
        return 0
    except ValueError as error:
        print(f"인자 오류: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
