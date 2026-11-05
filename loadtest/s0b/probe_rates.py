#!/usr/bin/env python3
"""ladder.sh 실행(계좌 수 N 고정, 계좌당 미체결 C를 단계별로 올림)에서 단계별 접수·거부 속도를 낸다.

입력: 결과 폴더의 logs/account.log(계좌 워커 보고기의 1초 단위 접수·거부 증분)와 plan.tsv(ladder_plan.py가 만든 단계 계획).
보고기 줄의 accepted·rejected는 직전 구간의 증분이다.

단계 나누기: 단계를 계획 순서대로(PREFILL, REJECT, 가끔 ACCEPT) 읽고, 쌓기·접수 검증은 접수 증분을, 거부는 거부 증분을 계획의 기대 건수
(종목당 per_stock x 종목 3개)에 닿을 때까지 모은다. 두 증분은 서로 독립된 통로로 처리하므로, 쌓기가 끝나는 구간에서 거부가 시작되는
섞인 구간도 양쪽 단계가 나눠 갖는다. 멈춘 구간(증분 0)은 단계를 나누지 않는다.
속도: 단계에 증분을 준 구간 중 앞뒤 한 개씩과 다른 통로의 증분이 섞인 구간을 뺀 나머지의 합 / 그 구간 시간의 합.
뺄 수 없을 만큼 구간이 적으면(3개 미만) 전부 쓰고 edges_trimmed를 거짓으로 둔다.

사용:
  probe_rates.py <결과 폴더>              한 실행을 JSON으로
  probe_rates.py --table <결과 폴더>...   N별 행, C별 열의 표(거부 속도, 같은 C에 접수 검증이 있으면 접수 대 거부 비)
"""
import datetime
import json
import os
import re
import statistics
import sys

STOCKS = 3
TOLERANCE = 0.02
REPORT_INTERVAL_SECONDS = 1.0


def parse_intervals(path):
    """[(보고 시각, 접수 증분, 거부 증분), ...] 시각순."""
    rows = []
    with open(path, errors="replace") as log:
        for line in log:
            if "metrics accepted=" not in line:
                continue
            m = re.search(r"accepted=(\d+) rejected=(\d+) ", line)
            if not m:
                continue
            ts = datetime.datetime.fromisoformat(line.split(" ")[0]).timestamp()
            rows.append((ts, int(m.group(1)), int(m.group(2))))
    rows.sort()
    return rows


def parse_plan(text):
    plan = {"hold": None, "steps": []}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if parts[0] == "HOLD":
            plan["hold"] = int(parts[1])
        else:
            plan["steps"].append({"kind": parts[0], "rung": int(parts[1]), "per_stock": int(parts[2]),
                                  "quantity": int(parts[3]), "label": parts[4]})
    return plan


def _channel(kind):
    return "R" if kind == "REJECT" else "A"


def analyze(intervals, plan, tolerance=TOLERANCE):
    steps = plan["steps"]
    state = [{"step": s, "expected": s["per_stock"] * STOCKS, "got": 0, "rows": []} for s in steps]
    # 첫 보고 줄은 이전 줄이 없으므로 보고 간격(1초)만큼의 구간으로 본다. 실제 실행에서는 시작 직후라 증분이 0이다.
    previous_times = [intervals[0][0] - REPORT_INTERVAL_SECONDS] + [row[0] for row in intervals[:-1]] if intervals else []
    for prev_ts, cur in zip(previous_times, intervals):
        duration = cur[0] - prev_ts
        if duration <= 0:
            continue
        counts = {"A": cur[1], "R": cur[2]}
        mixed = counts["A"] > 0 and counts["R"] > 0
        for channel in ("A", "R"):
            remaining = counts[channel]
            for st in state:
                if remaining <= 0:
                    break
                if _channel(st["step"]["kind"]) != channel or st["got"] >= st["expected"]:
                    continue
                take = min(remaining, st["expected"] - st["got"])
                st["got"] += take
                st["rows"].append({"count": take, "duration": duration, "mixed": mixed})
                remaining -= take
    results = []
    for st in state:
        s = st["step"]
        rows = st["rows"]
        trimmed = len(rows) >= 3
        kept = [r for r in rows[1:-1] if not r["mixed"]] if trimmed else rows
        result = {"kind": s["kind"], "rung": s["rung"], "label": s["label"], "expected": st["expected"],
                  "observed": st["got"], "count_ok": abs(st["got"] - st["expected"]) <= tolerance * st["expected"],
                  "edges_trimmed": trimmed}
        if kept:
            seconds = sum(r["duration"] for r in kept)
            total = sum(r["count"] for r in kept)
            per_sec = [r["count"] / r["duration"] for r in kept]
            result.update({"window_seconds": round(seconds, 3), "rate": round(total / seconds, 1), "samples": len(kept),
                           "sec_rate_median": round(statistics.median(per_sec), 1),
                           "sec_rate_min": round(min(per_sec), 1), "sec_rate_max": round(max(per_sec), 1)})
        results.append(result)
    return results


def read_run(result_dir):
    with open(os.path.join(result_dir, "params.txt")) as params_file:
        params = dict(kv.split("=") for kv in params_file.read().split())
    with open(os.path.join(result_dir, "plan.tsv"), encoding="utf-8") as plan_file:
        plan = parse_plan(plan_file.read())
    intervals = parse_intervals(os.path.join(result_dir, "logs", "account.log"))
    return int(params["ACCTS"]), plan, analyze(intervals, plan)


def report(result_dir):
    accounts, plan, results = read_run(result_dir)
    return {"accounts": accounts, "hold": plan["hold"], "steps": results}


def table(result_dirs):
    by_n = {}
    labels = []
    for d in result_dirs:
        accounts, _, results = read_run(d)
        by_n.setdefault(accounts, []).append(results)
        for r in results:
            if r["kind"] == "REJECT" and r["label"] not in labels:
                labels.append(r["label"])
    out = ["N | " + " | ".join(labels)]
    for n in sorted(by_n):
        cells = []
        for label in labels:
            rejects, ratios = [], []
            for results in by_n[n]:
                rej = next((r for r in results if r["kind"] == "REJECT" and r["label"] == label and "rate" in r), None)
                acc = next((r for r in results if r["kind"] == "ACCEPT" and r["label"] == label and "rate" in r), None)
                if rej:
                    rejects.append(rej)
                    if acc and rej["rate"] > 0:
                        ratios.append(acc["rate"] / rej["rate"])
            if not rejects:
                cells.append("안 쟀음")
                continue
            ok = "" if all(r["count_ok"] for r in rejects) else " 건수불일치"
            text = "거부 %s 건/초 (창 %s초, 표본 %s개)%s" % ("/".join("%.0f" % r["rate"] for r in rejects),
                                                      "/".join("%.1f" % r["window_seconds"] for r in rejects),
                                                      "/".join(str(r["samples"]) for r in rejects), ok)
            if ratios:
                text += ", 접수/거부 " + "/".join("%.2f" % x for x in ratios)
            cells.append(text)
        out.append("N=%d | " % n + " | ".join(cells))
    return "\n".join(out)


def main(argv):
    args = list(argv)
    if args and args[0] == "--table":
        return table(args[1:])
    if len(args) == 1:
        return json.dumps(report(args[0]), ensure_ascii=False)
    raise SystemExit(__doc__)


if __name__ == "__main__":
    print(main(sys.argv[1:]))
