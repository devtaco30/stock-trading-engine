#!/usr/bin/env python3
"""cpu_sample.sh 출력(cpu.log)에서 구간 안 프로세스별 평균 CPU 사용률(코어 1개 = 100%)과 최대 RSS를 계산하고,
jcmd Thread.print 두 번(threads-1.txt, threads-2.txt)의 차이로 스레드별 CPU 사용률을 계산한다.
사용: cpu_summary.py <결과 폴더> [시작 epoch초 끝 epoch초]   구간을 안 주면 steps.log의 START STEADY ~ END STEADY."""
import json
import os
import re
import sys


def parse_cputime(text):
    parts = text.split(":")
    seconds = 0.0
    for part in parts:
        seconds = seconds * 60 + float(part)
    return seconds


def parse_log(path):
    rows = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            fields = line.split()
            if len(fields) == 5:
                rows.append((int(fields[0]), fields[1], fields[2], parse_cputime(fields[3]), int(fields[4])))
    return rows


def summarize(rows, start, end):
    """역할마다 구간 안 첫 표본과 마지막 표본의 누적 CPU 차이 / 시간 차이 x 100. 같은 역할의 pid가 여럿이면 pid별로 계산해 가장 큰 값을 쓴다."""
    by_key = {}
    for epoch, role, pid, cpu, rss in rows:
        if start <= epoch <= end:
            by_key.setdefault((role, pid), []).append((epoch, cpu, rss))
    result = {}
    for (role, pid), samples in by_key.items():
        if len(samples) < 2 or samples[-1][0] == samples[0][0]:
            continue
        pct = (samples[-1][1] - samples[0][1]) / (samples[-1][0] - samples[0][0]) * 100.0
        max_rss_mb = max(sample[2] for sample in samples) / 1024.0
        if role not in result or pct > result[role]["avg_cpu_pct"]:
            result[role] = {"avg_cpu_pct": round(pct, 1), "max_rss_mb": round(max_rss_mb), "samples": len(samples)}
    return result


THREAD_RE = re.compile(r'^"([^"]+)".*?cpu=([0-9.]+)ms elapsed=([0-9.]+)s.*?nid=(\S+)')


def parse_threads(text):
    threads = {}
    for line in text.splitlines():
        match = THREAD_RE.match(line)
        if match:
            threads[(match.group(1), match.group(4))] = (float(match.group(2)), float(match.group(3)))
    return threads


def thread_usage(first, second, top=8):
    rows = []
    for key, (cpu2, elapsed2) in second.items():
        if key in first:
            cpu1, elapsed1 = first[key]
            wall = elapsed2 - elapsed1
            if wall > 0:
                rows.append({"thread": key[0], "cpu_pct": round((cpu2 - cpu1) / (wall * 1000.0) * 100.0, 1)})
    rows.sort(key=lambda row: -row["cpu_pct"])
    return rows[:top]


def steady_window(result_dir):
    start = end = None
    with open(os.path.join(result_dir, "steps.log"), encoding="utf-8") as handle:
        for line in handle:
            fields = line.split()
            if len(fields) >= 3 and fields[1] == "START" and fields[2] == "STEADY":
                start = float(fields[0])
            if len(fields) >= 3 and fields[1] == "END" and fields[2] == "STEADY":
                end = float(fields[0])
    return start, end


def report(result_dir, start=None, end=None):
    if start is None or end is None:
        start, end = steady_window(result_dir)
    result = {"window": [start, end], "processes": summarize(parse_log(os.path.join(result_dir, "cpu.log")), start, end)}
    first_path = os.path.join(result_dir, "threads-1.txt")
    second_path = os.path.join(result_dir, "threads-2.txt")
    if os.path.exists(first_path) and os.path.exists(second_path):
        with open(first_path, encoding="utf-8") as handle1, open(second_path, encoding="utf-8") as handle2:
            result["account_threads_top"] = thread_usage(parse_threads(handle1.read()), parse_threads(handle2.read()))
    return result


if __name__ == "__main__":
    args = sys.argv[1:]
    window = (float(args[1]), float(args[2])) if len(args) >= 3 else (None, None)
    print(json.dumps(report(args[0], *window), ensure_ascii=False))
