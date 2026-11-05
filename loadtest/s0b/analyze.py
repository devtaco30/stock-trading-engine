#!/usr/bin/env python3
"""run.sh의 결과 폴더 하나를 읽어 접수 속도, 계좌 쪽 보고기 값, GC 정지를 한 줄 JSON으로 낸다.
사용: analyze.py <결과 폴더>
- per_order_rate: 주문당 「매도 검증 통과」 로그 줄의 밀리초 시각으로 구한 구간 속도(로그를 켠 실행만). 측정 단계의 마지막 K줄 = 2단계 주문이다.
- reporter_rate: 보고기 metrics 줄의 acceptedTotal 1초 증분 평균(측정 구간 안, 첫·끝 부분 초 제외).
- account-engine 줄은 측정 구간 안의 값만 모은다. GC 정지는 계좌 JVM의 gc 로그에서 측정 구간 안의 Pause 합이다.
"""
import datetime, glob, json, os, re, sys

out = sys.argv[1]
params = dict(kv.split("=") for kv in open(os.path.join(out, "params.txt")).read().split())
per_stock, prefill = int(params["PER_STOCK"]), int(params["PREFILL_PER_STOCK"])
STOCKS = 3


def log_ts(line):
    return datetime.datetime.fromisoformat(line.split(" ")[0]).timestamp()


def read_float(name):
    p = os.path.join(out, name)
    return float(open(p).read().strip()) if os.path.exists(p) else None


m_start, m_end = read_float("measure-start"), read_float("measure-end")
account_log = os.path.join(out, "logs", "account.log")
order_ts, report = [], []
for line in open(account_log, errors="replace"):
    if "매도 검증 통과" in line:
        order_ts.append(log_ts(line))
    elif "metrics accepted=" in line:
        m = re.search(r"accepted=(\d+) rejected=(\d+) .*acceptedTotal=(\d+)", line)
        report.append((log_ts(line), int(m.group(1)), int(m.group(2)), int(m.group(3))))
    elif "account-engine ringRemaining=" in line:
        m = re.search(r"ringRemaining=(-?\d+) ringFullWaits=(\d+) (?:ringFullWaitMs=[\d.]+ )?journalBackPressure=(\d+) journalOtherRetries=(\d+)", line)
        report.append((log_ts(line), "E", *map(int, m.groups())))

result = {"params": params}
k = per_stock * STOCKS
if len(order_ts) >= k + 1:
    last = order_ts[-k:]
    result["per_order_rate"] = round((k - 1) / (last[-1] - last[0]), 1)
    result["per_order_seconds"] = round(last[-1] - last[0], 3)
    if prefill > 0 and len(order_ts) >= prefill * STOCKS + k:
        first = order_ts[:prefill * STOCKS]
        result["prefill_rate"] = round((len(first) - 1) / (first[-1] - first[0]), 1)
result["order_log_lines"] = len(order_ts)

win = lambda t: m_start is not None and m_start - 1 <= t <= m_end + 1
sec = [r for r in report if r[1] != "E" and win(r[0])]
# 첫·끝 부분 초를 뺀다. 증분이 0인 앞뒤 초도 같이 걸러진다.
active = [r for r in sec if r[1] > 0]
if len(active) > 2:
    inner = active[1:-1]
    result["reporter_rate"] = round(sum(r[1] for r in inner) / len(inner), 1)
    result["reporter_seconds_used"] = len(inner)
result["rejected_in_window"] = sum(r[2] for r in sec)
eng = [r for r in report if r[1] == "E" and win(r[0])]
if eng:
    result["ring_remaining_min"] = min(r[2] for r in eng)
    result["ring_full_waits_sum"] = sum(r[3] for r in eng)
    result["journal_back_pressure_sum"] = sum(r[4] for r in eng)
    result["journal_other_retries_sum"] = sum(r[5] for r in eng)
    result["engine_lines"] = len(eng)

gc_path = os.path.join(out, "logs", "gc-account.log")
pause_ms, pauses = 0.0, 0
if os.path.exists(gc_path) and m_start is not None:
    for line in open(gc_path, errors="replace"):
        m = re.match(r"\[([^\]]+)\].*Pause .* ([\d.]+)ms$", line.strip())
        if m:
            t = datetime.datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
            if m_start - 1 <= t <= m_end + 1:
                pause_ms += float(m.group(2))
                pauses += 1
result["gc_pause_ms_sum"] = round(pause_ms, 1)
result["gc_pauses"] = pauses
if m_start is not None:
    result["measure_seconds"] = round(m_end - m_start, 2)
print(json.dumps(result, ensure_ascii=False))
