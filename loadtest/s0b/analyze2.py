#!/usr/bin/env python3
"""run.sh 결과 폴더 하나를 읽어 한 줄 JSON을 낸다(analyze.py를 b4 22차 이후 지적에 맞게 고친 판). 원자료만 읽으므로 이미 돌린 실행도 다시 분석할 수 있다.
사용: analyze2.py <결과 폴더>
측정 구간은 시작·끝 시각 파일이 아니라 보고기 줄로 잡는다: 「쌓기 단계가 끝난 누적(PREFILL×3)에서 처음 늘기 시작한 행」부터 「누적이 기대값에 처음 닿은 행」까지.
속도는 (누적 증가) ÷ (로그 줄 시각 차)로 구한다(행 간격이 정확히 1초라고 가정하지 않는다). 가운데의 0 행도 구간에 넣는다.
"""
import datetime, json, os, re, statistics, sys

out = sys.argv[1]
params = dict(kv.split("=") for kv in open(os.path.join(out, "params.txt")).read().split())
n, per_stock, prefill, order_log = int(params["ACCTS"]), int(params["PER_STOCK"]), int(params["PREFILL_PER_STOCK"]), params["ORDER_LOG"]
STOCKS = 3
k = per_stock * STOCKS
prefill_total = prefill * STOCKS
expected = prefill_total + k


def ts(line):
    return datetime.datetime.fromisoformat(line.split(" ")[0]).timestamp()


account_log = os.path.join(out, "logs", "account.log")
order_ts, rows, engine, send, kinds = [], [], [], [], {}
for line in open(account_log, errors="replace"):
    if "매도 검증 통과" in line:
        order_ts.append(ts(line))
    elif "metrics accepted=" in line:
        m = re.search(r"accepted=(\d+) rejected=(\d+) .*acceptedTotal=(\d+)", line)
        rows.append((ts(line), int(m.group(1)), int(m.group(2)), int(m.group(3))))
    elif "account-engine ringRemaining=" in line:
        m = re.search(r"ringRemaining=(-?\d+) ringFullWaits=(\d+) journalBackPressure=(\d+) journalOtherRetries=(\d+)", line)
        engine.append((ts(line), *map(int, m.groups())))
    elif "matching-send outbox=" in line:
        m = re.search(r"outbox=(\d+) enqueueWaits=(\d+) enqueueWaitMs=([\d.]+) offerRetries=(\d+)", line)
        send.append((ts(line), int(m.group(1)), int(m.group(2)), float(m.group(3)), int(m.group(4))))
    m = re.search(r"\s(INFO|WARN|ERROR)\s+\d+ --- \[[^\]]*\]\s+(\S+)\s+:", line)
    if m:
        kinds[m.group(2)] = kinds.get(m.group(2), 0) + 1

result = {"params": params, "invalid": []}

# 측정 구간(보고기 줄 기준)
base = prefill_total
before = [r for r in rows if r[3] <= base]
after = [r for r in rows if r[3] > base]
t0 = t1 = None
if after:
    t0 = before[-1][0] if before else after[0][0] - 1.0
    reached = [r for r in rows if r[3] >= expected]
    if reached:
        t1 = reached[0][0]
if t0 is None or t1 is None or t1 <= t0:
    result["invalid"].append("보고기 줄로 측정 구간을 잡지 못함")
else:
    seconds = t1 - t0
    result["window_seconds"] = round(seconds, 2)
    result["reporter_rate"] = round(k / seconds, 1)
    if seconds < 8.0:
        # 보고기는 1초 간격이라 구간 양끝이 최대 1초씩 흔들린다. 구간이 8초 미만이면 속도 오차가 12%를 넘을 수 있어 주문당 로그의 밀리초 속도만 쓴다.
        result["reporter_rate_unreliable"] = "구간 %.1f초(8초 미만)" % seconds
    result["reporter_rows_in_window"] = len([r for r in rows if t0 < r[0] <= t1])
    result["rejected_in_window"] = sum(r[2] for r in rows if t0 < r[0] <= t1)
    inwin = lambda t: t0 - 0.0 <= t <= t1
    eng = [e for e in engine if inwin(e[0])]
    if eng:
        result["engine_rows"] = len(eng)
        result["ring_remaining_min"] = min(e[1] for e in eng)
        result["ring_full_waits_sum"] = sum(e[2] for e in eng)
        result["journal_back_pressure_sum"] = sum(e[3] for e in eng)
        result["journal_other_retries_sum"] = sum(e[4] for e in eng)
    sd = [s for s in send if inwin(s[0])]
    if sd:
        result["outbox_max"] = max(s[1] for s in sd)
        result["send_enqueue_waits_sum"] = sum(s[2] for s in sd)
        result["send_offer_retries_sum"] = sum(s[4] for s in sd)
        if result["send_enqueue_waits_sum"] > 0:
            result["matching_side_blocked"] = True
    # GC 정지(계좌 JVM)
    gc = os.path.join(out, "logs", "gc-account.log")
    pause_ms, pauses = 0.0, 0
    if os.path.exists(gc):
        for line in open(gc, errors="replace"):
            m = re.match(r"\[([^\]]+)\].*Pause .* ([\d.]+)ms$", line.strip())
            if m:
                t = datetime.datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
                if inwin(t):
                    pause_ms += float(m.group(2)); pauses += 1
    result["gc_pause_ms_sum"] = round(pause_ms, 1)
    result["gc_pauses"] = pauses
    result["gc_pause_share_pct"] = round(100.0 * pause_ms / 1000.0 / seconds, 2)

# 주문당 로그 기반 속도(로그를 켠 실행)
if order_log == "on":
    if len(order_ts) != expected:
        result["invalid"].append("주문당 로그 줄 %d개 != 보낸 수 %d" % (len(order_ts), expected))
    if len(order_ts) >= k + 1:
        last = order_ts[-k:]
        result["per_order_rate"] = round((k - 1) / (last[-1] - last[0]), 1)
        half = k // 2
        a, b = last[:half], last[half:]
        result["per_order_rate_first_half"] = round((len(a) - 1) / (a[-1] - a[0]), 1)
        result["per_order_rate_second_half"] = round((len(b) - 1) / (b[-1] - b[0]), 1)
        gaps = [y - x for x, y in zip(last, last[1:])]
        result["max_gap_ms"] = round(max(gaps) * 1000, 1)
        if prefill > 0 and len(order_ts) >= prefill_total + k:
            first = order_ts[:prefill_total]
            result["prefill_rate"] = round((len(first) - 1) / (first[-1] - first[0]), 1)
else:
    if order_ts:
        result["invalid"].append("주문당 로그를 껐는데 「매도 검증 통과」 줄이 %d개 남음" % len(order_ts))
# 계좌당 쌓인 수의 변화(고정 m 판정): 측정 단계가 시작 m에 더하는 비율
result["m_start_per_account"] = round(prefill_total / n, 1)
result["m_growth_pct"] = round(100.0 * k / prefill_total, 1) if prefill_total else None
if result.get("m_growth_pct") and result["m_growth_pct"] > 10.0:
    result["fixed_m_note"] = "측정 단계가 m을 %.0f%% 늘림(10%% 초과): 값은 m이 시작 m부터 그 이상까지 걸친 평균" % result["m_growth_pct"]
result["log_line_kinds_top"] = dict(sorted(kinds.items(), key=lambda x: -x[1])[:5])
if result.get("reporter_rows_in_window", 99) < 2:
    result["invalid"].append("측정 구간 안 보고기 행이 2개 미만(접수가 너무 짧아 보고기로 속도를 못 읽음): 주문당 로그 속도만 사용")
print(json.dumps(result, ensure_ascii=False))
