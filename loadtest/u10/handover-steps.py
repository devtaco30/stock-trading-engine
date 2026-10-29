#!/usr/bin/env python3
"""U10 이동 단계별 시간 — 종목 slot마다 네 구간을 내고 slot별 최소·중앙·최대를 출력한다.

사용: handover-steps.py <맵 기록 파일(slot<TAB>브로커 기록 시각 ms<TAB>값)> <matching-1 로그>
시각의 출처(이름에 적는다):
  REQ   = matching-1 로그 「[호가창 이동] 요청 보냄」 줄 시각(slot의 첫 줄)
  RESP  = matching-1 로그 「[호가창 이동] 응답 완성」 줄 시각
  LOAD  = matching-1 로그 「[호가창 이동] 적재 확정」 줄 시각
  ON    = matching-1 로그 「[호가창 이동] 담당 켜짐」 줄 시각
  MAP   = 맵 토픽에서 그 slot이 pod 1 주소의 평상시 값으로 바뀐 기록의 브로커 기록 시각(CreateTime)
로그 시각은 ms 단위 로컬 시각이고 맵 기록 시각은 브로커 기록 시각이라 같은 컴퓨터의 벽시계 기준이다.
"""
import datetime
import re
import statistics
import sys

POD1 = 'aeron:udp?endpoint=localhost:20021'


def log_ts(line):
    return int(datetime.datetime.fromisoformat(line.split(' ')[0]).timestamp() * 1000)


def collect(log_path):
    first = {}
    resend = {}
    pats = {
        'REQ': re.compile(r'\[호가창 이동\] 요청 보냄: .*종목 slot=(\d+)'),
        'RESEND': re.compile(r'\[호가창 이동\] 요청 다시 보냄: .*종목 slot=(\d+)'),
        'RESP': re.compile(r'\[호가창 이동\] 응답 완성: .*종목 slot=(\d+)'),
        'LOAD': re.compile(r'\[호가창 이동\] 적재 확정: .*종목 slot=(\d+)'),
        'ON': re.compile(r'\[호가창 이동\] 담당 켜짐: 종목 slot=(\d+)'),
    }
    for line in open(log_path, errors='replace'):
        for name, pat in pats.items():
            m = pat.search(line)
            if not m:
                continue
            slot = int(m.group(1))
            if name == 'RESEND':
                resend[slot] = resend.get(slot, 0) + 1
            else:
                first.setdefault((name, slot), log_ts(line))
    return first, resend


def map_switch(map_path):
    final = {}
    for line in open(map_path):
        parts = line.rstrip('\n').split('\t')
        if len(parts) < 3 or not parts[0].isdigit():
            continue
        final[int(parts[0])] = (int(parts[1]), parts[2])
    return {slot: ts for slot, (ts, value) in final.items() if value == POD1}


def summarize(name, values):
    if not values:
        print('STEP %s=계산 불가(slot 0개)' % name)
        return
    print('STEP %s min=%d median=%d max=%d slots=%d' % (
        name, min(values), int(statistics.median(values)), max(values), len(values)))


def main():
    first, resend = collect(sys.argv[2])
    to_map = map_switch(sys.argv[1])
    slots = sorted({slot for (name, slot) in first if name == 'REQ'})
    req_resp, resp_load, load_on, on_map, req_map = [], [], [], [], []
    for slot in slots:
        req, resp = first.get(('REQ', slot)), first.get(('RESP', slot))
        load, on, mapped = first.get(('LOAD', slot)), first.get(('ON', slot)), to_map.get(slot)
        if req is not None and resp is not None:
            req_resp.append(resp - req)
        if resp is not None and load is not None:
            resp_load.append(load - resp)
        if load is not None and on is not None:
            load_on.append(on - load)
        if on is not None and mapped is not None:
            on_map.append(mapped - on)
        if req is not None and mapped is not None:
            req_map.append(mapped - req)
    print('STEP 이동한 slot=%d개, 요청 다시 보냄이 있었던 slot=%d개' % (len(slots), len(resend)))
    summarize('REQ_TO_RESP_MS(요청 보냄 로그→응답 완성 로그)', req_resp)
    summarize('RESP_TO_LOAD_MS(응답 완성 로그→적재 확정 로그)', resp_load)
    summarize('LOAD_TO_ON_MS(적재 확정 로그→담당 켜짐 로그)', load_on)
    summarize('ON_TO_MAP_MS(담당 켜짐 로그→맵 전환 기록의 브로커 기록 시각)', on_map)
    summarize('REQ_TO_MAP_MS(요청 보냄 로그→맵 전환 기록의 브로커 기록 시각)', req_map)


main()
