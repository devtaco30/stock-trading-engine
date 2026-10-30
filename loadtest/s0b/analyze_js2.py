#!/usr/bin/env python3
"""thread dump를 역할별로 집계한다(analyze_js.py는 이름만 봐서 엔진의 저널·업무 thread가 「Thread-#」 하나로 묶였다).
사용: analyze_js2.py <결과 폴더>... — 접수 구간 안의 덤프만 쓴다. 폴더를 여러 개 주면 합산한다.
역할은 스택에 든 클래스로 정한다: journal(AccountJournalEventHandler), business(AccountEventHandler), receiver(AccountOrderReceiver),
sender(AeronMatchingOrderSender), snapshot-writer(AccountSnapshotWriter), recorder(OrderResultRecorder). 그 밖은 thread 이름에서 숫자를 지운 값.
각 역할에서 「가장 안쪽 프로젝트(com.flab) 프레임」을 세어 시간이 어느 메서드에서 쓰였는지 낸다. 엔진 thread가 이벤트를 기다리는 중(BatchEventProcessor 안)은 따로 센다.
"""
import collections, glob, os, re, sys

ROLES = [("journal", "AccountJournalEventHandler"), ("business", "disruptor.handler.AccountEventHandler"), ("receiver", "AccountOrderReceiver"),
         ("sender", "AeronMatchingOrderSender"), ("snapshot-writer", "AccountSnapshotWriter"), ("recorder", "OrderResultRecorder")]
roles = collections.defaultdict(lambda: {"n": 0, "state": collections.Counter(), "leaf": collections.Counter(), "top": collections.Counter()})
dumps = 0
for out in sys.argv[1:]:
    def rf(name):
        p = os.path.join(out, name)
        return float(open(p).read().strip()) if os.path.exists(p) else None
    m0, m1 = rf("measure-start"), rf("measure-end")
    for ts_path in glob.glob(os.path.join(out, "js", "*.ts")):
        t = float(open(ts_path).read().strip())
        if m0 is not None and not (m0 <= t <= (m1 or t)):
            continue
        txt = ts_path[:-3] + ".txt"
        if not os.path.exists(txt):
            continue
        dumps += 1
        block = None
        def flush(b):
            if not b:
                return
            frames = b["frames"]
            role = None
            joined = "\n".join(frames)
            for r, needle in ROLES:
                if needle in joined:
                    role = r
                    break
            if role is None:
                role = re.sub(r"\d+", "#", b["name"])
            e = roles[role]
            e["n"] += 1
            e["state"][b["state"] or "?"] += 1
            if frames:
                e["top"][frames[0]] += 1
            leaf = next((f for f in frames if f.startswith("com.flab")), None)
            waiting = any("BatchEventProcessor.processEvents" in f for f in frames) and leaf is None
            e["leaf"][leaf if leaf else ("(이벤트 대기: BatchEventProcessor)" if waiting else "(프로젝트 프레임 없음)")] += 1
        for line in open(txt, errors="replace"):
            m = re.match(r'^"([^"]+)"', line)
            if m:
                flush(block)
                block = {"name": m.group(1), "state": None, "frames": []}
                continue
            if block is None:
                continue
            m = re.match(r"\s+java\.lang\.Thread\.State: (\w+)", line)
            if m:
                block["state"] = m.group(1)
                continue
            m = re.match(r"\s+at (\S+)", line)
            if m:
                block["frames"].append(m.group(1))
        flush(block)
print("덤프 %d개(접수 구간 안, 폴더 %d개)" % (dumps, len(sys.argv) - 1))
interesting = ["receiver", "journal", "business", "sender", "snapshot-writer", "recorder"]
for role in interesting + [r for r in roles if r not in interesting and roles[r]["state"].get("RUNNABLE", 0) > dumps * 0.2 and r not in ("Reference Handler",)]:
    e = roles.get(role)
    if not e:
        continue
    total = sum(e["state"].values()) or 1
    print("- %s [%d개] %s" % (role, e["n"], ", ".join("%s %d%%" % (k, round(100 * v / total)) for k, v in e["state"].most_common())))
    for frame, c in e["leaf"].most_common(6):
        print("    %3d%%  %s" % (round(100 * c / total), frame))
