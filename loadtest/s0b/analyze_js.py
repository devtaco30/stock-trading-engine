#!/usr/bin/env python3
"""run.sh가 JSTACK_INTERVAL로 뜬 thread dump(결과 폴더/js/*.txt)를 thread별로 모은다.
사용: analyze_js.py <결과 폴더> [상위 thread 수]
- 접수 구간(measure-start~measure-end) 안의 덤프만 쓴다.
- thread 이름에서 숫자를 지워 같은 종류로 묶고, 종류마다 상태(RUNNABLE/WAITING/TIMED_WAITING/BLOCKED) 비율과 가장 많이 나온 맨 위 프레임, 가장 많이 나온 프로젝트 프레임(com.flab…)을 낸다.
- RUNNABLE이라도 소켓·파일 읽기, Unsafe.park 같은 기다림일 수 있어 맨 위 프레임을 같이 본다.
"""
import collections, glob, os, re, sys

out = sys.argv[1]
top = int(sys.argv[2]) if len(sys.argv) > 2 else 12
def read_float(name):
    p = os.path.join(out, name)
    return float(open(p).read().strip()) if os.path.exists(p) else None
m0, m1 = read_float("measure-start"), read_float("measure-end")
stats = collections.defaultdict(lambda: {"n": 0, "states": collections.Counter(), "top": collections.Counter(), "proj": collections.Counter()})
dumps = 0
for ts_path in sorted(glob.glob(os.path.join(out, "js", "*.ts")), key=lambda p: int(os.path.basename(p)[:-3])):
    t = float(open(ts_path).read().strip())
    if m0 is not None and not (m0 <= t <= (m1 or t)):
        continue
    txt_path = ts_path[:-3] + ".txt"
    if not os.path.exists(txt_path):
        continue
    dumps += 1
    block = None
    for line in open(txt_path, errors="replace"):
        m = re.match(r'^"([^"]+)"', line)
        if m:
            name = re.sub(r"\d+", "#", m.group(1))
            block = {"name": name, "state": None, "frames": []}
            blocks = stats[name]
            blocks["n"] += 1
            cur = blocks
            continue
        if block is None:
            continue
        m = re.match(r"\s+java\.lang\.Thread\.State: (\w+)", line)
        if m:
            cur["states"][m.group(1)] += 1
            continue
        m = re.match(r"\s+at (\S+)", line)
        if m:
            frame = m.group(1)
            if not block["frames"]:
                cur["top"][frame] += 1
            block["frames"].append(frame)
            if frame.startswith("com.flab") and not block.get("proj"):
                block["proj"] = True
                cur["proj"][frame] += 1
rows = sorted(stats.items(), key=lambda kv: -kv[1]["states"].get("RUNNABLE", 0))
print("덤프 %d개(접수 구간 안)" % dumps)
for name, s in rows[:top]:
    total = sum(s["states"].values()) or 1
    st = ", ".join("%s %d%%" % (k, round(100 * v / total)) for k, v in s["states"].most_common())
    print("- %s [%d개 덤프] %s" % (name, s["n"], st))
    print("    맨 위 프레임: " + "; ".join("%s ×%d" % kv for kv in s["top"].most_common(3)))
    if s["proj"]:
        print("    프로젝트 프레임: " + "; ".join("%s ×%d" % kv for kv in s["proj"].most_common(3)))
