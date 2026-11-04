#!/usr/bin/env bash
# 프로세스별 누적 CPU 시간을 2초마다 남긴다(macOS ps의 %CPU는 프로세스 수명 평균이라 쓰지 않는다).
# 사용: cpu_sample.sh <출력 파일> <역할=pid>...   발신기(U10Tool JVM)는 매번 pgrep으로 찾아 역할 sender로 남긴다.
# 줄 형식: <epoch초> <역할> <pid> <cputime h:mm:ss.cc 또는 m:ss.cc> <rss KB>
OUT=$1; shift
STARTED=$(date +%s)
# 발신기 후보는 pgrep으로 찾되 이 표본기가 시작된 뒤에 뜬 프로세스만 고른다(다른 세션이 먼저 띄워 둔 U10Tool이 섞이지 않게). 시험은 시험 잠금 안에서 한 번에 하나만 돈다.
elapsed_seconds() { ps -o etime= -p "$1" 2>/dev/null | awk '{n=split($1,a,/[-:]/); if(n==2)s=a[1]*60+a[2]; else if(n==3)s=a[1]*3600+a[2]*60+a[3]; else if(n==4)s=a[1]*86400+a[2]*3600+a[3]*60+a[4]; else s=-1; print s}'; }
while true; do
  now=$(date +%s)
  for kv in "$@"; do
    role=${kv%%=*}; pid=${kv##*=}
    ps -o cputime=,rss= -p "$pid" 2>/dev/null | awk -v n="$now" -v r="$role" -v p="$pid" 'NF==2{print n, r, p, $1, $2}' >> "$OUT"
  done
  for sp in $(pgrep -u "$(id -u)" -f 'u10.U10Tool' 2>/dev/null); do
    age=$(elapsed_seconds "$sp"); [ -n "$age" ] && [ "$age" -ge 0 ] && [ "$age" -le $((now - STARTED + 2)) ] || continue
    ps -o cputime=,rss= -p "$sp" 2>/dev/null | awk -v n="$now" -v p="$sp" 'NF==2{print n, "sender", p, $1, $2}' >> "$OUT"
  done
  sleep 2
done
