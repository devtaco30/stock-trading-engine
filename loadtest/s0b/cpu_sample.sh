#!/usr/bin/env bash
# 프로세스별 누적 CPU 시간을 2초마다 남긴다(macOS ps의 %CPU는 프로세스 수명 평균이라 쓰지 않는다).
# 사용: cpu_sample.sh <출력 파일> <역할=pid>...   발신기(U10Tool JVM)는 매번 pgrep으로 찾아 역할 sender로 남긴다.
# 줄 형식: <epoch초> <역할> <pid> <cputime h:mm:ss.cc 또는 m:ss.cc> <rss KB>
OUT=$1; shift
while true; do
  now=$(date +%s)
  for kv in "$@"; do
    role=${kv%%=*}; pid=${kv##*=}
    ps -o cputime=,rss= -p "$pid" 2>/dev/null | awk -v n="$now" -v r="$role" -v p="$pid" 'NF==2{print n, r, p, $1, $2}' >> "$OUT"
  done
  for sp in $(pgrep -f 'u10.U10Tool' 2>/dev/null); do
    ps -o cputime=,rss= -p "$sp" 2>/dev/null | awk -v n="$now" -v p="$sp" 'NF==2{print n, "sender", p, $1, $2}' >> "$OUT"
  done
  sleep 2
done
