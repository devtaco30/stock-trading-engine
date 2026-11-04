#!/usr/bin/env bash
# 2시간 유지 시험 1회: 체결 섞기(GROUP_BY_SHARD=1, 종목 30, N=300 + 매수 전용 30, C=2010), 계좌 서버 1개, 매칭 1개를 속도 R로 SEC초(기본 7200) 유지한다.
# 순서: 시작 전 디스크 기록 → 추세 표본기(trend_sample.sh) 시작 → mix.sh → 표본기 종료 → 계좌 스냅샷 dump → balance_check → window_report(10분 창) → cpu_summary → 통과 판정 파일 →
#       결과 이동 확인 뒤 /tmp/s0b 비우기(비우기 전후 디스크 기록).
# 사용(반드시 시험 잠금 안에서): python3 -I loadtest/s0b/runs/lock.py loadtest/s0b/sustained-run.sh <R> [태그]
#   환경: SEC(기본 7200) RES(기본 /Users/jack/Study/develop/F-lab/s0-sustained-results-20261105) NS(제품 jar 폴더, 기본 s0-nscale-results-20261103) DISK_MIN_GB(기본 20) DRY_RUN=1(계획만 출력)
# 결과(RES 아래, BASE=sustained-R<R>-<태그>): BASE.stdout BASE.check.json BASE.windows.json BASE.verdict.json BASE.cpu.json BASE.trend.log BASE.disk.txt (+ BASE.guard가 있으면 디스크 가드로 멈춘 것) 와 폴더 BASE/
# 통과 판정은 BASE.verdict.json의 verdict(PASS / FAIL / INVALID)와 reasons, 창별 값은 BASE.windows.json의 windows[].checks.
R=${1:-}; TAG=${2:-run}
if [ -z "$R" ]; then echo "사용: sustained-run.sh <R> [태그]" >&2; exit 1; fi
SEC=${SEC:-7200}
if [ $((R * SEC % 60)) -ne 0 ]; then echo "R x SEC는 60의 배수여야 한다(종목 30 x 매도·매수 2): $((R * SEC))" >&2; exit 1; fi
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
S=$(cd "$HERE/../.." && pwd)
NS=${NS:-/Users/jack/Study/develop/F-lab/s0-nscale-results-20261103}
RES=${RES:-/Users/jack/Study/develop/F-lab/s0-sustained-results-20261105}
DISK_MIN_GB=${DISK_MIN_GB:-20}
BASE=$RES/sustained-R$R-$TAG; OUT=$BASE
JCMD_AT="300,$((SEC / 2 + 60)),$((SEC - 60))"
ACCOUNT_JAR=$NS/account-a84c1b3.jar; MATCHING_JAR=$NS/matching-a84c1b3.jar
mkdir -p "$RES" "$OUT"
export ACCOUNT_WORKER_MAX_OPEN_ORDERS_PER_ACCOUNT=1000000 MAX_OPEN_ORDERS=1000000
export ACCOUNT_JAR MATCHING_JAR SKIP_BUILD=1
export STOCK_COUNT=30 ACCTS=300 BUYERS=30 C=2010 GROUP_BY_SHARD=1 ACCOUNT_WORKERS=1 RATE=$R SECONDS_STEADY=$SEC
export BUYER_BALANCE=${BUYER_BALANCE:-100000000000}   # 기본 100억은 2시간치 매수 필요량(mix.sh NEED)보다 작다
export HEAP_SAMPLE=1 CPU_SAMPLE=1 THREAD_CPU=0 ACCEPT_WAIT_SECS=${ACCEPT_WAIT_SECS:-1200} FILL_WAIT_SECS=${FILL_WAIT_SECS:-600} OUT
cd "$S"
if [ "${DRY_RUN:-0}" = 1 ]; then
  echo "PLAN R=$R SEC=$SEC TAG=$TAG"
  echo "GROUP_BY_SHARD=1 ACCOUNT_WORKERS=1 ACCTS=300 BUYERS=30 C=2010 BUYER_BALANCE=$BUYER_BALANCE"
  echo "DISK_MIN_GB=$DISK_MIN_GB JCMD_AT=$JCMD_AT"
  echo "BASE=$BASE"
  DRY_RUN=1 loadtest/s0b/mix.sh 2>&1 | tr ' ' '\n' | grep -E '^(PAIRS_PER_STOCK|TOTAL_ORDERS|TOTAL_TRADES|HOLD)=' | tr '\n' ' '; echo
  DRY_RUN=1 loadtest/s0b/mix.sh > /dev/null 2>&1 || { echo "mix.sh DRY_RUN 실패" >&2; exit 1; }
  exit 0
fi
disk_line() { df -k /tmp | awk -v l="$1" -v t="$(date +%s)" 'NR==2{printf "%s avail_gb=%d used_pct=%s ts=%s\n", l, $4/1048576, $5, t}'; }
disk_line START > "$BASE.disk.txt"
rm -f "$BASE.guard" "$BASE.trend.stop"
W=/tmp/s0b DISK_MIN_GB=$DISK_MIN_GB JCMD_AT=$JCMD_AT JCMD_OUT="$BASE.jcmd" ACCOUNT_PID_PATTERN="$ACCOUNT_JAR" \
  "$HERE/trend_sample.sh" "$BASE.trend.log" "$BASE.trend.stop" "$BASE.guard" > /dev/null 2>&1 &
TREND_PID=$!
loadtest/s0b/mix.sh > "$BASE.stdout" 2>&1
echo "exit=$?" >> "$BASE.stdout"
touch "$BASE.trend.stop"; wait $TREND_PID 2>/dev/null
pkill -f "jstat -gc" 2>/dev/null; pkill -f cpu_sample.sh 2>/dev/null
./gradlew --offline -q :account-worker:u10Tool -Pu10Args="dump-account /tmp/s0b/account" > "$OUT/dump-account.txt" 2>&1
python3 -I loadtest/s0b/balance_check.py "$OUT" > "$BASE.check.json" 2> "$BASE.check.err"
python3 loadtest/s0b/window_report.py --window 600 --trend-span 1800 --trend-log "$BASE.trend.log" "$OUT" > "$BASE.windows.json" 2> "$BASE.windows.err"
(cd loadtest/s0b && python3 -I cpu_summary.py "$OUT" > "$BASE.cpu.json" 2> "$BASE.cpu.err")
python3 -I loadtest/s0b/sustained_verdict.py "$BASE" > /dev/null 2>&1
# 결과가 RES로 옮겨진 것(mix.sh의 cleanup이 logs·steps·paced를 OUT에 복사)을 확인한 뒤 /tmp/s0b를 비운다. 프로세스가 남아 있으면 비우지 않는다.
disk_line BEFORE_CLEANUP >> "$BASE.disk.txt"
if [ -d "$OUT/logs" ] && [ -f "$BASE.verdict.json" ] && ! pgrep -f 'a84c1b3.jar' > /dev/null; then
  rm -rf /tmp/s0b; echo "CLEANED /tmp/s0b" >> "$BASE.disk.txt"
else
  echo "NOT_CLEANED /tmp/s0b (결과 이동 또는 프로세스 종료를 확인하지 못했다)" >> "$BASE.disk.txt"
fi
disk_line AFTER_CLEANUP >> "$BASE.disk.txt"
cat "$BASE.verdict.json"
