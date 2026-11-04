#!/usr/bin/env bash
set -uo pipefail
# 매칭 shard N개만 기동해서 배정과 종목 선택을 확인하고 끝낸다(계좌 워커·발신기는 띄우지 않는다).
# loadtest/s0b/lib.sh(s0b_multi/patches 0001 적용본)의 함수만 부른다. 저장소 루트에 loadtest/s0b/lib.sh 가 있어야 한다.
# 사용(시험 잠금 안에서):
#   /Users/jack/Study/develop/F-lab/ste-test-lock.sh env MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=120 SKIP_BUILD=1 OUT=<결과 폴더> bash loadtest/s0b_multi/start_matching_only.sh
# 환경 변수
#   OUT                      결과 폴더(필수)
#   MATCHING_COUNT=1         1·2·4·8. 2 이상이면 MATCHING_ASSIGN_TIMEOUT(초) 필수
#   S0B_WORK=/tmp/s0b-c1     작업 폴더. 시작할 때 통째로 지운다. lib.sh 의 기본값 /tmp/s0b 는 다른 시험이 쓸 수 있어 바꿨다
#   STOCK_COUNT=30           고를 종목 수. N=1이고 PICK_STOCKS가 없으면 고르지 않는다(기존 방식은 gradle u10Tool 을 부른다)
#   PICK_STOCKS=1            N=1에서도 종목을 고른다. BALANCED_STOCKS=1이면 N=1에서도 균등 선택기를 쓴다
#   HOLD_SEC=0               배정과 종목 선택 뒤 JVM을 살려 두는 시간(초). 이 뒤에 rss 를 잰다
#   CPU_SAMPLE=1             HOLD_SEC 동안 cpu_sample.sh 를 돌려 OUT/cpu.log 에 남긴다(역할 이름은 matching-0=pid ... 형식 확인용)
# 결과 파일(OUT 아래): matching-cmdline.txt(JVM별 명령줄) · started-lines.txt(로그의 Started 줄) · assigned-map.txt(slot<탭>주소, ;moving·null 걸러낸 표) · assigned-map-raw.txt(맵 토픽 원본 덤프)
#   · assigned-summary.txt(slot 수와 주소별 개수) · rss.txt(JVM별 rss KB) · cpu.log(CPU_SAMPLE=1) · slots.txt·shard-stocks.txt(종목 선택 시) · logs/ · result.txt(PASS·FAIL 한 줄)
cd "$(dirname "$0")/../.."
OUT=${OUT:?}
STOCK_COUNT=${STOCK_COUNT:-30}
S0B_WORK="${S0B_WORK:-/tmp/s0b-c1}"; export S0B_WORK
mkdir -p "$OUT"

source loadtest/s0b/lib.sh
s0b_matching_init || exit 1
s0b_check_ports || exit $?
s0b_setup_workdir
s0b_start_kafka
cleanup() {
    s0b_kill_pids
    rm -rf "$OUT/logs"; cp -R "$LOG" "$OUT/logs" 2>/dev/null
}
trap cleanup EXIT
s0b_prepare_jars_and_yml || exit 1

START_TS=$SECONDS
s0b_start_matching
START_RC=$?
echo "START_RC=$START_RC 기동·배정 대기 $((SECONDS - START_TS))초" | tee "$OUT/result.txt"
if [ "$START_RC" != 0 ]; then echo "FAIL 기동" | tee -a "$OUT/result.txt"; exit "$START_RC"; fi

: > "$OUT/matching-cmdline.txt"; : > "$OUT/started-lines.txt"
for i in $(seq 0 $((MATCHING_COUNT - 1))); do
    ps -o command= -p "${MP_LIST[$i]}" | sed "s/^/matching-$i pid=${MP_LIST[$i]} /" >> "$OUT/matching-cmdline.txt"
    grep -H "Started MatchingWorkerApplication" "$LOG/matching-$i.log" >> "$OUT/started-lines.txt"
done

s0b_assigned_map > "$OUT/assigned-map.txt"
matching_map_dump > "$OUT/assigned-map-raw.txt"
{
    echo "slots=$(cut -f1 "$OUT/assigned-map.txt" | sort -un | wc -l | tr -d ' ')"
    echo "addresses=$(cut -f2 "$OUT/assigned-map.txt" | sort -u | grep -c .)"
    cut -f2 "$OUT/assigned-map.txt" | sort | uniq -c | sed 's/^ *//; s/^/per-address /'
} > "$OUT/assigned-summary.txt"

# 판정. 어느 조건이 틀렸는지 알 수 있게 FAIL 사유를 result.txt 에 한 줄로 남긴다.
fail() { echo "FAIL $*" | tee -a "$OUT/result.txt"; exit 1; }
SLOTS=$(sed -n 's/^slots=//p' "$OUT/assigned-summary.txt"); ADDRS=$(sed -n 's/^addresses=//p' "$OUT/assigned-summary.txt")
[ "$SLOTS" = 64 ] && [ "$ADDRS" = "$MATCHING_COUNT" ] || fail "배정 slots=$SLOTS(기대 64) addresses=$ADDRS(기대 $MATCHING_COUNT)"
# 1) 정리된 표와 원본 덤프 모두에 ;moving·null·빈 주소가 없어야 한다.
#    원본은 slot별 마지막 레코드만 본다(compacted 토픽이라 앞선 레코드는 옛 값이어도 정상).
MAP_BAD=$(awk -F'\t' '$2=="" || $2=="null" || $2 ~ /;moving$/ {b++} END {print b+0}' "$OUT/assigned-map.txt")
RAW_BAD=$(awk -F'\t' 'NF>=1 && $1!="" {last[$1]=$2} END {for (k in last) if (last[k]=="" || last[k]=="null" || last[k] ~ /;moving$/) b++; print b+0}' "$OUT/assigned-map-raw.txt")
[ "$MAP_BAD" = 0 ] || fail "map-bad=$MAP_BAD (assigned-map.txt 에 ;moving·null·빈 주소)"
[ "$RAW_BAD" = 0 ] || fail "raw-map-bad=$RAW_BAD (원본 덤프의 slot별 마지막 값에 ;moving·null·빈 주소)"

PICK_RC=skipped
if [ "$MATCHING_COUNT" != 1 ] || [ "${BALANCED_STOCKS:-0}" = 1 ] || [ "${PICK_STOCKS:-0}" = 1 ]; then
    s0b_pick_stocks "$STOCK_COUNT" 0; PICK_RC=$?
    if [ "$PICK_RC" = 0 ]; then echo "종목: $STOCKS"; s0b_write_stock_report "$OUT" always; fi
fi

if [ "${HOLD_SEC:-0}" -gt 0 ]; then
    if [ "${CPU_SAMPLE:-0}" = 1 ]; then
        read -ra SAMPLE_ROLES <<< "$(s0b_matching_roles)"
        loadtest/s0b/cpu_sample.sh "$OUT/cpu.log" "${SAMPLE_ROLES[@]}" &
        SAMPLE_PID=$!; echo "$SAMPLE_PID" >> "$W/pids"
    fi
    sleep "$HOLD_SEC"
fi
: > "$OUT/rss.txt"
for i in $(seq 0 $((MATCHING_COUNT - 1))); do
    echo "matching-$i pid=${MP_LIST[$i]} rss_kb=$(ps -o rss= -p "${MP_LIST[$i]}" | tr -d ' ')" >> "$OUT/rss.txt"
done

[ "$PICK_RC" = 0 ] || [ "$PICK_RC" = skipped ] || fail "종목 선택 pick=$PICK_RC"

# 2) Started 줄 수 == MATCHING_COUNT
STARTED=$(wc -l < "$OUT/started-lines.txt" | tr -d ' ')
[ "$STARTED" = "$MATCHING_COUNT" ] || fail "started-lines=$STARTED expected=$MATCHING_COUNT"

# 3) 종목을 골랐으면 shard별 종목 수의 최대-최소 <= 1
SPREAD=na
if [ "$PICK_RC" = 0 ]; then
    SPREAD=$(sed -n 's/^SHARD_STOCKS [^=]*=//p' "$OUT/shard-stocks.txt" | sort -n | awk 'NR==1{min=$1} {max=$1} END {print max-min}')
    [ -n "$SPREAD" ] && [ "$SPREAD" -le 1 ] || fail "shard-stocks spread=${SPREAD:-없음} (shard별 종목 수 최대-최소가 1 이하여야 한다)"
fi
echo "PASS slots=$SLOTS addresses=$ADDRS pick=$PICK_RC raw-map-bad=$RAW_BAD started-lines=$STARTED spread=$SPREAD" | tee -a "$OUT/result.txt"
