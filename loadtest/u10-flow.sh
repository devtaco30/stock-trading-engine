#!/usr/bin/env bash
set -uo pipefail

# U10 이동 중 주문 시나리오 — 매칭 pod 0만 띄워 종목 slot 64개를 맡기고, pod 1이 합류해 종목 slot이 이동하는 동안 주문을 계속 보낸 뒤,
# 모든 주문의 결말(체결·호가창 대기·잔고 초과 거부)이 유실과 중복 없이 합계와 맞는지 본다. 종료 코드: 0 통과, 1 실패, 3 미발동.
#   X = 이동하는 쪽 종목 slot(32 이상)의 종목, Y = 이동하지 않는 쪽 종목 slot(32 미만)의 종목. 가격은 모두 20,000원, 수량은 1이다.
#   같은 가격이라 도착 순서와 무관하게 체결 수가 정해진다: 매도 합 S, 매수 합 B(잔고 초과 주문 제외)면 체결 F = min(S, B).
# 시나리오가 실제로 이동 중 주문을 받았는지 전제를 단언한다(발신 속도 조건 포함 네 개). 하나라도 거짓이면 통과가 아니라 「미발동」이고 최대 3회 다시 시도한다.
#   1. 종목 X의 slot이 맵 기록으로 pod 0에서 pod 1로 바뀌었다
#   2. 「이동 중」 표시가 붙은 구간(X의 slot이 이동 중으로 표시된 시각 ~ pod 1 주소로 바뀐 시각)에 발신한 주문이 1건 이상이다
#   3. 이동 중 표시를 본 종목 slot이라 보관 목록에 둔 새 주문이 1건 이상이다(계좌 워커 종료 로그의 「보관 이유별 이동 중 표시」 건수, 설계서 10-1절 수정본 3번의 구간 안 동작).
#      동시 보관 최대는 이 이유와 담당 없음·재전송 대기 뒤·거부 회신 보관을 구분하지 않으므로 전제로 쓰지 않는다.
#      거부 회신은 전제가 아니다: 맵에 이동 중이 표시되기 전에 이미 나가 있던 주문이 이전 pod에 도착했을 때만 생기며 실행마다 0건일 수 있다.
#      이 시험은 거부 경로의 end-to-end 증명이 아니다(거부 경로는 단위 시험이 맡는다). 거부 회신 건수와 구간 시작과의 시각 차는 보고 수치로만 출력한다.
# 값(발신 속도, 사전 매도 수)의 근거: 이전 측정에서 「이동 중」 표시 구간이 중앙 약 99ms, 최대 약 146ms였으므로 X에 초당 200건이면
# 그 구간에 기대 약 20건이다. 발신 간격이 실제로 지켜졌는지는 stream이 출력하는 지연(maxLatenessMs)으로 본다.
cd "$(dirname "$0")/.."
source loadtest/u10/lib.sh

RATE_X=${RATE_X:-200}
RATE_Y=${RATE_Y:-20}
DURATION_MS=${DURATION_MS:-8000}
PRESTACK_X=${PRESTACK_X:-20}
PRESTACK_Y=${PRESTACK_Y:-5}
HOLDING=${HOLDING:-100000}
MAX_ATTEMPTS=${MAX_ATTEMPTS:-3}
POD0=aeron:udp?endpoint=localhost:20020
POD1=aeron:udp?endpoint=localhost:20021
START_TIME_LIMIT_SECONDS=120

# RESULT는 마지막에야 대입되므로, 그 전에 exit 1로 끝난 경우(포트 점검 실패 등)에는 원래 종료 코드를 그대로 쓴다(set -u에서 미정의 오류로 가려지지 않게).
trap 'rc=$?; stop_pids; docker rm -f "$KAFKA_NAME" > /dev/null 2>&1; exit "${RESULT:-$rc}"' EXIT

OK=1
expect() { # <설명> <기대> <실제>
    if [ "$2" = "$3" ]; then echo "통과: $1 ($3)"; else echo "실패: $1 (기대 $2 실제 $3)"; OK=0; fi
}
field() { echo "$1" | sed -n "s/.* $2=\([^ ]*\).*/\1/p"; }
# holdings_string <코드=수량>... — 계좌 스냅샷의 보유 표기({코드=수량,...}, 코드 오름차순, 수량 0은 뺀다)
holdings_string() {
    local items
    items=$(for pair in "$@"; do [ "${pair#*=}" -gt 0 ] && echo "$pair"; done | sort | paste -sd, -)
    echo "{$items}"
}
now_ms() { python3 -c 'import time;print(int(time.time()*1000))'; }

# 한 시도. 종료 코드 0 통과, 1 실패, 3 미발동(전제 거짓)
run_attempt() {
    OK=1
    check_ports
    rm -rf "$WORK"; mkdir -p "$LOG_DIR"; : > "$PID_FILE"
    start_kafka

    local codes xslot x y
    codes=$(tool codes 64 32 | tail -1); echo "$codes"
    xslot=$(echo "$codes" | sed -n 's/.*XSLOT=\([0-9]*\) .*/\1/p')
    x=$(echo "$codes" | sed -n 's/.*X=\([0-9]*\) .*/\1/p'); y=$(echo "$codes" | sed -n 's/.* Y=\([0-9]*\) .*/\1/p')
    [ -n "$x" ] && [ -n "$y" ] && [ -n "$xslot" ] || { fail "종목을 고르지 못했다"; return 1; }

    start_app matching-0 "$MATCHING_JAR" loadtest/u10/u10-matching-0.yml
    local state=""
    for _ in $(seq 1 40); do
        state=$(tool assigned localhost:9092 64 | grep "^ASSIGNED" | tail -1)
        echo "$state" | grep -q "^ASSIGNED 64/64 MOVING 0" && break
        sleep 3
    done
    echo "pod 0만 기동한 뒤: $state"
    echo "$state" | grep -q "^ASSIGNED 64/64 MOVING 0 ENDPOINTS {$POD0=64}" || { fail "pod 0이 64개를 모두 맡지 못했다"; return 1; }

    # 90001은 현금이 넉넉한 매수 계좌, 90002는 두 종목을 많이 가진 매도 계좌(잔고 초과 주문 1건은 90001이 낸다).
    start_account \
        --account-worker.seed-accounts[0].account-id=90001 --account-worker.seed-accounts[0].balance=100000000000 \
        --account-worker.seed-accounts[0].margin-rate=0.40 \
        --account-worker.seed-accounts[1].account-id=90002 --account-worker.seed-accounts[1].balance=100000000 \
        --account-worker.seed-accounts[1].margin-rate=0.40 \
        --account-worker.seed-accounts[1].holdings.$x=$HOLDING --account-worker.seed-accounts[1].holdings.$y=$HOLDING

    # 이동 전에 호가창에 같은 가격의 대기 매도를 먼저 쌓는다.
    local bulk_out reason
    bulk_out=$(tool send-bulk aeron:udp?endpoint=localhost:20040 90002 SELL 20000 "$PRESTACK_X" u10-flow-pre-x "$x" --price-step=0)
    reason=$(tool_failure_reason "$bulk_out"); [ -z "$reason" ] || { fail "도구 실패(시나리오 미발동이 아님): 사전 매도 X 발신 $reason"; return 1; }
    bulk_out=$(tool send-bulk aeron:udp?endpoint=localhost:20040 90002 SELL 20000 "$PRESTACK_Y" u10-flow-pre-y "$y" --price-step=0)
    reason=$(tool_failure_reason "$bulk_out"); [ -z "$reason" ] || { fail "도구 실패(시나리오 미발동이 아님): 사전 매도 Y 발신 $reason"; return 1; }
    sleep 2

    # pod 1 합류. pod 1의 컨슈머가 그룹 합류를 요청했다는 로그가 나오는 즉시 주문을 연속 발신한다(실측: 이 로그 약 1.5초 뒤 pod 0가 담당을 내놓기
    # 시작하고, 그 약 3초 뒤 맵이 pod 1 주소로 바뀐다).
    start_app matching-1 "$MATCHING_JAR" loadtest/u10/u10-matching-1.yml
    local waited=0
    until grep -qF "(Re-)joining group" "$LOG_DIR/matching-1.log" 2>/dev/null; do
        sleep 0.02; waited=$((waited + 1))
        [ "$waited" -lt $((START_TIME_LIMIT_SECONDS * 50)) ] || { fail "pod 1이 합류 요청을 내지 않았다"; return 1; }
    done
    tool stream aeron:udp?endpoint=localhost:20040 "$DURATION_MS" "$RATE_X" "$RATE_Y" "$x" "$y" 90002 90001 20000 > "$WORK/stream.txt" 2>&1 &
    local stream_pid=$!
    wait "$stream_pid"
    local stream_reason
    stream_reason=$(tool_failure_reason "$(cat "$WORK/stream.txt")")
    [ -z "$stream_reason" ] || { fail "도구 실패(시나리오 미발동이 아님): stream $stream_reason"; return 1; }
    grep "^STREAM_DONE" "$WORK/stream.txt" || { fail "stream이 끝나지 않았다"; tail -3 "$WORK/stream.txt"; return 1; }
    grep -q "^SENDER_POSITION_REACHED" "$WORK/stream.txt" || { fail "도구 실패(시나리오 미발동이 아님): stream이 송신 위치 확인 없이 끝났다"; return 1; }

    local moved_done=0
    for _ in $(seq 1 60); do
        state=$(tool assigned localhost:9092 64 | grep "^ASSIGNED" | tail -1)
        if echo "$state" | grep -q "^ASSIGNED 64/64 MOVING 0 ENDPOINTS {$POD0=32, $POD1=32}"; then moved_done=1; break; fi
        sleep 3
    done
    echo "이동 뒤: $state"
    expect "이동이 끝나 두 pod가 32개씩 맡는다" "1" "$moved_done"

    # 합계: 매도 합 S, 매수 합 B, 체결 F = min(S, B)
    local done_line xs xb ys yb invalid
    done_line=$(grep "^STREAM_DONE" "$WORK/stream.txt")
    xs=$(field "$done_line" xSells); xb=$(field "$done_line" xBuys); ys=$(field "$done_line" ySells); yb=$(field "$done_line" yBuys)
    invalid=$(field "$done_line" invalid)
    local sx sy fx fy f
    sx=$((PRESTACK_X + xs)); sy=$((PRESTACK_Y + ys))
    fx=$(( sx < xb ? sx : xb )); fy=$(( sy < yb ? sy : yb )); f=$((fx + fy))
    echo "합계: X 매도 $sx 매수 $xb 체결 $fx / Y 매도 $sy 매수 $yb 체결 $fy / 체결 합계 $f / 잔고 초과 주문 $invalid"
    echo "$done_line" | cut -d' ' -f7-8

    # 모든 체결이 계좌에 반영될 때까지(상한 240초) 기다린다.
    local applied
    applied=$(wait_for_applied_fills "$LOG_DIR/account.log" 90001 "$f" 120 2)
    expect "계좌에 반영된 체결(applied=true)이 합계와 같다" "$f" "$applied"
    sleep 3

    tool map-history localhost:9092 > "$WORK/map-history.raw" || true
    grep -q WARN_PARTIAL_MAP_READ "$WORK/map-history.raw" && { echo "실패: 맵 기록을 끝까지 읽지 못했다(부분 결과)"; OK=0; }
    awk -F'\t' 'NF >= 3 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/' "$WORK/map-history.raw" > "$WORK/map-history.txt"

    # 전제 1: X의 slot이 pod 0에서 pod 1로 바뀌었고 이동 표시가 한 번만 올라왔다(여러 번 오가면 구간이 넓어져 전제 2·3이 의미를 잃는다)
    local premise_moved=0 premise_window=0 premise_hold=0 premise_rate=0
    python3 loadtest/u10/assert-moved.py "$WORK/map-history.txt" "$xslot" "$POD0" "$POD1" && premise_moved=1
    local window_info window_count window_start window_end move_marks account_read_at window_start_source
    # 구간의 시작: 계좌 shard가 X slot의 이동 중 표시를 읽은 시각(계좌 워커 로그 「이동 중 표시를 읽음」). 그 줄이 없으면 Kafka에 기록된 이동 중 값의 시각을 쓴다.
    account_read_at=$(sed -n 's/.*이동 중 표시를 읽음: slot='"$xslot"' 시각(epoch ms)=\([0-9]*\).*/\1/p' "$LOG_DIR/account.log" | head -1)
    window_start_source="계좌 shard가 표시를 읽은 시각"
    if [ -z "$account_read_at" ]; then account_read_at=0; window_start_source="Kafka 기록 시각(읽은 시각 로그 없음)"; fi
    window_info=$(python3 - "$WORK/map-history.txt" "$WORK/stream.txt" "$xslot" "$POD1" "$account_read_at" <<'PY'
import sys
rows = [l.rstrip('\n').split('\t') for l in open(sys.argv[1]) if l.strip()]
slot, pod1 = int(sys.argv[3]), sys.argv[4]
xs = [(int(r[1]), r[2]) for r in rows if int(r[0]) == slot]
moving = [t for t, v in xs if v.endswith(';moving')]
to_pod1 = [t for t, v in xs if v == pod1]
if not moving or not to_pod1:
    print("0 0 0 %d" % len(moving)); sys.exit()
start, end = min(moving), max(to_pod1)
if int(sys.argv[5]) > 0:
    start = int(sys.argv[5])
sent = [int(l.split()[1]) for l in open(sys.argv[2]) if l.startswith('ORDER ')]
print("%d %d %d %d" % (sum(1 for t in sent if start <= t <= end), start, end, len(moving)))
PY
)
    read -r window_count window_start window_end move_marks <<< "$window_info"
    [ "$move_marks" = 1 ] || { premise_moved=0; echo "이동 중 표시가 ${move_marks}번 올라왔다(한 번이어야 한다)"; }
    # 전제 2: 이동 중 표시 구간에 발신한 주문 수. 발신 시각은 stream JVM의 시계, 구간은 Kafka 기록 시각이라 같은 컴퓨터의 시계끼리 비교한 값이며,
    # 그 주문이 실제로 이동 중 표시를 만났다는 증거는 아니다(한계).
    [ "${window_count:-0}" -ge 1 ] && premise_window=1
    # 발신 속도: 발신 지연의 최대가 이동 중 표시 구간 길이의 10% 미만이어야 계획한 속도가 지켜진 것이다.
    # (offerBlocking이 백프레셔로 막히면 지연이 쌓여 계획 속도가 깨지지만 합계 단언은 영향을 받지 않는다.)
    local max_lateness window_len
    max_lateness=$(field "$done_line" maxLatenessMs); window_len=$((window_end - window_start))
    [ "$window_len" -gt 0 ] && [ $((max_lateness * 10)) -lt "$window_len" ] && premise_rate=1
    echo "발신 속도: 최대 지연 ${max_lateness}ms, 이동 중 표시 구간 ${window_len}ms (최대 지연이 구간의 10% 미만이어야 속도 충족)"

    stop_pids
    # 전제 3: 이동 중 표시를 본 종목 slot의 새 주문이 보관 목록에 쌓였다(동시 보관 최대 1건 이상). 설계서 10-1절 수정본 3번이 정한 구간 안 동작이다.
    # 「구간 안에서 거부 회신이 생긴다」는 전제가 아니다. 거부 회신은 맵에 이동 중이 표시되기 전에 이미 나가 있던 주문이 이전 pod에 도착했을 때만 생기며
    # 실행마다 0건일 수 있다. 그래서 거부 회신 건수와 구간 시작과의 시각 차는 아래에서 보고 수치로만 출력하고 판정에 쓰지 않는다.
    local reject_count held_left held_max reject_in_window reject_offset_list
    reject_count=$(sed -n 's/.*담당 아님 거부 회신 수신 \([0-9]*\)건.*/\1/p' "$LOG_DIR/account.log" | tail -1)
    local reject_times
    reject_times=$(sed -n 's/.*거부 회신 수신: 시각(epoch ms)=\([0-9]*\).*/\1/p' "$LOG_DIR/account.log")
    reject_in_window=$(echo "$reject_times" | count_in_window "$window_start" "$window_end")
    reject_offset_list=$(echo "$reject_times" | reject_offsets "$window_start" | paste -sd, -)
    held_left=$(sed -n 's/.*보관 목록에 남은 주문 \([0-9]*\)건.*/\1/p' "$LOG_DIR/account.log" | tail -1)
    held_max=$(sed -n 's/.*보관 목록 최대 \([0-9]*\)건.*/\1/p' "$LOG_DIR/account.log" | tail -1)
    local held_moving
    held_moving=$(sed -n 's/.*보관 이유별 이동 중 표시 \([0-9]*\)건.*/\1/p' "$LOG_DIR/account.log" | tail -1)
    if [ -z "$held_moving" ]; then echo "계좌 워커 종료 로그가 없다(강제 종료로 보임): 이동 중 표시 때문에 보관한 주문 수를 알 수 없어 전제 3은 거짓이다"; fi
    premise_hold=$(held_premise "$held_moving")
    echo "전제: X slot 이동=$premise_moved / 이동 중 표시 구간(${window_start}~${window_end} epoch ms, 시작=${window_start_source})에 발신한 주문=${window_count:-0}건 / 이동 중 표시 때문에 보관한 새 주문=${held_moving:-없음}건(1건 이상이어야 참, 참=$premise_hold)"
    echo "보고 수치(판정에 쓰지 않음): 담당 아님 거부 회신 수신=${reject_count:-없음}건, 그중 구간 안에서 받은 건수=${reject_in_window:-0}건, 각 수신 시각 − 구간 시작(ms, 음수는 구간 앞)=${reject_offset_list:-없음}. 구간 시작은 ${window_start_source}이다"
    echo "구분: 잔고 초과 주문(계좌가 거부)=${invalid}건, 담당 아님 거부 회신(이벤트)=${reject_count:-없음}건 — 서로 다른 항목이며 주문 수에 더하지 않는다. 같은 주문이 여러 번 거부되면 여러 번 센다"
    echo "보관 목록(보고 수치): 보관 이유를 가리지 않은 동시 보관 최대 ${held_max:-없음}건, 종료 때 남은 주문 ${held_left:-없음}건"

    local a d0 d1 buyer seller
    a=$(dump_account); d0=$(dump_matching /tmp/u10/matching-0); d1=$(dump_matching /tmp/u10/matching-1)
    buyer=$(echo "$a" | grep "ACCOUNT id=90001 " || true); seller=$(echo "$a" | grep "ACCOUNT id=90002 " || true)
    [ -n "$buyer" ] && [ -n "$seller" ] || { fail "계좌 스냅샷에 90001, 90002 줄이 없다"; return 1; }

    # 합계 단언
    local expected_holdings
    expected_holdings=$(holdings_string "$x=$fx" "$y=$fy")
    expect "매수 계좌 잔고 = 초기 − 체결 × 20,000 × 증거금률" "$((100000000000 - f * 8000))" "$(field "$buyer" balance)"
    expect "매수 계좌 미수금 = 체결 × 20,000 × 0.60" "$((f * 12000))" "$(field "$buyer" unpaid)"
    expect "매수 계좌 보유 = 체결 수(잔고 초과 주문은 반영되지 않았다)" "$expected_holdings" "$(field "$buyer" holdings)"
    expect "매수 계좌에 남은 예약이 없다(매수가 매도보다 적거나 같아 전부 체결)" "0" "$(field "$buyer" reservations)"
    expect "매도 계좌 보유는 체결된 수만큼 줄었다(호가창에 대기 중인 매도는 보유를 줄이지 않고 예약으로 잡힌다)" "$(holdings_string "$x=$((HOLDING - fx))" "$y=$((HOLDING - fy))")" "$(field "$seller" holdings)"
    expect "매도 계좌의 대기 매도 예약 수 = 대기 중인 매도 수" "$((sx - fx + sy - fy))" "$(field "$seller" sellReservations)"
    local trade_ids distinct_ids
    trade_ids=$(field "$buyer" tradeIds)
    distinct_ids=$(echo "$trade_ids" | tr ',' '\n' | grep . | sort -u | wc -l | tr -d ' ')
    expect "체결 번호가 체결 수만큼 있고 서로 다르다(중복·유실 없음)" "$f" "$distinct_ids"
    expect "체결 번호 개수와 서로 다른 개수가 같다" "$distinct_ids" "$(echo "$trade_ids" | tr ',' '\n' | grep -c . || true)"

    check_owned_slots "$d0" "$d1"
    resting_total() { # <종목> → 두 pod의 대기 주문 합
        local total=0 r d
        for d in "$d0" "$d1"; do
            r=$(echo "$d" | grep "BOOK stock=$1 " | sed -n 's/.*resting=\([0-9]*\).*/\1/p'); total=$((total + ${r:-0}))
        done
        echo "$total"
    }
    book_pods() { # <종목> → 그 종목 호가창을 가진 pod 수
        local n=0
        [ "$(echo "$d0" | grep -c "BOOK stock=$1 ")" != 0 ] && n=$((n + 1))
        [ "$(echo "$d1" | grep -c "BOOK stock=$1 ")" != 0 ] && n=$((n + 1))
        echo "$n"
    }
    expect "종목 X 두 pod 호가창의 대기 주문 합 = 매도 − 체결" "$((sx - fx))" "$(resting_total "$x")"
    expect "종목 Y 두 pod 호가창의 대기 주문 합 = 매도 − 체결" "$((sy - fy))" "$(resting_total "$y")"
    expect "종목 X 호가창은 한쪽 pod에만 있다" "1" "$(book_pods "$x")"
    expect "종목 Y 호가창은 한쪽 pod에만 있다" "1" "$(book_pods "$y")"
    expect "종료 때 계좌의 보관 목록에 남은 주문(단계 안에서 기다리는 주문)이 없다" "0" "${held_left:-없음}"
    report_send_failures

    local premises_ok=0 verdict
    if [ "$premise_moved" = 1 ] && [ "$premise_window" = 1 ] && [ "$premise_hold" = 1 ] && [ "$premise_rate" = 1 ]; then premises_ok=1; fi
    verdict=$(flow_verdict "$OK" "$premises_ok")
    if [ "$verdict" = 3 ]; then
        echo "미발동: 전제 이동=$premise_moved 이동 중 구간 발신=$premise_window 이동 중 보관 주문=$premise_hold 발신 속도=$premise_rate (합계 단언은 모두 맞았다)"
    elif [ "$verdict" = 1 ]; then
        echo "실패: 합계 단언이 틀렸다(전제 이동=$premise_moved 구간 발신=$premise_window 이동 중 보관 주문=$premise_hold 속도=$premise_rate) — 전제와 무관하게 실패로 끝낸다"
    fi
    return "$verdict"
}

build_jars_once=0
FINAL=3
for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
    echo "===== 시도 $attempt/$MAX_ATTEMPTS ====="
    mkdir -p "$LOG_DIR" 2>/dev/null
    if [ "$build_jars_once" = 0 ]; then
        build_jars; build_jars_once=1
    fi
    run_attempt
    code=$?
    echo "시도 $attempt 결과: 종료 코드 $code (0 통과, 1 실패, 3 미발동)"
    ATTEMPT_RESULTS="${ATTEMPT_RESULTS:-}시도 $attempt=$code; "
    stop_pids
    docker rm -f "$KAFKA_NAME" > /dev/null 2>&1
    if [ "$code" != 3 ]; then FINAL=$code; break; fi
done
echo "시도별 결과: ${ATTEMPT_RESULTS:-없음}"
if [ "$FINAL" = 3 ]; then echo "미발동: ${MAX_ATTEMPTS}회 모두 전제가 거짓이어서 통과로 처리하지 않는다"; fi
RESULT=$FINAL
exit "$FINAL"
