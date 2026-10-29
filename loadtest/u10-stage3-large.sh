#!/usr/bin/env bash
set -uo pipefail

# U10 3단계 변형 — 호가창을 키운다. 사용: loadtest/u10-stage3-large.sh <종목당 대기 매도 주문 수>
# stage3와 같은 시나리오(pod 0만 → 대기 매도 쌓기 → pod 1 합류·이동 → 반대쪽 매수 체결 → 정상 종료 스냅샷)인데,
# 이동하는 종목 slot의 종목 3개에 대기 매도 주문을 종목마다 N건씩(가격 20,000, 20,001, …, 수량 1) 쌓는다.
# 이동 뒤 매수 계좌가 종목마다 가장 싼 매도 1건(20,000원 1주)을 사서, 호가창이 통째로 옮겨졌다면 종목마다 N-1건이 남는다.
# 종료 코드 0이면 통과, 그 밖이면 실패다. 로컬 한 대의 값이며 일반화하지 않는다.
cd "$(dirname "$0")/.."
source loadtest/u10/lib.sh
trap cleanup EXIT

N=${1:?종목당 대기 매도 주문 수를 인자로 준다}
MOVING_STOCKS=3
OK=1
expect() { # <설명> <기대> <실제>
    if [ "$2" = "$3" ]; then echo "통과: $1 ($3)"; else echo "실패: $1 (기대 $2 실제 $3)"; OK=0; fi
}

check_ports
rm -rf "$WORK"; mkdir -p "$LOG_DIR"; : > "$PID_FILE"
start_kafka
build_jars

HIGH=$(tool codes-many 64 32 "$MOVING_STOCKS" | grep '^HIGH=' | tail -1 | sed 's/^HIGH=//')
STOCKS=$(echo "$HIGH" | tr ',' ' ')
echo "이동하는 종목 slot의 종목: $STOCKS, 종목당 대기 매도 주문 ${N}건"
[ "$(echo "$STOCKS" | wc -w | tr -d ' ')" = "$MOVING_STOCKS" ] || { fail "종목을 고르지 못했다"; exit 1; }

start_app matching-0 "$MATCHING_JAR" loadtest/u10/u10-matching-0.yml
STATE=""
for _ in $(seq 1 40); do
    STATE=$(tool assigned localhost:9092 64 | grep "^ASSIGNED" | tail -1)
    echo "$STATE" | grep -q "^ASSIGNED 64/64 MOVING 0 ENDPOINTS {aeron:udp?endpoint=localhost:20020=64}" && break
    sleep 3
done
echo "pod 0만 기동한 뒤: $STATE"
echo "$STATE" | grep -q "^ASSIGNED 64/64 MOVING 0 ENDPOINTS {aeron:udp?endpoint=localhost:20020=64}" || { fail "pod 0이 64개를 모두 맡지 못했다"; exit 1; }

# 계좌 워커: 90001 현금, 90002는 세 종목을 (N+5)주씩 보유
SEED_ARGS=(--account-worker.seed-accounts[0].account-id=90001 --account-worker.seed-accounts[0].balance=100000000000
    --account-worker.seed-accounts[0].margin-rate=0.40
    --account-worker.seed-accounts[1].account-id=90002 --account-worker.seed-accounts[1].balance=100000000
    --account-worker.seed-accounts[1].margin-rate=0.40)
for stock in $STOCKS; do SEED_ARGS+=(--account-worker.seed-accounts[1].holdings.$stock=$((N + 5))); done
start_account "${SEED_ARGS[@]}"

# 대기 매도 주문을 대량으로 쌓는다.
BULK=$(tool send-bulk aeron:udp?endpoint=localhost:20040 90002 SELL 20000 "$N" u10-big $STOCKS | grep -E "^(SENT_BULK|OFFER_FAILED|NOT_CONNECTED)" | tail -1)
echo "$BULK"
echo "$BULK" | grep -q "^SENT_BULK" || { fail "대량 매도 주문을 보내지 못했다: $BULK"; exit 1; }
# 계좌 엔진과 매칭이 처리할 시간 — 주문 수에 비례해 기다린다(상한 120초).
WAIT=$(( 5 + N * MOVING_STOCKS / 300 )); [ "$WAIT" -gt 120 ] && WAIT=120
echo "처리 대기 ${WAIT}초"; sleep "$WAIT"

# pod 1 합류 → 이동이 끝날 때까지 대기
start_app matching-1 "$MATCHING_JAR" loadtest/u10/u10-matching-1.yml
DONE=0
for _ in $(seq 1 100); do
    STATE=$(tool assigned localhost:9092 64 | grep "^ASSIGNED" | tail -1)
    if echo "$STATE" | grep -q "^ASSIGNED 64/64 MOVING 0 ENDPOINTS {aeron:udp?endpoint=localhost:20020=32, aeron:udp?endpoint=localhost:20021=32}"; then DONE=1; break; fi
    sleep 3
done
echo "pod 1 합류 뒤: $STATE"
expect "pod 1 합류 뒤 이동이 끝나 두 pod가 32개씩 맡는다" "1" "$DONE"

# 이동 뒤 매수: 종목마다 가장 싼 매도 1건(20,000원 1주)
for stock in $STOCKS; do send_order 90001 "$stock" BUY 20000 1 "u10-big-buy-$stock"; done
sleep 8

tool map-history localhost:9092 | awk -F'\t' 'NF >= 3 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/' > "$WORK/map-history.txt" || true
python3 - "$WORK/map-history.txt" "$LOG_DIR" <<'PY'
import sys,re,datetime
rows=[l.rstrip('\n').split('\t') for l in open(sys.argv[1]) if l.strip()]
rows=[(int(s),int(t),v) for s,t,v in rows]
logdir=sys.argv[2]
def ts(line):
    return int(datetime.datetime.fromisoformat(line.split(' ')[0]).timestamp()*1000)
def first(path,pat):
    for l in open(path,errors='replace'):
        if re.search(pat,l): return ts(l)
    return None
def all_ts(path,pat):
    return [ts(l) for l in open(path,errors='replace') if re.search(pat,l)]
moving=[t for s,t,v in rows if v.endswith(';moving')]
final={}
for s,t,v in rows: final[s]=(t,v)
to1=[t for s,(t,v) in final.items() if v=='aeron:udp?endpoint=localhost:20021']
print("맵 기록 %d건, 이동 중 표시 %d건, pod 1 담당으로 끝난 slot %d개" % (len(rows), len(moving), len(to1)))
if moving and to1: print("MOVE_MS_MAP_RECORD_TIME=%d" % (max(to1)-min(moving)))
join=first(logdir+'/matching-1.log',r'담당 종목 slot 배정')
if join and to1: print("JOIN_TO_SPLIT_MS_LOG_AND_MAP_RECORD_TIME=%d" % (max(to1)-join))
rev=first(logdir+'/matching-0.log',r'담당 종목 slot 회수')
rcv=first(logdir+'/matching-1.log',r'이동으로 받는 중=\[[0-9]')
if rev and rcv: print("REVOKE_TO_RECEIVE_MS_LOG=%d" % (rcv-rev))
# 호가창 전송 시간의 대리값: 직접 알리는 INFO 로그가 없어(요청·응답·적재 확정은 ERROR/WARN만 있다)
# pod 1이 이동 대상을 받았다는 로그 시각 ~ 마지막 pod 1 담당 전환 기록 시각으로 낸다.
if rcv and to1: print("RECEIVE_TO_SWITCH_MS_LOG_AND_MAP_RECORD_TIME=%d" % (max(to1)-rcv))
warn=all_ts(logdir+'/matching-1.log',r'적재 뒤 강제 스냅샷이 거부됨')
if warn: print("FORCED_SNAPSHOT_REJECTED_WARN count=%d first_to_last_ms=%d" % (len(warn), max(warn)-min(warn)))
PY

# 이동 한 건의 단계별 시간(종목 slot별 최소·중앙·최대)
python3 loadtest/u10/handover-steps.py "$WORK/map-history.txt" "$LOG_DIR/matching-1.log"

stop_pids
A=$(dump_account); D0=$(dump_matching /tmp/u10/matching-0); D1=$(dump_matching /tmp/u10/matching-1)
echo "--- 매칭 pod 0 스냅샷(요약)"; echo "$D0" | cut -c1-90
echo "--- 매칭 pod 1 스냅샷(요약)"; echo "$D1" | cut -c1-90
check_owned_slots "$D0" "$D1"
resting_total() {
    local total=0 r
    for d in "$D0" "$D1"; do
        r=$(echo "$d" | grep "BOOK stock=$1 " | sed -n 's/.*resting=\([0-9]*\).*/\1/p'); total=$((total + ${r:-0}))
    done
    echo "$total"
}
book_pods() {
    local n=0
    # grep -q는 긴 덤프 줄을 다 읽기 전에 끝나 pipefail에서 파이프가 실패로 처리되므로 쓰지 않는다.
    [ "$(echo "$D0" | grep -c "BOOK stock=$1 ")" != 0 ] && n=$((n + 1))
    [ "$(echo "$D1" | grep -c "BOOK stock=$1 ")" != 0 ] && n=$((n + 1))
    echo "$n"
}
for stock in $STOCKS; do
    expect "종목 $stock 이동 뒤 한 건이 체결되고 호가창에 N-1건이 남는다" "$((N - 1))" "$(resting_total "$stock")"
    expect "종목 $stock 호가창은 한쪽 pod에만 있다" "1" "$(book_pods "$stock")"
done
BUYER=$(echo "$A" | grep "ACCOUNT id=90001 " || true)
TRADES=$(echo "$BUYER" | sed -n 's/.* tradeIds=\(.*\)$/\1/p' | tr ',' '\n' | grep . | sort -u | wc -l | tr -d ' ')
expect "매수 계좌에 반영된 체결이 종목 수만큼이고 tradeId가 모두 다르다" "$MOVING_STOCKS" "$TRADES"

[ "$OK" = 1 ] && RESULT=0
exit "$RESULT"
