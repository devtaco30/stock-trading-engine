#!/usr/bin/env bash
set -uo pipefail

# U10 3단계 — 실제 증설 시나리오. 매칭 pod 0만 띄워 종목 slot 64개를 모두 맡기고, 체결되지 않는 매도 주문을
# 호가창에 쌓은 뒤 매칭 pod 1을 합류시켜 종목 slot 이동을 일으키고, 이동이 끝난 뒤 반대쪽 계좌의 매수 주문이
# 이동한 호가창의 매도 주문과 체결되는지 본다. 종료 코드 0이면 통과, 그 밖이면 실패다.
#   X = 이동하는 쪽 종목 slot(32 이상)의 종목, Y = 이동하지 않는 쪽 종목 slot(32 미만)의 종목
#   매도 계좌 90002가 X 4주, Y 3주를 20,000원에 먼저 쌓고 → 합류·이동 → 매수 계좌 90001이 같은 가격·수량을 산다.
cd "$(dirname "$0")/.."
source loadtest/u10/lib.sh
trap cleanup EXIT

OK=1
expect() { # <설명> <기대> <실제>
    if [ "$2" = "$3" ]; then echo "통과: $1 ($3)"; else echo "실패: $1 (기대 $2 실제 $3)"; OK=0; fi
}
field() { echo "$1" | sed -n "s/.* $2=\([^ ]*\).*/\1/p"; }

check_ports
rm -rf "$WORK"; mkdir -p "$LOG_DIR"; : > "$PID_FILE"
start_kafka
build_jars

# 종목 선택(Kafka 없이 계산): 두 pod가 나누면 종목 slot 32 이상이 pod 1로 옮겨 간다는 기대를 쓴다.
CODES=$(tool codes 64 32 | tail -1); echo "$CODES"
XSLOT=$(echo "$CODES" | sed -n 's/.*XSLOT=\([0-9]*\) .*/\1/p')
X=$(echo "$CODES" | sed -n 's/.*X=\([0-9]*\) .*/\1/p'); Y=$(echo "$CODES" | sed -n 's/.* Y=\([0-9]*\) .*/\1/p')
[ -n "$X" ] && [ -n "$Y" ] || { fail "종목을 고르지 못했다"; exit 1; }

# 1. 매칭 pod 0만 기동 → 종목 slot 64개를 모두 맡을 때까지 대기
start_app matching-0 "$MATCHING_JAR" loadtest/u10/u10-matching-0.yml
STATE=""
for _ in $(seq 1 40); do
    STATE=$(tool assigned localhost:9092 64 | grep "^ASSIGNED" | tail -1)
    echo "$STATE" | grep -q "^ASSIGNED 64/64 MOVING 0" && break
    sleep 3
done
echo "pod 0만 기동한 뒤: $STATE"
echo "$STATE" | grep -q "^ASSIGNED 64/64 MOVING 0 ENDPOINTS {aeron:udp?endpoint=localhost:20020=64}" \
    || { fail "pod 0이 64개를 모두 맡지 못했다"; exit 1; }

# 계좌 워커 기동(시드: 90001 현금, 90002 두 종목 보유 10주)
start_account \
    --account-worker.seed-accounts[0].account-id=90001 --account-worker.seed-accounts[0].balance=100000000 \
    --account-worker.seed-accounts[0].margin-rate=0.40 \
    --account-worker.seed-accounts[1].account-id=90002 --account-worker.seed-accounts[1].balance=100000000 \
    --account-worker.seed-accounts[1].margin-rate=0.40 \
    --account-worker.seed-accounts[1].holdings.$X=10 --account-worker.seed-accounts[1].holdings.$Y=10

# 매도 호가를 20,000원에 먼저 쌓는다(맞는 매수가 없어 체결 없이 호가창에 남는다).
send_order 90002 "$X" SELL 20000 4 u10-sell-x
send_order 90002 "$Y" SELL 20000 3 u10-sell-y
sleep 3

# 2. 매칭 pod 1 합류 → 이동이 끝날 때까지 대기(맵에서 이동 중 0, 두 주소가 모두 담당)
start_app matching-1 "$MATCHING_JAR" loadtest/u10/u10-matching-1.yml
JOIN_EPOCH_MS=$(python3 -c 'import time;print(int(time.time()*1000))')
DONE=0
for _ in $(seq 1 60); do
    STATE=$(tool assigned localhost:9092 64 | grep "^ASSIGNED" | tail -1)
    if echo "$STATE" | grep -q "^ASSIGNED 64/64 MOVING 0 ENDPOINTS {aeron:udp?endpoint=localhost:20020=32, aeron:udp?endpoint=localhost:20021=32}"; then DONE=1; break; fi
    sleep 3
done
echo "pod 1 합류 뒤: $STATE"
expect "pod 1 합류 뒤 이동이 끝나 두 pod가 32개씩 맡는다" "1" "$DONE"

# 3. 이동 뒤 반대쪽 계좌의 매수 주문(같은 종목·가격·수량)
send_order 90001 "$X" BUY 20000 4 u10-buy-x
send_order 90001 "$Y" BUY 20000 3 u10-buy-y
sleep 6

# 이동 시간 계산 — 기준 시각이 서로 다른 값을 따로 낸다(이름에 기준을 적는다).
#   MOVE_MS_MAP_RECORD_TIME: 맵 토픽 기록의 브로커 기록 시각(CreateTime)만 쓴다. 「이동 중」 표시가 처음 올라온 기록 ~ 마지막
#                            slot이 pod 1 담당으로 바뀐 기록.
#   JOIN_TO_SPLIT_MS_LOG_AND_MAP_RECORD_TIME: matching-1 로그의 첫 「담당 종목 slot 배정」 줄 시각(pod 1 합류) ~ 맵이
#                            「이동 중 0, 32개씩」이 된 시각(마지막 slot이 pod 1 담당으로 바뀐 기록의 브로커 기록 시각).
#   REVOKE_TO_RECEIVE_MS_LOG: matching-0 로그의 「담당 종목 slot 회수」 줄 ~ matching-1 로그의 「이동으로 받는 중」 줄(로그 시각만).
# 모두 이 컴퓨터 한 대의 값이며 일반화하지 않는다.
tool map-history localhost:9092 > "$WORK/map-history.raw" || true
grep -q WARN_PARTIAL_MAP_READ "$WORK/map-history.raw" && { echo "실패: 맵 기록을 끝까지 읽지 못했다(부분 결과)"; OK=0; }
awk -F'\t' 'NF >= 3 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/' "$WORK/map-history.raw" > "$WORK/map-history.txt"
# 이동 완료를 맵 개수(32/32)로만 판정하면 협력 배정이 반대쪽 slot을 옮겨도 통과하므로, 체결을 기대한 종목 X의 slot이 실제로 pod 0에서 pod 1로 바뀌었는지 맵 기록으로 단언한다.
python3 loadtest/u10/assert-moved.py "$WORK/map-history.txt" "$XSLOT" aeron:udp?endpoint=localhost:20020 aeron:udp?endpoint=localhost:20021
expect "종목 X의 slot이 맵 기록으로 pod 0에서 pod 1로 바뀌었다" "0" "$?"
python3 - "$WORK/map-history.txt" "$LOG_DIR" <<'PY'
import sys,re,datetime
rows=[l.rstrip('\n').split('\t') for l in open(sys.argv[1]) if l.strip()]
rows=[(int(r[0]),int(r[1]),r[2]) for r in rows]  # 4번째 필드(timestampType)는 쓰지 않는다
logdir=sys.argv[2]
def ts(line):
    return int(datetime.datetime.fromisoformat(line.split(' ')[0]).timestamp()*1000)
def first(path,pat):
    for l in open(path,errors='replace'):
        if re.search(pat,l): return ts(l),l
    return None,None
moving=[t for s,t,v in rows if v.endswith(';moving')]
final={}
for s,t,v in rows:
    final[s]=(t,v)
to1=[t for s,(t,v) in final.items() if v=='aeron:udp?endpoint=localhost:20021']
print("맵 기록 %d건, 이동 중 표시 %d건, pod 1 담당으로 끝난 slot %d개" % (len(rows), len(moving), len(to1)))
if moving and to1:
    print("MOVE_MS_MAP_RECORD_TIME=%d  (이동 중 첫 기록 ~ 마지막 pod 1 담당 전환 기록, 브로커 기록 시각)" % (max(to1)-min(moving)))
else:
    print("MOVE_MS_MAP_RECORD_TIME=계산 불가(이동 중 표시 %d건)" % len(moving))
join,_=first(logdir+'/matching-1.log',r'담당 종목 slot 배정')
if join and to1:
    print("JOIN_TO_SPLIT_MS_LOG_AND_MAP_RECORD_TIME=%d  (matching-1 첫 배정 로그 ~ 마지막 pod 1 담당 전환 기록)" % (max(to1)-join))
else:
    print("JOIN_TO_SPLIT_MS_LOG_AND_MAP_RECORD_TIME=계산 불가")
rev,_=first(logdir+'/matching-0.log',r'담당 종목 slot 회수')
rcv,_=first(logdir+'/matching-1.log',r'이동으로 받는 중=\[[0-9]')
if rev and rcv:
    print("REVOKE_TO_RECEIVE_MS_LOG=%d  (matching-0 회수 로그 ~ matching-1 이동으로 받는 중 로그)" % (rcv-rev))
else:
    print("REVOKE_TO_RECEIVE_MS_LOG=계산 불가")
PY
expect "맵 기록 분석 python이 오류 없이 끝났다" "0" "$?"

# 4. 정상 종료 → 스냅샷 판독
# 이동 한 건의 단계별 시간(종목 slot별 최소·중앙·최대)
python3 loadtest/u10/handover-steps.py "$WORK/map-history.txt" "$LOG_DIR/matching-1.log"
expect "단계별 시간이 이동한 slot 전부에 대해 계산됐다" "0" "$?"

stop_pids
A=$(dump_account)
D0=$(dump_matching /tmp/u10/matching-0); D1=$(dump_matching /tmp/u10/matching-1)
echo "--- 계좌 워커 스냅샷"; echo "$A"
echo "--- 매칭 pod 0 스냅샷"; echo "$D0"; echo "--- 매칭 pod 1 스냅샷"; echo "$D1"
BUYER=$(echo "$A" | grep "ACCOUNT id=90001 " || true); SELLER=$(echo "$A" | grep "ACCOUNT id=90002 " || true)
[ -n "$BUYER" ] && [ -n "$SELLER" ] || { fail "계좌 스냅샷에 90001, 90002 줄이 없다"; exit 1; }

# 이동한 호가창의 매도 주문이 이동 뒤에도 체결됐는지: 매수 계좌 잔고 = 1억 − (80,000+60,000)×0.40, 미수금 = 140,000×0.60
expect "매수 계좌 잔고에 두 체결의 증거금이 반영됐다" "99944000" "$(field "$BUYER" balance)"
expect "매수 계좌 미수금에 두 체결의 외상 몫이 반영됐다" "84000" "$(field "$BUYER" unpaid)"
expect "매수 계좌가 두 종목을 보유한다" "{$X=4,$Y=3}" "$(field "$BUYER" holdings)"
expect "매수 계좌에 남은 예약이 없다" "0" "$(field "$BUYER" reservations)"
expect "매도 계좌의 두 종목 보유가 줄었다" "{$X=6,$Y=7}" "$(field "$SELLER" holdings)"
expect "매도 계좌에 남은 매도 예약이 없다" "0" "$(field "$SELLER" sellReservations)"
TRADE_IDS=$(field "$BUYER" tradeIds)
expect "두 체결의 tradeId가 서로 다르다" "2" "$(echo "$TRADE_IDS" | tr ',' '\n' | grep . | sort -u | wc -l | tr -d ' ')"

# 호가창: 체결돼 남은 주문이 없고, 이동한 종목 X의 호가창은 한쪽 pod에만 있다.
check_owned_slots "$D0" "$D1"
book_pods() { # <종목> → 그 종목의 BOOK 줄이 있는 pod 수
    local n=0
    echo "$D0" | grep -q "BOOK stock=$1 " && n=$((n + 1))
    echo "$D1" | grep -q "BOOK stock=$1 " && n=$((n + 1))
    echo "$n"
}
resting_total() { # <종목> → 두 pod의 대기 주문 합
    local total=0 r
    for d in "$D0" "$D1"; do
        r=$(echo "$d" | grep "BOOK stock=$1 " | sed -n 's/.*resting=\([0-9]*\).*/\1/p'); total=$((total + ${r:-0}))
    done
    echo "$total"
}
expect "이동한 종목 X는 체결돼 두 pod 호가창에 남은 주문이 없다" "0" "$(resting_total "$X")"
expect "이동하지 않은 종목 Y는 체결돼 두 pod 호가창에 남은 주문이 없다" "0" "$(resting_total "$Y")"
expect "이동한 종목 X의 호가창은 한쪽 pod에만 있다" "1" "$(book_pods "$X")"
expect "이동하지 않은 종목 Y의 호가창은 한쪽 pod에만 있다" "1" "$(book_pods "$Y")"

echo "--- 호가창 이동 관련 로그(matching-0, matching-1, account 중 앞부분)"
grep -hE "호가창 이동|handover|Handover" "$LOG_DIR"/matching-*.log | cut -c1-200 | head -20

report_send_failures
[ "$OK" = 1 ] && RESULT=0
exit "$RESULT"
