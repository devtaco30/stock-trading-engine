#!/usr/bin/env bash
set -uo pipefail

# U10 2단계 — 서로 다른 매칭 shard가 맡는 두 종목(X, Y)을 한 계좌(90001)가 매수하고 다른 계좌(90002)가 매도해
# 체결 두 건이 모두 계좌에 반영되는지 본다. 매수 4주@10,000(X)와 3주@10,000(Y)이고, 증거금률 0.40이라
# 매수 계좌의 잔고는 (40,000+30,000)*0.40=28,000이 줄고 나머지 42,000은 미수금이 된다.
# 종료 코드 0이면 통과, 그 밖이면 실패다.
cd "$(dirname "$0")/.."
source loadtest/u10/lib.sh
trap cleanup EXIT

check_ports
rm -rf "$WORK"; mkdir -p "$LOG_DIR"; : > "$PID_FILE"
start_kafka
build_jars
start_matching_and_pick

# 시드 목록 전체를 인자로 넘긴다(인덱스 목록은 설정 파일과 인자가 합쳐지지 않고 우선순위가 높은 쪽 전체가 쓰인다).
start_account \
    --account-worker.seed-accounts[0].account-id=90001 --account-worker.seed-accounts[0].balance=100000000 \
    --account-worker.seed-accounts[0].margin-rate=0.40 \
    --account-worker.seed-accounts[1].account-id=90002 --account-worker.seed-accounts[1].balance=100000000 \
    --account-worker.seed-accounts[1].margin-rate=0.40 \
    --account-worker.seed-accounts[1].holdings.$X=10 --account-worker.seed-accounts[1].holdings.$Y=10

send_order 90001 "$X" BUY 10000 4 u10-buy-x
send_order 90002 "$X" SELL 10000 4 u10-sell-x
send_order 90001 "$Y" BUY 10000 3 u10-buy-y
send_order 90002 "$Y" SELL 10000 3 u10-sell-y
sleep 5

stop_pids

A=$(dump_account)
D0=$(dump_matching /tmp/u10/matching-0); D1=$(dump_matching /tmp/u10/matching-1)
echo "--- 계좌 워커 스냅샷"; echo "$A"
echo "--- 매칭 shard 0 스냅샷"; echo "$D0"; echo "--- 매칭 shard 1 스냅샷"; echo "$D1"
OK=1
expect() { # <설명> <기대> <실제>
    if [ "$2" = "$3" ]; then echo "통과: $1 ($3)"; else echo "실패: $1 (기대 $2 실제 $3)"; OK=0; fi
}
field() { # <줄> <필드>
    echo "$1" | sed -n "s/.* $2=\([^ ]*\).*/\1/p"
}
BUYER=$(echo "$A" | grep "ACCOUNT id=90001 " || true)
SELLER=$(echo "$A" | grep "ACCOUNT id=90002 " || true)
[ -n "$BUYER" ] && [ -n "$SELLER" ] || { fail "계좌 스냅샷에 90001, 90002 줄이 없다"; exit 1; }

expect "매수 계좌 잔고에 두 체결의 증거금이 반영됐다" "99972000" "$(field "$BUYER" balance)"
expect "매수 계좌 미수금에 두 체결의 외상 몫이 반영됐다" "42000" "$(field "$BUYER" unpaid)"
expect "매수 계좌가 두 종목을 보유한다" "{$X=4,$Y=3}" "$(field "$BUYER" holdings)"
expect "매수 계좌에 남은 예약이 없다" "0" "$(field "$BUYER" reservations)"
expect "매도 계좌의 두 종목 보유가 줄었다" "{$X=6,$Y=7}" "$(field "$SELLER" holdings)"
expect "매도 계좌에 남은 매도 예약이 없다" "0" "$(field "$SELLER" sellReservations)"

TRADE_IDS=$(field "$BUYER" tradeIds)
TRADE_COUNT=$(echo "$TRADE_IDS" | tr ',' '\n' | grep -c .)
UNIQUE_COUNT=$(echo "$TRADE_IDS" | tr ',' '\n' | grep . | sort -u | wc -l | tr -d ' ')
expect "매수 계좌에 반영된 체결이 두 건이다" "2" "$TRADE_COUNT"
expect "두 체결의 tradeId가 서로 다르다" "2" "$UNIQUE_COUNT"

resting() { # <매칭 shard 번호> <종목>
    local dump="$D0"; [ "$1" = 1 ] && dump="$D1"
    local line; line=$(echo "$dump" | grep "BOOK stock=$2 " || true)
    if [ -z "$line" ]; then echo 0; else echo "$line" | sed -n 's/.*resting=\([0-9]*\).*/\1/p'; fi
}
expect "종목 X는 전량 체결돼 담당 매칭 shard 호가창에 남은 주문이 없다" "0" "$(resting "$XS" "$X")"
expect "종목 Y는 전량 체결돼 담당 매칭 shard 호가창에 남은 주문이 없다" "0" "$(resting "$YS" "$Y")"

[ "$OK" = 1 ] && RESULT=0
exit "$RESULT"
