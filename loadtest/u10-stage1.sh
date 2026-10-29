#!/usr/bin/env bash
set -uo pipefail

# U10 1단계 — 매칭 shard 둘과 계좌 워커 하나를 실제 프로세스로 띄워, 종목 slot 64개가 모두 배정되고
# 서로 다른 매칭 shard가 맡는 두 종목(X, Y)의 주문이 각자의 매칭 shard 호가창에만 들어가는지 본다.
# 종료 코드 0이면 통과, 그 밖이면 실패다.
cd "$(dirname "$0")/.."
source loadtest/u10/lib.sh
trap cleanup EXIT

check_ports
rm -rf "$WORK"; mkdir -p "$LOG_DIR"; : > "$PID_FILE"
start_kafka
build_jars
start_matching_and_pick
start_account

# 계좌 90001이 X와 Y를 각각 매수한다(매도가 없어 호가창에 남는다).
send_order 90001 "$X" BUY 10000 1 u10-x
send_order 90001 "$Y" BUY 10000 1 u10-y
sleep 3

# 정상 종료 → 스냅샷 기록
stop_pids

D0=$(dump_matching /tmp/u10/matching-0); D1=$(dump_matching /tmp/u10/matching-1)
echo "--- 매칭 shard 0 스냅샷"; echo "$D0"; echo "--- 매칭 shard 1 스냅샷"; echo "$D1"
DUMPS=("$D0" "$D1")
OK=1
check() { # <desc> <shard> <stock> <expected resting>
    local line
    line=$(echo "${DUMPS[$2]}" | grep "BOOK stock=$3 " || true)
    local got=0; [ -n "$line" ] && got=$(echo "$line" | sed -n 's/.*resting=\([0-9]*\).*/\1/p')
    if [ "$got" = "$4" ]; then echo "통과: $1 (매칭 shard $2, 종목 $3, 대기 주문 $got)"; else echo "실패: $1 (매칭 shard $2, 종목 $3, 기대 $4 실제 $got)"; OK=0; fi
}
[ "$XS" != "?" ] && [ "$YS" != "?" ] && [ "$XS" != "$YS" ] || { fail "X와 Y의 담당 shard를 알 수 없거나 같다: X=$XEND Y=$YEND"; exit 1; }
check_owned_slots "$D0" "$D1"
OTHER_X=$((1 - XS)); OTHER_Y=$((1 - YS))
check "종목 X 주문이 담당 매칭 shard의 호가창에 있다" "$XS" "$X" 1
check "종목 Y 주문이 담당 매칭 shard의 호가창에 있다" "$YS" "$Y" 1
check "종목 X 주문이 다른 매칭 shard에는 없다" "$OTHER_X" "$X" 0
check "종목 Y 주문이 다른 매칭 shard에는 없다" "$OTHER_Y" "$Y" 0

[ "$OK" = 1 ] && RESULT=0
exit "$RESULT"
