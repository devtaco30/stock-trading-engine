#!/usr/bin/env bash
# loadtest/u10/lib.sh의 send_order 실패 집계 시험. tool을 대신하는 함수를 넣어 gradle·Aeron 없이 돈다.
# 실행: bash loadtest/u10/test_lib.sh   (저장소 루트에서)
set -uo pipefail
cd "$(dirname "$0")/../.."
source loadtest/u10/lib.sh
WORK=$(mktemp -d); LOG_DIR="$WORK/logs"; mkdir -p "$LOG_DIR"
PASS=0; FAIL=0
check() { if [ "$2" = "$3" ]; then PASS=$((PASS + 1)); else FAIL=$((FAIL + 1)); echo "실패: $1 (기대 $2 실제 $3)"; fi; }

tool() { echo "$FAKE_TOOL_OUTPUT"; }

FAKE_TOOL_OUTPUT="SENT a"; send_order 1 000001 BUY 1 1 a > /dev/null
check "발신 성공은 실패로 세지 않는다" "0" "$(send_failure_count)"

FAKE_TOOL_OUTPUT="OFFER_FAILED -1"; send_order 1 000001 BUY 1 1 b > /dev/null
FAKE_TOOL_OUTPUT="NOT_CONNECTED aeron:udp?endpoint=localhost:20040"; send_order 1 000001 BUY 1 1 c > /dev/null
check "OFFER_FAILED와 NOT_CONNECTED를 실패로 센다" "2" "$(send_failure_count)"

FAKE_TOOL_OUTPUT=""; send_order 1 000001 BUY 1 1 d > /dev/null
check "출력이 비어도 실패로 센다" "3" "$(send_failure_count)"

# wait_for_applied_fills <로그> <계좌> <기대 건수> <시도 횟수> <간격 초>: applied=true인 체결 반영 줄만 센다
FILLS_LOG="$WORK/account.log"
cat > "$FILLS_LOG" <<'LOG'
INFO [계좌 워커] 체결 반영: accountId=90001 orderId=1 tradeId=10 applied=true
INFO [계좌 워커] 체결 반영: accountId=90001 orderId=1 tradeId=10 applied=false
INFO [계좌 워커] 체결 반영: accountId=90002 orderId=2 tradeId=10 applied=true
INFO [계좌 워커] 체결 반영: accountId=90001 orderId=3 tradeId=11 applied=true
LOG
check "applied=false와 다른 계좌의 줄은 세지 않는다" "2" "$(wait_for_applied_fills "$FILLS_LOG" 90001 3 3 0.05)"
echo 'INFO [계좌 워커] 체결 반영: accountId=90001 orderId=4 tradeId=12 applied=true' >> "$FILLS_LOG"
check "기대 건수에 닿으면 바로 그 건수를 돌려준다" "3" "$(wait_for_applied_fills "$FILLS_LOG" 90001 3 3 0.05)"

MISSING_OUT=$(wait_for_applied_fills "$WORK/없는-로그.log" 90001 1 2 0.01 2>"$WORK/missing.err")
check "로그 파일이 없으면 센 건수는 0이다" "0" "$MISSING_OUT"
check "로그 파일이 없으면 파일 없음을 메시지로 알린다" "1" "$(grep -c '로그 파일이 없다' "$WORK/missing.err")"
check "로그 파일이 없을 때 shell 오류가 나지 않는다" "0" "$(grep -c 'integer expression' "$WORK/missing.err")"

NONNUM_ERR=$(bash loadtest/u10-stage3-large.sh abc 2>&1 >/dev/null)
check "종목당 주문 수가 숫자가 아니면 숫자 메시지를 낸다" "1" "$(echo "$NONNUM_ERR" | grep -c '숫자')"
check "숫자가 아닐 때 shell 오류가 나지 않는다" "0" "$(echo "$NONNUM_ERR" | grep -c 'integer expression')"

# flow_verdict <합계 단언 통과 1|0> <전제 모두 참 1|0> — 합계가 틀리면 전제와 무관하게 실패(1), 합계가 맞고 전제만 거짓이면 미발동(3)
check "합계가 틀리면 전제가 거짓이어도 실패" "1" "$(flow_verdict 0 0)"
check "합계가 틀리면 전제가 참이어도 실패" "1" "$(flow_verdict 0 1)"
check "합계가 맞고 전제가 거짓이면 미발동" "3" "$(flow_verdict 1 0)"
check "합계가 맞고 전제가 참이면 통과" "0" "$(flow_verdict 1 1)"

# tool_failure_reason <도구 출력> — 도구 자체의 실패 표지를 찾는다(시나리오 미발동과 구별)
check "송신 위치 미도달은 도구 실패" "SENDER_POSITION_NOT_REACHED" "$(tool_failure_reason 'SENT_BULK 5
SENDER_POSITION_NOT_REACHED 발행 위치=1')"
check "송신 위치 카운터 없음은 도구 실패" "SENDER_POSITION_COUNTER_NOT_FOUND" "$(tool_failure_reason 'SENDER_POSITION_COUNTER_NOT_FOUND x')"
check "연결 실패는 도구 실패" "NOT_CONNECTED" "$(tool_failure_reason 'NOT_CONNECTED ch')"
check "정상 출력은 실패 표지가 없다" "" "$(tool_failure_reason 'SENT_BULK 5
SENDER_POSITION_REACHED 99')"


# count_in_window <시작 epoch ms> <끝 epoch ms> — 표준 입력의 시각(한 줄에 하나) 중 [시작, 끝] 안에 든 개수. 첫·마지막 시각만으로 겹침을 판정하면
# 구간 앞뒤에만 거부가 있어도 참이 되므로, 거부 회신마다 남긴 시각을 직접 센다.
check "구간 안의 시각만 센다(경계 포함)" "3" "$(printf '99\n100\n150\n200\n201\n' | count_in_window 100 200)"
check "구간 앞뒤에만 시각이 있으면 0이다" "0" "$(printf '10\n500\n' | count_in_window 100 200)"
check "입력이 비어도 0이다" "0" "$(printf '' | count_in_window 100 200)"
check "숫자가 아닌 줄은 무시한다" "1" "$(printf 'abc\n150\n\n' | count_in_window 100 200)"


# held_premise <동시 보관 최대 건수> — 이동 중 표시를 본 종목 slot의 새 주문이 보관 목록에 쌓였는지(1) 아닌지(0). 설계서 10-1절 수정본 3번.
# 거부 회신 건수는 전제가 아니다(맵에 이동 중이 표시되기 전에 나간 주문이 있을 때만 생기며 실행마다 0일 수 있다).
check "동시 보관 최대가 1 이상이면 전제 참" "1" "$(held_premise 402)"
check "동시 보관 최대가 정확히 1이어도 참" "1" "$(held_premise 1)"
check "동시 보관 최대가 0이면 거짓" "0" "$(held_premise 0)"
check "값이 없으면(종료 로그 없음) 거짓" "0" "$(held_premise '')"
check "숫자가 아니면 거짓" "0" "$(held_premise abc)"
# reject_offsets <구간 시작 epoch ms> — 표준 입력의 거부 수신 시각마다 「시각 − 구간 시작」(ms, 음수면 구간 앞)을 한 줄에 하나씩 출력한다. 보고 수치용.
check "구간 시작과의 차를 부호 있게 낸다" "-10 5" "$(printf '990\n1005\n' | reject_offsets 1000 | paste -sd' ' -)"
check "입력이 비면 아무것도 내지 않는다" "" "$(printf '' | reject_offsets 1000)"

rm -rf "$WORK"
echo "통과 ${PASS}건, 실패 ${FAIL}건"
[ "$FAIL" = 0 ]
