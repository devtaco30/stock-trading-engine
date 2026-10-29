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

rm -rf "$WORK"
echo "통과 ${PASS}건, 실패 ${FAIL}건"
[ "$FAIL" = 0 ]
