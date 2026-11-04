#!/usr/bin/env bash
# sustained-run.sh의 DRY_RUN(서버·도커·gradle 없이 계획만 출력) 시험. 실행: bash loadtest/s0b/test_sustained_run.sh (저장소 루트에서)
cd "$(dirname "$0")/../.."
PASS=0; FAIL=0
check() { if printf '%s' "$3" | grep -qF -- "$2"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); echo "FAIL $1: [$2] 가 없다"; fi; }
check_not() { if printf '%s' "$3" | grep -qF -- "$2"; then FAIL=$((FAIL+1)); echo "FAIL $1: [$2] 가 있으면 안 된다"; else PASS=$((PASS+1)); fi; }
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT

OUT=$(DRY_RUN=1 RES="$T" bash loadtest/s0b/sustained-run.sh 10800 t1 2>&1); RC=$?
check "종료 코드 0" "rc=0" "rc=$RC"
check "R이 계획에 찍힌다" "PLAN R=10800 SEC=7200 TAG=t1" "$OUT"
check "체결 섞기 한 서버" "GROUP_BY_SHARD=1 ACCOUNT_WORKERS=1" "$OUT"
check "종목 쌍 수는 R x 초 / 60" "PAIRS_PER_STOCK=1296000" "$OUT"
check "매수 잔고는 1000억" "BUYER_BALANCE=100000000000" "$OUT"
check "C는 2010" "C=2010" "$OUT"
check "디스크 가드 한도" "DISK_MIN_GB=20" "$OUT"
check "스레드 덤프 시각 세 번" "JCMD_AT=300,3660,7140" "$OUT"
check "결과 이름 접두사" "BASE=$T/sustained-R10800-t1" "$OUT"
check "총 주문 수" "TOTAL_ORDERS=" "$OUT"

OUT2=$(DRY_RUN=1 SEC=601 RES="$T" bash loadtest/s0b/sustained-run.sh 10801 t2 2>&1); RC2=$?
check "R x 초가 60의 배수가 아니면 실패" "rc=1" "rc=$RC2"

OUT3=$(DRY_RUN=1 RES="$T" bash loadtest/s0b/sustained-run.sh 2>&1); RC3=$?
check "R이 없으면 실패" "rc=1" "rc=$RC3"

OUT4=$(DRY_RUN=1 SEC=600 RES="$T" bash loadtest/s0b/sustained-run.sh 9900 short 2>&1); RC4=$?
check "SEC로 줄일 수 있다" "rc=0" "rc=$RC4"
check "줄인 SEC의 덤프 시각" "JCMD_AT=300,360,540" "$OUT4"

echo "통과 $PASS, 실패 $FAIL"
[ "$FAIL" = 0 ]
