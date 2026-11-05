#!/usr/bin/env bash
# ladder.sh의 DRY_RUN(인프라·앱 없이 단계별 명령만 출력) 시험. 실행: bash loadtest/s0b/test_ladder.sh (저장소 루트에서)
cd "$(dirname "$0")/../.."
PASS=0; FAIL=0
check() { # 이름, 기대 문자열, 실제 문자열
  if printf '%s' "$3" | grep -qF -- "$2"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); echo "FAIL $1: [$2] 가 없다"; fi
}
check_not() {
  if printf '%s' "$3" | grep -qF -- "$2"; then FAIL=$((FAIL+1)); echo "FAIL $1: [$2] 가 있으면 안 된다"; else PASS=$((PASS+1)); fi
}

OUT=$(DRY_RUN=1 ACCTS=100 LADDER=3,9,30 ACCEPT_PROBES=2:4 OUT=/tmp/ladder-test-out bash loadtest/s0b/ladder.sh 2>&1); RC=$?
check "종료 코드 0" "rc=0" "rc=$RC"
check "시드 보유는 마지막 단계 + 접수 검증량 + 여유" "HOLD=19" "$OUT"
check "첫 단계 쌓기" "PREFILL 0 per_stock=100 quantity=1" "$OUT"
check "거부는 가용분+1 수량" "REJECT 0 per_stock=" "$OUT"
check "거부 수량 옵션" "--quantity=19" "$OUT"
check "접수 검증 단계" "ACCEPT 2 per_stock=400 quantity=1" "$OUT"
check "계좌 수는 환경변수로" "BULK_ACCOUNTS=100" "$OUT"
check "로그 수준을 ERROR로" "LoggingAccountResultListener=ERROR" "$OUT"
check_not "DRY_RUN은 도커를 띄우지 않는다" "docker run" "$OUT"

OUT2=$(DRY_RUN=1 ACCTS=100 LADDER=4,9 OUT=/tmp/ladder-test-out bash loadtest/s0b/ladder.sh 2>&1); RC2=$?
check "C가 3의 배수가 아니면 실패" "rc=1" "rc=$RC2"

OUT3=$(DRY_RUN=1 OUT=/tmp/ladder-test-out bash loadtest/s0b/ladder.sh 2>&1); RC3=$?
check "ACCTS가 없으면 실패" "rc=1" "rc=$RC3"

echo "통과 ${PASS}, 실패 ${FAIL}"
[ "$FAIL" -eq 0 ]
