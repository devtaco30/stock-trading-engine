#!/usr/bin/env bash
# mix.sh의 DRY_RUN(인프라·앱 없이 계획과 명령만 출력) 시험. 실행: bash loadtest/s0b/test_mix.sh (저장소 루트에서)
cd "$(dirname "$0")/../.."
PASS=0; FAIL=0
check() { # 이름, 기대 문자열, 실제 문자열
  if printf '%s' "$3" | grep -qF -- "$2"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); echo "FAIL $1: [$2] 가 없다"; fi
}
check_not() {
  if printf '%s' "$3" | grep -qF -- "$2"; then FAIL=$((FAIL+1)); echo "FAIL $1: [$2] 가 있으면 안 된다"; else PASS=$((PASS+1)); fi
}

OUT=$(DRY_RUN=1 ACCTS=300 BUYERS=30 C=2001 RATE=4000 SECONDS_STEADY=180 OUT=/tmp/mix-test-out bash loadtest/s0b/mix.sh 2>&1); RC=$?
check "종료 코드 0" "rc=0" "rc=$RC"
check "종목당 쌓은 수는 C x N / 3" "PREFILL_PER_STOCK=200100" "$OUT"
check "종목당 쌍 수는 R x 초 / 6" "PAIRS_PER_STOCK=120000" "$OUT"
check "보유 수량은 쌓은 몫 + 유지 몫 + 5" "HOLD=1072" "$OUT"
check "총 주문 수" "TOTAL_ORDERS=1320300" "$OUT"
check "총 체결 수" "TOTAL_TRADES=360000" "$OUT"
check "매수 잔고 기본값" "BUYER_BALANCE=10000000000" "$OUT"
check "쌓기는 같은 가격" "--price-step=0" "$OUT"
check "쌓기 매도 가격" "send-bulk aeron:udp?endpoint=localhost:20040 90002 SELL 20000 200100" "$OUT"
check "유지 발신 명령" "send-paced aeron:udp?endpoint=localhost:20040 90002 300 90302 30 20000 200100 120000 4000" "$OUT"
check "계좌 수는 환경변수로" "BULK_ACCOUNTS=300" "$OUT"
check_not "DRY_RUN은 도커를 띄우지 않는다" "docker run" "$OUT"

OUT2=$(DRY_RUN=1 ACCTS=300 BUYERS=30 C=2000 RATE=4000 SECONDS_STEADY=180 OUT=/tmp/mix-test-out bash loadtest/s0b/mix.sh 2>&1); RC2=$?
check "C가 3의 배수가 아니면 실패" "rc=1" "rc=$RC2"

OUT3=$(DRY_RUN=1 OUT=/tmp/mix-test-out bash loadtest/s0b/mix.sh 2>&1); RC3=$?
check "인자가 없으면 실패" "rc=1" "rc=$RC3"

OUT4=$(DRY_RUN=1 ACCTS=300 BUYERS=30 C=2001 RATE=4000 SECONDS_STEADY=180 BUYER_BALANCE=1000 OUT=/tmp/mix-test-out bash loadtest/s0b/mix.sh 2>&1); RC4=$?
check "매수 잔고가 필요량보다 작으면 실패" "rc=1" "rc=$RC4"

OUT5=$(DRY_RUN=1 ACCTS=300 BUYERS=30 C=2001 RATE=4001 SECONDS_STEADY=181 OUT=/tmp/mix-test-out bash loadtest/s0b/mix.sh 2>&1); RC5=$?
check "R x 초가 6의 배수가 아니면 실패" "rc=1" "rc=$RC5"

OUT6=$(DRY_RUN=1 STOCK_COUNT=30 ACCTS=300 BUYERS=30 C=2010 RATE=8000 SECONDS_STEADY=180 OUT=/tmp/mix-test-out bash loadtest/s0b/mix.sh 2>&1); RC6=$?
check "종목 30개 실행" "rc=0" "rc=$RC6"
check "종목 30개 쌓은 수" "PREFILL_PER_STOCK=20100" "$OUT6"
check "종목 30개 쌍 수" "PAIRS_PER_STOCK=24000" "$OUT6"
check "종목 30개 보유 수량" "HOLD=152" "$OUT6"
check "종목 30개 총 주문 수" "TOTAL_ORDERS=2043000" "$OUT6"
check "종목 30개 총 체결 수" "TOTAL_TRADES=720000" "$OUT6"
OUT7=$(DRY_RUN=1 STOCK_COUNT=30 ACCTS=300 BUYERS=30 C=2001 RATE=8000 SECONDS_STEADY=180 OUT=/tmp/mix-test-out bash loadtest/s0b/mix.sh 2>&1); RC7=$?
check "종목 30개에서 C가 30의 배수가 아니면 실패" "rc=1" "rc=$RC7"

OUT8=$(DRY_RUN=1 ACCOUNT_WORKERS=2 STOCK_COUNT=30 ACCTS=300 BUYERS=30 C=2010 RATE=8000 SECONDS_STEADY=180 OUT=/tmp/mix-test-out bash loadtest/s0b/mix.sh 2>&1); RC8=$?
check "계좌 워커 2개 실행" "rc=0" "rc=$RC8"
check "계좌 워커 수가 params에 남는다" "ACCOUNT_WORKERS=2" "$OUT8"
check "쌓기는 채널 둘" "send-bulk aeron:udp?endpoint=localhost:20040,aeron:udp?endpoint=localhost:20041 90002 SELL" "$OUT8"
check "유지도 채널 둘" "send-paced aeron:udp?endpoint=localhost:20040,aeron:udp?endpoint=localhost:20041 90002 300" "$OUT8"
check "워커 1이면 params에 1" "ACCOUNT_WORKERS=1" "$OUT"
check_not "워커 1이면 채널이 하나" "20041" "$OUT"
OUT9=$(DRY_RUN=1 ACCOUNT_WORKERS=3 ACCTS=300 BUYERS=30 C=2001 RATE=4000 SECONDS_STEADY=180 OUT=/tmp/mix-test-out bash loadtest/s0b/mix.sh 2>&1); RC9=$?
check "계좌 워커 수가 1,2가 아니면 실패" "rc=1" "rc=$RC9"

echo "통과 ${PASS}, 실패 ${FAIL}"
[ "$FAIL" -eq 0 ]
