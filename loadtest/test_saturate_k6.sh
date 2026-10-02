#!/usr/bin/env bash
set -uo pipefail

# saturate-k6.sh 의 인자 처리와 계획 출력을 확인하는 테스트. 서버·k6·부하를 시작하지 않는다(--dry-run 과 함수만 쓴다).
# 실행: bash loadtest/test_saturate_k6.sh   (저장소 루트에서)

cd "$(dirname "$0")/.."

PASSES=0
FAILS=0

assert_eq() {
    if [ "$2" = "$3" ]; then PASSES=$((PASSES + 1)); else FAILS=$((FAILS + 1)); echo "FAIL $1: 기대=[$2] 실제=[$3]"; fi
}
assert_contains() {
    if printf '%s' "$2" | grep -qF -- "$3"; then PASSES=$((PASSES + 1)); else FAILS=$((FAILS + 1)); echo "FAIL $1: [$3] 가 없다"; fi
}
assert_not_contains() {
    if printf '%s' "$2" | grep -qF -- "$3"; then FAILS=$((FAILS + 1)); echo "FAIL $1: [$3] 가 있으면 안 된다"; else PASSES=$((PASSES + 1)); fi
}

out="$(bash loadtest/saturate-k6.sh --dry-run 2>&1)"
assert_eq "기본 계획 종료 코드" "0" "$?"
assert_contains "기본 속도" "$out" "192 500 1000 2000 4000 6000"
assert_contains "속도마다 N" "$out" "192/초 N=3840"
assert_contains "6000은 입력 줄 수로 제한" "$out" "6000/초 N=100000"
assert_contains "예상 시간" "$out" "예상 소요"
assert_contains "결과 폴더 이름" "$out" "loadtest/results/k6-solo-"
assert_not_contains "dry-run 은 서버를 띄우지 않는다" "$out" "더미 서버 기동"

out="$(RATES="192 8000" bash loadtest/saturate-k6.sh --dry-run 2>&1)"
assert_contains "RATES 환경 변수" "$out" "192 8000"
assert_contains "8000 도 N 제한" "$out" "8000/초 N=100000"

out="$(bash loadtest/saturate-k6.sh --dry-run --rates "500 1000" 2>&1)"
assert_contains "--rates 인자" "$out" "500 1000"
assert_not_contains "--rates 가 기본 속도를 대체" "$out" "6000/초"

out="$(RATES="abc" bash loadtest/saturate-k6.sh --dry-run 2>&1)"
assert_eq "잘못된 속도는 실패" "1" "$?"
assert_contains "잘못된 속도 안내" "$out" "속도"

out="$(bash loadtest/saturate-k6.sh --bogus 2>&1)"
assert_eq "모르는 인자는 실패" "2" "$?"
assert_contains "사용법 안내" "$out" "사용:"

out="$(ARRIVAL_SEC=10 bash loadtest/saturate-k6.sh --dry-run --rates "1000" 2>&1)"
assert_contains "ARRIVAL_SEC 반영" "$out" "1000/초 N=10000"

echo "통과 ${PASSES}, 실패 ${FAILS}"
[ "$FAILS" -eq 0 ]
