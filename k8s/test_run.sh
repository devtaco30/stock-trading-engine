#!/usr/bin/env bash
# k8s/run.sh의 시작 전 검사와 수명 만료 삭제 시험(작업 3.6, 4.1). 실행: bash k8s/test_run.sh (워크트리 루트에서)
# kind 클러스터 ste가 떠 있어야 한다. 시험용 네임스페이스 ste-test-run을 만들었다 지운다. 수명 만료 시험 때문에 3분쯤 걸린다.
# SKIP_LIFETIME=1이면 수명 만료 시험(2분 대기)을 건너뛴다.
cd "$(dirname "$0")/.."
PASS=0; FAIL=0
check() { if printf '%s' "$3" | grep -qF -- "$2"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); echo "FAIL $1: [$2] 가 없다"; fi; }
check_not() { if printf '%s' "$3" | grep -qF -- "$2"; then FAIL=$((FAIL+1)); echo "FAIL $1: [$2] 가 있으면 안 된다"; else PASS=$((PASS+1)); fi; }
KCTX=${KCTX:-kind-ste}
TNS=ste-test-run
T=$(mktemp -d); trap 'rm -rf "$T"; kubectl --context "$KCTX" delete ns "$TNS" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT
ns_exists() { kubectl --context "$KCTX" get ns "$TNS" >/dev/null 2>&1 && echo yes || echo no; }
kubectl --context "$KCTX" delete ns "$TNS" --ignore-not-found --wait=true >/dev/null 2>&1

# 1) 수명 인자가 없으면 아무것도 만들지 않고 멈춘다
OUT=$(NS=$TNS bash k8s/run.sh 2>&1); RC=$?
check "수명 없음: 종료 코드 1" "rc=1;" "rc=$RC;"
check "수명 없음: 사용법" "사용:" "$OUT"
check "수명 없음: 네임스페이스 없음" "ns=no" "ns=$(ns_exists)"

# 2) 수명이 양의 정수가 아니면 멈춘다
OUT=$(NS=$TNS bash k8s/run.sh abc "$T/r2" 2>&1); RC=$?
check "수명 abc: 종료 코드 1" "rc=1;" "rc=$RC;"
check "수명 abc: 네임스페이스 없음" "ns=no" "ns=$(ns_exists)"
OUT=$(NS=$TNS bash k8s/run.sh 0 "$T/r2" 2>&1); RC=$?
check "수명 0: 종료 코드 1" "rc=1;" "rc=$RC;"

# 3) 한도를 뺀 매니페스트면 시작 전에 멈추고 컨테이너 이름을 출력한다
mkdir -p "$T/m3"; cp k8s/*.yaml "$T/m3/"
python3 -I - "$T/m3/account-statefulset.yaml" <<'EOF'
import re, sys
p = sys.argv[1]
s = open(p, encoding="utf-8").read()
# account-worker 컨테이너(limits cpu "2", init 컨테이너는 100m)의 limits에서 memory만 뺀다. 메모리 값이 바뀌어도 시험이 그대로 돌게 정규식으로 찾는다.
s, n = re.subn(r'(\n {12}limits: \{ cpu: "2"), memory: "[^"]+" \}', r'\1 }', s, count=1)
if n != 1:
    sys.exit("시험 준비 실패: account-statefulset.yaml에서 limits 줄을 찾지 못했다")
open(p, "w", encoding="utf-8").write(s)
EOF
[ $? = 0 ] || { echo "FAIL 시험 준비(한도 줄 지우기)"; exit 1; }
OUT=$(NS=$TNS MANIFESTS_DIR="$T/m3" bash k8s/run.sh 2 "$T/r3" 2>&1); RC=$?
check "한도 누락: 종료 코드 2" "rc=2;" "rc=$RC;"
check "한도 누락: 컨테이너 이름" "account-worker limits.memory" "$OUT"
check "한도 누락: 네임스페이스 없음" "ns=no" "ns=$(ns_exists)"

# 4) 이미지가 노드에 없으면 파드를 띄우기 전에 멈추고 이미지 이름을 출력한다
mkdir -p "$T/m4"; cp k8s/*.yaml "$T/m4/"
sed -i '' 's#stock-trading-engine/matching-worker:local#stock-trading-engine/no-such-image:local#' "$T/m4/matching-statefulset.yaml"
OUT=$(NS=$TNS MANIFESTS_DIR="$T/m4" bash k8s/run.sh 2 "$T/r4" 2>&1); RC=$?
check "이미지 없음: 종료 코드 3" "rc=3;" "rc=$RC;"
check "이미지 없음: 이미지 이름" "MISSING_IMAGE stock-trading-engine/no-such-image:local" "$OUT"
check_not "이미지 없음: 있는 이미지는 안 나온다" "MISSING_IMAGE stock-trading-engine/account-worker:local" "$OUT"
check "이미지 없음: 네임스페이스 없음" "ns=no" "ns=$(ns_exists)"

# 5) 수명 2분: 네임스페이스를 만들고, 2분 뒤 지우고, 지운 시각을 결과 폴더에 남긴다
if [ "${SKIP_LIFETIME:-0}" != 1 ]; then
  START=$(date +%s)
  OUT=$(NS=$TNS SETUP_ONLY=1 bash k8s/run.sh 2 "$T/r5" 2>&1); RC=$?
  check "수명 2분: 종료 코드 0" "rc=0;" "rc=$RC;"
  check "수명 2분: 만든 직후 네임스페이스 있음" "ns=yes" "ns=$(ns_exists)"
  sleep 100
  check "수명 2분: 100초 뒤에는 아직 있음" "ns=yes" "ns=$(ns_exists)"
  for _ in $(seq 1 60); do [ "$(ns_exists)" = no ] && break; sleep 2; done
  GONE=$(date +%s)
  check "수명 2분: 만료 뒤 네임스페이스 없음" "ns=no" "ns=$(ns_exists)"
  ELAPSED=$((GONE - START))
  [ "$ELAPSED" -ge 120 ] && [ "$ELAPSED" -le 200 ] && PASS=$((PASS+1)) || { FAIL=$((FAIL+1)); echo "FAIL 수명 2분: 사라진 시각이 120~200초 밖이다: ${ELAPSED}초"; }
  for _ in $(seq 1 15); do [ -s "$T/r5/namespace-deleted.txt" ] && break; sleep 1; done
  DEL=$(cat "$T/r5/namespace-deleted.txt" 2>/dev/null)
  check "수명 2분: 삭제 기록" "DELETED_BY=lifetime" "$DEL"
  check "수명 2분: 삭제 시각" "deleted_at=" "$DEL"
  check "수명 2분: 생성 기록" "created_at=" "$(cat "$T/r5/namespace-created.txt" 2>/dev/null)"
fi

echo "통과 $PASS, 실패 $FAIL"
[ "$FAIL" = 0 ]
