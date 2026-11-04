#!/usr/bin/env bash
# start_matching_only.sh 의 시험. 가짜 docker·java·lsof 로 돈다. 실제 JVM·컨테이너·gradle 은 쓰지 않는다.
# 실행: 저장소 루트(loadtest/s0b/lib.sh 가 있는 트리)에서 bash loadtest/s0b_multi/test_start_matching_only.sh
cd "$(dirname "$0")/../.."
ROOT="$(pwd)"
PASS=0; FAIL=0
check() { if printf '%s' "$3" | grep -qF -- "$2"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); echo "FAIL $1: [$2] 없음 (실제: $3)"; fi; }
check_not() { if printf '%s' "$3" | grep -qF -- "$2"; then FAIL=$((FAIL+1)); echo "FAIL $1: [$2] 있으면 안 됨 (실제: $3)"; else PASS=$((PASS+1)); fi; }
equals() { if [ "$2" = "$3" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); echo "FAIL $1: 기대 [$2] 실제 [$3]"; fi; }

T=$(mktemp -d); FAKEBIN=$T/bin; mkdir -p "$FAKEBIN"
# 가짜 docker: 배정 표(FAKE_N개 주소)를 console-consumer 출력으로 준다. FAKE_MOVING=1이면 slot 3을 ;moving 으로 둔다.
cat > "$FAKEBIN/docker" <<'D'
#!/usr/bin/env bash
echo "docker $*" >> "$FAKE_LOG"
case "$*" in
  *kafka-console-consumer*)
    n=$(cat "$FAKE_LOG.cnt" 2>/dev/null || echo 0); n=$((n + 1)); echo $n > "$FAKE_LOG.cnt"
    [ "${FAKE_WARN:-0}" = 1 ] && echo "Option --property is deprecated and will be removed in a future version. Use --formatter-property instead."
    for s in $(seq 0 63); do
      v="localhost:$((20020 + s % FAKE_N))"
      if [ "$s" = 3 ] && [ -n "${FAKE_BAD_AFTER:-}" ] && [ "$n" -ge "$FAKE_BAD_AFTER" ]; then
        case "${FAKE_BAD:-moving}" in moving) v="$v;moving" ;; null) v=null ;; empty) v="" ;; esac
      fi
      printf '%s\t%s\n' "$s" "$v"
    done ;;
esac
exit 0
D
cat > "$FAKEBIN/java" <<'J'
#!/usr/bin/env bash
echo "java $*" >> "$FAKE_LOG"
echo "Started MatchingWorkerApplication in 1 seconds"
[ "${FAKE_DOUBLE_STARTED:-0}" = 1 ] && echo "Started MatchingWorkerApplication in 2 seconds"
exec sleep 60
J
printf '#!/usr/bin/env bash\nexit 1\n' > "$FAKEBIN/lsof"
# 가짜 종목 선택기: 몫이 한쪽으로 치우친 결과를 낸다(최대-최소 = 5)
cat > "$T/skewed_select.py" <<'P'
print("STOCKS=000001 000002")
print("SLOT 000001 0 localhost:20020")
print("SHARD_STOCKS localhost:20020=10")
print("SHARD_STOCKS localhost:20021=5")
P
chmod +x "$FAKEBIN"/*
mkdir -p matching-worker/build/libs account-worker/build/libs
JARS_MADE=""
[ -e matching-worker/build/libs/fake.jar ] || { : > matching-worker/build/libs/fake.jar; JARS_MADE=1; }
[ -e account-worker/build/libs/fake.jar ] || : > account-worker/build/libs/fake.jar

run() { # N, 추가 환경 → OUT 폴더 경로를 R_OUT, 출력을 R_TEXT, 종료 코드를 R_RC
  R_OUT=$(mktemp -d "$T/out.XXXX"); FAKE_LOG=$R_OUT/fake.log
  R_TEXT=$(env PATH="$FAKEBIN:$PATH" FAKE_LOG="$FAKE_LOG" FAKE_N="$1" S0B_WORK="$T/work" OUT="$R_OUT" SKIP_BUILD=1 \
    S0B_SLEEP_AFTER_START=0 S0B_POLL_SEC=0 S0B_KILL_WAIT=0 MATCHING_ASSIGN_POLL_SEC=0 "${@:2}" \
    bash loadtest/s0b_multi/start_matching_only.sh 2>&1); R_RC=$?
}

run 2 MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=5
equals "N=2 종료 코드" "0" "$R_RC"
check "N=2 PASS" "PASS slots=64 addresses=2" "$(cat $R_OUT/result.txt)"
equals "N=2 배정 표 64줄" "64" "$(wc -l < $R_OUT/assigned-map.txt | tr -d ' ')"
check "N=2 주소별 개수" "per-address 32 localhost:20020" "$(cat $R_OUT/assigned-summary.txt)"
check "N=2 shard 몫 15" "SHARD_STOCKS localhost:20021=15" "$(cat $R_OUT/shard-stocks.txt)"
equals "N=2 JVM 2개 기동" "2" "$(grep -c '^java ' $R_OUT/fake.log)"
equals "N=2 Started 줄 2개" "2" "$(wc -l < $R_OUT/started-lines.txt | tr -d ' ')"
check "N=2 cmdline 기록" "matching-1" "$(cat $R_OUT/matching-cmdline.txt)"
check "N=2 종료 시 컨테이너 제거" "docker rm -f s0b-kafka" "$(cat $R_OUT/fake.log)"
check "N=2 logs 복사" "matching-0.log" "$(ls $R_OUT/logs)"

run 4 MATCHING_COUNT=4 MATCHING_ASSIGN_TIMEOUT=5
check "N=4 PASS" "PASS slots=64 addresses=4" "$(cat $R_OUT/result.txt)"
check "N=4 몫 8·8·7·7" "SHARD_STOCKS localhost:20023=7" "$(cat $R_OUT/shard-stocks.txt)"

run 1 MATCHING_COUNT=1
check "N=1 PASS(종목 선택 생략)" "PASS slots=64 addresses=1 pick=skipped" "$(cat $R_OUT/result.txt)"
check_not "N=1 추가 인자 없음" "instance-id" "$(grep '^java ' $R_OUT/fake.log)"
check_not "N=1 종목 파일 없음" "slots.txt" "$(ls $R_OUT)"

run 1 MATCHING_COUNT=1 BALANCED_STOCKS=1
check "N=1 균등 선택" "pick=0" "$(cat $R_OUT/result.txt)"

run 2 MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=2 FAKE_BAD_AFTER=1 FAKE_BAD=moving
check "배정 안 끝나면 FAIL" "FAIL" "$(cat $R_OUT/result.txt)"
equals "배정 안 끝나면 종료 코드 6" "6" "$R_RC"

run 2 MATCHING_COUNT=2
equals "N=2 제한 시간 없으면 거부" "1" "$R_RC"

run 2 MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=5 HOLD_SEC=1 CPU_SAMPLE=1
check "cpu.log 역할 이름" " matching-0 " "$(cat $R_OUT/cpu.log 2>/dev/null)"
check "rss 기록" "matching-1 pid=" "$(cat $R_OUT/rss.txt)"

# --- 보강한 PASS 조건 ---
# 1) 원본 덤프에서 slot별 마지막 값이 ;moving·null·빈 값이면 FAIL (정리된 assigned-map.txt는 이미 걸러져 있어 원본으로 본다)
for bad in moving null empty; do
  run 2 MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=5 FAKE_BAD_AFTER=3 FAKE_BAD=$bad
  check "원본 덤프 $bad → FAIL" "FAIL raw-map-bad=1" "$(cat $R_OUT/result.txt)"
  equals "원본 덤프 $bad → 종료 코드 1" "1" "$R_RC"
done
run 2 MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=5 FAKE_BAD_AFTER=2 FAKE_BAD=moving
check "정리된 표에서 slot이 빠져도 FAIL" "FAIL" "$(cat $R_OUT/result.txt)"
run 2 MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=5
check "정상 입력은 raw-map-bad=0" "raw-map-bad=0" "$(cat $R_OUT/result.txt)"
check "정상 입력 assigned-map 에 moving 없음" "bad=0" "$(awk -F'\t' '$2==""||$2=="null"||$2~/;moving$/{b++} END{print "bad=" b+0}' $R_OUT/assigned-map.txt)"
# 1-1) Kafka 콘솔 경고 줄이 stdout 맨 앞에 섞여도 slot 줄만 본다. 숫자가 아닌 줄 수는 assigned-summary.txt 에 남긴다
run 2 MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=5 FAKE_WARN=1
check "경고 줄이 섞인 정상 덤프 → PASS" "PASS" "$(cat $R_OUT/result.txt)"
check "경고 줄이 섞여도 raw-map-bad=0" "raw-map-bad=0" "$(cat $R_OUT/result.txt)"
check "경고 줄 수 기록" "raw-nonslot-lines=1" "$(cat $R_OUT/assigned-summary.txt)"
equals "경고 줄이 섞인 원본은 65줄" "65" "$(wc -l < $R_OUT/assigned-map-raw.txt | tr -d ' ')"
check "경고 줄이 있어도 종목 선택 성공" "pick=0" "$(cat $R_OUT/result.txt)"
run 2 MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=5 FAKE_WARN=1 FAKE_BAD_AFTER=3 FAKE_BAD=moving
check "경고 줄 + slot 3 ;moving → FAIL" "FAIL raw-map-bad=1" "$(cat $R_OUT/result.txt)"
run 2 MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=5
check "경고 줄이 없으면 raw-nonslot-lines=0" "raw-nonslot-lines=0" "$(cat $R_OUT/assigned-summary.txt)"
run 1 MATCHING_COUNT=1 FAKE_WARN=1
check "N=1 경고 줄 → PASS" "PASS slots=64 addresses=1" "$(cat $R_OUT/result.txt)"
# 2) started-lines.txt 줄 수 == MATCHING_COUNT
run 2 MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=5 FAKE_DOUBLE_STARTED=1
check "Started 줄이 더 많으면 FAIL" "FAIL started-lines=4 expected=2" "$(cat $R_OUT/result.txt)"
run 2 MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=5
check "Started 줄 수가 같으면 PASS" "started-lines=2" "$(cat $R_OUT/result.txt)"
# 3) shard별 종목 수 최대-최소 <= 1
run 2 MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=5 S0B_SELECT_TOOL="$T/skewed_select.py"
check "몫이 치우치면 FAIL" "FAIL shard-stocks spread=5" "$(cat $R_OUT/result.txt)"
run 8 MATCHING_COUNT=8 MATCHING_ASSIGN_TIMEOUT=5
check "N=8 몫 4×6+3×2 는 PASS" "PASS" "$(cat $R_OUT/result.txt)"
check "N=8 spread=1" "spread=1" "$(cat $R_OUT/result.txt)"
check "N=8 shard 4개 여섯" "6" "$(grep -c '=4$' $R_OUT/shard-stocks.txt)"
run 4 MATCHING_COUNT=4 MATCHING_ASSIGN_TIMEOUT=5
check "N=4 spread=1" "spread=1" "$(cat $R_OUT/result.txt)"
run 2 MATCHING_COUNT=2 MATCHING_ASSIGN_TIMEOUT=5
check "N=2 spread=0" "spread=0" "$(cat $R_OUT/result.txt)"

[ -n "$JARS_MADE" ] && rm -f matching-worker/build/libs/fake.jar account-worker/build/libs/fake.jar
rm -rf "$T"
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = 0 ]
