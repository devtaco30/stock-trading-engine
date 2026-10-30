#!/usr/bin/env bash
set -uo pipefail

# 실행 중 증설(H9)의 매칭 쪽. MATCHING_COUNT 개로 시작해 부하를 건 뒤, MATCHING_EXPAND_AT_SEC 초 뒤에 matching-<MATCHING_COUNT> 를 같은 방식으로 띄운다.
# 사전 조건: run-e2e-v2.sh 를 같은 MATCHING_COUNT·MATCHING_EXPAND_AT_SEC 로 실행해 시작 shard 들의 호가창 이동 주소 풀에 새 shard 의 주소가 이미 들어 있어야 한다.
# 부하(k6)를 시작한 직후 이 스크립트를 부른다. 값(증설 시각, 제한 시간)은 정해지지 않았으므로 환경 변수로 받는다.
#   MATCHING_COUNT              시작 shard 수(1~7)
#   MATCHING_EXPAND_AT_SEC      부하 시작 뒤 증설까지 기다리는 초
#   MATCHING_ASSIGN_TIMEOUT     새 shard 의 주소가 맵에 올라오기를 기다리는 제한 시간(초)
# 기록(loadtest/logs/expansion.txt, epoch ms):
#   EXPANSION_LAUNCH_COMMAND_AT_MS          gradlew 를 띄우기 직전 시각. JVM 이 뜬 시각이 아니다.
#   EXPANSION_ADDRESS_FIRST_RECORD_AT_MS    새 shard 의 주소가 평상시 값으로 맵 토픽에 처음 쓰인 레코드의 시각.
#   EXPANSION_ADDRESS_OBSERVED_AT_MS        이 스크립트가 그것을 확인한 시각(수 초 늦다).
# 두 번째 시각은 「증설 완료」가 아니다. 새 shard 가 받는 모든 종목 slot 의 호가창 이동이 끝난 시각은 따로 재야 한다.
cd "$(dirname "$0")/.."
source loadtest/matching-plan.sh

MATCHING_COUNT="${MATCHING_COUNT:?MATCHING_COUNT(시작 shard 수)가 필요하다}"
MATCHING_EXPAND_AT_SEC="${MATCHING_EXPAND_AT_SEC:?MATCHING_EXPAND_AT_SEC(증설 시각, 초)가 필요하다}"
MATCHING_ASSIGN_TIMEOUT="${MATCHING_ASSIGN_TIMEOUT:?MATCHING_ASSIGN_TIMEOUT(제한 시간, 초)이 필요하다}"
matching_expansion_valid "$MATCHING_COUNT" "$MATCHING_EXPAND_AT_SEC" || exit 1

LOG_DIR="loadtest/logs"
PID_DIR="loadtest/pids"
mkdir -p "$LOG_DIR" "$PID_DIR"
NEW_INDEX="$MATCHING_COUNT"
NEW_LABEL="$(matching_label "$((MATCHING_COUNT + 1))" "$NEW_INDEX")"
NEW_ENDPOINT="$(matching_intake_endpoint "$NEW_INDEX")"

sleep "$MATCHING_EXPAND_AT_SEC"
STARTED_AT_MS="$(python3 -I -c 'import time; print(int(time.time()*1000))')"
nohup ./gradlew ":matching-worker:bootRun" --args="--spring.profiles.active=udp,e2e$(matching_args "$((MATCHING_COUNT + 1))" "$MATCHING_COUNT")" \
    > "${LOG_DIR}/${NEW_LABEL}.log" 2>&1 &
echo $! > "${PID_DIR}/${NEW_LABEL}.pid"
echo "${NEW_LABEL} 기동 시작(런처 PID $(cat "${PID_DIR}/${NEW_LABEL}.pid"), 로그 ${LOG_DIR}/${NEW_LABEL}.log)"
{
    echo "EXPANSION_LAUNCH_COMMAND_AT_MS=${STARTED_AT_MS}"
    echo "EXPANSION_SHARD=${NEW_LABEL} ${NEW_ENDPOINT}"
} > "${LOG_DIR}/expansion.txt"

await_matching_shard_address "$NEW_ENDPOINT" "$MATCHING_ASSIGN_TIMEOUT" | tee -a "${LOG_DIR}/expansion.txt"
exit "${PIPESTATUS[0]}"
