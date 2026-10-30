#!/usr/bin/env bash
set -euo pipefail

# 39 설계 전 과정(e2e) 측정용 v2 기동 스크립트. run-v2.sh와 같은 결이되 프로파일만
# udp,loadtest 대신 udp,e2e — ec의 100계좌 시드(account-worker seed-accounts)와 a4의
# 히스토그램 훅 게이트가 이 프로파일에서 켜진다.
#
# I8 U3 — WORKER_COUNT=2로 계좌 워커를 둘 띄운다(D5: 새 하네스 안 만들고 기존 걸 확장).
# 기본값(WORKER_COUNT 안 주거나 1)은 기존 동작과 바이트 단위로 동일 — shard-routing 인자를
# 아예 안 붙인다. 슬롯 경계(0~127/128~255)는 LLD D1(256칸 고정) 그대로.
cd "$(dirname "$0")/.."

# 구간 분해 측정(MEASURE_STAGES=1)이면 프로세스마다 --measure.stages.* 인자를 붙인다. 기본은 끔이라 기존 인자와 같다.
# 계좌 워커를 둘 띄우는 WORKER_COUNT=2 분기는 계측 대상이 아니다(묶음 C는 워커 하나).
source loadtest/stage-args.sh
# 매칭 shard 여러 개(MATCHING_COUNT)의 기동 인자 계산과 배정 완료 대기. 기본(1)은 기존 인자와 같다.
source loadtest/matching-plan.sh

WORKER_COUNT="${WORKER_COUNT:-1}"
if [ "$WORKER_COUNT" != "1" ] && [ "$WORKER_COUNT" != "2" ]; then
    echo "WORKER_COUNT는 1 또는 2만 지원한다 (받은 값: ${WORKER_COUNT})" >&2
    exit 1
fi

MATCHING_COUNT="${MATCHING_COUNT:-1}"
matching_count_valid "$MATCHING_COUNT" || exit 1
# 실행 중 증설(MATCHING_EXPAND_AT_SEC)이 있으면 증설할 shard의 주소까지 시작 shard의 호가창 이동 주소 풀에 넣는다(matching_pool_count).
# 증설 자체는 부하가 시작된 뒤 loadtest/expand-matching.sh 가 한다.
MATCHING_POOL_COUNT="$(matching_pool_count "$MATCHING_COUNT")"
if [ -n "${MATCHING_EXPAND_AT_SEC:-}" ]; then
    matching_expansion_valid "$MATCHING_COUNT" "$MATCHING_EXPAND_AT_SEC" || exit 1
fi
# 정할 조건: 배정 완료를 기다리는 제한 시간(초). 값은 아직 정하지 않았다. 매칭 shard가 2개 이상일 때만 필요하다.
if [ "$MATCHING_POOL_COUNT" != "1" ] && [ -z "${MATCHING_ASSIGN_TIMEOUT:-}" ]; then
    echo "MATCHING_COUNT=${MATCHING_COUNT}에는 MATCHING_ASSIGN_TIMEOUT(배정 완료 대기 제한 시간, 초)이 필요하다 — 값은 정해지지 않았다" >&2
    exit 1
fi

LOG_DIR="loadtest/logs"
PID_DIR="loadtest/pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

echo "인프라 기동 중 (postgres:9702, redis:6379, kafka:9092)..."
docker compose -p stock-trading-engine -f docker-compose.yml -f docker-compose.loadtest.yml up -d

# e2e 계좌는 user_id=2로 시드된다(run-e2e-v1.sh와 같은 이유) — v2도 같은 계좌 풀(2001~2100)을
# 쓰므로 토큰을 userId 2로 매핑한다.
TOKEN="e2e-token-1"
TOKEN_HASH=$(printf '%s' "$TOKEN" | shasum -a 256 | awk '{print $1}')
echo "Redis 토큰 시드: auth:token:${TOKEN_HASH} -> userId 2"
seeded=false
for _ in $(seq 1 30); do
    if docker exec stock-trading-redis redis-cli SET "auth:token:${TOKEN_HASH}" 2 EX 86400 > /dev/null 2>&1; then
        seeded=true
        break
    fi
    sleep 1
done
if [ "$seeded" != true ]; then
    echo "Redis에 토큰을 30초 안에 못 심었다 — 컨테이너 상태 확인" >&2
    exit 1
fi

# label=pid/로그 파일명(계좌 워커를 둘 띄우면 module만으로는 구분이 안 된다), module=gradle
# 태스크 타깃(:module:bootRun).
start_app() {
    local label="$1" extra_args="$2" module="$3"
    nohup ./gradlew ":${module}:bootRun" --args="--spring.profiles.active=udp,e2e${extra_args}$(app_log_args)" \
        > "${LOG_DIR}/${label}.log" 2>&1 &
    echo $! > "${PID_DIR}/${label}.pid"
    echo "${label} 기동 시작(런처 PID $(cat "${PID_DIR}/${label}.pid"), 로그 ${LOG_DIR}/${label}.log)"
}

# 매칭 shard 를 MATCHING_COUNT 개 띄운다. $1=shard 공통 추가 인자(단계 계측 등). 1개면 라벨·인자가 기존과 같다.
start_matching_workers() {
    local common_args="$1" index
    for index in $(seq 0 $((MATCHING_COUNT - 1))); do
        start_app "$(matching_label "$MATCHING_COUNT" "$index" "$MATCHING_POOL_COUNT")" "${common_args}$(matching_args "$MATCHING_COUNT" "$index" "$MATCHING_POOL_COUNT")" matching-worker
    done
}

# DB 를 쓰는 v2 워커(settlement-worker, account-projection-worker)용. 두 모듈에는 udp·e2e 프로파일이 없고(프로파일을 붙여도 아무 설정도
# 바뀌지 않는다) Postgres 접속은 application-local.yml(gitignore, Jack 관리)에 있다. 그 파일을 만들지 않고 api 의 e2e 설정과 같은
# 접속 값을 명령행 인자로 준다. account-projection-worker 는 이 인자가 없으면 인메모리 H2 에 경고 없이 기록하므로,
# 하네스가 준비 단계(await_v2_workers)에서 Postgres 에 테이블이 생겼는지 확인한다.
WORKER_DB_ARGS=" --spring.datasource.url=jdbc:postgresql://localhost:9702/stock_trading --spring.datasource.username=postgres --spring.datasource.password=postgres --spring.datasource.driver-class-name=org.postgresql.Driver --spring.jpa.database-platform=org.hibernate.dialect.PostgreSQLDialect"
start_worker() {
    local label="$1" module="$2" extra_args="$3"
    nohup ./gradlew ":${module}:bootRun" --args="${WORKER_DB_ARGS}${extra_args}$(app_log_args)" \
        > "${LOG_DIR}/${label}.log" 2>&1 &
    echo $! > "${PID_DIR}/${label}.pid"
    echo "${label} 기동 시작(런처 PID $(cat "${PID_DIR}/${label}.pid"), 로그 ${LOG_DIR}/${label}.log)"
}

# I8 U3 — api·계좌워커 공용(담당 슬롯 판정은 계좌워커가 자기 인테이크 채널과 대조해 정한다,
# D2) / 매칭워커 전용(체결 fan-out 목적지) 두 갈래. 슬롯 0~127=워커A, 128~255=워커B.
shard_args_intake() {
    printf -- ' --shard-routing.slot-count=256 --shard-routing.shards[0].endpoint=aeron:udp?endpoint=localhost:20040 --shard-routing.shards[0].slot-from=0 --shard-routing.shards[0].slot-to=127 --shard-routing.shards[1].endpoint=aeron:udp?endpoint=localhost:20041 --shard-routing.shards[1].slot-from=128 --shard-routing.shards[1].slot-to=255'
}
shard_args_fill() {
    printf -- ' --shard-routing.slot-count=256 --shard-routing.shards[0].endpoint=aeron:udp?endpoint=localhost:20060 --shard-routing.shards[0].slot-from=0 --shard-routing.shards[0].slot-to=127 --shard-routing.shards[1].endpoint=aeron:udp?endpoint=localhost:20061 --shard-routing.shards[1].slot-from=128 --shard-routing.shards[1].slot-to=255'
}

# 끝점①(주문 접수·예약) 지연 측정 훅은 account-worker(v2의 실제 검증·예약 지점)에만 켠다.
# ⚠️ 절대경로 이유는 run-e2e-v1.sh 참고 — gradlew bootRun의 JVM 작업 디렉터리가 모듈
# 디렉터리라 상대경로는 폴링 스레드가 엉뚱한 곳을 본다.
E2E_RESULTS_ABS="$(pwd)/loadtest/results"

# 매칭 shard 가 둘 이상이면 앱을 띄우기 전에 맵 토픽을 지워, 직전 실행이 남긴 값이 「배정 완료」로 읽히지 않게 한다.
if [ "$MATCHING_POOL_COUNT" != "1" ]; then
    reset_matching_map_topic || exit 1
fi

if [ "$WORKER_COUNT" = "2" ]; then
    start_app account-worker-a " --transport.account-intake.channel=aeron:udp?endpoint=localhost:20040 --transport.fill.channel=aeron:udp?endpoint=localhost:20060 --account.worker.archive.control-channel=aeron:udp?endpoint=localhost:8010$(shard_args_intake) --measure.latency.enabled=true --measure.latency.snapshot-trigger-path=${E2E_RESULTS_ABS}/latency-trigger-v2-a --measure.latency.output-path=${E2E_RESULTS_ABS}/latency-v2-account-worker-a.json" account-worker
    start_app account-worker-b " --transport.account-intake.channel=aeron:udp?endpoint=localhost:20041 --transport.fill.channel=aeron:udp?endpoint=localhost:20061 --account.worker.archive.control-channel=aeron:udp?endpoint=localhost:8012$(shard_args_intake) --measure.latency.enabled=true --measure.latency.snapshot-trigger-path=${E2E_RESULTS_ABS}/latency-trigger-v2-b --measure.latency.output-path=${E2E_RESULTS_ABS}/latency-v2-account-worker-b.json" account-worker
    start_matching_workers "$(shard_args_fill)"
    start_app api "$(shard_args_intake)" api
else
    start_app account-worker " --measure.latency.enabled=true --measure.latency.snapshot-trigger-path=${E2E_RESULTS_ABS}/latency-trigger-v2 --measure.latency.output-path=${E2E_RESULTS_ABS}/latency-v2-account-worker.json$(stage_args v2 account-worker)" account-worker
    start_matching_workers "$(stage_args v2 matching-worker)"
    start_app api "$(stage_args v2 api)" api
    start_worker settlement-worker settlement-worker ""
    start_worker account-projection-worker account-projection-worker "$(stage_args v2 account-projection-worker)"
fi

await_udp_port() {
    local port="$1"
    local name="$2"
    echo -n "포트 ${port}(${name}) 바인딩 대기 중"
    for _ in $(seq 1 120); do
        if lsof -nP -iUDP:"${port}" > /dev/null 2>&1; then
            echo " OK"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    echo " TIMEOUT"
    echo "${name}(포트 ${port})이 120초 안에 안 열렸다 — ${LOG_DIR}/*.log 확인" >&2
    exit 1
}

await_udp_port 20040 "account-worker(A) account-intake"
for matching_port in $(matching_intake_ports "$MATCHING_COUNT"); do
    await_udp_port "$matching_port" "matching-worker matching-intake"
done
await_udp_port 20060 "account-worker(A) fill"
if [ "$WORKER_COUNT" = "2" ]; then
    await_udp_port 20041 "account-worker(B) account-intake"
    await_udp_port 20061 "account-worker(B) fill"
fi

# 매칭 shard 가 둘 이상이면 종목 slot 64개의 주소가 맵 토픽에 모두 올라온 뒤에 READY 를 낸다(부하는 READY 뒤에 시작한다).
# 제한 시간을 넘기면 주인 없는 종목 slot 번호를 출력하고 끝낸다.
if [ "$MATCHING_POOL_COUNT" != "1" ]; then
    await_matching_slots_assigned "${MATCHING_STOCK_SLOT_COUNT:-64}" "$MATCHING_ASSIGN_TIMEOUT" "$MATCHING_COUNT" || exit 1
fi

echo -n "api HTTP(8080) 대기 중"
for _ in $(seq 1 120); do
    if lsof -nP -iTCP:8080 -sTCP:LISTEN > /dev/null 2>&1; then
        echo " OK"
        echo "READY"
        exit 0
    fi
    echo -n "."
    sleep 1
done
echo " TIMEOUT"
echo "api(8080)가 120초 안에 안 열렸다 — ${LOG_DIR}/api.log 확인" >&2
exit 1
