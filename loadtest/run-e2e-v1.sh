#!/usr/bin/env bash
set -euo pipefail

# 39 설계(decision_records/v1-v2-e2e-measurement.md) 전 과정(e2e) 측정용 v1 기동 스크립트.
# run-v1.sh와 같은 결이되 프로파일만 loadtest 대신 e2e — ec가 만드는 100계좌·30종목
# 시드(application-e2e.yml 계열)와 a4가 만드는 히스토그램 훅 게이트가 이 프로파일에서 켜진다.
cd "$(dirname "$0")/.."

LOG_DIR="loadtest/logs"
PID_DIR="loadtest/pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

echo "인프라 기동 중 (postgres:9702, redis:6379, kafka:9092)..."
docker compose -p stock-trading-engine -f docker-compose.yml -f docker-compose.loadtest.yml up -d

# e2e-seed.sql이 계좌를 user_id=2로 시드한다(기존 loadtest-seed.sql의 user_id=1과 겹치지
# 않게) — 그래서 토큰도 userId 2로 매핑해야 AccountAccessResolver의 소유 검증을 통과한다.
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

start_app() {
    local module="$1" extra_args="$2"
    nohup ./gradlew ":${module}:bootRun" --args="--spring.profiles.active=e2e${extra_args}" \
        > "${LOG_DIR}/${module}.log" 2>&1 &
    echo $! > "${PID_DIR}/${module}.pid"
    echo "${module} 기동 시작(런처 PID $(cat "${PID_DIR}/${module}.pid"), 로그 ${LOG_DIR}/${module}.log)"
}

# 끝점①(주문 접수·예약) 지연 측정 훅은 order-engine(v1의 실제 검증·저장 지점)에만 켠다.
# 트리거 파일로 measure-e2e.sh가 패스 경계마다 스냅샷+리셋을 요청한다.
# ⚠️ 경로는 절대경로로 — gradlew bootRun의 JVM 작업 디렉터리는 리포 루트가 아니라 모듈
# 디렉터리(예: order-engine/)라, 상대경로를 쓰면 폴링 스레드가 엉뚱한 곳을 본다(실측정 확인,
# 2026-09-16 — 리포 루트에 트리거 파일을 놔도 계속 안 읽혀서 모듈 서브디렉터리에 생긴 걸 발견).
E2E_RESULTS_ABS="$(pwd)/loadtest/results"
start_app order-engine " --measure.latency.enabled=true --measure.latency.snapshot-trigger-path=${E2E_RESULTS_ABS}/latency-trigger-v1 --measure.latency.output-path=${E2E_RESULTS_ABS}/latency-v1-order-engine.json"
start_app matching-engine ""
start_app settlement-engine ""
start_app api ""

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
