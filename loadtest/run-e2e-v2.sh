#!/usr/bin/env bash
set -euo pipefail

# 39 설계 전 과정(e2e) 측정용 v2 기동 스크립트. run-v2.sh와 같은 결이되 프로파일만
# udp,loadtest 대신 udp,e2e — ec의 100계좌 시드(account-worker seed-accounts)와 a4의
# 히스토그램 훅 게이트가 이 프로파일에서 켜진다.
cd "$(dirname "$0")/.."

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

start_app() {
    local module="$1" extra_args="$2"
    nohup ./gradlew ":${module}:bootRun" --args="--spring.profiles.active=udp,e2e${extra_args}" \
        > "${LOG_DIR}/${module}.log" 2>&1 &
    echo $! > "${PID_DIR}/${module}.pid"
    echo "${module} 기동 시작(런처 PID $(cat "${PID_DIR}/${module}.pid"), 로그 ${LOG_DIR}/${module}.log)"
}

# 끝점①(주문 접수·예약) 지연 측정 훅은 account-worker(v2의 실제 검증·예약 지점)에만 켠다.
# ⚠️ 절대경로 이유는 run-e2e-v1.sh 참고 — gradlew bootRun의 JVM 작업 디렉터리가 모듈
# 디렉터리라 상대경로는 폴링 스레드가 엉뚱한 곳을 본다.
E2E_RESULTS_ABS="$(pwd)/loadtest/results"
start_app account-worker " --measure.latency.enabled=true --measure.latency.snapshot-trigger-path=${E2E_RESULTS_ABS}/latency-trigger-v2 --measure.latency.output-path=${E2E_RESULTS_ABS}/latency-v2-account-worker.json"
start_app matching-worker ""
start_app api ""

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

await_udp_port 20040 "account-worker account-intake"
await_udp_port 20020 "matching-worker matching-intake"
await_udp_port 20060 "account-worker fill"

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
