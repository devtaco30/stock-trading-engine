#!/usr/bin/env bash
set -euo pipefail

# C7 부하 테스트 하네스 — v2(api·account-worker·matching-worker)를 로컬 백그라운드로 기동한다.
# 접수 경로만 본다 — settlement-worker·account-projection-worker는 이 경로 밖이라 안 띄운다.
# 매칭도 안 일어난다(전부 같은 종목 매수만, 매도 없음) — 순수 접수 처리량 측정.
cd "$(dirname "$0")/.."

LOG_DIR="loadtest/logs"
PID_DIR="loadtest/pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

echo "인프라 기동 중 (postgres:9702, redis:6379, kafka:9092)..."
docker compose -p stock-trading-engine -f docker-compose.yml -f docker-compose.loadtest.yml up -d

TOKEN="loadtest-token-1"
TOKEN_HASH=$(printf '%s' "$TOKEN" | shasum -a 256 | awk '{print $1}')
echo "Redis 토큰 시드: auth:token:${TOKEN_HASH} -> userId 1"
seeded=false
for _ in $(seq 1 30); do
    if docker exec stock-trading-redis redis-cli SET "auth:token:${TOKEN_HASH}" 1 EX 86400 > /dev/null 2>&1; then
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
    local module="$1"
    nohup ./gradlew ":${module}:bootRun" --args='--spring.profiles.active=udp,loadtest' \
        > "${LOG_DIR}/${module}.log" 2>&1 &
    echo $! > "${PID_DIR}/${module}.pid"
    echo "${module} 기동 시작(런처 PID $(cat "${PID_DIR}/${module}.pid"), 로그 ${LOG_DIR}/${module}.log)"
}

start_app account-worker
start_app matching-worker
start_app api

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

echo "READY"
