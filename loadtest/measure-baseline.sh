#!/usr/bin/env bash
set -uo pipefail

# 기준선 재측정(묶음 B 단계 1) 오케스트레이터. 설계서 lld-B-measurement.md 2-4절(점검), 2-6절(조건별 준비·회차 절차).
# 제외 규칙과 임계값은 이 스크립트에 없다. loadtest/measure-protocol-b0.md 와 analyze-baseline.py 에만 있다.
#
# 사용:
#   PREFLIGHT_ONLY=1 loadtest/measure-baseline.sh     점검 1~8만 읽기 전용으로 실행하고 끝낸다. 앱·Docker·k6를 띄우지 않는다
#   loadtest/measure-baseline.sh                      점검 통과 후 조건별로 앱을 기동하고 11회씩 측정한다 (약 1시간)
#
# 환경 변수 (기본값). ★는 설계서에 없어서 이 스크립트에서 새로 정한 이름이다.
#   ROUND_GAP_SEC=30          회차 사이 대기 초 (설계서 편차 1)
#   INCLUDE_KAFKA_UI=0        1이면 kafka-ui도 유지한다. 0이면 postgres redis kafka만 쓴다 (아래 주의 참고)
#   CONDITIONS="v1 v2"        조건 실행 순서
#   PREFLIGHT_ONLY=0          1이면 점검만 실행
#   ★RUN_ID=b0-<UTC yyyymmdd-HHMM>   결과 디렉터리 이름 (loadtest/results/<RUN_ID>/)
#   ★RATE=192, N=10000, ROUNDS=11     k6 도착률, 회차당 주문 수, 회차 수
#   ★V1_LISTENER_CONCURRENCY=9        v1 order-engine 컨슈머 수. run-e2e-v1.sh 로 넘기며 기본 3 인 그 스크립트와 달리 여기서는 9
#   ★V1_PARTITIONS=$V1_LISTENER_CONCURRENCY   order-requests 토픽 파티션 수
#   ★V2_RESET_PARTITIONS=3            v2 준비에서 reset-e2e-v1-state.sh 에 넘기는 파티션 수 (DB 행 정리가 목적)
#   ★DRAIN_TIMEOUT_V1=60, DRAIN_WAIT_V2=10   회차 후 배수 대기 초 (v1: DB 증가분이 N이 될 때까지 최대, v2: 고정)
#   ★ASSIGN_TIMEOUT=120               v1 파티션 배정 확인 최대 초
#   ★E6_PATTERN='...'                 파티션 재배정 로그를 찾는 정규식. 실제 문구는 리허설에서 확인해 고정한다
#   ★CAFFEINATE=1                     1이면 측정 동안 절전을 막는다 (caffeinate -i)
#
# 주의: run-e2e-v1.sh / run-e2e-v2.sh 는 `docker compose up -d` 를 서비스 이름 없이 부르므로 kafka-ui 도 같이 올라온다.
#       INCLUDE_KAFKA_UI=0 이면 앱이 READY 가 된 뒤 kafka-ui 컨테이너를 멈춘다 (그 스크립트들을 고치지 않기 위한 선택).

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

ROUND_GAP_SEC="${ROUND_GAP_SEC:-30}"
INCLUDE_KAFKA_UI="${INCLUDE_KAFKA_UI:-0}"
CONDITIONS="${CONDITIONS:-v1 v2}"
PREFLIGHT_ONLY="${PREFLIGHT_ONLY:-0}"
RATE="${RATE:-192}"
N="${N:-10000}"
ROUNDS="${ROUNDS:-11}"
V1_LISTENER_CONCURRENCY="${V1_LISTENER_CONCURRENCY:-9}"
V1_PARTITIONS="${V1_PARTITIONS:-$V1_LISTENER_CONCURRENCY}"
V2_RESET_PARTITIONS="${V2_RESET_PARTITIONS:-3}"
DRAIN_TIMEOUT_V1="${DRAIN_TIMEOUT_V1:-60}"
DRAIN_WAIT_V2="${DRAIN_WAIT_V2:-10}"
ASSIGN_TIMEOUT="${ASSIGN_TIMEOUT:-120}"
E6_PATTERN="${E6_PATTERN:-partitions (revoked|assigned)|Revoking previously assigned|\\(Re-\\)joining group|rebalanc}"
CAFFEINATE="${CAFFEINATE:-1}"
export V1_LISTENER_CONCURRENCY

RUN_ID="${RUN_ID:-b0-$(date -u +%Y%m%d-%H%M)}"
RESULTS_DIR="${REPO_ROOT}/loadtest/results"
RUN_DIR="${RESULTS_DIR}/${RUN_ID}"
INPUT_FILE="${RESULTS_DIR}/e2e-input.jsonl"
SEED_YML="${REPO_ROOT}/account-worker/src/main/resources/application-e2e.yml"
PROTOCOL_FILE="loadtest/measure-protocol-b0.md"
COMPOSE=(docker compose -p stock-trading-engine -f docker-compose.yml -f docker-compose.loadtest.yml)

PREFLIGHT_PATTERN_APPS='StockTradingEngineApplication|OrderEngineApplication|MatchingApplication|SettlementEngineApplication|AccountWorkerApplication|MatchingWorkerApplication|AccountProjection|SettlementWorker'

PREFLIGHT_FAILURES=0

main_class_pattern_for() {
    case "$1" in
        v1) echo 'StockTradingEngineApplication|OrderEngineApplication|matching\.MatchingApplication|SettlementEngineApplication' ;;
        v2) echo 'StockTradingEngineApplication|AccountWorkerApplication|MatchingWorkerApplication' ;;
    esac
}

# ---------------------------------------------------------------- 점검 (2-4절, 읽기 전용)

section() {
    echo
    echo "=== $1"
}

fail() {
    echo "[걸림] $1"
    PREFLIGHT_FAILURES=$((PREFLIGHT_FAILURES + 1))
}

note() {
    echo "[기록] $1"
}

# 점검 1: 다른 세션·워킹트리의 앱·부하 테스트 (stop 스크립트의 pkill -f 가 이들도 죽인다)
preflight_processes() {
    section "점검 1: 떠 있는 앱·부하 테스트"
    local apps k6s daemons
    apps="$(pgrep -fl "$PREFLIGHT_PATTERN_APPS" || true)"
    k6s="$(pgrep -fl 'k6|bootRun|GradleWrapperMain' || true)"
    daemons="$(pgrep -fl GradleDaemon || true)"
    if [ -n "$apps" ]; then
        echo "$apps"
        fail "앱 JVM 이 이미 떠 있다 (stop-*.sh 의 pkill -f 는 워킹트리를 구분하지 못한다)"
    fi
    if [ -n "$k6s" ]; then
        echo "$k6s"
        fail "k6 또는 bootRun 또는 GradleWrapperMain 프로세스가 있다"
    fi
    if [ -n "$daemons" ]; then
        echo "$daemons"
        note "Gradle 데몬이 있다 (유휴 데몬은 허용, 어느 디렉터리 것인지 기록)"
    fi
    [ -z "$apps" ] && [ -z "$k6s" ] && echo "떠 있는 앱·k6 없음"
}

# 점검 2: 포트 점유. 앱이 쓰는 포트가 잡혀 있으면 걸림, 인프라 포트는 기록만 한다
preflight_ports() {
    section "점검 2: 포트 점유"
    local app_tcp app_udp infra_tcp
    app_tcp="$(lsof -nP -iTCP:8080 -sTCP:LISTEN 2>/dev/null || true)"
    app_udp="$(lsof -nP -iUDP:20020,20040,20060,8010,8011 2>/dev/null || true)"
    infra_tcp="$(lsof -nP -iTCP:9092,9702,6379,9090,5432 -sTCP:LISTEN 2>/dev/null || true)"
    if [ -n "$app_tcp" ] || [ -n "$app_udp" ]; then
        [ -n "$app_tcp" ] && echo "$app_tcp"
        [ -n "$app_udp" ] && echo "$app_udp"
        fail "앱 포트(8080, UDP 20020/20040/20060/8010/8011)가 점유돼 있다"
    else
        echo "앱 포트 비어 있음"
    fi
    if [ -n "$infra_tcp" ]; then
        echo "$infra_tcp"
        note "인프라 포트(9092/9702/6379/9090/5432)가 점유돼 있다 (이전 compose 가 떠 있으면 정상)"
    fi
}

# 점검 3: Docker 컨테이너와 compose 프로젝트
preflight_docker() {
    section "점검 3: Docker 컨테이너"
    if ! docker info > /dev/null 2>&1; then
        note "Docker 데몬에 연결할 수 없다 (측정을 시작하면 compose up 에서 실패한다)"
        return
    fi
    docker ps -a --format '{{.Names}}\t{{.Image}}\t{{.Ports}}\t{{.Status}}'
    docker compose ls
}

# 점검 4: 공유 DB 에 다른 클라이언트가 붙어 있는지. 이 조회 자신의 연결은 센 값에서 뺀다
preflight_pg_clients() {
    section "점검 4: Postgres 연결 수"
    if ! docker exec stock-trading-postgres true > /dev/null 2>&1; then
        note "stock-trading-postgres 컨테이너가 떠 있지 않아 건너뛴다"
        return
    fi
    local clients
    clients="$(docker exec stock-trading-postgres psql -U postgres -d stock_trading -Atc \
        "select count(*) from pg_stat_activity where datname='stock_trading' and pid <> pg_backend_pid()" 2>&1)"
    echo "연결 수: ${clients}"
    if [ "$clients" != "0" ]; then
        fail "stock_trading DB 에 다른 클라이언트가 ${clients}개 붙어 있다 (앱 기동 전에는 0이어야 한다)"
    fi
}

# 점검 5: Kafka 에 이전 측정의 컨슈머 그룹·토픽이 남아 있는지
preflight_kafka() {
    section "점검 5: Kafka 그룹·토픽"
    if ! docker exec stock-trading-kafka true > /dev/null 2>&1; then
        note "stock-trading-kafka 컨테이너가 떠 있지 않아 건너뛴다"
        return
    fi
    echo "-- 컨슈머 그룹"
    docker exec stock-trading-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list 2>&1
    echo "-- 토픽"
    docker exec stock-trading-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>&1
    note "이전 그룹·토픽이 보이면 기록만 한다 (조건 준비에서 인프라를 새로 올리고 order-requests 를 다시 만든다)"
}

# 점검 6: 노트북 상태. AC 전원, 저전력 모드 꺼짐, CPU 속도 제한 없음이 아니면 걸림
preflight_laptop() {
    section "점검 6: 노트북 상태"
    local batt low therm
    batt="$(pmset -g batt)"
    echo "$batt"
    if ! echo "$batt" | head -1 | grep -q "AC Power"; then
        fail "AC 전원이 아니다"
    fi
    pmset -g | grep -iE 'lowpowermode|sleep|disablesleep'
    low="$(pmset -g | awk '/lowpowermode/ { print $2 }')"
    if [ -n "$low" ] && [ "$low" != "0" ]; then
        fail "Low Power Mode 가 켜져 있다 (lowpowermode=${low})"
    fi
    therm="$(pmset -g therm)"
    echo "$therm"
    local limit
    limit="$(echo "$therm" | awk -F= '/CPU_Speed_Limit/ { gsub(/ /, "", $2); print $2 }')"
    if [ -n "$limit" ] && [ "$limit" -lt 100 ] 2> /dev/null; then
        fail "CPU 속도 제한이 걸려 있다 (CPU_Speed_Limit=${limit})"
    fi
    uptime
    vm_stat | head -5
    echo "-- 아무것도 안 돌 때의 상위 프로세스 (E4 허용 목록 확정용)"
    top -l 2 -s 2 -n 10 -o cpu | tail -15
    df -h /tmp /private/var/folders | tail -3
}

# 점검 7: 환경 기록 (env.txt 에 붙인다)
preflight_env_record() {
    section "점검 7: 환경 기록"
    sw_vers
    sysctl -n machdep.cpu.brand_string hw.ncpu hw.memsize hw.model
    java -version 2>&1
    k6 version 2>&1
    if docker info > /dev/null 2>&1; then
        docker version --format '{{.Server.Version}}'
        docker info --format '{{.NCPU}} {{.MemTotal}}'
        docker image inspect apache/kafka:latest postgres:16-alpine redis:7-alpine \
            --format '{{.RepoTags}} {{.Id}} {{.Created}}' 2>&1
    else
        note "Docker 데몬에 연결할 수 없어 Docker 버전과 이미지 digest 를 기록하지 못했다"
    fi
    echo "-- 해상된 kafka-clients 버전"
    ./gradlew :order-engine:dependencies --configuration runtimeClasspath 2>&1 | grep -i kafka-clients
    echo "HEAD=$(git rev-parse HEAD)"
}

# 점검 8: 시드가 11회차에 충분한지. 입력 앞 N건의 계좌별 필요량 x ROUNDS 가 시드 잔고·보유 안에 들어와야 한다
preflight_seed() {
    section "점검 8: 시드 충분성 (입력 앞 ${N}건 x ${ROUNDS}회차)"
    if python3 loadtest/analyze-baseline.py seed-check --input "$INPUT_FILE" --seed-yml "$SEED_YML" \
        --count "$N" --rounds "$ROUNDS"; then
        :
    else
        fail "시드가 모자라거나 입력 파일이 없다"
    fi
}

# 점검 1~6, 8. 점검 7(환경 기록)은 측정할 때 env.txt 로 따로 보내므로, 점검만 실행할 때만 같이 출력한다
run_preflight() {
    PREFLIGHT_FAILURES=0
    preflight_processes
    preflight_ports
    preflight_docker
    preflight_pg_clients
    preflight_kafka
    preflight_laptop
    [ "${PREFLIGHT_ONLY}" = "1" ] && preflight_env_record
    preflight_seed
    echo
    if [ "$PREFLIGHT_FAILURES" -gt 0 ]; then
        echo "점검 결과: 걸린 항목 ${PREFLIGHT_FAILURES}개 — 측정을 시작하지 않는다"
        return 1
    fi
    echo "점검 결과: 걸린 항목 없음"
    return 0
}

# ---------------------------------------------------------------- 측정

# measure-e2e.sh 는 끝까지 실행되는 스크립트라 source 할 수 없다. 파일은 고치지 않고 함수 정의만 꺼내 쓴다.
# 이 함수는 RESULTS_DIR 아래 latency-<버전>-<라벨>.json 에 쓴다. 앱이 감시하는 트리거 파일 경로와 같다.
load_snapshot_function() {
    local definition
    definition="$(sed -n '/^request_latency_snapshot() {/,/^}/p' loadtest/measure-e2e.sh)"
    if [ -z "$definition" ]; then
        echo "measure-e2e.sh 에서 request_latency_snapshot 함수를 찾지 못했다" >&2
        exit 1
    fi
    eval "$definition"
}

orders_count() {
    docker exec stock-trading-postgres psql -U postgres -d stock_trading -Atc \
        "SELECT count(*) FROM orders WHERE account_id BETWEEN 2001 AND 2100" 2> /dev/null
}

record_autovacuum() {
    local label="$1" cond_dir="$2" cond="$3"
    docker exec stock-trading-postgres psql -U postgres -d stock_trading -c \
        "select relname, n_dead_tup, last_autovacuum from pg_stat_user_tables where relname in ('orders','accounts','holdings')" \
        > "${cond_dir}/autovacuum-${cond}-${label}.txt" 2>&1
}

stop_all_apps() {
    ./loadtest/stop-v1.sh
    ./loadtest/stop-v2.sh
}

prepare_condition() {
    local cond="$1"
    echo "--- ${cond} 준비 1: 이전 앱 정지"
    stop_all_apps

    echo "--- ${cond} 준비 2: 인프라 초기화 (볼륨은 지우지 않는다)"
    "${COMPOSE[@]}" down
    if [ "$INCLUDE_KAFKA_UI" = "1" ]; then
        "${COMPOSE[@]}" up -d postgres redis kafka kafka-ui
    else
        "${COMPOSE[@]}" up -d postgres redis kafka
    fi

    echo "--- ${cond} 준비 3: 토픽·DB 행 정리"
    local partitions="$V2_RESET_PARTITIONS"
    [ "$cond" = "v1" ] && partitions="$V1_PARTITIONS"
    ./loadtest/reset-e2e-v1-state.sh "$partitions"

    echo "--- ${cond} 준비 4: 앱 기동"
    ./loadtest/run-e2e-"${cond}".sh | tee "${RUN_DIR}/${cond}/logs/start-${cond}.out"
    if ! grep -q '^READY' "${RUN_DIR}/${cond}/logs/start-${cond}.out"; then
        echo "${cond} 앱이 READY 가 되지 않았다" >&2
        return 1
    fi
    if [ "$INCLUDE_KAFKA_UI" != "1" ]; then
        "${COMPOSE[@]}" stop kafka-ui > /dev/null 2>&1 || true
    fi

    echo "--- ${cond} 준비 5: 준비 완료 확인"
    if [ "$cond" = "v1" ]; then
        await_v1_assignment
    else
        await_v2_ports
    fi
}

# v1: order-engine 이 order-engine-e2e 그룹에서 파티션을 모두 배정받았는지. 배정 전에 도착한 메시지는 읽히지 않는다(F11)
await_v1_assignment() {
    local waited=0 assigned
    echo -n "order-requests 파티션 ${V1_PARTITIONS}개 배정 대기"
    while [ "$waited" -lt "$ASSIGN_TIMEOUT" ]; do
        assigned="$(docker exec stock-trading-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
            --bootstrap-server localhost:9092 --describe --group order-engine-e2e 2> /dev/null \
            | awk -v topic="order-requests" '$2 == topic && $7 != "-" && $7 != "" { count++ } END { print count + 0 }')"
        if [ "$assigned" -ge "$V1_PARTITIONS" ]; then
            echo " OK (${assigned}개)"
            return 0
        fi
        echo -n "."
        sleep 2
        waited=$((waited + 2))
    done
    echo " TIMEOUT (배정 ${assigned}개)"
    return 1
}

# v2: run-e2e-v2.sh 가 기다리는 UDP 포트 3개가 모두 열렸는지
await_v2_ports() {
    local port
    for port in 20040 20020 20060; do
        if ! lsof -nP -iUDP:"${port}" > /dev/null 2>&1; then
            echo "UDP ${port} 가 열려 있지 않다" >&2
            return 1
        fi
    done
    echo "UDP 20040 20020 20060 열림"
}

log_line_count() {
    local file="$1"
    if [ -f "$file" ]; then wc -l < "$file" | tr -d ' '; else echo 0; fi
}

# E6 검사: 구간 안에 늘어난 로그 줄에서 재배정 문구를 찾는다 (v1 만. order-engine 과 matching-engine)
detect_rebalance() {
    local cond="$1" before_order="$2" before_match="$3"
    [ "$cond" = "v1" ] || { echo 0; return; }
    local after_order after_match found=0
    after_order="$(log_line_count loadtest/logs/order-engine.log)"
    after_match="$(log_line_count loadtest/logs/matching-engine.log)"
    if sed -n "$((before_order + 1)),${after_order}p" loadtest/logs/order-engine.log 2> /dev/null | grep -Eq "$E6_PATTERN"; then
        found=1
    fi
    if sed -n "$((before_match + 1)),${after_match}p" loadtest/logs/matching-engine.log 2> /dev/null | grep -Eq "$E6_PATTERN"; then
        found=1
    fi
    echo "$found"
}

# E7: 조건에 필요한 앱 JVM 이 모두 살아 있는지
all_apps_alive() {
    local cond="$1" pattern
    for pattern in $(main_class_pattern_for "$cond" | tr '|' ' '); do
        if ! pgrep -f "$pattern" > /dev/null 2>&1; then
            echo "죽은 프로세스: ${pattern}" >&2
            return 1
        fi
    done
    return 0
}

wait_for_drain() {
    local cond="$1" before="$2"
    if [ "$cond" = "v1" ]; then
        local waited=0 current
        while [ "$waited" -lt "$DRAIN_TIMEOUT_V1" ]; do
            current="$(orders_count)"
            if [ -n "$current" ] && [ $((current - before)) -ge "$N" ]; then
                return 0
            fi
            sleep 1
            waited=$((waited + 1))
        done
        echo "v1 배수 대기 ${DRAIN_TIMEOUT_V1}초 초과 (DB 증가분 $((current - before)))" >&2
    else
        sleep "$DRAIN_WAIT_V2"
    fi
}

run_round() {
    local cond="$1" round="$2" cond_dir="$3"
    echo "--- ${cond} ${round}"
    local db_before="" db_after="" db_delta=""
    local order_lines match_lines
    if [ "$cond" = "v1" ]; then db_before="$(orders_count)"; fi
    order_lines="$(log_line_count loadtest/logs/order-engine.log)"
    match_lines="$(log_line_count loadtest/logs/matching-engine.log)"

    SAMPLER_COND="$cond" ./loadtest/sample-env.sh "${cond_dir}/cpu-${cond}-${round}.csv" &
    local sampler_pid=$!
    sleep 5

    local k6_start k6_end k6_exit
    k6_start="$(date +%s)"
    RATE="$RATE" N="$N" VERSION="$cond" PASS="$round" INPUT_FILE="$INPUT_FILE" \
        k6 run --summary-export="${cond_dir}/k6-${cond}-${round}.json" loadtest/replay-e2e.js \
        > "${cond_dir}/logs/k6-${cond}-${round}.out" 2>&1
    k6_exit=$?
    k6_end="$(date +%s)"

    wait_for_drain "$cond" "${db_before:-0}"

    rm -f "${RESULTS_DIR}/latency-${cond}-${round}.json"
    request_latency_snapshot "$cond" "$round"
    if [ -f "${RESULTS_DIR}/latency-${cond}-${round}.json" ]; then
        mv "${RESULTS_DIR}/latency-${cond}-${round}.json" "${cond_dir}/latency-${cond}-${round}.json"
    fi

    kill "$sampler_pid" 2> /dev/null
    wait "$sampler_pid" 2> /dev/null

    if [ "$cond" = "v1" ]; then
        db_after="$(orders_count)"
        db_delta=$((db_after - db_before))
    fi

    local e6=0 e7=0
    e6="$(detect_rebalance "$cond" "$order_lines" "$match_lines")"
    all_apps_alive "$cond" || e7=1

    local args=(judge --dir "$cond_dir" --cond "$cond" --round "$round" --k6-start "$k6_start" --k6-end "$k6_end"
        --k6-exit "$k6_exit")
    [ -n "$db_delta" ] && args+=(--db-delta "$db_delta")
    [ "$e6" = "1" ] && args+=(--e6)
    [ "$e7" = "1" ] && args+=(--e7)
    python3 loadtest/analyze-baseline.py "${args[@]}"

    if [ "$e7" = "1" ]; then
        echo "E7: 앱이 죽었다 — ${cond} 조건을 중단한다. 처음부터 다시 돌린다 (파일은 지우지 않는다)" >&2
        return 2
    fi
}

run_condition() {
    local cond="$1" cond_dir="${RUN_DIR}/${1}"
    mkdir -p "${cond_dir}/logs"
    echo "=== ${cond} 조건 시작"
    prepare_condition "$cond" || return 1
    record_autovacuum before "$cond_dir" "$cond"

    local number round status=0
    for number in $(seq 1 "$ROUNDS"); do
        round="$(printf 'r%02d' "$number")"
        run_round "$cond" "$round" "$cond_dir"
        status=$?
        [ "$status" -eq 2 ] && break
        if [ "$number" -lt "$ROUNDS" ]; then sleep "$ROUND_GAP_SEC"; fi
    done

    record_autovacuum after "$cond_dir" "$cond"
    cp loadtest/logs/*.log "${cond_dir}/logs/" 2> /dev/null || true
    ./loadtest/stop-"${cond}".sh
    return "$status"
}

# 프로토콜 문서가 커밋돼 있고 바뀌지 않았는지 확인하고 해시를 남긴다 (첫 회차 전에 고정)
fix_protocol() {
    if [ -n "$(git status --porcelain -- "$PROTOCOL_FILE")" ]; then
        echo "${PROTOCOL_FILE} 에 커밋되지 않은 변경이 있다 — 먼저 커밋한다" >&2
        return 1
    fi
    {
        echo "commit=$(git log -1 --format=%H -- "$PROTOCOL_FILE")"
        shasum -a 256 "$PROTOCOL_FILE"
    } > "${RUN_DIR}/protocol.sha256"
    cat "${RUN_DIR}/protocol.sha256"
}

main() {
    if [ "$PREFLIGHT_ONLY" = "1" ]; then
        run_preflight
        exit $?
    fi

    mkdir -p "$RUN_DIR"
    run_preflight 2>&1 | tee "${RUN_DIR}/preflight.txt"
    if [ "${PIPESTATUS[0]}" -ne 0 ]; then
        exit 1
    fi
    {
        preflight_env_record
        echo "run_id=${RUN_ID}"
        echo "start=$(date +%s) $(date)"
        echo "HEAD=$(git rev-parse HEAD)"
        echo "RATE=${RATE} N=${N} ROUNDS=${ROUNDS} ROUND_GAP_SEC=${ROUND_GAP_SEC} INCLUDE_KAFKA_UI=${INCLUDE_KAFKA_UI}"
        echo "V1_LISTENER_CONCURRENCY=${V1_LISTENER_CONCURRENCY} V1_PARTITIONS=${V1_PARTITIONS}"
        echo "CONDITIONS=${CONDITIONS}"
    } > "${RUN_DIR}/env.txt"
    fix_protocol >> "${RUN_DIR}/env.txt" || exit 1

    if [ "$CAFFEINATE" = "1" ]; then
        caffeinate -i -w $$ &
    fi
    load_snapshot_function

    local cond
    for cond in $CONDITIONS; do
        run_condition "$cond" || echo "${cond} 조건이 끝까지 가지 못했다 (분석에서 확인한다)" >&2
    done
    python3 loadtest/analyze-baseline.py report "$RUN_DIR" --out "${RUN_DIR}/analysis.md" > /dev/null
    echo "완료 — ${RUN_DIR}/analysis.md"
}

main
