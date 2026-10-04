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
#   ★V1_ORDERS_PARTITIONS=50, V1_FILLS_PARTITIONS=50   orders, fills 토픽 파티션 수 (앱의 KafkaTopicConfig 기본값. v1 앱 기동 전에 이 수로 만든다)
#   ★V2_RESET_PARTITIONS=3            v2 준비에서 reset-e2e-v1-state.sh 에 넘기는 파티션 수 (DB 행 정리가 목적)
#   ★DRAIN_TIMEOUT_V1=60, DRAIN_WAIT_V2=10   회차 후 배수 대기 초 (v1: DB 증가분이 N이 될 때까지 최대, v2: 고정)
#   ★ASSIGN_TIMEOUT=120               v1 파티션 배정 확인 최대 초
#   ★V1_GROUP                         v1 컨슈머 그룹 이름을 직접 지정 (기본: order-engine 로그의 groupId)
#   ★E6_PATTERN='...'                 파티션 재배정 로그를 찾는 정규식. 실제 문구는 리허설에서 확인해 고정한다
#   ★CAFFEINATE=1                     1이면 측정 동안 절전을 막는다 (caffeinate -i)
#   ★INDEX_MODE=0                     1이면 조건 준비에서 orders·holdings 인덱스(ix_b1_*)를 SQL로 만들고, 끝나면 지운다
#                                     (measure-protocol-b1.md). v1 하나(CONDITIONS="v1")일 때만 허용. 앱 코드는 바꾸지 않는다
#   ★ROUND_CLEAR=0                    1이면 v1 회차가 끝날 때마다 후속 처리를 기다린 뒤 계좌 2001~2100의 FILLED 주문만 지운다
#   ★QUIESCE_TIMEOUT=120, QUIESCE_POLL_SEC=2   비움 전에 후속 처리가 끝나기를 기다리는 최대 초와 확인 간격
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
# orders 와 fills 의 파티션 수는 각 앱의 KafkaTopicConfig 기본값(app.kafka.orders.partitions, app.kafka.fills.partitions: 50)이다
V1_ORDERS_PARTITIONS="${V1_ORDERS_PARTITIONS:-50}"
V1_FILLS_PARTITIONS="${V1_FILLS_PARTITIONS:-50}"
V2_RESET_PARTITIONS="${V2_RESET_PARTITIONS:-3}"
DRAIN_TIMEOUT_V1="${DRAIN_TIMEOUT_V1:-60}"
DRAIN_WAIT_V2="${DRAIN_WAIT_V2:-10}"
ASSIGN_TIMEOUT="${ASSIGN_TIMEOUT:-120}"
E6_PATTERN="${E6_PATTERN:-partitions (revoked|assigned)|Revoking previously assigned|\\(Re-\\)joining group|rebalanc}"
CAFFEINATE="${CAFFEINATE:-1}"
export V1_LISTENER_CONCURRENCY

INDEX_MODE="${INDEX_MODE:-0}"
ROUND_CLEAR="${ROUND_CLEAR:-0}"
QUIESCE_TIMEOUT="${QUIESCE_TIMEOUT:-120}"
QUIESCE_POLL_SEC="${QUIESCE_POLL_SEC:-2}"
B1_BASE_SHA="ee55642fc3b045189f6b1b37fdb5fb5b5d1e6f5f"
B1_ORDERS_INDEX="ix_b1_orders_acct_status_side_stock"
B1_HOLDINGS_INDEX="ix_b1_holdings_acct_stock"
B1_INDEX_CREATED=0

default_run_id_prefix() {
    if [ "$INDEX_MODE" = "1" ] && [ "$ROUND_CLEAR" = "1" ]; then echo "b1-idx-clear"
    elif [ "$INDEX_MODE" = "1" ]; then echo "b1-idx-keep"
    elif [ "$ROUND_CLEAR" = "1" ]; then echo "b1-clear"
    else echo "b0"
    fi
}
RUN_ID="${RUN_ID:-$(default_run_id_prefix)-$(date -u +%Y%m%d-%H%M)}"
RESULTS_DIR="${REPO_ROOT}/loadtest/results"
RUN_DIR="${RESULTS_DIR}/${RUN_ID}"
INPUT_FILE="${RESULTS_DIR}/e2e-input.jsonl"
SEED_YML="${REPO_ROOT}/account-worker/src/main/resources/application-e2e.yml"
PROTOCOL_FILES="loadtest/measure-protocol-b0.md"
if [ "$INDEX_MODE" = "1" ] || [ "$ROUND_CLEAR" = "1" ]; then
    PROTOCOL_FILES="loadtest/measure-protocol-b0.md loadtest/measure-protocol-b1.md"
fi
COMPOSE=(docker compose -p stock-trading-engine -f docker-compose.yml -f docker-compose.loadtest.yml)

PREFLIGHT_PATTERN_APPS='StockTradingEngineApplication|OrderEngineApplication|MatchingApplication|SettlementEngineApplication|AccountWorkerApplication|MatchingWorkerApplication|AccountProjection|SettlementWorker'

PREFLIGHT_FAILURES=0

main_class_pattern_for() {
    case "$1" in
        v1) echo 'StockTradingEngineApplication|OrderEngineApplication|matching\.MatchingApplication|SettlementEngineApplication' ;;
        v2) echo 'StockTradingEngineApplication|AccountWorkerApplication|MatchingWorkerApplication|SettlementWorkerApplication|AccountProjectionWorkerApplication' ;;
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

# 점검 6: 노트북 상태. CPU 속도 제한이 걸려 있으면 걸림. AC 전원과 저전력 모드는 기록만 한다
preflight_laptop() {
    section "점검 6: 노트북 상태"
    local batt low therm
    batt="$(pmset -g batt)"
    echo "$batt"
    # 배터리 전원과 저전력 모드는 실패 항목이 아니라 기록 전용이다 (Jack 결정)
    if ! echo "$batt" | head -1 | grep -q "AC Power"; then
        note "AC 전원이 아니다 (기록 전용)"
    fi
    pmset -g | grep -iE 'lowpowermode|sleep|disablesleep'
    low="$(pmset -g | awk '/lowpowermode/ { print $2 }')"
    if [ -n "$low" ] && [ "$low" != "0" ]; then
        note "Low Power Mode 가 켜져 있다 (lowpowermode=${low}, 기록 전용)"
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
# 시드 확인 인자(한 줄에 하나). 계단 세트는 회차마다 건수가 달라 회차별 건수 목록을, 그 밖에는 건수 x 회차 수를 쓴다
seed_check_args() {
    if [ "${LADDER_ACTIVE:-0}" = "1" ]; then
        echo "--counts"
        ladder_counts
    else
        echo "--count"
        echo "$N"
        echo "--rounds"
        echo "$ROUNDS"
    fi
}

preflight_seed() {
    local args=() arg
    while IFS= read -r arg; do
        args+=("$arg")
    done < <(seed_check_args)
    if [ "${LADDER_ACTIVE:-0}" = "1" ]; then
        section "점검 8: 시드 충분성 (계단, 회차별 건수 $(ladder_counts))"
    else
        section "점검 8: 시드 충분성 (입력 앞 ${N}건 x ${ROUNDS}회차)"
    fi
    if python3 loadtest/analyze-baseline.py seed-check --input "$INPUT_FILE" --seed-yml "$SEED_YML" "${args[@]}"; then
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

# ---------------------------------------------------------------- 인덱스·비움 (measure-protocol-b1.md)

b1_schema() {
    echo "${B1_SCHEMA:-public}"
}

# 접수 경로의 Seq Scan 을 없애는 인덱스. 앱 코드는 두고 SQL 로만 만든다 (이름 접두사 ix_b1_)
index_create_sql() {
    local schema
    schema="$(b1_schema)"
    cat <<SQL
CREATE INDEX IF NOT EXISTS ${B1_ORDERS_INDEX} ON ${schema}.orders (account_id, status, side, stock_code);
CREATE INDEX IF NOT EXISTS ${B1_HOLDINGS_INDEX} ON ${schema}.holdings (account_id, stock_code);
ANALYZE ${schema}.orders;
ANALYZE ${schema}.holdings;
SQL
}

index_drop_sql() {
    local schema
    schema="$(b1_schema)"
    cat <<SQL
DROP INDEX IF EXISTS ${schema}.${B1_ORDERS_INDEX};
DROP INDEX IF EXISTS ${schema}.${B1_HOLDINGS_INDEX};
SQL
}

# 비움: 계좌 2001~2100 의 FILLED 주문과 그 주문을 참조하는 unpaids 만 지운다.
# PENDING 행은 지우지 않는다. 매칭 엔진이 메모리 호가창에 들고 있는 미체결 주문의 DB 행이기 때문이다.
# unpaids.order_id 에는 인덱스가 없어서, orders 행을 지울 때마다 도는 외래키 검사가 unpaids 전체를 읽는다.
# unpaids 삭제를 먼저 커밋하고 VACUUM 으로 죽은 행을 치운 뒤에 orders 를 지워야 이 검사가 싸다(같은 트랜잭션이면 3.4초, 나누면 아래 시간).
clear_sql() {
    local schema
    schema="$(b1_schema)"
    cat <<SQL
ANALYZE ${schema}.orders;
ANALYZE ${schema}.unpaids;
BEGIN;
WITH deleted AS (
  DELETE FROM ${schema}.unpaids
  WHERE order_id IN (SELECT id FROM ${schema}.orders WHERE account_id BETWEEN 2001 AND 2100 AND status = 'FILLED')
  RETURNING 1)
SELECT 'unpaids|' || count(*) FROM deleted;
COMMIT;
VACUUM (ANALYZE) ${schema}.unpaids;
BEGIN;
WITH deleted AS (
  DELETE FROM ${schema}.orders
  WHERE account_id BETWEEN 2001 AND 2100 AND status = 'FILLED'
  RETURNING 1)
SELECT 'orders|' || count(*) FROM deleted;
COMMIT;
SQL
}

clear_vacuum_sql() {
    local schema
    schema="$(b1_schema)"
    cat <<SQL
VACUUM (ANALYZE) ${schema}.orders;
VACUUM (ANALYZE) ${schema}.unpaids;
SQL
}

deleted_orders_from_clear_output() {
    awk -F'|' '$1 == "orders" { value = $2 } END { if (value == "") print "NA"; else print value }'
}

# 한 행: orders 전체|계좌 2001~2100|PENDING|FILLED|unpaids(그 계좌 주문분)|dead tuple|orders 파일 바이트
db_state_sql() {
    local schema
    schema="$(b1_schema)"
    cat <<SQL
SELECT concat_ws('|',
  (SELECT count(*) FROM ${schema}.orders),
  (SELECT count(*) FROM ${schema}.orders WHERE account_id BETWEEN 2001 AND 2100),
  (SELECT count(*) FROM ${schema}.orders WHERE account_id BETWEEN 2001 AND 2100 AND status = 'PENDING'),
  (SELECT count(*) FROM ${schema}.orders WHERE account_id BETWEEN 2001 AND 2100 AND status = 'FILLED'),
  (SELECT count(*) FROM ${schema}.unpaids u JOIN ${schema}.orders o ON o.id = u.order_id WHERE o.account_id BETWEEN 2001 AND 2100),
  COALESCE((SELECT n_dead_tup FROM pg_stat_user_tables WHERE schemaname = '${schema}' AND relname = 'orders'), 0),
  pg_relation_size('${schema}.orders'));
SQL
}

# 접수 경로의 쿼리 세 개 (계좌 2001 매수, 계좌 2051·종목 A000020 매도, 보유 락 쿼리의 조회 형태)
q1_sql() {
    echo "SELECT COALESCE(SUM(reserved_margin), 0) FROM $(b1_schema).orders WHERE account_id = 2001 AND status = 'PENDING' AND side = 'BUY'"
}
q2_sql() {
    echo "SELECT COALESCE(SUM(quantity - filled_quantity), 0) FROM $(b1_schema).orders WHERE account_id = 2051 AND stock_code = 'A000020' AND status = 'PENDING' AND side = 'SELL'"
}
qh_sql() {
    echo "SELECT h.* FROM $(b1_schema).holdings h JOIN $(b1_schema).accounts a ON a.id = h.account_id WHERE a.account_id = 2051 AND h.stock_code = 'A000020'"
}

explain_ms() {
    awk '/Execution Time:/ { print $3; exit }'
}

explain_plan() {
    grep -E 'Scan' | sed -E 's/^[[:space:]]*(->)?[[:space:]]*//; s/[[:space:]]+(on|using)[[:space:]].*//; s/[[:space:]]+\(cost.*//' \
        | paste -sd '/' - | sed 's#/# / #g'
}

# kafka-consumer-groups --describe 출력에서 LAG 합. LAG 가 숫자인 행이 하나도 없으면 NA
lag_total_from_describe() {
    awk 'NF >= 6 && $6 ~ /^[0-9]+$/ { sum += $6; rows++ } END { if (rows == 0) print "NA"; else print sum }'
}

# 후속 처리가 끝났는지: 이전과 지금의 FILLED 행 수가 같고 두 컨슈머 그룹의 lag 가 모두 0
quiescent_step() {
    local prev="$1" cur="$2" lag_matching="$3" lag_settlement="$4"
    if [ -n "$prev" ] && [ "$prev" = "$cur" ] && [ "$lag_matching" = "0" ] && [ "$lag_settlement" = "0" ]; then
        echo yes
    else
        echo no
    fi
}

psql_app() {
    docker exec -i stock-trading-postgres psql -X -q -At -U postgres -d stock_trading -v ON_ERROR_STOP=1 "$@"
}

group_lag() {
    docker exec stock-trading-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
        --describe --group "$1" 2> /dev/null | lag_total_from_describe
}

filled_e2e_count() {
    psql_app -c "SELECT count(*) FROM $(b1_schema).orders WHERE account_id BETWEEN 2001 AND 2100 AND status = 'FILLED'"
}

now_ms() {
    perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'
}

await_quiescent() {
    local waited=0 prev="" cur="" lag_matching="" lag_settlement=""
    while [ "$waited" -lt "$QUIESCE_TIMEOUT" ]; do
        cur="$(filled_e2e_count)"
        lag_matching="$(group_lag matching-engine)"
        lag_settlement="$(group_lag settlement-engine)"
        if [ "$(quiescent_step "$prev" "$cur" "$lag_matching" "$lag_settlement")" = "yes" ]; then
            echo "후속 처리 완료: FILLED ${cur}, lag matching=${lag_matching} settlement=${lag_settlement}, 대기 ${waited}초"
            return 0
        fi
        prev="$cur"
        sleep "$QUIESCE_POLL_SEC"
        waited=$((waited + QUIESCE_POLL_SEC))
    done
    echo "후속 처리 대기 ${QUIESCE_TIMEOUT}초 초과: FILLED ${cur}, lag matching=${lag_matching} settlement=${lag_settlement}" >&2
    return 1
}

# 비움 + VACUUM. 걸린 시간(ms)과 지운 orders 행 수를 CLEAR_MS, CLEAR_DELETED 에 남긴다
clear_orders() {
    local started output
    started="$(now_ms)"
    output="$(clear_sql | psql_app)" || return 1
    clear_vacuum_sql | psql_app > /dev/null || return 1
    CLEAR_MS=$(($(now_ms) - started))
    CLEAR_DELETED="$(printf '%s\n' "$output" | deleted_orders_from_clear_output)"
}

DBSTATS_HEADER="round,phase,orders_total,orders_e2e,pending_e2e,filled_e2e,unpaids_e2e,dead_orders,orders_bytes,q1_ms,q2_ms,qh_ms,q1_plan,q2_plan,qh_plan,clear_ms,deleted_orders"

# 회차 시작 직전(start)과 비움 직후(cleared)의 DB 상태를 dbstats-<cond>.csv 에 한 줄 쓴다.
# start 에서는 접수 경로 쿼리 세 개의 EXPLAIN (ANALYZE) 시간과 계획도 남긴다 (k6 구간 밖, 판정에 쓰지 않는다)
record_dbstats() {
    local cond_dir="$1" cond="$2" round="$3" phase="$4" clear_ms="${5:-}" deleted="${6:-}"
    local file="${cond_dir}/dbstats-${cond}.csv"
    [ -f "$file" ] || echo "$DBSTATS_HEADER" > "$file"
    local state total e2e pending filled unpaids dead bytes
    state="$(db_state_sql | psql_app)"
    IFS='|' read -r total e2e pending filled unpaids dead bytes <<< "$state"
    local q1_ms="" q2_ms="" qh_ms="" q1_plan="" q2_plan="" qh_plan="" plan_text
    if [ "$phase" = "start" ]; then
        plan_text="$(psql_app -c "EXPLAIN (ANALYZE) $(q1_sql)")"
        q1_ms="$(printf '%s\n' "$plan_text" | explain_ms)"
        q1_plan="$(printf '%s\n' "$plan_text" | explain_plan)"
        plan_text="$(psql_app -c "EXPLAIN (ANALYZE) $(q2_sql)")"
        q2_ms="$(printf '%s\n' "$plan_text" | explain_ms)"
        q2_plan="$(printf '%s\n' "$plan_text" | explain_plan)"
        plan_text="$(psql_app -c "EXPLAIN (ANALYZE) $(qh_sql)")"
        qh_ms="$(printf '%s\n' "$plan_text" | explain_ms)"
        qh_plan="$(printf '%s\n' "$plan_text" | explain_plan)"
    fi
    echo "${round},${phase},${total},${e2e},${pending},${filled},${unpaids},${dead},${bytes},${q1_ms},${q2_ms},${qh_ms},${q1_plan},${q2_plan},${qh_plan},${clear_ms},${deleted}" >> "$file"
}

record_schema() {
    local file="$1"
    {
        docker exec -i stock-trading-postgres psql -X -U postgres -d stock_trading -c '\d orders' -c '\d holdings'
    } > "$file" 2>&1
}

create_b1_indexes() {
    index_create_sql | psql_app
}

drop_b1_indexes() {
    index_drop_sql | psql_app
}

# 종료 때(중단 포함) 인덱스를 원복한다. 이 DB 는 다른 워킹트리와 공유한다
b1_cleanup() {
    if [ "$INDEX_MODE" = "1" ] && [ "$B1_INDEX_CREATED" = "1" ]; then
        echo "인덱스 원복(ix_b1_*)"
        drop_b1_indexes
        B1_INDEX_CREATED=0
    fi
}

# 접수 경로의 쿼리 세 개의 계획에 Seq Scan 이 있는지 기록한다 (EXPLAIN 만, 실행하지 않음). 있으면 경고
check_b1_plans() {
    local file="$1" plan_text query_sql name
    : > "$file"
    for name in q1 q2 qh; do
        query_sql="$("${name}_sql")"
        plan_text="$(psql_app -c "EXPLAIN $query_sql")"
        {
            echo "== ${name}: ${query_sql}"
            printf '%s\n' "$plan_text"
        } >> "$file"
        if printf '%s\n' "$plan_text" | grep -qE 'Seq Scan on (orders|holdings)'; then
            echo "경고: ${name} 계획에 Seq Scan on orders/holdings 가 있다 (${file})" >&2
        fi
    done
}

assert_src_main_unchanged() {
    local changed
    changed="$(git diff --stat "$B1_BASE_SHA" -- '*/src/main/*')"
    if [ -n "$changed" ]; then
        echo "앱 코드(src/main)가 ${B1_BASE_SHA} 와 다르다 — 인덱스 측정은 앱 코드를 바꾸지 않는 조건이다" >&2
        echo "$changed" >&2
        return 1
    fi
    echo "src/main diff vs ${B1_BASE_SHA}: 없음"
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

# kafka-topics.sh 호출 한 곳. 테스트가 이 함수를 바꿔 Docker 없이 확인한다
kafka_topics() {
    docker exec stock-trading-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"
}

# v1 앱이 쓰는 토픽과 파티션 수 ("이름 수" 한 줄씩)
v1_topic_specs() {
    echo "order-requests ${V1_PARTITIONS}"
    echo "orders ${V1_ORDERS_PARTITIONS}"
    echo "fills ${V1_FILLS_PARTITIONS}"
}

# v1 앱을 띄우기 전에 토픽을 정해진 파티션 수로 만들고, 읽은 파티션 수가 같은지 확인한다.
# 정산 앱이 fills 보다 먼저 떠서 토픽이 없으면 브로커가 1 파티션으로 자동 생성하고, 컨슈머는 약 5분 뒤에야 나머지 파티션을 받는다
ensure_v1_topics() {
    local name expected described actual
    while read -r name expected; do
        kafka_topics --create --if-not-exists --topic "$name" --partitions "$expected" --replication-factor 1 > /dev/null 2>&1
        described="$(kafka_topics --describe --topic "$name" 2>&1)"
        actual="$(printf '%s\n' "$described" | sed -n 's/.*PartitionCount: *\([0-9][0-9]*\).*/\1/p' | head -n 1)"
        if [ "$actual" != "$expected" ]; then
            echo "토픽 ${name} 의 파티션 수가 ${expected} 이어야 하는데 ${actual:-읽지 못함} 이다" >&2
            return 1
        fi
        echo "토픽 확인: ${name} 파티션 ${actual}"
    done < <(v1_topic_specs)
}

# v2 에서 account-worker 가 쓰고 두 워커가 읽는 토픽과 파티션 수 ("이름 수" 한 줄씩). 두 워커 모두 컨슈머 1개라 파티션은 1개다
v2_topic_specs() {
    echo "account-state 1"
    echo "settlement-requests 1"
    echo "account-settlements 1"
    echo "order-results 1"
}

kafka_consumer_groups() {
    docker exec stock-trading-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 "$@"
}

# v2 앱을 띄우기 전에 위 토픽을 지우고 다시 만든다. 지금까지 어떤 하네스도 이 토픽을 만들지 않아 브로커가 자동 생성했고, 이전 실행의 메시지가 남아 있다.
# 남은 메시지가 있으면 새 워커가 그것부터 소비해 지연이 부풀려진다. 지운 토픽이 사라질 때까지 기다린 뒤 만들고 파티션 수를 확인한다
V2_TOPIC_GONE_POLL_SEC="${V2_TOPIC_GONE_POLL_SEC:-1}"
reset_v2_topics() {
    local name expected described actual waited
    while read -r name expected; do
        kafka_topics --delete --topic "$name" > /dev/null 2>&1
        waited=0
        while kafka_topics --list 2> /dev/null | grep -qx -- "$name" && [ "$waited" -lt 30 ]; do
            sleep "$V2_TOPIC_GONE_POLL_SEC"
            waited=$((waited + 1))
        done
        kafka_topics --create --topic "$name" --partitions "$expected" --replication-factor 1 > /dev/null 2>&1
        described="$(kafka_topics --describe --topic "$name" 2>&1)"
        actual="$(printf '%s\n' "$described" | sed -n 's/.*PartitionCount: *\([0-9][0-9]*\).*/\1/p' | head -n 1)"
        if [ "$actual" != "$expected" ]; then
            echo "토픽 ${name} 의 파티션 수가 ${expected} 이어야 하는데 ${actual:-읽지 못함} 이다" >&2
            return 1
        fi
        echo "토픽 초기화: ${name} 파티션 ${actual}"
    done < <(v2_topic_specs)
}

# 두 워커의 컨슈머 그룹 오프셋을 지운다(앱이 떠 있지 않을 때만 지워진다. 없거나 지워지지 않아도 계속한다)
reset_v2_groups() {
    local group
    for group in account-projection-worker settlement-worker; do
        kafka_consumer_groups --delete --group "$group" > /dev/null 2>&1 || true
    done
}

# 이전 실행이 만든 projection·미수금 예약 테이블을 지운다. 남아 있으면 이전 seq 때문에 새 이벤트가 stale 로 무시된다.
# 워커가 기동하면서 ddl-auto=update 로 다시 만든다
reset_v2_projection_state() {
    psql_app -c "DROP TABLE IF EXISTS account_projection_holding, account_projection, pending_settlements"
}

# account_projection 의 seq 합. 워커가 DB 에 반영한 진행량이다. 테이블이 없으면 NA
v2_projection_progress() {
    local value
    value="$(psql_app -c "SELECT coalesce(sum(seq), 0) FROM account_projection" 2> /dev/null)"
    if [ -n "$value" ]; then echo "$value"; else echo NA; fi
}

# 토픽의 모든 파티션 끝 오프셋 합(= 지금까지 쓰인 메시지 수). 읽지 못하면 NA
topic_end_total() {
    local out
    out="$(docker exec stock-trading-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" --time -1 2> /dev/null)"
    if [ -z "$out" ]; then echo NA; return; fi
    printf '%s\n' "$out" | awk -F: 'NF >= 3 && $3 ~ /^[0-9]+$/ { sum += $3; rows++ } END { if (rows == 0) print "NA"; else print sum }'
}

# v2 컨슈머 그룹 lag. 그룹이 아직 오프셋을 커밋한 적이 없으면 describe 에 lag 숫자가 없어 NA 가 나온다. 입력 토픽이 비어 있으면 읽을 것이 없으니 0 이다
v2_group_lag() {
    local group="$1" topic="$2" lag ends
    lag="$(group_lag "$group")"
    if [ "$lag" != "NA" ]; then echo "$lag"; return; fi
    ends="$(topic_end_total "$topic")"
    if [ "$ends" = "0" ]; then echo 0; else echo NA; fi
}

# v2 후속 처리가 끝났는지: DB 반영 진행량이 이전과 같고 두 워커 그룹의 lag 가 모두 0. v1 의 quiescent_step 과 같은 판정이다.
# 한도는 실제 경과 시간(SECONDS)으로 센다: 한 번 확인할 때 docker exec 가 여러 번 돌아 반복 한 번이 폴링 간격보다 훨씬 길다
await_quiescent_v2() {
    local started="$SECONDS" prev="" cur="" lag_projection="" lag_settlement=""
    while [ $((SECONDS - started)) -lt "$QUIESCE_TIMEOUT" ]; do
        cur="$(v2_projection_progress)"
        lag_projection="$(v2_group_lag account-projection-worker account-state)"
        lag_settlement="$(v2_group_lag settlement-worker settlement-requests)"
        if [ "$cur" != "NA" ] && [ "$(quiescent_step "$prev" "$cur" "$lag_projection" "$lag_settlement")" = "yes" ]; then
            echo "v2 후속 처리 완료: seq 합 ${cur}, lag projection=${lag_projection} settlement=${lag_settlement}, 대기 $((SECONDS - started))초"
            return 0
        fi
        prev="$cur"
        sleep "$QUIESCE_POLL_SEC"
    done
    echo "v2 후속 처리 대기 ${QUIESCE_TIMEOUT}초 초과: seq 합 ${cur}, lag projection=${lag_projection} settlement=${lag_settlement}" >&2
    return 1
}

# 그룹이 토픽 파티션을 배정받은 개수
v2_group_assigned() {
    local group="$1" topic="$2"
    kafka_consumer_groups --describe --group "$group" 2> /dev/null \
        | awk -v topic="$topic" '$2 == topic && $7 != "-" && $7 != "" { count++ } END { print count + 0 }'
}

# Postgres(public)에 테이블이 있는지. projection 워커가 DB 인자를 못 받으면 인메모리 H2 에 경고 없이 기록하므로 이것으로 걸러낸다
v2_table_exists() {
    local found
    found="$(psql_app -c "SELECT to_regclass('public.$1') IS NOT NULL" 2> /dev/null)"
    [ "$found" = "t" ]
}

# v2: 두 워커가 컨슈머 그룹 파티션을 받고 자기 테이블을 Postgres 에 만들었는지
await_v2_workers() {
    local waited=0 proj_assigned=0 settle_assigned=0
    echo -n "v2 워커 준비 대기(그룹 배정과 Postgres 테이블)"
    while [ "$waited" -lt "$ASSIGN_TIMEOUT" ]; do
        proj_assigned="$(v2_group_assigned account-projection-worker account-state)"
        settle_assigned="$(v2_group_assigned settlement-worker settlement-requests)"
        if [ "${proj_assigned:-0}" -ge 1 ] && [ "${settle_assigned:-0}" -ge 1 ] \
            && v2_table_exists account_projection && v2_table_exists pending_settlements; then
            echo " OK (projection 파티션 ${proj_assigned}, settlement 파티션 ${settle_assigned})"
            return 0
        fi
        echo -n "."
        sleep 1
        waited=$((waited + 1))
    done
    echo " TIMEOUT (projection 배정 ${proj_assigned}, settlement 배정 ${settle_assigned}) — DB 인자가 빠지면 projection 워커가 H2 로 떨어진다" >&2
    return 1
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

    if [ "$cond" = "v1" ]; then
        echo "--- ${cond} 준비 3-0: 토픽 생성·확인 (앱 기동 전)"
        ensure_v1_topics || return 1
    else
        echo "--- ${cond} 준비 3-0: v2 토픽·그룹·projection 테이블 초기화 (앱 기동 전)"
        reset_v2_topics || return 1
        reset_v2_groups
        reset_v2_projection_state || return 1
    fi

    if [ "$INDEX_MODE" = "1" ] && [ "$cond" = "v1" ]; then
        echo "--- ${cond} 준비 3-1: 인덱스 생성 (앱 기동 전, orders 테이블이 없으면 앱 기동 뒤로 미룬다)"
        drop_b1_indexes
        if create_b1_indexes; then
            B1_INDEX_CREATED=1
        else
            echo "orders 또는 holdings 테이블이 아직 없다 — 앱 기동 뒤에 만든다"
        fi
    fi

    echo "--- ${cond} 준비 4: 앱 기동"
    ./loadtest/run-e2e-"${cond}".sh | tee "${RUN_DIR}/${cond}/logs/start-${cond}.out"
    if ! grep -q '^READY' "${RUN_DIR}/${cond}/logs/start-${cond}.out"; then
        echo "${cond} 앱이 READY 가 되지 않았다" >&2
        return 1
    fi
    if [ "$INCLUDE_KAFKA_UI" != "1" ]; then
        "${COMPOSE[@]}" stop kafka-ui > /dev/null 2>&1 || true
    fi

    if [ "$INDEX_MODE" = "1" ] && [ "$cond" = "v1" ]; then
        echo "--- ${cond} 준비 4-1: 인덱스 확인 (앱 기동 뒤)"
        create_b1_indexes || { echo "인덱스를 만들지 못했다" >&2; return 1; }
        B1_INDEX_CREATED=1
        record_schema "${RUN_DIR}/${cond}/schema-before.txt"
        check_b1_plans "${RUN_DIR}/${cond}/plans-${cond}.txt"
    fi

    echo "--- ${cond} 준비 5: 준비 완료 확인"
    if [ "$cond" = "v1" ]; then
        await_v1_assignment
    else
        await_v2_ports || return 1
        await_v2_workers
    fi
}

# order-engine 이 실제로 쓰는 컨슈머 그룹. application-e2e.yml 에는 order-engine-e2e 라고 적혀 있지만 첫 리허설에서
# 기동 로그의 groupId 가 order-engine 이었다. 그래서 order-engine 로그에서 읽고, 못 읽으면 V1_GROUP, 그것도 없으면 order-engine 으로 한다
v1_consumer_group() {
    local detected
    detected="$(grep -m1 -oE 'groupId=[^],]+' loadtest/logs/order-engine.log 2> /dev/null | sed 's/groupId=//')"
    echo "${V1_GROUP:-${detected:-order-engine}}"
}

# v1: order-engine 이 컨슈머 그룹에서 파티션을 모두 배정받았는지. 배정 전에 도착한 메시지는 읽히지 않는다(F11)
await_v1_assignment() {
    local waited=0 assigned group
    group="$(v1_consumer_group)"
    echo -n "그룹 ${group} 의 order-requests 파티션 ${V1_PARTITIONS}개 배정 대기"
    while [ "$waited" -lt "$ASSIGN_TIMEOUT" ]; do
        assigned="$(docker exec stock-trading-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
            --bootstrap-server localhost:9092 --describe --group "$group" 2> /dev/null \
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

    if [ "$cond" = "v1" ] && { [ "$INDEX_MODE" = "1" ] || [ "$ROUND_CLEAR" = "1" ]; }; then
        record_dbstats "$cond_dir" "$cond" "$round" start
    fi

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

    if [ "$ROUND_CLEAR" = "1" ] && [ "$cond" = "v1" ]; then
        if ! await_quiescent; then
            echo "후속 처리가 끝나지 않아 비우지 않고 ${cond} 조건을 중단한다" >&2
            return 3
        fi
        CLEAR_MS=0
        CLEAR_DELETED=NA
        clear_orders || { echo "비움 실패 — ${cond} 조건을 중단한다" >&2; return 3; }
        echo "비움: orders ${CLEAR_DELETED}행 삭제, ${CLEAR_MS}ms (VACUUM 포함)"
        record_dbstats "$cond_dir" "$cond" "$round" cleared "$CLEAR_MS" "$CLEAR_DELETED"
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
        [ "$status" -eq 3 ] && break
        if [ "$number" -lt "$ROUNDS" ]; then sleep "$ROUND_GAP_SEC"; fi
    done

    record_autovacuum after "$cond_dir" "$cond"
    if [ "$INDEX_MODE" = "1" ] && [ "$cond" = "v1" ]; then
        record_schema "${cond_dir}/schema-after.txt"
    fi
    cp loadtest/logs/*.log "${cond_dir}/logs/" 2> /dev/null || true
    ./loadtest/stop-"${cond}".sh
    return "$status"
}

# 프로토콜 문서가 커밋돼 있고 바뀌지 않았는지 확인하고 해시를 남긴다 (첫 회차 전에 고정)
fix_protocol() {
    local file
    : > "${RUN_DIR}/protocol.sha256"
    for file in $PROTOCOL_FILES; do
        if [ -n "$(git status --porcelain -- "$file")" ]; then
            echo "${file} 에 커밋되지 않은 변경이 있다 — 먼저 커밋한다" >&2
            return 1
        fi
        {
            echo "commit=$(git log -1 --format=%H -- "$file")"
            shasum -a 256 "$file"
        } >> "${RUN_DIR}/protocol.sha256"
    done
    cat "${RUN_DIR}/protocol.sha256"
}

main() {
    if [ "$PREFLIGHT_ONLY" = "1" ]; then
        run_preflight
        exit $?
    fi

    if [ "$INDEX_MODE" = "1" ] && [ "$CONDITIONS" != "v1" ]; then
        echo "INDEX_MODE=1 은 CONDITIONS=\"v1\" 일 때만 쓴다 (받은 값: ${CONDITIONS})" >&2
        exit 1
    fi
    if [ "$ROUND_CLEAR" = "1" ] && [ "$CONDITIONS" != "v1" ]; then
        echo "ROUND_CLEAR=1 은 CONDITIONS=\"v1\" 일 때만 쓴다 (받은 값: ${CONDITIONS})" >&2
        exit 1
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
        echo "CONDITIONS=${CONDITIONS} INDEX_MODE=${INDEX_MODE} ROUND_CLEAR=${ROUND_CLEAR}"
        if [ "$INDEX_MODE" = "1" ] || [ "$ROUND_CLEAR" = "1" ]; then
            assert_src_main_unchanged
        fi
    } > "${RUN_DIR}/env.txt"
    if ! grep -q "^src/main diff" "${RUN_DIR}/env.txt" && { [ "$INDEX_MODE" = "1" ] || [ "$ROUND_CLEAR" = "1" ]; }; then
        echo "src/main 변경 확인 실패 — env.txt 를 본다" >&2
        exit 1
    fi
    if [ "$INDEX_MODE" = "1" ]; then
        trap 'b1_cleanup' EXIT
        trap 'exit 130' INT TERM
    fi
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

# 테스트가 함수만 불러올 수 있게, MEASURE_SOURCE_ONLY=1 이면 main 을 실행하지 않는다
if [ "${MEASURE_SOURCE_ONLY:-0}" != "1" ]; then
    main
fi
