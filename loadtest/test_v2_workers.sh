#!/usr/bin/env bash
set -uo pipefail

# v2 측정에 settlement-worker 와 account-projection-worker 를 함께 띄우는 하네스 변경(토픽·DB 초기화, 기동, 그룹 배정 확인,
# 후속 처리 완료 대기, 종료, 사망 점검)을 확인하는 테스트. 앱·Docker·부하 없이 돈다.
# 실행: bash loadtest/test_v2_workers.sh   (저장소 루트에서)

cd "$(dirname "$0")/.."
MEASURE_SOURCE_ONLY=1 source loadtest/measure-segments.sh
set +e

PASSES=0
FAILS=0

assert_eq() {
    local name="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        PASSES=$((PASSES + 1))
    else
        FAILS=$((FAILS + 1))
        echo "FAIL ${name}: 기대=[${expected}] 실제=[${actual}]"
    fi
}

assert_contains() {
    local name="$1" haystack="$2" needle="$3"
    if printf '%s' "$haystack" | grep -qF -- "$needle"; then
        PASSES=$((PASSES + 1))
    else
        FAILS=$((FAILS + 1))
        echo "FAIL ${name}: [${needle}] 가 없다"
    fi
}

assert_not_contains() {
    local name="$1" haystack="$2" needle="$3"
    if printf '%s' "$haystack" | grep -qF -- "$needle"; then
        FAILS=$((FAILS + 1))
        echo "FAIL ${name}: [${needle}] 가 있으면 안 된다"
    else
        PASSES=$((PASSES + 1))
    fi
}

line_of() {
    printf '%s\n' "$1" | grep -n -- "$2" | head -n 1 | cut -d: -f1
}

test_topic_specs() {
    local specs
    specs="$(v2_topic_specs)"
    assert_contains "account-state 파티션 1" "$specs" "account-state 1"
    assert_contains "settlement-requests 파티션 1" "$specs" "settlement-requests 1"
    assert_contains "account-settlements 파티션 1" "$specs" "account-settlements 1"
    assert_contains "order-results 파티션 1" "$specs" "order-results 1"
}

test_reset_v2_topics() {
    local calls_file
    calls_file="$(mktemp)"
    kafka_topics() {
        echo "$*" >> "$calls_file"
        case "$*" in
            *--describe*) echo "Topic: x PartitionCount: ${FAKE_PARTITIONS:-1} ReplicationFactor: 1" ;;
        esac
        return 0
    }
    V2_TOPIC_GONE_POLL_SEC=0 reset_v2_topics > /dev/null 2>&1
    assert_eq "정상이면 성공" "0" "$?"
    local calls
    calls="$(cat "$calls_file")"
    assert_contains "account-state 를 지운다" "$calls" "--delete --topic account-state"
    assert_contains "account-state 를 다시 만든다" "$calls" "--create --topic account-state --partitions 1"
    assert_contains "order-results 를 지우고 만든다" "$calls" "--delete --topic order-results"
    assert_contains "settlement-requests 를 만든다" "$calls" "--create --topic settlement-requests --partitions 1"
    local delete_line create_line
    delete_line="$(line_of "$calls" '--delete --topic account-state')"
    create_line="$(line_of "$calls" '--create --topic account-state')"
    assert_eq "지운 뒤에 만든다" "1" "$([ -n "$delete_line" ] && [ -n "$create_line" ] && [ "$delete_line" -lt "$create_line" ] && echo 1 || echo 0)"

    FAKE_PARTITIONS=3 V2_TOPIC_GONE_POLL_SEC=0 reset_v2_topics > /dev/null 2>&1
    assert_eq "읽은 파티션 수가 다르면 실패" "1" "$?"
    unset -f kafka_topics
    rm -f "$calls_file"
}

test_reset_v2_groups_and_state() {
    local calls_file
    calls_file="$(mktemp)"
    kafka_consumer_groups() { echo "$*" >> "$calls_file"; return 0; }
    reset_v2_groups > /dev/null 2>&1
    local calls
    calls="$(cat "$calls_file")"
    assert_contains "projection 그룹을 지운다" "$calls" "--delete --group account-projection-worker"
    assert_contains "settlement 그룹을 지운다" "$calls" "--delete --group settlement-worker"
    unset -f kafka_consumer_groups

    psql_app() { echo "$*" >> "$calls_file"; return 0; }
    : > "$calls_file"
    reset_v2_projection_state > /dev/null 2>&1
    calls="$(cat "$calls_file")"
    assert_contains "projection 테이블을 지운다" "$calls" "DROP TABLE IF EXISTS account_projection_holding, account_projection"
    assert_contains "settlement 테이블을 지운다" "$calls" "pending_settlements"
    unset -f psql_app
    rm -f "$calls_file"
}

test_await_quiescent_v2() {
    local dir
    dir="$(mktemp -d)"
    printf '5 8 8 8 8\n' > "${dir}/progress"
    printf '3 1 0 0 0\n' > "${dir}/lag"
    next_value() {
        local file="$1" first rest
        read -r first rest < "$file"
        printf '%s\n' "$rest" > "$file"
        echo "$first"
    }
    v2_projection_progress() { next_value "${dir}/progress"; }
    group_lag() {
        case "$1" in
            account-projection-worker) next_value "${dir}/lag" ;;
            *) echo 0 ;;
        esac
    }
    QUIESCE_POLL_SEC=0 QUIESCE_TIMEOUT=10 await_quiescent_v2 > /dev/null 2>&1
    assert_eq "진행이 멈추고 lag 가 0 이 되면 성공" "0" "$?"

    printf '5 6 7 8 9 10 11 12 13 14 15 16\n' > "${dir}/progress"
    printf '3 3 3 3 3 3 3 3 3 3 3 3\n' > "${dir}/lag"
    QUIESCE_POLL_SEC=1 QUIESCE_TIMEOUT=2 await_quiescent_v2 > /dev/null 2>&1
    assert_eq "끝나지 않으면 시간 초과로 실패" "1" "$?"
    unset -f v2_projection_progress group_lag next_value
    rm -rf "$dir"
}

test_await_v2_workers() {
    v2_group_assigned() { echo "${FAKE_ASSIGNED:-1}"; }
    v2_table_exists() { [ "${FAKE_TABLES:-yes}" = "yes" ]; }
    ASSIGN_TIMEOUT=2 await_v2_workers > /dev/null 2>&1
    assert_eq "그룹 배정과 테이블이 있으면 성공" "0" "$?"
    FAKE_TABLES=no ASSIGN_TIMEOUT=2 await_v2_workers > /dev/null 2>&1
    assert_eq "프로젝션 테이블이 Postgres 에 없으면(H2 로 떨어진 경우) 실패" "1" "$?"
    FAKE_ASSIGNED=0 ASSIGN_TIMEOUT=2 await_v2_workers > /dev/null 2>&1
    assert_eq "파티션이 배정되지 않으면 실패" "1" "$?"
    unset -f v2_group_assigned v2_table_exists
}

test_process_lists() {
    assert_contains "사망 점검 패턴에 settlement-worker" "$(main_class_pattern_for v2)" "SettlementWorkerApplication"
    assert_contains "사망 점검 패턴에 projection 워커" "$(main_class_pattern_for v2)" "AccountProjectionWorkerApplication"
    assert_contains "단계 덤프 대상에 projection 워커" "$(stage_trigger_targets v2)" "account-projection-worker"
    assert_not_contains "settlement-worker 는 단계 계측이 없어 덤프 대상이 아니다" "$(stage_trigger_targets v2)" "settlement-worker"

    local stop_script
    stop_script="$(cat loadtest/stop-v2.sh)"
    assert_contains "stop-v2 루프에 settlement-worker" "$stop_script" "for module in api matching-worker account-worker settlement-worker account-projection-worker"
    assert_contains "stop-v2 가 settlement-worker 클래스를 안다" "$stop_script" "com.flab.stocktradingengine.settlement.worker.SettlementWorkerApplication"
    assert_contains "stop-v2 가 projection 워커 클래스를 안다" "$stop_script" "com.flab.stocktradingengine.account.projection.worker.AccountProjectionWorkerApplication"
}

test_start_script() {
    local script
    script="$(cat loadtest/run-e2e-v2.sh)"
    assert_contains "settlement-worker 를 띄운다" "$script" "start_worker settlement-worker settlement-worker"
    assert_contains "projection 워커를 띄운다" "$script" "start_worker account-projection-worker account-projection-worker"
    assert_contains "워커는 Postgres 접속 인자를 받는다" "$script" "--spring.datasource.url=jdbc:postgresql://localhost:9702/stock_trading"
    assert_contains "워커는 Postgres 방언을 받는다" "$script" "--spring.jpa.database-platform=org.hibernate.dialect.PostgreSQLDialect"
    assert_contains "projection 워커 단계 계측 인자" "$script" 'stage_args v2 account-projection-worker'
    local worker_fn
    worker_fn="$(sed -n '/^start_worker()/,/^}/p' loadtest/run-e2e-v2.sh)"
    assert_contains "start_worker 가 있다" "$worker_fn" "bootRun"
    assert_not_contains "워커에는 udp,e2e 프로파일이 없다" "$worker_fn" "spring.profiles.active"
}

test_prepare_condition_order() {
    local body topics_line state_line app_line await_line
    body="$(declare -f prepare_condition)"
    topics_line="$(line_of "$body" 'reset_v2_topics')"
    state_line="$(line_of "$body" 'reset_v2_projection_state')"
    app_line="$(line_of "$body" 'run-e2e-')"
    await_line="$(line_of "$body" 'await_v2_workers')"
    assert_eq "토픽 초기화 호출이 있다" "1" "$([ -n "$topics_line" ] && echo 1 || echo 0)"
    assert_eq "토픽 초기화가 앱 기동보다 앞이다" "1" "$([ -n "$topics_line" ] && [ -n "$app_line" ] && [ "$topics_line" -lt "$app_line" ] && echo 1 || echo 0)"
    assert_eq "DB 초기화가 앱 기동보다 앞이다" "1" "$([ -n "$state_line" ] && [ -n "$app_line" ] && [ "$state_line" -lt "$app_line" ] && echo 1 || echo 0)"
    assert_eq "그룹 배정 확인이 앱 기동보다 뒤이다" "1" "$([ -n "$await_line" ] && [ -n "$app_line" ] && [ "$await_line" -gt "$app_line" ] && echo 1 || echo 0)"
    assert_contains "초기화 실패하면 앱 기동으로 넘어가지 않는다" "$body" "reset_v2_topics || return 1"
}

test_round_uses_v2_quiescence() {
    local body
    body="$(declare -f run_round_segments)"
    assert_contains "v2 도 후속 처리 완료를 기다린다" "$body" "await_quiescent_v2"
    assert_contains "v2 lag 는 두 그룹의 합" "$body" "account-projection-worker"
}

test_topic_specs
test_reset_v2_topics
test_reset_v2_groups_and_state
test_await_quiescent_v2
test_await_v2_workers
test_process_lists
test_start_script
test_prepare_condition_order
test_round_uses_v2_quiescence

echo "통과 ${PASSES}, 실패 ${FAILS}"
[ "$FAILS" -eq 0 ]
