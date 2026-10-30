#!/usr/bin/env bash
set -uo pipefail

# H1·H2(매칭 shard 기동 인자 계산), H7(stop-v2의 매칭 shard 정리), H8(맵 토픽 배정 완료 대기)을 확인하는 테스트.
# 앱·Docker·Kafka 없이 돈다.
# 실행: bash loadtest/test_matching_plan.sh   (저장소 루트에서)

cd "$(dirname "$0")/.."
source loadtest/matching-plan.sh
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

# --- H1: 개수 검사 ---
test_count_validation() {
    local n
    for n in 1 2 4 8; do
        matching_count_valid "$n"
        assert_eq "MATCHING_COUNT=${n} 허용" "0" "$?"
    done
    for n in 0 3 9 abc ""; do
        matching_count_valid "$n" 2> /dev/null
        assert_eq "MATCHING_COUNT=[${n}] 거부" "1" "$?"
    done
}

# --- H1: 기본값은 지금과 같은 인자 ---
test_default_args_unchanged() {
    assert_eq "1개면 인자가 비어 있다(application-udp.yml 기본값 그대로)" "" "$(matching_args 1 0)"
    assert_eq "1개면 라벨이 기존 matching-worker" "matching-worker" "$(matching_label 1 0)"
    assert_eq "1개면 입구 포트 목록은 20020 하나" "20020" "$(matching_intake_ports 1)"
}

# --- H1: 2개 이상이면 shard마다 조정 모드와 서로 다른 신원 ---
test_two_shards_args() {
    local a0 a1
    a0="$(matching_args 2 0)"
    a1="$(matching_args 2 1)"
    assert_contains "0번 조정 모드" "$a0" "--matching-shard.coordination.enabled=true"
    assert_contains "1번 조정 모드" "$a1" "--matching-shard.coordination.enabled=true"
    assert_contains "0번 신원" "$a0" "--matching-worker.instance-id=matching-0"
    assert_contains "1번 신원" "$a1" "--matching-worker.instance-id=matching-1"
    assert_not_contains "서버 번호는 신원에서 계산되므로 따로 주지 않는다" "$a1" "node-id"
    assert_eq "라벨 0" "matching-worker-0" "$(matching_label 2 0)"
    assert_eq "라벨 1" "matching-worker-1" "$(matching_label 2 1)"
}

# --- H2: 포트·신원·경로가 서로 겹치지 않는다 ---
test_values_are_distinct() {
    local count=8 i all ports controls ids dirs
    ports="" controls="" ids="" dirs=""
    for i in $(seq 0 $((count - 1))); do
        all="$(matching_args "$count" "$i")"
        ports="${ports}$(printf '%s' "$all" | grep -o 'transport.matching-intake.channel=[^ ]*')"$'\n'
        controls="${controls}$(printf '%s' "$all" | grep -o 'archive.control-channel=[^ ]*')"$'\n'
        ids="${ids}$(printf '%s' "$all" | grep -o 'instance-id=[^ ]*')"$'\n'
        dirs="${dirs}$(printf '%s' "$all" | grep -o 'archive-dir=[^ ]*')"$'\n'
    done
    assert_eq "주문 입구 8개가 모두 다르다" "8" "$(printf '%s' "$ports" | sort -u | grep -c .)"
    assert_eq "Archive 제어 채널 8개가 모두 다르다" "8" "$(printf '%s' "$controls" | sort -u | grep -c .)"
    assert_eq "신원 8개가 모두 다르다" "8" "$(printf '%s' "$ids" | sort -u | grep -c .)"
    assert_eq "Archive 폴더 8개가 모두 다르다" "8" "$(printf '%s' "$dirs" | sort -u | grep -c .)"

    # 계좌 워커의 Archive 제어 포트(8010·8012)와 매칭 기본값 8011, 계좌 입구·체결 포트와 겹치지 않는다.
    for i in $(seq 0 $((count - 1))); do
        case "$(matching_intake_port "$i") $(matching_control_port "$i")" in
            *" 8010"|*" 8011"|*" 8012") FAILS=$((FAILS + 1)); echo "FAIL 제어 포트 ${i}가 기존 값과 겹친다" ;;
            *) PASSES=$((PASSES + 1)) ;;
        esac
        case "$(matching_intake_port "$i")" in
            20040|20041|20060|20061) FAILS=$((FAILS + 1)); echo "FAIL 입구 포트 ${i}가 계좌 워커 포트와 겹친다" ;;
            *) PASSES=$((PASSES + 1)) ;;
        esac
    done
}

# --- H2: 호가창 이동 주소 풀과 계좌 쪽에 넘길 주소 목록이 입구 주소와 같다 ---
test_pool_equals_intake_endpoints() {
    local a1 pool
    a1="$(matching_args 4 1)"
    for i in 0 1 2 3; do
        assert_contains "풀에 ${i}번 입구 주소" "$a1" "--matching-shard.handover.endpoints[${i}]=aeron:udp?endpoint=localhost:$((20020 + i))"
    done
    assert_not_contains "풀은 개수만큼만" "$a1" "handover.endpoints[4]"
    pool="$(matching_endpoint_pool 4)"
    assert_eq "계좌 쪽에 줄 주소 목록(쉼표 구분)" "aeron:udp?endpoint=localhost:20020,aeron:udp?endpoint=localhost:20021,aeron:udp?endpoint=localhost:20022,aeron:udp?endpoint=localhost:20023" "$pool"
    assert_contains "자기 입구 주소를 채널로 준다" "$a1" "--transport.matching-intake.channel=aeron:udp?endpoint=localhost:20021"
}

# --- H2: Archive 폴더는 절대경로(bootRun의 작업 디렉터리가 모듈 폴더라 상대경로가 틀어진다) ---
test_archive_dir_absolute() {
    local a1
    a1="$(MATCHING_ARCHIVE_ROOT=/tmp/x matching_args 2 1)"
    assert_contains "절대경로 폴더" "$a1" "--matching.worker.archive-dir=/tmp/x/matching-1"
}

# --- H8: 배정 완료 대기 ---
# 맵 읽기 대역이 내는 "종목 slot<탭>주소" 줄들. $1~$2 범위의 종목 slot이 모두 주소 $3에 올라온 상태.
map_lines() {
    local slot
    for slot in $(seq "$1" "$2"); do
        printf '%s\t%s\n' "$slot" "$3"
    done
}

test_await_assigned() {
    local dir out A B
    dir="$(mktemp -d)"
    A="aeron:udp?endpoint=localhost:20020"
    B="aeron:udp?endpoint=localhost:20021"
    # 세 번째 호출부터 두 번째 상태를 낸다.
    matching_assigned_map() {
        local calls
        calls=$(( $(cat "${dir}/calls" 2> /dev/null || echo 0) + 1 ))
        echo "$calls" > "${dir}/calls"
        if [ "$calls" -lt 3 ]; then cat "${dir}/first"; else cat "${dir}/second"; fi
    }

    # 63개만 올라온 상태에서는 끝나지 않고, 64개가 두 shard 주소로 나뉘어 올라오면 성공한다.
    { map_lines 0 31 "$A"; map_lines 32 62 "$B"; } > "${dir}/first"
    { map_lines 0 31 "$A"; map_lines 32 63 "$B"; } > "${dir}/second"
    out="$(MATCHING_ASSIGN_POLL_SEC=0 await_matching_slots_assigned 64 5 2 2>&1)"
    assert_eq "63개일 땐 시작하지 않고 64개가 되면 성공" "0" "$?"
    assert_contains "성공 메시지" "$out" "64"
    assert_eq "세 번째 읽기에서야 끝난다" "3" "$(cat "${dir}/calls")"

    # 64개가 모두 올라왔어도 주소가 하나뿐이면(다른 shard의 consumer 가입이 늦음) 기대한 shard 수가 될 때까지 끝나지 않는다.
    echo 0 > "${dir}/calls"
    map_lines 0 63 "$A" > "${dir}/first"
    cp "${dir}/first" "${dir}/second"
    out="$(MATCHING_ASSIGN_POLL_SEC=1 await_matching_slots_assigned 64 2 2 2>&1)"
    assert_eq "주소가 하나뿐이면 실패" "1" "$?"
    assert_contains "주소 수를 출력" "$out" "서로 다른 주소 1개, 기대 2개"

    # 64개가 두 주소로 나뉘고 기대가 1개면(shard 수를 안 넘긴 경우와 달리) 기대 수를 넘는 주소도 거부한다.
    echo 0 > "${dir}/calls"
    { map_lines 0 31 "$A"; map_lines 32 63 "$B"; } > "${dir}/first"
    cp "${dir}/first" "${dir}/second"
    out="$(MATCHING_ASSIGN_POLL_SEC=1 await_matching_slots_assigned 64 2 1 2>&1)"
    assert_eq "기대보다 주소가 많아도 실패" "1" "$?"
    assert_contains "주소 수를 출력(많은 경우)" "$out" "서로 다른 주소 2개, 기대 1개"

    # 63개에서 멈추면 제한 시간 뒤 실패하고, 빠진 번호 63을 출력한다.
    echo 0 > "${dir}/calls"
    { map_lines 0 31 "$A"; map_lines 32 62 "$B"; } > "${dir}/first"
    cp "${dir}/first" "${dir}/second"
    out="$(MATCHING_ASSIGN_POLL_SEC=1 await_matching_slots_assigned 64 2 2 2>&1)"
    assert_eq "63개에서 멈추면 실패" "1" "$?"
    assert_contains "빠진 종목 slot 번호를 출력" "$out" "주인 없는 종목 slot: 63"

    # 여러 개가 빠지면 모두 출력한다.
    echo 0 > "${dir}/calls"
    { printf '0\t%s\n1\t%s\n2\t%s\n5\t%s\n' "$A" "$A" "$B" "$B"; } > "${dir}/first"
    cp "${dir}/first" "${dir}/second"
    out="$(MATCHING_ASSIGN_POLL_SEC=1 await_matching_slots_assigned 8 2 2 2>&1)"
    assert_contains "빠진 번호 여럿" "$out" "주인 없는 종목 slot: 3 4 6 7"
    unset -f matching_assigned_map
    rm -rf "$dir"
}

# --- H8: 맵 덤프 해석(같은 키는 마지막 값, null 값과 이동 중 표시는 배정 끝으로 세지 않는다) ---
test_parse_map_dump() {
    local dump parsed
    dump=$'0\taeron:udp?endpoint=localhost:20020\n1\taeron:udp?endpoint=localhost:20020\n1\tnull\n2\taeron:udp?endpoint=localhost:20021;moving\n3\taeron:udp?endpoint=localhost:20021\n3\taeron:udp?endpoint=localhost:20020'
    parsed="$(printf '%s\n' "$dump" | matching_parse_assigned_map)"
    assert_eq "마지막 값이 null이거나 이동 중이면 뺀다" $'0\taeron:udp?endpoint=localhost:20020\n3\taeron:udp?endpoint=localhost:20020' "$parsed"
}

# --- H8: 시작 전 맵 토픽 초기화(직전 실행의 마지막 값이 남아 배정 완료로 읽히는 것을 막는다) ---
test_reset_map_topic() {
    local calls_file calls_n
    calls_file="$(mktemp)"
    : > "${calls_file}.n"
    matching_kafka_topics() {
        echo "$*" >> "$calls_file"
        case "$*" in
            *--list*)
                # 삭제 직후 한 번은 아직 보이고 그다음부터 사라진다.
                calls_n=$(( $(cat "${calls_file}.n" 2> /dev/null || echo 0) + 1 ))
                echo "$calls_n" > "${calls_file}.n"
                if [ "$calls_n" -lt 2 ] && [ "${FAKE_NEVER_GONE:-0}" != "1" ]; then echo "matching-shard-map"; fi
                [ "${FAKE_NEVER_GONE:-0}" = "1" ] && echo "matching-shard-map"
                echo "other-topic"
                ;;
        esac
        return 0
    }
    MATCHING_TOPIC_GONE_POLL_SEC=0 reset_matching_map_topic > /dev/null 2>&1
    assert_eq "삭제가 확인되면 성공" "0" "$?"
    assert_contains "맵 토픽을 지운다" "$(cat "$calls_file")" "--delete --topic matching-shard-map"
    assert_not_contains "다른 토픽은 지우지 않는다" "$(grep -- '--delete' "$calls_file")" "other-topic"

    FAKE_NEVER_GONE=1 MATCHING_TOPIC_GONE_POLL_SEC=1 MATCHING_TOPIC_GONE_TIMEOUT_SEC=2 reset_matching_map_topic > /dev/null 2>&1
    assert_eq "토픽이 사라지지 않으면 실패" "1" "$?"
    unset -f matching_kafka_topics
    rm -f "$calls_file" "${calls_file}.n"
}

# --- H7: stop-v2가 매칭 shard의 pid 파일까지 정리 대상으로 든다 ---
test_stop_includes_matching_shard_pids() {
    local dir
    dir="$(mktemp -d)"
    touch "${dir}/matching-worker.pid" "${dir}/matching-worker-0.pid" "${dir}/matching-worker-1.pid" "${dir}/matching-worker-7.pid" "${dir}/api.pid" "${dir}/matching-worker-x.pid"
    local listed
    listed="$(STOP_V2_SOURCE_ONLY=1 bash -c 'source loadtest/stop-v2.sh; PID_DIR="$1"; module_pid_files matching-worker' _ "$dir" | sort | xargs -n1 basename | tr '\n' ' ')"
    assert_eq "매칭 pid 파일 셋이 모두 든다(숫자 아닌 이름과 다른 모듈은 뺀다)" "matching-worker-0.pid matching-worker-1.pid matching-worker-7.pid matching-worker.pid " "$listed"
    rm -rf "$dir"
}

test_run_script_uses_plan() {
    local script
    script="$(cat loadtest/run-e2e-v2.sh)"
    assert_contains "계획 파일을 source" "$script" "source loadtest/matching-plan.sh"
    assert_contains "MATCHING_COUNT 검사" "$script" "matching_count_valid"
    assert_contains "shard마다 띄운다" "$script" "start_matching_workers"
    assert_contains "배정 대기에 shard 수를 넘긴다" "$script" 'await_matching_slots_assigned "${MATCHING_STOCK_SLOT_COUNT:-64}" "$MATCHING_ASSIGN_TIMEOUT" "$MATCHING_COUNT"'
    local reset_line start_line
    reset_line="$(grep -n 'reset_matching_map_topic ||' loadtest/run-e2e-v2.sh | head -n 1 | cut -d: -f1)"
    start_line="$(grep -n '^if \[ "\$WORKER_COUNT" = "2" \]' loadtest/run-e2e-v2.sh | head -n 1 | cut -d: -f1)"
    assert_eq "맵 토픽 초기화 호출이 있다" "1" "$([ -n "$reset_line" ] && echo 1 || echo 0)"
    assert_eq "맵 토픽 초기화가 앱 기동보다 앞이다" "1" "$([ -n "$reset_line" ] && [ -n "$start_line" ] && [ "$reset_line" -lt "$start_line" ] && echo 1 || echo 0)"
    assert_contains "모든 입구 포트를 기다린다" "$script" "matching_intake_ports"
}

# --- H9: 실행 중 증설. 시작 shard는 증설할 shard의 입구 주소까지 포함한 풀로 떠야 한다 ---
test_expansion_pool() {
    local a b new
    # 풀은 기동 때 고정이고(HandoverRequester·Responder 생성자가 풀의 peer마다 Publication을 연다), 풀에 없는 주소가 맵에 나오면 멈춘다.
    a="$(matching_args 1 0 2)"
    assert_contains "shard 1개로 시작해도 증설하면 조정 모드" "$a" "--matching-shard.coordination.enabled=true"
    assert_contains "시작 shard의 풀에 증설할 shard 주소가 있다" "$a" "--matching-shard.handover.endpoints[1]=aeron:udp?endpoint=localhost:20021"
    assert_eq "증설하는 경우 라벨에 순번이 붙는다" "matching-worker-0" "$(matching_label 1 0 2)"
    b="$(matching_args 2 1 3)"
    assert_contains "시작 shard 1의 풀에 증설 shard(2)" "$b" "--matching-shard.handover.endpoints[2]=aeron:udp?endpoint=localhost:20022"
    assert_not_contains "풀은 증설 뒤 개수까지만" "$b" "handover.endpoints[3]"
    new="$(matching_args 3 2)"
    assert_contains "증설 shard의 신원" "$new" "--matching-worker.instance-id=matching-2"
    assert_contains "증설 shard의 입구" "$new" "--transport.matching-intake.channel=aeron:udp?endpoint=localhost:20022"
    assert_eq "증설 shard의 풀은 시작 shard의 풀과 같다" \
        "$(printf '%s' "$b" | grep -o 'handover.endpoints\[[0-9]*\]=[^ ]*' | sort)" \
        "$(printf '%s' "$new" | grep -o 'handover.endpoints\[[0-9]*\]=[^ ]*' | sort)"
    assert_eq "증설이 없으면 풀 개수는 시작 개수" "2" "$(unset MATCHING_EXPAND_AT_SEC; matching_pool_count 2)"
    assert_eq "증설이 있으면 풀 개수는 시작 개수 + 1" "3" "$(MATCHING_EXPAND_AT_SEC=30 matching_pool_count 2)"
}

test_expansion_validation() {
    matching_expansion_valid 2 30 2> /dev/null
    assert_eq "시작 2개, 30초 뒤 증설 허용" "0" "$?"
    local start delay
    for start in 0 8 abc ""; do
        matching_expansion_valid "$start" 30 2> /dev/null
        assert_eq "시작 shard 수 [${start}] 거부" "1" "$?"
    done
    for delay in 0 -5 abc ""; do
        matching_expansion_valid 2 "$delay" 2> /dev/null
        assert_eq "증설 시각 [${delay}] 거부" "1" "$?"
    done
    matching_expansion_valid 7 10 2> /dev/null
    assert_eq "시작 7개면 8번째까지 증설 허용" "0" "$?"
}

test_await_shard_address() {
    local dir out A B
    dir="$(mktemp -d)"
    A="aeron:udp?endpoint=localhost:20020"
    B="aeron:udp?endpoint=localhost:20021"
    matching_assigned_map() {
        local calls
        calls=$(( $(cat "${dir}/calls" 2> /dev/null || echo 0) + 1 ))
        echo "$calls" > "${dir}/calls"
        if [ "$calls" -lt 3 ]; then cat "${dir}/first"; else cat "${dir}/second"; fi
    }
    map_lines 0 63 "$A" > "${dir}/first"
    { map_lines 0 31 "$A"; map_lines 32 63 "$B"; } > "${dir}/second"
    out="$(MATCHING_ASSIGN_POLL_SEC=0 await_matching_shard_address "$B" 5 2>&1)"
    assert_eq "새 주소가 맵에 올라오면 성공" "0" "$?"
    assert_contains "올라온 시각(epoch ms)을 출력" "$out" "EXPANSION_ADDRESS_SEEN_AT_MS="
    echo 0 > "${dir}/calls"
    map_lines 0 63 "$A" > "${dir}/second"
    MATCHING_ASSIGN_POLL_SEC=1 await_matching_shard_address "$B" 2 > /dev/null 2>&1
    assert_eq "끝내 안 올라오면 실패" "1" "$?"
    unset -f matching_assigned_map
    rm -rf "$dir"
}

test_expand_script() {
    local script
    script="$(cat loadtest/expand-matching.sh 2> /dev/null)"
    assert_contains "증설 검사" "$script" "matching_expansion_valid"
    assert_contains "증설 shard의 인자" "$script" 'matching_args "$((MATCHING_COUNT + 1))" "$MATCHING_COUNT"'
    assert_contains "대기 뒤에 띄운다" "$script" 'sleep "$MATCHING_EXPAND_AT_SEC"'
    assert_contains "새 주소가 맵에 올라온 시각을 기록" "$script" "await_matching_shard_address"
}

test_count_validation
test_default_args_unchanged
test_two_shards_args
test_values_are_distinct
test_pool_equals_intake_endpoints
test_archive_dir_absolute
test_await_assigned
test_parse_map_dump
test_reset_map_topic
test_stop_includes_matching_shard_pids
test_run_script_uses_plan
test_expansion_pool
test_expansion_validation
test_await_shard_address
test_expand_script

echo "통과 ${PASSES}, 실패 ${FAILS}"
[ "$FAILS" -eq 0 ]
