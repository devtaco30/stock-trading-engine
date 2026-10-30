#!/usr/bin/env bash
# 매칭 shard(matching-worker) 여러 개를 띄우는 하네스의 인자 계산과 배정 완료 대기. run-e2e-v2.sh 가 source 한다. 단독으로 실행하지 않는다.
#
# 환경 변수 (기본값)
#   MATCHING_COUNT=1              매칭 shard 수. 1·2·4·8만 허용. 1이면 인자를 하나도 붙이지 않아 지금과 같은 기동이다.
#   MATCHING_ARCHIVE_ROOT         shard별 Archive 폴더의 부모(절대경로). 기본 <저장소>/loadtest/archives.
#                                 gradlew bootRun 의 JVM 작업 디렉터리가 모듈 폴더라 상대경로는 쓰지 않는다.
#   MATCHING_ASSIGN_POLL_SEC=2    배정 완료를 확인하는 간격(초)
#
# 포트 배치(제안값, 근거 없이 겹치지 않게만 골랐다)
#   주문 입구 UDP = 20020 + shard 번호  (계좌 워커 입구 20040·20041, 체결 20060·20061과 겹치지 않는다)
#   Archive 제어 UDP = 8100 + shard 번호 (계좌 워커 8010·8012, 매칭 기본 8011과 겹치지 않는다)

MATCHING_INTAKE_BASE_PORT=20020
MATCHING_CONTROL_BASE_PORT=8100
MATCHING_MAP_TOPIC="matching-shard-map"
MATCHING_MAX_COUNT=8

# 1·2·4·8 이외는 거부. 최대 8은 호가창 이동 주소 풀의 상한(HandoverEndpointPool.MAX_ENDPOINTS)과 같다.
matching_count_valid() {
    case "${1:-}" in
        1|2|4|8) return 0 ;;
        *) echo "MATCHING_COUNT는 1·2·4·8만 지원한다 (받은 값: ${1:-})" >&2; return 1 ;;
    esac
}

matching_intake_port() { echo $((MATCHING_INTAKE_BASE_PORT + $1)); }
matching_control_port() { echo $((MATCHING_CONTROL_BASE_PORT + $1)); }
matching_instance_id() { echo "matching-$1"; }
matching_intake_endpoint() { echo "aeron:udp?endpoint=localhost:$(matching_intake_port "$1")"; }

# pid·로그 파일 라벨. 1개이고 증설도 없을 때만 기존 이름을 그대로 쓴다. $3=풀 개수(기본은 $1).
matching_label() {
    if [ "${3:-$1}" = "1" ]; then
        echo "matching-worker"
    else
        echo "matching-worker-$2"
    fi
}

# 입구 UDP 포트 목록(줄바꿈 구분). 기동 대기에 쓴다.
matching_intake_ports() {
    local i
    for i in $(seq 0 $(($1 - 1))); do
        matching_intake_port "$i"
    done
}

# 계좌 워커 쪽 주소 풀(matching-shard-routing.endpoints)에 줄 값. 입구 주소를 쉼표로 이은 것이다.
# 계좌 워커 인자를 만드는 일은 계좌 쪽 하네스 담당이라 이 함수는 값만 낸다.
matching_endpoint_pool() {
    local i out=""
    for i in $(seq 0 $(($1 - 1))); do
        out="${out:+${out},}$(matching_intake_endpoint "$i")"
    done
    echo "$out"
}

# 호가창 이동 주소 풀에 넣을 주소 개수. 증설(MATCHING_EXPAND_AT_SEC)이 있으면 증설할 shard의 입구 주소까지 시작 때부터 넣는다.
# 풀은 기동 때 고정이다: HandoverRequester·HandoverResponder 생성자가 풀의 peer마다 Publication을 열고, 풀에 없는 주소가 맵에 나오면
# HandoverEndpointPool.requireInPool 이 예외로 멈춘다. 아직 뜨지 않은 상대가 풀에 있는 것은 허용된다(2 shard 실제 기동에서 확인).
matching_pool_count() {
    if [ -n "${MATCHING_EXPAND_AT_SEC:-}" ]; then
        echo $(($1 + 1))
    else
        echo "$1"
    fi
}

# 실행 중 증설(H9) 인자 검사. $1=시작 shard 수(1~7, 증설 뒤 8개 이하), $2=증설 시각(부하 시작 뒤 몇 초, 1 이상 정수).
# 증설 시각의 값은 정해지지 않았다. 호출하는 쪽이 정해서 넘긴다.
matching_expansion_valid() {
    case "${1:-}" in
        [1-7]) ;;
        *) echo "증설하려면 시작 shard 수는 1~7이어야 한다 (받은 값: ${1:-})" >&2; return 1 ;;
    esac
    case "${2:-}" in
        ""|*[!0-9]*|0) echo "증설 시각은 1 이상의 정수(초)여야 한다 (받은 값: ${2:-})" >&2; return 1 ;;
    esac
    return 0
}

# shard 하나의 추가 기동 인자. 1개이고 증설도 없으면 빈 문자열(기본 application-udp.yml 값 그대로).
# 서버 번호는 신원(matching-<순번>)에서 계산되므로 따로 주지 않는다. $3=풀 개수(기본은 $1).
matching_args() {
    local count="$1" index="$2" pool="${3:-$1}" i
    if [ "$count" = "1" ] && [ "$pool" = "1" ]; then
        return 0
    fi
    local archive_root="${MATCHING_ARCHIVE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/loadtest/archives}"
    printf -- ' --matching-shard.coordination.enabled=true'
    printf -- ' --matching-worker.instance-id=%s' "$(matching_instance_id "$index")"
    printf -- ' --transport.matching-intake.channel=%s' "$(matching_intake_endpoint "$index")"
    printf -- ' --matching.worker.archive.control-channel=aeron:udp?endpoint=localhost:%s' "$(matching_control_port "$index")"
    printf -- ' --matching.worker.archive-dir=%s/matching-%s' "$archive_root" "$index"
    for i in $(seq 0 $((pool - 1))); do
        printf -- ' --matching-shard.handover.endpoints[%s]=%s' "$i" "$(matching_intake_endpoint "$i")"
    done
}

# --- H8: 맵 토픽에 종목 slot 주소가 모두 올라올 때까지 기다린다 ---

# 맵 토픽을 처음부터 끝까지 읽어 "종목 slot<탭>값" 줄로 낸다. 테스트가 이 함수를 바꿔 Docker 없이 확인한다.
# 키는 Integer, 값은 String(MatchingShardOwnershipConfig). 값이 없는 레코드(삭제 표시)는 null 로 찍힌다.
matching_map_dump() {
    docker exec stock-trading-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
        --topic "$MATCHING_MAP_TOPIC" --from-beginning --timeout-ms 3000 \
        --property print.key=true --property key.separator=$'\t' \
        --property key.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer 2> /dev/null
}

# 표준입력의 덤프를 해석해 "종목 slot<탭>주소" 줄을 종목 slot 오름차순으로 낸다.
# 같은 키는 마지막 값이 유효하다(compacted 토픽). 값이 null 이거나 `;moving` 이 붙은 종목 slot은 배정이 끝난 것으로 세지 않는다.
matching_parse_assigned_map() {
    awk -F'\t' 'NF >= 2 { last[$1] = $2 }
        END { for (k in last) if (last[k] != "null" && last[k] != "" && last[k] !~ /;moving$/) printf "%s\t%s\n", k, last[k] }' \
        | sort -n
}

# 지금 주소가 올라온 종목 slot과 그 주소("종목 slot<탭>주소" 줄들). 테스트가 이 함수를 바꾼다.
matching_assigned_map() {
    matching_map_dump | matching_parse_assigned_map
}

# --- 시작 전 맵 토픽 초기화 ---
# 하네스가 쓰는 자체 broker(docker compose 컨테이너 stock-trading-kafka) 한 곳만 대상으로 한다. 테스트가 이 함수를 바꿔 Docker 없이 확인한다.
matching_kafka_topics() {
    docker exec stock-trading-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"
}

# 맵 토픽은 compacted 라 죽은 pod 의 마지막 값이 지워지지 않고 남는다. 직전 실행(예: shard 8개)의 값이 남은 채로 새 shard 가 뜨기도 전에
# 「배정 완료」로 읽히는 것을 막으려고 매칭 shard 를 띄우기 전에 이 토픽을 지운다. 사라질 때까지 기다린다(없던 토픽이면 바로 끝난다).
# 조회·삭제 명령의 종료 코드를 확인한다: 컨테이너가 없거나 broker에 연결하지 못했는데 초기화했다고 말하지 않는다.
# 조정 토픽(ASSIGNMENT_TOPIC)과 컨슈머 그룹의 정적 멤버는 지우지 않는다.
reset_matching_map_topic() {
    local timeout="${MATCHING_TOPIC_GONE_TIMEOUT_SEC:-30}" poll="${MATCHING_TOPIC_GONE_POLL_SEC:-1}" start=$SECONDS topics
    topics="$(matching_kafka_topics --list 2>&1)" || {
        echo "broker에서 토픽 목록을 읽지 못했다(컨테이너 stock-trading-kafka 상태 확인): ${topics}" >&2
        return 1
    }
    if printf '%s\n' "$topics" | grep -qx -- "$MATCHING_MAP_TOPIC"; then
        matching_kafka_topics --delete --topic "$MATCHING_MAP_TOPIC" > /dev/null 2>&1 || {
            echo "맵 토픽 ${MATCHING_MAP_TOPIC} 삭제 명령이 실패했다" >&2
            return 1
        }
        while topics="$(matching_kafka_topics --list 2>&1)" && printf '%s\n' "$topics" | grep -qx -- "$MATCHING_MAP_TOPIC"; do
            if [ $((SECONDS - start)) -ge "$timeout" ]; then
                echo "맵 토픽 ${MATCHING_MAP_TOPIC}이 ${timeout}초 안에 지워지지 않았다" >&2
                return 1
            fi
            sleep "$poll"
        done
    fi
    echo "맵 토픽 ${MATCHING_MAP_TOPIC} 초기화 완료"
}

# $1=종목 slot 개수, $2=제한 시간(초), $3=매칭 shard 수.
# 끝나는 조건은 둘이다. (1) 종목 slot 번호가 모두 평상시 주소로 올라왔다. (2) 올라온 서로 다른 주소가 매칭 shard 수와 같다.
# (2)가 없으면 한 shard 가 먼저 모든 slot 을 받은 시점에 통과하고, 다른 shard 가 가입하면서 일어나는 재배정으로 slot 이 이동 중이 되는 동안 부하가 시작된다.
# 제한 시간을 넘기면 원인을 출력하고 1.
await_matching_slots_assigned() {
    local slot_count="$1" timeout="$2" shard_count="$3" start=$SECONDS map assigned missing slot addresses
    local poll="${MATCHING_ASSIGN_POLL_SEC:-2}"
    while :; do
        map="$(matching_assigned_map)"
        assigned=" $(printf '%s\n' "$map" | cut -f1 | tr '\n' ' ')"
        addresses="$(printf '%s\n' "$map" | cut -f2 | grep . | sort -u | grep -c .)"
        missing=""
        for slot in $(seq 0 $((slot_count - 1))); do
            case "$assigned" in
                *" ${slot} "*) ;;
                *) missing="${missing:+${missing} }${slot}" ;;
            esac
        done
        if [ -z "$missing" ] && [ "$addresses" = "$shard_count" ]; then
            echo "종목 slot ${slot_count}개가 매칭 shard ${shard_count}개의 주소로 배정 완료"
            return 0
        fi
        if [ $((SECONDS - start)) -ge "$timeout" ]; then
            [ -n "$missing" ] && echo "제한 시간 ${timeout}초 안에 배정이 끝나지 않았다 — 주인 없는 종목 slot: ${missing}. 직전 실행보다 매칭 shard 수를 줄여 다시 띄웠다면 이전 정적 멤버(group.instance.id)가 조정 토픽의 컨슈머 그룹에 남아 배정이 끝나지 않을 수 있다(그룹은 지우지 않는다)" >&2
            [ "$addresses" != "$shard_count" ] && echo "제한 시간 ${timeout}초 안에 shard가 다 모이지 않았다 — 서로 다른 주소 ${addresses}개, 기대 ${shard_count}개" >&2
            return 1
        fi
        sleep "$poll"
    done
}

# 맵 토픽에 주소 $1이 평상시 값으로 올라올 때까지 기다린다(증설한 shard가 종목 slot을 받은 시각 기록용).
# $2=제한 시간(초). 올라오면 `EXPANSION_ADDRESS_SEEN_AT_MS=<epoch ms>`를 출력하고 0, 제한 시간을 넘기면 1.
# 시각은 poll 간격(MATCHING_ASSIGN_POLL_SEC)과 맵 덤프 시간만큼 늦게 찍힌다.
await_matching_shard_address() {
    local address="$1" timeout="$2" start=$SECONDS
    local poll="${MATCHING_ASSIGN_POLL_SEC:-2}"
    while :; do
        if matching_assigned_map | cut -f2 | grep -qxF -- "$address"; then
            echo "EXPANSION_ADDRESS_SEEN_AT_MS=$(python3 -I -c 'import time; print(int(time.time()*1000))')"
            return 0
        fi
        if [ $((SECONDS - start)) -ge "$timeout" ]; then
            echo "제한 시간 ${timeout}초 안에 맵 토픽에 ${address} 가 올라오지 않았다" >&2
            return 1
        fi
        sleep "$poll"
    done
}
