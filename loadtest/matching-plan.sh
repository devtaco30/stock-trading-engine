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

# pid·로그 파일 라벨. 1개일 때는 기존 이름을 그대로 쓴다.
matching_label() {
    if [ "$1" = "1" ]; then
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

# shard 하나의 추가 기동 인자. 1개면 빈 문자열(기본 application-udp.yml 값 그대로).
# 서버 번호는 신원(matching-<순번>)에서 계산되므로 따로 주지 않는다.
matching_args() {
    local count="$1" index="$2" i
    if [ "$count" = "1" ]; then
        return 0
    fi
    local archive_root="${MATCHING_ARCHIVE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/loadtest/archives}"
    printf -- ' --matching-shard.coordination.enabled=true'
    printf -- ' --matching-worker.instance-id=%s' "$(matching_instance_id "$index")"
    printf -- ' --transport.matching-intake.channel=%s' "$(matching_intake_endpoint "$index")"
    printf -- ' --matching.worker.archive.control-channel=aeron:udp?endpoint=localhost:%s' "$(matching_control_port "$index")"
    printf -- ' --matching.worker.archive-dir=%s/matching-%s' "$archive_root" "$index"
    for i in $(seq 0 $((count - 1))); do
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

# 표준입력의 덤프를 해석해 주소가 올라온 종목 slot 번호를 오름차순 한 줄(공백 구분)로 낸다.
# 같은 키는 마지막 값이 유효하다(compacted 토픽). 값이 null 이거나 `;moving` 이 붙은 종목 slot은 배정이 끝난 것으로 세지 않는다.
matching_parse_assigned_slots() {
    awk -F'\t' 'NF >= 2 { last[$1] = $2 }
        END { for (k in last) if (last[k] != "null" && last[k] != "" && last[k] !~ /;moving$/) print k }' \
        | sort -n | tr '\n' ' ' | sed 's/ $//'
    echo
}

# 지금 주소가 올라온 종목 slot 번호들(공백 구분 한 줄). 테스트가 이 함수를 바꾼다.
matching_assigned_slots() {
    matching_map_dump | matching_parse_assigned_slots
}

# $1=종목 slot 개수, $2=제한 시간(초). 모두 올라오면 0. 제한 시간을 넘기면 주인 없는 종목 slot 번호를 출력하고 1.
await_matching_slots_assigned() {
    local slot_count="$1" timeout="$2" start=$SECONDS assigned missing slot
    local poll="${MATCHING_ASSIGN_POLL_SEC:-2}"
    while :; do
        assigned=" $(matching_assigned_slots) "
        missing=""
        for slot in $(seq 0 $((slot_count - 1))); do
            case "$assigned" in
                *" ${slot} "*) ;;
                *) missing="${missing:+${missing} }${slot}" ;;
            esac
        done
        if [ -z "$missing" ]; then
            echo "종목 slot ${slot_count}개 배정 완료"
            return 0
        fi
        if [ $((SECONDS - start)) -ge "$timeout" ]; then
            echo "제한 시간 ${timeout}초 안에 배정이 끝나지 않았다 — 주인 없는 종목 slot: ${missing}" >&2
            return 1
        fi
        sleep "$poll"
    done
}
