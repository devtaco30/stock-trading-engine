#!/usr/bin/env bash
# U10 확인 스크립트 공통 부분 — loadtest/u10-stage1.sh, u10-stage2.sh가 source한다.
# 정리는 이 스크립트가 띄운 PID와 컨테이너만 지운다(이름·패턴 일괄 종료 없음).

WORK=/tmp/u10
LOG_DIR="$WORK/logs"
PID_FILE="$WORK/pids"
KAFKA_NAME=u10-kafka
JAVA_OPENS=(--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED)
RESULT=1

fail() { echo "실패: $*" >&2; }

stop_pids() {
    [ -f "$PID_FILE" ] || return 0
    while read -r pid; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null
            for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
            kill -0 "$pid" 2>/dev/null && echo "PID ${pid}가 60초 안에 정상 종료하지 않았다" >&2
        fi
    done < "$PID_FILE"
    : > "$PID_FILE"
}

cleanup() {
    stop_pids
    docker rm -f "$KAFKA_NAME" > /dev/null 2>&1
    exit "$RESULT"
}

check_ports() {
    if lsof -nP -iTCP:9092 -sTCP:LISTEN > /dev/null 2>&1; then fail "TCP 9092를 이미 쓰는 프로세스가 있다"; exit 1; fi
    for port in 20020 20021 20040 20060 8010 8011 8012; do
        if lsof -nP -iUDP:"$port" > /dev/null 2>&1; then fail "UDP ${port}를 이미 쓰는 프로세스가 있다"; exit 1; fi
    done
}

start_kafka() {
    docker run -d --name "$KAFKA_NAME" -p 9092:9092 \
        -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
        -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
        -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka:latest > /dev/null || { fail "Kafka 컨테이너를 띄우지 못했다"; exit 1; }
    for _ in $(seq 1 60); do
        docker exec "$KAFKA_NAME" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1 && break
        sleep 1
    done
    echo "Kafka 브로커 버전: $(docker exec "$KAFKA_NAME" /opt/kafka/bin/kafka-topics.sh --version 2>/dev/null | tail -1) (이미지 apache/kafka:latest, 고정하지 않음)"
}

build_jars() {
    ./gradlew --offline -q :matching-worker:bootJar :account-worker:bootJar > "$LOG_DIR/build.log" 2>&1 || { fail "bootJar 빌드 실패(로그 $LOG_DIR/build.log)"; exit 1; }
    MATCHING_JAR=$(ls matching-worker/build/libs/*.jar | grep -v plain | head -1)
    ACCOUNT_JAR=$(ls account-worker/build/libs/*.jar | grep -v plain | head -1)
}

# start_app <이름> <jar> <설정 yml> [추가 인자...]
start_app() {
    local name="$1" jar="$2" config="$3"
    shift 3
    java "${JAVA_OPENS[@]}" -jar "$jar" --spring.config.additional-location="file:$config" "$@" > "$LOG_DIR/$name.log" 2>&1 &
    echo $! >> "$PID_FILE"
    echo "$name 기동(PID $!, 로그 $LOG_DIR/$name.log)"
}

tool() { ./gradlew --offline -q :account-worker:u10Tool -Pu10Args="$*" 2>&1; }

# 매칭 shard 둘을 띄우고 종목 slot 64개가 배정될 때까지 기다린 뒤 X, Y(서로 다른 매칭 shard의 종목)를 고른다.
# 결과: X XEND Y YEND XS YS
start_matching_and_pick() {
    start_app matching-0 "$MATCHING_JAR" loadtest/u10/u10-matching-0.yml
    start_app matching-1 "$MATCHING_JAR" loadtest/u10/u10-matching-1.yml
    local picked=""
    for _ in $(seq 1 40); do
        picked=$(tool pick localhost:9092 64) && break
        sleep 3
    done
    echo "$picked" | grep -E "ASSIGNED|X=|ONE_ENDPOINT" || true
    echo "$picked" | grep -q "ASSIGNED 64/64" || { fail "종목 slot 64개가 배정되지 않았다"; exit 1; }
    X=$(echo "$picked" | sed -n 's/.*X=\([0-9]*\) .*/\1/p'); XEND=$(echo "$picked" | sed -n 's/.*XEND=\(.*\) Y=.*/\1/p')
    Y=$(echo "$picked" | sed -n 's/.* Y=\([0-9]*\) .*/\1/p'); YEND=$(echo "$picked" | sed -n 's/.*YEND=\(.*\)$/\1/p')
    [ -n "$X" ] && [ -n "$Y" ] || { fail "서로 다른 매칭 shard의 종목 둘을 고르지 못했다"; exit 1; }
    XS=$(shard_of "$XEND"); YS=$(shard_of "$YEND")
    [ "$XS" != "?" ] && [ "$YS" != "?" ] && [ "$XS" != "$YS" ] || { fail "X와 Y의 담당 shard를 알 수 없거나 같다: X=$XEND Y=$YEND"; exit 1; }
}

shard_of() { case "$1" in *20020) echo 0;; *20021) echo 1;; *) echo "?";; esac; }

# start_account [추가 인자...] — 기동이 끝날 때까지 기다린다.
start_account() {
    start_app account "$ACCOUNT_JAR" loadtest/u10/u10-account.yml "$@"
    for _ in $(seq 1 60); do grep -q "Started AccountWorkerApplication" "$LOG_DIR/account.log" && break; sleep 2; done
    grep -q "Started AccountWorkerApplication" "$LOG_DIR/account.log" || { fail "계좌 워커가 기동하지 않았다(로그 $LOG_DIR/account.log)"; exit 1; }
}

# 발신 결과가 「SENT」로 시작하지 않으면 실패로 보고 $WORK/send-failures에 한 줄 적는다(subshell에서 불러도 집계된다).
send_order() { # <계좌> <종목> <BUY|SELL> <가격> <수량> <requestId>
    local result
    result=$(tool send aeron:udp?endpoint=localhost:20040 "$1" "$2" "$3" "$4" "$5" "$6" | tail -1)
    echo "$result"
    case "$result" in SENT*) ;; *) echo "$6: ${result:-출력 없음}" >> "$WORK/send-failures";; esac
}

# wait_for_applied_fills <계좌 로그> <계좌 번호> <기대 건수> <시도 횟수> <간격 초> — 그 계좌의 「체결 반영」 줄 중 applied=true만 세어
# 기대 건수에 닿으면 바로, 못 닿으면 시도를 모두 쓴 뒤 센 건수를 출력한다. applied=false는 같은 체결이 중복 도착한 것이라 세지 않는다.
wait_for_applied_fills() {
    local log="$1" account="$2" expected="$3" attempts="$4" interval="$5" count=0 _
    if [ ! -f "$log" ]; then echo "로그 파일이 없다: $log" >&2; echo 0; return 0; fi
    for _ in $(seq 1 "$attempts"); do
        count=$(grep -c "체결 반영: accountId=${account} .*applied=true" "$log" || true)
        [ "$count" -ge "$expected" ] && break
        sleep "$interval"
    done
    echo "$count"
}

# flow_verdict <합계 단언 통과 1|0> <전제 모두 참 1|0> — 종료 코드를 출력한다. 합계가 틀리면 전제와 무관하게 1(실패), 합계가 맞고 전제만 거짓이면 3(미발동).
flow_verdict() {
    if [ "$1" != 1 ]; then echo 1; elif [ "$2" != 1 ]; then echo 3; else echo 0; fi
}

# tool_failure_reason <도구 출력> — 도구 자체가 실패했다고 알리는 표지(연결 실패, 송신 위치 미도달, 송신 위치 카운터 없음)를 찾아 출력한다. 없으면 빈 값.
# 도구 실패는 시나리오 미발동이 아니라 별도 실패다.
tool_failure_reason() {
    echo "$1" | grep -oE "SENDER_POSITION_NOT_REACHED|SENDER_POSITION_COUNTER_NOT_FOUND|NOT_CONNECTED|OFFER_FAILED" | head -1
}

send_failure_count() { if [ -f "$WORK/send-failures" ]; then grep -c . "$WORK/send-failures"; else echo 0; fi; }

report_send_failures() {
    local count
    count=$(send_failure_count)
    echo "주문 발신 실패 ${count}건"
    [ "$count" = 0 ] || { cat "$WORK/send-failures"; OK=0; }
}

dump_matching() { ./gradlew --offline -q :matching-worker:u10MatchingDump -Pu10Args="$1" 2>&1; }
dump_account() { tool dump-account /tmp/u10/account; }

# check_owned_slots <shard 0 덤프> <shard 1 덤프> — 두 매칭 shard의 정상 종료 스냅샷에 적힌 담당 종목 slot이
# 겹치지 않고 합이 64개인지 단언한다(OK 변수를 0으로 내리는 방식은 각 단계 스크립트와 같다).
check_owned_slots() {
    local s0 s1 c0 c1 total unique
    s0=$(echo "$1" | sed -n 's/^OWNED .* slots=\(.*\)$/\1/p'); s1=$(echo "$2" | sed -n 's/^OWNED .* slots=\(.*\)$/\1/p')
    c0=$(echo "$s0" | tr ',' '\n' | grep -c .); c1=$(echo "$s1" | tr ',' '\n' | grep -c .)
    total=$((c0 + c1))
    unique=$(printf '%s,%s' "$s0" "$s1" | tr ',' '\n' | grep . | sort -u | wc -l | tr -d ' ')
    if [ "$total" = 64 ] && [ "$unique" = 64 ]; then echo "통과: 두 매칭 shard의 담당 종목 slot이 겹치지 않고 합이 64개다 (shard 0 ${c0}개, shard 1 ${c1}개)"
    else echo "실패: 담당 종목 slot 합 ${total}개, 서로 다른 값 ${unique}개 (shard 0 ${c0}개, shard 1 ${c1}개)"; OK=0; fi
}

# count_in_window <시작 epoch ms> <끝 epoch ms> — 표준 입력의 시각(한 줄에 하나) 중 [시작, 끝] 안에 든 개수를 출력한다. 숫자가 아닌 줄은 무시한다.
count_in_window() {
    awk -v s="$1" -v e="$2" '$1 ~ /^[0-9]+$/ && $1 + 0 >= s + 0 && $1 + 0 <= e + 0 { n++ } END { print n + 0 }'
}

# held_premise <동시 보관 최대 건수> — 이동 중 표시를 본 종목 slot의 새 주문이 보관 목록에 쌓였으면 1, 아니면 0을 출력한다(설계서 10-1절 수정본 3번의 구간 안 동작).
# 거부 회신 건수는 전제로 쓰지 않는다. 거부 회신은 맵에 이동 중이 표시되기 전에 나간 주문이 있을 때만 생기므로 실행마다 0일 수 있다.
held_premise() {
    case "$1" in ''|*[!0-9]*) echo 0;; *) if [ "$1" -ge 1 ]; then echo 1; else echo 0; fi;; esac
}

# reject_offsets <구간 시작 epoch ms> — 표준 입력의 거부 수신 시각마다 「시각 − 구간 시작」(ms, 음수면 구간 앞)을 한 줄에 하나씩 출력한다. 보고 수치용이다.
reject_offsets() {
    awk -v s="$1" '$1 ~ /^[0-9]+$/ { print $1 - s }'
}
