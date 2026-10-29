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
            kill -0 "$pid" 2>/dev/null && echo "PID $pid가 60초 안에 정상 종료하지 않았다" >&2
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
        if lsof -nP -iUDP:"$port" > /dev/null 2>&1; then fail "UDP $port를 이미 쓰는 프로세스가 있다"; exit 1; fi
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

send_order() { # <계좌> <종목> <BUY|SELL> <가격> <수량> <requestId>
    tool send aeron:udp?endpoint=localhost:20040 "$1" "$2" "$3" "$4" "$5" "$6" | tail -1
}

dump_matching() { ./gradlew --offline -q :matching-worker:u10MatchingDump -Pu10Args="$1" 2>&1; }
dump_account() { tool dump-account /tmp/u10/account; }
