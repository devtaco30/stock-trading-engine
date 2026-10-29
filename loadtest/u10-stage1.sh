#!/usr/bin/env bash
set -uo pipefail

# U10 1단계 — 매칭 shard 둘과 계좌 워커 하나를 실제 프로세스로 띄워, 종목 slot 64개가 모두 배정되고
# 서로 다른 매칭 shard가 맡는 두 종목(X, Y)의 주문이 각자의 매칭 shard 호가창에만 들어가는지 본다.
# 종료 코드 0이면 통과, 그 밖이면 실패다. 정리는 이 스크립트가 띄운 PID와 컨테이너만 지운다.
cd "$(dirname "$0")/.."

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
            kill -0 "$pid" 2>/dev/null && { echo "PID $pid가 60초 안에 정상 종료하지 않았다" >&2; }
        fi
    done < "$PID_FILE"
    : > "$PID_FILE"
}

cleanup() {
    stop_pids
    docker rm -f "$KAFKA_NAME" > /dev/null 2>&1
    exit "$RESULT"
}
trap cleanup EXIT

# 1. 포트 점검 — 다른 프로세스가 쓰고 있으면 끄지 않고 멈춘다.
for port in 9092; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN > /dev/null 2>&1; then fail "TCP $port를 이미 쓰는 프로세스가 있다"; exit 1; fi
done
for port in 20020 20021 20040 20060 8010 8011 8012; do
    if lsof -nP -iUDP:"$port" > /dev/null 2>&1; then fail "UDP $port를 이미 쓰는 프로세스가 있다"; exit 1; fi
done

rm -rf "$WORK"; mkdir -p "$LOG_DIR"; : > "$PID_FILE"

# 2. 임시 Kafka
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

# 3. 실행 jar
./gradlew --offline -q :matching-worker:bootJar :account-worker:bootJar > "$LOG_DIR/build.log" 2>&1 || { fail "bootJar 빌드 실패(로그 $LOG_DIR/build.log)"; exit 1; }
MATCHING_JAR=$(ls matching-worker/build/libs/*.jar | grep -v plain | head -1)
ACCOUNT_JAR=$(ls account-worker/build/libs/*.jar | grep -v plain | head -1)

start_app() {
    local name="$1" jar="$2" config="$3"
    java "${JAVA_OPENS[@]}" -jar "$jar" --spring.config.additional-location="file:$config" > "$LOG_DIR/$name.log" 2>&1 &
    echo $! >> "$PID_FILE"
    echo "$name 기동(PID $!, 로그 $LOG_DIR/$name.log)"
}

tool() { ./gradlew --offline -q :account-worker:u10Tool -Pu10Args="$*" 2>&1; }

# 4. 매칭 shard 둘 기동 → 종목 slot 64개 배정 대기
start_app matching-0 "$MATCHING_JAR" loadtest/u10/u10-matching-0.yml
start_app matching-1 "$MATCHING_JAR" loadtest/u10/u10-matching-1.yml
PICKED=""
for _ in $(seq 1 40); do
    PICKED=$(tool pick localhost:9092 64) && break
    sleep 3
done
echo "$PICKED"
echo "$PICKED" | grep -q "ASSIGNED 64/64" || { fail "종목 slot 64개가 배정되지 않았다"; exit 1; }
X=$(echo "$PICKED" | sed -n 's/.*X=\([0-9]*\) .*/\1/p'); XEND=$(echo "$PICKED" | sed -n 's/.*XEND=\(.*\) Y=.*/\1/p')
Y=$(echo "$PICKED" | sed -n 's/.* Y=\([0-9]*\) .*/\1/p'); YEND=$(echo "$PICKED" | sed -n 's/.*YEND=\(.*\)$/\1/p')
[ -n "$X" ] && [ -n "$Y" ] || { fail "서로 다른 매칭 shard의 종목 둘을 고르지 못했다"; exit 1; }
shard_of() { case "$1" in *20020) echo 0;; *20021) echo 1;; *) echo "?";; esac; }
XS=$(shard_of "$XEND"); YS=$(shard_of "$YEND")

# 5. 계좌 워커 기동
start_app account "$ACCOUNT_JAR" loadtest/u10/u10-account.yml
for _ in $(seq 1 60); do grep -q "Started AccountWorkerApplication" "$LOG_DIR/account.log" && break; sleep 2; done
grep -q "Started AccountWorkerApplication" "$LOG_DIR/account.log" || { fail "계좌 워커가 기동하지 않았다(로그 $LOG_DIR/account.log)"; exit 1; }

# 6. 주문: 계좌 90001이 X와 Y를 각각 매수(매도가 없어 호가창에 남는다)
tool send aeron:udp?endpoint=localhost:20040 90001 "$X" BUY 10000 1 u10-x | tail -1
tool send aeron:udp?endpoint=localhost:20040 90001 "$Y" BUY 10000 1 u10-y | tail -1
sleep 3

# 7. 정상 종료 → 스냅샷 기록
stop_pids

# 8. 스냅샷 단언
dump() { ./gradlew --offline -q :matching-worker:u10MatchingDump -Pu10Args="$1" 2>&1; }
D0=$(dump /tmp/u10/matching-0); D1=$(dump /tmp/u10/matching-1)
echo "--- 매칭 shard 0 스냅샷"; echo "$D0"; echo "--- 매칭 shard 1 스냅샷"; echo "$D1"
DUMPS=("$D0" "$D1")
OK=1
check() { # <desc> <shard> <stock> <expected resting>
    local line
    line=$(echo "${DUMPS[$2]}" | grep "BOOK stock=$3 " || true)
    local got=0; [ -n "$line" ] && got=$(echo "$line" | sed -n 's/.*resting=\([0-9]*\).*/\1/p')
    if [ "$got" = "$4" ]; then echo "통과: $1 (매칭 shard $2, 종목 $3, 대기 주문 $got)"; else echo "실패: $1 (매칭 shard $2, 종목 $3, 기대 $4 실제 $got)"; OK=0; fi
}
[ "$XS" != "?" ] && [ "$YS" != "?" ] && [ "$XS" != "$YS" ] || { fail "X와 Y의 담당 shard를 알 수 없거나 같다: X=$XEND Y=$YEND"; exit 1; }
OTHER_X=$((1 - XS)); OTHER_Y=$((1 - YS))
check "종목 X 주문이 담당 매칭 shard의 호가창에 있다" "$XS" "$X" 1
check "종목 Y 주문이 담당 매칭 shard의 호가창에 있다" "$YS" "$Y" 1
check "종목 X 주문이 다른 매칭 shard에는 없다" "$OTHER_X" "$X" 0
check "종목 Y 주문이 다른 매칭 shard에는 없다" "$OTHER_Y" "$Y" 0

[ "$OK" = 1 ] && RESULT=0
exit "$RESULT"
