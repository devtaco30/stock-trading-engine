#!/usr/bin/env bash
set -uo pipefail
# S0(가) 계좌 수 변형 측정. 같은 종목당 주문 수 N을 계좌 ACCTS개로 나눠 접수 속도와 jstack 상위 프레임을 비교한다.
# 사용: ACCTS=<계좌 수> loadtest/s0/probe.sh <종목당 주문 수> [송신 뒤 대기 초]   (시험 잠금 안에서 실행)
# 전제: 작업 트리의 account-worker U10Tool이 BULK_ACCOUNTS(계좌 90002부터 i % 계좌 수로 분산)와 송신 위치 확인 출력을 가진 상태(ec 브랜치 65b3135 이후).
# 송신 완료 확인은 U10Tool의 SENDER_POSITION_REACHED / _NOT_REACHED(종료 코드 3) / _COUNTER_NOT_FOUND(종료 코드 4)를 따른다. 종료 코드가 0이 아니면 이 스크립트도 그 코드로 끝난다.
# 계좌가 300개를 넘으면 시딩이 명령행 한도(macOS ARG_MAX 1,048,576바이트)에 닿으므로 설정 파일로 준다.
# 자체 Kafka(도커, 19092)와 /tmp/probe 아래 작업 폴더를 쓰고 끝나면 정리한다.
cd "$(dirname "$0")/../.."
N=${1:?}
W=/tmp/probe; LOG=$W/logs; rm -rf $W; mkdir -p $LOG; : > $W/pids
JO=(--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED)
KB=--spring.kafka.bootstrap-servers=localhost:19092
# u10-account.yml이 ${MAX_OPEN_ORDERS}를 읽는다. 기본값을 두지 않으므로 값이 없으면 앱을 띄우기 전에 멈춘다(u10/lib.sh start_account와 같은 검사).
: "${MAX_OPEN_ORDERS:?MAX_OPEN_ORDERS를 지정해 주세요(계좌당 미체결 주문 수 상한, 기본값 없음. 상한 없는 비교용은 1000000)}"
export MAX_OPEN_ORDERS
docker rm -f probe-kafka >/dev/null 2>&1
docker run -d --name probe-kafka -p 19092:19092 \
 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:19093 \
 -e KAFKA_LISTENERS=PLAINTEXT://:19092,CONTROLLER://:19093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092 \
 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
 -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837 >/dev/null
for _ in $(seq 1 60); do docker exec probe-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list >/dev/null 2>&1 && break; sleep 1; done
cleanup(){ while read -r p; do kill $p 2>/dev/null; done < $W/pids; sleep 3; docker rm -f probe-kafka >/dev/null 2>&1; }
trap cleanup EXIT
./gradlew --offline -q :matching-worker:bootJar :account-worker:bootJar > $LOG/build.log 2>&1 || { echo BUILD_FAIL; tail -20 $LOG/build.log; exit 1; }
MJ=$(ls matching-worker/build/libs/*.jar | grep -v plain | head -1); AJ=$(ls account-worker/build/libs/*.jar | grep -v plain | head -1)
sed "s#/tmp/u10#$W#" loadtest/u10/u10-matching-0.yml > $W/m0.yml
sed "s#/tmp/u10#$W#" loadtest/u10/u10-account.yml > $W/acc.yml
java "${JO[@]}" -jar $MJ --spring.config.additional-location=file:$W/m0.yml $KB > $LOG/matching-0.log 2>&1 & echo $! >> $W/pids; MP=$!
for _ in $(seq 1 60); do grep -q "Started MatchingWorkerApplication" $LOG/matching-0.log && break; sleep 2; done
sleep 8
STOCKS=$(./gradlew --offline -q :account-worker:u10Tool -Pu10Args="codes-many 64 32 3" 2>&1 | grep '^HIGH=' | tail -1 | sed 's/^HIGH=//' | tr ',' ' ')
echo "종목: $STOCKS"
ACCTS=${ACCTS:-1}
export BULK_ACCOUNTS=$ACCTS
SEED=()
CFG=file:$W/acc.yml
if [ "$ACCTS" -le 300 ]; then
  SEED+=(--account-worker.seed-accounts[0].account-id=90001 --account-worker.seed-accounts[0].balance=100000000000 --account-worker.seed-accounts[0].margin-rate=0.40)
  for k in $(seq 1 $ACCTS); do
    SEED+=(--account-worker.seed-accounts[$k].account-id=$((90001+k)) --account-worker.seed-accounts[$k].balance=100000000 --account-worker.seed-accounts[$k].margin-rate=0.40)
    for s in $STOCKS; do SEED+=(--account-worker.seed-accounts[$k].holdings.$s=$((N+5))); done
  done
else
  python3 -I "$(dirname "$0")/make-seed.py" "$W/seed.yml" "$ACCTS" "$((N+5))" $STOCKS
  CFG=file:$W/acc.yml,file:$W/seed.yml
fi
java "${JO[@]}" -jar $AJ --spring.config.additional-location=$CFG $KB --account-worker.metrics.enabled=true ${SEED[@]+"${SEED[@]}"} > $LOG/account.log 2>&1 & echo $! >> $W/pids; AP=$!
for _ in $(seq 1 60); do grep -q "Started AccountWorkerApplication" $LOG/account.log && break; sleep 2; done
echo "MP=$MP AP=$AP"; echo "$MP $AP" > $W/jvm
(
 for i in $(seq 1 400); do
   t=$(date +%s.%N)
   jstack $AP > $W/js-acc-$i.txt 2>&1; jstack $MP > $W/js-mat-$i.txt 2>&1
   sleep 1.5
 done
) & SAMP=$!
date +%s.%N > $W/send-start
SEND_OUT=$(./gradlew --offline -q :account-worker:u10Tool -Pu10Args="send-bulk aeron:udp?endpoint=localhost:20040 90002 SELL 20000 $N probe $STOCKS" 2>&1)
echo "$SEND_OUT" | grep -E "SENT_BULK|OFFER_FAILED|NOT_CONNECTED|SENDER_POSITION"
# gradle은 U10Tool의 종료 코드(3, 4)를 그대로 돌려주지 않으므로 송신 위치 도달 출력이 없으면 실패로 끝낸다.
if ! grep -q SENDER_POSITION_REACHED <<<"$SEND_OUT"; then echo SEND_NOT_CONFIRMED; exit 3; fi
date +%s.%N > $W/send-end
sleep ${2:-20}; date +%s.%N > $W/settle-end
kill $SAMP 2>/dev/null
echo done
