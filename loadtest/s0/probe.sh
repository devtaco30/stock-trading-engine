#!/usr/bin/env bash
set -uo pipefail
# S0(가) 계좌 수 변형 측정. 같은 종목당 주문 수 N을 계좌 ACCTS개로 나눠 접수 속도와 jstack 상위 프레임을 비교한다.
# 사용: ACCTS=<계좌 수> loadtest/s0/probe.sh <종목당 주문 수> [송신 뒤 대기 초]   (시험 잠금 안에서 실행)
# 전제: 이 저장소 작업 트리에 loadtest/s0/u10tool-bulk-accounts-and-drain.patch를 적용한 상태여야 한다
#   (패치는 ab6b667 기준이라 그 뒤 바뀐 U10Tool에는 손으로 합쳐야 할 수 있다. account-worker의 U10Tool에 BULK_ACCOUNTS와 송신 완료 대기를 더하는 변경. U10Tool은 ec 소관이라 이 커밋은 패치 파일만 둔다).
# 자체 Kafka(도커, 19092)와 /tmp/probe 아래 작업 폴더를 쓰고 끝나면 정리한다.
cd "$(dirname "$0")/../.."
N=${1:?}
W=/tmp/probe; LOG=$W/logs; rm -rf $W; mkdir -p $LOG; : > $W/pids
JO=(--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED)
KB=--spring.kafka.bootstrap-servers=localhost:19092
docker rm -f probe-kafka >/dev/null 2>&1
docker run -d --name probe-kafka -p 19092:19092 \
 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:19093 \
 -e KAFKA_LISTENERS=PLAINTEXT://:19092,CONTROLLER://:19093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092 \
 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
 -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka:latest >/dev/null
for _ in $(seq 1 60); do docker exec probe-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list >/dev/null 2>&1 && break; sleep 1; done
cleanup(){ while read -r p; do kill $p 2>/dev/null; done < $W/pids; sleep 3; docker rm -f probe-kafka >/dev/null 2>&1; }
trap cleanup EXIT
[ -f $W/../probe-built ] || true
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
SEED=(--account-worker.seed-accounts[0].account-id=90001 --account-worker.seed-accounts[0].balance=100000000000 --account-worker.seed-accounts[0].margin-rate=0.40)
for k in $(seq 1 $ACCTS); do
  SEED+=(--account-worker.seed-accounts[$k].account-id=$((90001+k)) --account-worker.seed-accounts[$k].balance=100000000 --account-worker.seed-accounts[$k].margin-rate=0.40)
  for s in $STOCKS; do SEED+=(--account-worker.seed-accounts[$k].holdings.$s=$((N+5))); done
done
java "${JO[@]}" -jar $AJ --spring.config.additional-location=file:$W/acc.yml $KB --account-worker.metrics.enabled=true "${SEED[@]}" > $LOG/account.log 2>&1 & echo $! >> $W/pids; AP=$!
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
./gradlew --offline -q :account-worker:u10Tool -Pu10Args="send-bulk aeron:udp?endpoint=localhost:20040 90002 SELL 20000 $N probe $STOCKS" 2>&1 | grep -E "SENT_BULK|OFFER_FAILED|NOT_CONNECTED"
date +%s.%N > $W/send-end
sleep ${2:-20}; date +%s.%N > $W/settle-end
kill $SAMP 2>/dev/null
echo done
