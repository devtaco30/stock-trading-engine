#!/usr/bin/env bash
set -uo pipefail
# S0 보강 실행(2026-10-30, ec). 계좌 수 N을 나눠 쌓인 매도 수 M이 접수 속도에 주는 영향을, 쌓은 뒤 고정 M에서(PREFILL) 또는 로그·GC 조건을 바꿔 잰다.
# 사용(시험 잠금 안에서): ACCTS=<N> PREFILL_PER_STOCK=<n> PER_STOCK=<n> ORDER_LOG=on|off OUT=<결과 폴더> loadtest/s0b/run.sh
#   1단계: PREFILL_PER_STOCK건씩 종목 3개에 매도를 쌓고 접수가 끝날 때까지 기다린다(0이면 건너뜀).
#   2단계: PER_STOCK건씩 더 보낸다. 고정 m 측정은 이 단계만 본다(analyze.py).
# 주문 i는 계좌 90002 + (i % N)로 간다. 가격은 20000+i 이고 수량 1, 매도만이라 체결이 없다. 계좌마다 종목별 보유는 (두 단계에서 받을 수 + 5)로 시딩한다.
# ACCOUNT_JAR·MATCHING_JAR로 실행할 jar를 직접 고를 수 있다(변형 빌드 비교용). SKIP_BUILD=1이면 bootJar 빌드를 건너뛴다(한 번 빌드한 뒤 여러 번 돌릴 때). 자체 Kafka(도커 19092)와 /tmp/s0b 작업 폴더를 쓰고 끝나면 정리한다. 종료 코드: 0 유효, 3 송신 위치 미확인, 4 접수 합계 어긋남(무효), 그 밖은 실패.
cd "$(dirname "$0")/../.."
N=${ACCTS:?}; PREFILL=${PREFILL_PER_STOCK:-0}; PER=${PER_STOCK:?}; OLOG=${ORDER_LOG:-on}; OUT=${OUT:?}
STOCK_COUNT=3
for p in 20020 20040 20060 8010 8011 8012; do lsof -nP -iUDP:$p >/dev/null 2>&1 && { echo "PORT_BUSY UDP $p"; exit 5; }; done
lsof -nP -iTCP:19092 -sTCP:LISTEN >/dev/null 2>&1 && { echo "PORT_BUSY TCP 19092"; exit 5; }
W=/tmp/s0b; LOG=$W/logs; rm -rf $W; mkdir -p $LOG "$OUT"; : > $W/pids
JO=(--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED)
KB=--spring.kafka.bootstrap-servers=localhost:19092
# u10-account.yml이 ${MAX_OPEN_ORDERS}를 읽는다. 기본값을 두지 않으므로 값이 없으면 앱을 띄우기 전에 멈춘다(u10/lib.sh start_account와 같은 검사).
: "${MAX_OPEN_ORDERS:?MAX_OPEN_ORDERS를 지정해 주세요(계좌당 미체결 주문 수 상한, 기본값 없음. 상한 없는 비교용은 1000000)}"
export MAX_OPEN_ORDERS
docker rm -f s0b-kafka >/dev/null 2>&1
docker run -d --name s0b-kafka -p 19092:19092 \
 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:19093 \
 -e KAFKA_LISTENERS=PLAINTEXT://:19092,CONTROLLER://:19093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092 \
 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
 -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837 >/dev/null
for _ in $(seq 1 60); do docker exec s0b-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list >/dev/null 2>&1 && break; sleep 1; done
cleanup(){ while read -r p; do kill $p 2>/dev/null; done < $W/pids; sleep 3; docker rm -f s0b-kafka >/dev/null 2>&1
  mkdir -p "$OUT"; rm -rf "$OUT/logs"; cp -R $LOG "$OUT/logs" 2>/dev/null; cp $W/prefill-start $W/prefill-end $W/measure-start $W/measure-end "$OUT/" 2>/dev/null; [ -d $W/js ] && cp -R $W/js "$OUT/js"
  printf 'ACCTS=%s PREFILL_PER_STOCK=%s PER_STOCK=%s ORDER_LOG=%s HOLD=%s\n' "$N" "$PREFILL" "$PER" "$OLOG" "${HOLD:-0}" > "$OUT/params.txt"; }
trap cleanup EXIT
if [ "${SKIP_BUILD:-0}" != 1 ]; then
  ./gradlew --offline -q :matching-worker:bootJar :account-worker:bootJar > $LOG/build.log 2>&1 || { echo BUILD_FAIL; tail -20 $LOG/build.log; exit 1; }
fi
MJ=${MATCHING_JAR:-$(ls matching-worker/build/libs/*.jar | grep -v plain | head -1)}; AJ=${ACCOUNT_JAR:-$(ls account-worker/build/libs/*.jar | grep -v plain | head -1)}
sed "s#/tmp/u10#$W#" loadtest/u10/u10-matching-0.yml > $W/m0.yml
sed "s#/tmp/u10#$W#" loadtest/u10/u10-account.yml > $W/acc.yml
java "${JO[@]}" -Xlog:gc*:file=$LOG/gc-matching.log:time,uptime -jar $MJ --spring.config.additional-location=file:$W/m0.yml $KB > $LOG/matching-0.log 2>&1 & echo $! >> $W/pids; MP=$!
for _ in $(seq 1 60); do grep -q "Started MatchingWorkerApplication" $LOG/matching-0.log && break; sleep 2; done
grep -q "Started MatchingWorkerApplication" $LOG/matching-0.log || { echo START_FAIL matching; exit 6; }
sleep 8
STOCKS=$(./gradlew --offline -q :account-worker:u10Tool -Pu10Args="codes-many 64 32 3" 2>&1 | grep '^HIGH=' | tail -1 | sed 's/^HIGH=//' | tr ',' ' ')
echo "종목: $STOCKS"
export BULK_ACCOUNTS=$N
HOLD=$(( (PREFILL + PER + N - 1) / N + 5 ))   # 계좌 하나가 한 종목에서 받는 최대 주문 수 + 5
python3 -I "$(dirname "$0")/make-seed.py" "$W/seed.yml" "$N" "$HOLD" $STOCKS || exit 1
LOGARG=()
[ "$OLOG" = off ] && LOGARG=(--logging.level.com.flab.stocktradingengine.account.worker.listener.LoggingAccountResultListener=WARN)
java "${JO[@]}" -Xlog:gc*:file=$LOG/gc-account.log:time,uptime -jar $AJ --spring.config.additional-location=file:$W/acc.yml,file:$W/seed.yml $KB \
  --account-worker.metrics.enabled=true ${LOGARG[@]+"${LOGARG[@]}"} > $LOG/account.log 2>&1 & echo $! >> $W/pids; AP=$!
for _ in $(seq 1 60); do grep -q "Started AccountWorkerApplication" $LOG/account.log && break; sleep 2; done
grep -q "Started AccountWorkerApplication" $LOG/account.log || { echo START_FAIL account; exit 6; }
echo "MP=$MP AP=$AP"
accepted_total() { grep -o "acceptedTotal=[0-9]*" $LOG/account.log | tail -1 | sed 's/acceptedTotal=//'; }
send() { # <종목당 주문 수> <prefix> -> 출력에서 송신 위치 도달을 확인한다
  local out; out=$(./gradlew --offline -q :account-worker:u10Tool -Pu10Args="send-bulk aeron:udp?endpoint=localhost:20040 90002 SELL 20000 $1 $2 $STOCKS" 2>&1)
  echo "$out" | grep -E "SENT_BULK|OFFER_FAILED|NOT_CONNECTED|SENDER_POSITION"
  grep -q SENDER_POSITION_REACHED <<<"$out"
}
wait_accepted() { # <기대 누적> — 보고기의 acceptedTotal이 기대값에 닿을 때까지(최대 ACCEPT_WAIT_SECS초, 기본 180초. 느린 실행은 길게 준다)
  for _ in $(seq 1 "${ACCEPT_WAIT_SECS:-180}"); do [ "$(accepted_total)" = "$1" ] && return 0; sleep 1; done; return 1
}
EXPECT=0
if [ "$PREFILL" -gt 0 ]; then
  date +%s.%N > $W/prefill-start
  send "$PREFILL" pre || { echo SEND_NOT_CONFIRMED; exit 3; }
  EXPECT=$((PREFILL * STOCK_COUNT))
  wait_accepted $EXPECT || { echo "ACCEPTED_MISMATCH 쌓기 단계 기대 $EXPECT 실제 $(accepted_total)"; exit 4; }
  sleep 3
  date +%s.%N > $W/prefill-end
fi
date +%s.%N > $W/measure-start
# JSTACK_INTERVAL(초)이 있으면 측정 단계 동안 그 간격으로 계좌 JVM의 thread dump를 뜬다(jstack 한 번이 수십~수백 ms 걸려 실제 간격은 더 길다). 덤프마다 시각 파일을 남긴다.
SAMPLER=""
if [ -n "${JSTACK_INTERVAL:-}" ]; then
  mkdir -p $W/js
  ( i=0; while true; do i=$((i+1)); date +%s.%N > $W/js/$i.ts; jstack $AP > $W/js/$i.txt 2>&1; sleep $JSTACK_INTERVAL; done ) & SAMPLER=$!
  echo $SAMPLER >> $W/pids
fi
send "$PER" run || { echo SEND_NOT_CONFIRMED; exit 3; }
EXPECT=$((EXPECT + PER * STOCK_COUNT))
wait_accepted $EXPECT || { echo "ACCEPTED_MISMATCH 기대 $EXPECT 실제 $(accepted_total)"; exit 4; }
date +%s.%N > $W/measure-end
[ -n "$SAMPLER" ] && kill $SAMPLER 2>/dev/null
sleep 3
echo "VALID accepted=$EXPECT"
