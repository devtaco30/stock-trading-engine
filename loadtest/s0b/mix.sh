#!/usr/bin/env bash
set -uo pipefail
# 체결 섞기 균형 실행(2026-11-04, ec). 계좌당 미체결 C를 같게 맞춘 채 주문 속도 R로 매도·매수 쌍을 보내, C가 일정하게 유지되는지와 접수 속도를 잰다.
# 사용(시험 잠금 안에서): ACCTS=<매도 계좌 수> BUYERS=<매수 전용 계좌 수> C=<계좌당 미체결, 종목 수 3의 배수> RATE=<초당 주문 수, 매도+매수 합> SECONDS_STEADY=<유지 초> OUT=<결과 폴더> loadtest/s0b/mix.sh
#   1) 쌓기: send-bulk로 종목마다 C x N / 3건의 매도를 모두 같은 가격(--price-step=0)으로 보낸다(주문 i는 계좌 90002 + i % N).
#   2) 유지: send-paced로 종목마다 「매도 1건 → 매수 1건」을 초당 RATE건으로 보낸다. 매수 1건이 그 종목에서 가장 오래된 매도 1건을 체결시킨다.
#   RATE x SECONDS_STEADY는 6(종목 3 x 매도·매수 2)의 배수여야 한다. 가격은 PRICE(기본 20000). 매수 전용 계좌는 90002 + N부터 BUYERS개, 시작 잔고 BUYER_BALANCE(기본 100억)이고
#   「3 x (체결 수 / BUYERS) x PRICE x 증거금률 0.40」보다 작으면 거부한다. 계좌 보유 수량은 (C/3) + ceil(종목당 쌍 수 / N) + 5.
#   DRY_RUN=1이면 인프라와 앱을 띄우지 않고 계획과 명령만 출력한다(시험용). ACCOUNT_JAR·MATCHING_JAR·SKIP_BUILD·ACCEPT_WAIT_SECS는 ladder.sh와 같다.
#   HEAP_SAMPLE=1이면 계좌 JVM의 힙 사용량을 jstat으로 2초마다 OUT/heap.jstat에 남긴다. 거부 로그는 ERROR로 올린다(ladder.sh와 같은 이유).
cd "$(dirname "$0")/../.."
N=${ACCTS:-}; BUYERS=${BUYERS:-}; C=${C:-}; RATE=${RATE:-}; SEC=${SECONDS_STEADY:-}; OUT=${OUT:?}
PRICE=${PRICE:-20000}; BUYER_BALANCE=${BUYER_BALANCE:-10000000000}
if [ -z "$N" ] || [ -z "$BUYERS" ] || [ -z "$C" ] || [ -z "$RATE" ] || [ -z "$SEC" ]; then echo "ACCTS, BUYERS, C, RATE, SECONDS_STEADY가 필요하다" >&2; exit 1; fi
STOCK_COUNT=3
if [ $((C % STOCK_COUNT)) -ne 0 ]; then echo "C는 종목 수 3의 배수여야 한다: $C" >&2; exit 1; fi
if [ $((RATE * SEC % (2 * STOCK_COUNT))) -ne 0 ] || [ $((RATE * SEC)) -lt 6 ]; then echo "RATE x SECONDS_STEADY는 6의 배수여야 한다: $((RATE * SEC))" >&2; exit 1; fi
PREFILL=$((C * N / STOCK_COUNT))
PAIRS=$((RATE * SEC / (2 * STOCK_COUNT)))
HOLD=$((C / STOCK_COUNT + (PAIRS + N - 1) / N + 5))
TOTAL_TRADES=$((PAIRS * STOCK_COUNT))
TOTAL_ORDERS=$((PREFILL * STOCK_COUNT + PAIRS * STOCK_COUNT * 2))
NEED=$(( (3 * TOTAL_TRADES + BUYERS - 1) / BUYERS * PRICE * 4 / 10 ))
if [ "$BUYER_BALANCE" -lt "$NEED" ]; then echo "BUYER_BALANCE $BUYER_BALANCE 가 필요량 $NEED 보다 작다" >&2; exit 1; fi
CHANNEL="aeron:udp?endpoint=localhost:20040"
BULK_CMD="send-bulk $CHANNEL 90002 SELL $PRICE $PREFILL pre --price-step=0"
PACED_CMD="send-paced $CHANNEL 90002 $N $((90002 + N)) $BUYERS $PRICE $PREFILL $PAIRS $RATE mix"
mkdir -p "$OUT"
printf 'ACCTS=%s BUYERS=%s C=%s RATE=%s SECONDS_STEADY=%s PRICE=%s PREFILL_PER_STOCK=%s PAIRS_PER_STOCK=%s HOLD=%s BUYER_BALANCE=%s TOTAL_ORDERS=%s TOTAL_TRADES=%s\n' \
  "$N" "$BUYERS" "$C" "$RATE" "$SEC" "$PRICE" "$PREFILL" "$PAIRS" "$HOLD" "$BUYER_BALANCE" "$TOTAL_ORDERS" "$TOTAL_TRADES" > "$OUT/params.txt"
LOGARG="--logging.level.com.flab.stocktradingengine.account.worker.listener.LoggingAccountResultListener=ERROR"

if [ "${DRY_RUN:-0}" = 1 ]; then
  cat "$OUT/params.txt" | tr ' ' '\n'
  echo "BULK_ACCOUNTS=$N 계좌 수, 로그 $LOGARG"
  echo "쌓기 -> $BULK_CMD <종목 3개>"
  echo "유지 -> $PACED_CMD <종목 3개>"
  exit 0
fi

for p in 20020 20040 20060 8010 8011 8012; do lsof -nP -iUDP:$p >/dev/null 2>&1 && { echo "PORT_BUSY UDP $p"; exit 5; }; done
lsof -nP -iTCP:19092 -sTCP:LISTEN >/dev/null 2>&1 && { echo "PORT_BUSY TCP 19092"; exit 5; }
W=/tmp/s0b; LOG=$W/logs; rm -rf $W; mkdir -p $LOG; : > $W/pids
JO=(--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED)
KB=--spring.kafka.bootstrap-servers=localhost:19092
docker rm -f s0b-kafka >/dev/null 2>&1
docker run -d --name s0b-kafka -p 19092:19092 \
 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:19093 \
 -e KAFKA_LISTENERS=PLAINTEXT://:19092,CONTROLLER://:19093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092 \
 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
 -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837 >/dev/null
for _ in $(seq 1 60); do docker exec s0b-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list >/dev/null 2>&1 && break; sleep 1; done
cleanup(){ while read -r p; do kill $p 2>/dev/null; done < $W/pids; sleep 3; while read -r p; do kill -9 $p 2>/dev/null; done < $W/pids; docker rm -f s0b-kafka >/dev/null 2>&1
  rm -rf "$OUT/logs"; cp -R $LOG "$OUT/logs" 2>/dev/null; cp $W/steps.log $W/paced.out "$OUT/" 2>/dev/null; }
trap cleanup EXIT
if [ "${SKIP_BUILD:-0}" != 1 ]; then
  ./gradlew --offline -q :matching-worker:bootJar :account-worker:bootJar > $LOG/build.log 2>&1 || { echo BUILD_FAIL; tail -20 $LOG/build.log; exit 1; }
fi
MJ=${MATCHING_JAR:-$(ls matching-worker/build/libs/*.jar | grep -v plain | head -1)}; AJ=${ACCOUNT_JAR:-$(ls account-worker/build/libs/*.jar | grep -v plain | head -1)}
sed "s#/tmp/u10#$W#" loadtest/u10/u10-matching-0.yml > $W/m0.yml
sed "s#/tmp/u10#$W#" loadtest/u10/u10-account.yml > $W/acc.yml
java "${JO[@]}" -Xlog:gc*:file=$LOG/gc-matching.log:time,uptime -jar $MJ --spring.config.additional-location=file:$W/m0.yml $KB > $LOG/matching-0.log 2>&1 & echo $! >> $W/pids
for _ in $(seq 1 60); do grep -q "Started MatchingWorkerApplication" $LOG/matching-0.log && break; sleep 2; done
grep -q "Started MatchingWorkerApplication" $LOG/matching-0.log || { echo START_FAIL matching; exit 6; }
sleep 8
STOCKS=$(./gradlew --offline -q :account-worker:u10Tool -Pu10Args="codes-many 64 32 3" 2>&1 | grep '^HIGH=' | tail -1 | sed 's/^HIGH=//' | tr ',' ' ')
echo "종목: $STOCKS"
export BULK_ACCOUNTS=$N
BUYER_ACCOUNTS=$BUYERS BUYER_BALANCE=$BUYER_BALANCE python3 -I loadtest/s0b/make-seed.py "$W/seed.yml" "$N" "$HOLD" $STOCKS || exit 1
java "${JO[@]}" -Xlog:gc*:file=$LOG/gc-account.log:time,uptime -jar $AJ --spring.config.additional-location=file:$W/acc.yml,file:$W/seed.yml $KB \
  --account-worker.metrics.enabled=true $LOGARG > $LOG/account.log 2>&1 & AP=$!; echo $AP >> $W/pids
for _ in $(seq 1 ${START_WAIT_TRIES:-60}); do grep -q "Started AccountWorkerApplication" $LOG/account.log && break; sleep 2; done
grep -q "Started AccountWorkerApplication" $LOG/account.log || { echo START_FAIL account; exit 6; }
if [ "${HEAP_SAMPLE:-0}" = 1 ]; then jstat -gc $AP 2000 > "$OUT/heap.jstat" 2>&1 & echo $! >> $W/pids; fi
accepted_total() { grep -o "acceptedTotal=[0-9]*" $LOG/account.log | tail -1 | sed 's/acceptedTotal=//'; }
rejected_total() { grep "metrics accepted=" $LOG/account.log | sed 's/.*rejected=\([0-9]*\) .*/\1/' | awk '{s+=$1} END{print s+0}'; }
wait_accepted() { for _ in $(seq 1 "${ACCEPT_WAIT_SECS:-180}"); do [ "$(accepted_total)" = "$1" ] && return 0; sleep 1; done; return 1; }
: > $W/steps.log
echo "$(date +%s.%N) START PREFILL per_stock=$PREFILL" >> $W/steps.log
OUT1=$(./gradlew --offline -q :account-worker:u10Tool -Pu10Args="$BULK_CMD $STOCKS" 2>&1)
echo "$OUT1" | grep -E "SENT_BULK|OFFER_FAILED|NOT_CONNECTED|SENDER_POSITION"
grep -q SENDER_POSITION_REACHED <<<"$OUT1" || { echo SEND_NOT_CONFIRMED; exit 3; }
EXPECT=$((PREFILL * STOCK_COUNT))
wait_accepted $EXPECT || { echo "ACCEPTED_MISMATCH 쌓기 기대 $EXPECT 실제 $(accepted_total)"; exit 4; }
echo "$(date +%s.%N) END PREFILL" >> $W/steps.log
sleep 3
echo "$(date +%s.%N) START STEADY rate=$RATE seconds=$SEC pairs_per_stock=$PAIRS" >> $W/steps.log
OUT2=$(./gradlew --offline -q :account-worker:u10Tool -Pu10Args="$PACED_CMD $STOCKS" 2>&1)
echo "$OUT2" > $W/paced.out
echo "$OUT2" | grep -E "PACED_START|PACED_DONE|OFFER_FAILED|NOT_CONNECTED|SENDER_POSITION"
grep -q SENDER_POSITION_REACHED <<<"$OUT2" || { echo SEND_NOT_CONFIRMED; exit 3; }
EXPECT=$((EXPECT + PAIRS * STOCK_COUNT * 2))
wait_accepted $EXPECT || { echo "ACCEPTED_MISMATCH 유지 기대 $EXPECT 실제 $(accepted_total)"; exit 4; }
echo "$(date +%s.%N) END STEADY" >> $W/steps.log
REJ=$(rejected_total)
[ "$REJ" = 0 ] || { echo "REJECTED_NONZERO rejected=$REJ"; exit 4; }
echo "VALID accepted=$EXPECT rejected=$REJ"
