#!/usr/bin/env bash
set -uo pipefail
# 체결 섞기 균형 실행(2026-11-04, ec). 계좌당 미체결 C를 같게 맞춘 채 주문 속도 R로 매도·매수 쌍을 보내, C가 일정하게 유지되는지와 접수 속도를 잰다.
# 사용(시험 잠금 안에서): ACCTS=<매도 계좌 수> BUYERS=<매수 전용 계좌 수> C=<계좌당 미체결, 종목 수 3의 배수> RATE=<초당 주문 수, 매도+매수 합> SECONDS_STEADY=<유지 초> OUT=<결과 폴더> loadtest/s0b/mix.sh
#   1) 쌓기: send-bulk로 종목마다 C x N / 3건의 매도를 모두 같은 가격(--price-step=0)으로 보낸다(주문 i는 계좌 90002 + i % N).
#   2) 유지: send-paced로 종목마다 「매도 1건 → 매수 1건」을 초당 RATE건으로 보낸다. 매수 1건이 그 종목에서 가장 오래된 매도 1건을 체결시킨다.
#   RATE x SECONDS_STEADY는 6(종목 3 x 매도·매수 2)의 배수여야 한다. 가격은 PRICE(기본 20000). 매수 전용 계좌는 90002 + N부터 BUYERS개, 시작 잔고 BUYER_BALANCE(기본 100억)이고
#   「3 x (체결 수 / BUYERS) x PRICE x 증거금률 0.40」보다 작으면 거부한다. 계좌 보유 수량은 (C/3) + ceil(종목당 쌍 수 / N) + 5.
#   DRY_RUN=1이면 인프라와 앱을 띄우지 않고 계획과 명령만 출력한다(시험용). ACCOUNT_JAR·MATCHING_JAR·SKIP_BUILD·ACCEPT_WAIT_SECS는 ladder.sh와 같다.
#   STOCK_COUNT=<종목 수>(기본 3): 3이 아니면 slot 전체에서 그 수만큼 고르고 OUT/slots.txt에 종목별 slot을 남긴다. C는 종목 수의 배수, RATE x 초는 종목 수 x 2의 배수.
#   CPU_SAMPLE=1이면 계좌·매칭·발신기 JVM의 누적 CPU 시간을 2초마다 OUT/cpu.log에(cpu_sample.sh, 요약은 cpu_summary.py). THREAD_CPU=1이면 유지 구간 1/3 지점에 계좌 JVM의 jcmd Thread.print를 10초 간격으로 두 번(OUT/threads-1.txt, threads-2.txt).
#   ACCOUNT_WORKERS=<1|2>(기본 1): 2이면 계좌 워커 둘을 정적 shard 배정(슬롯 256칸, 0~127은 A, 128~255는 B)으로 띄우고(A: 주문 20040·체결 20060·archive 제어 8100·폴더 account-0, B: 20041·20061·8101·account-1·snowflake node-id 5),
#   매칭 워커의 체결 목적지도 같은 정적 표(20060/20061)로 준다. 발신기는 계좌 슬롯으로 두 채널에 나눠 보낸다. 로그는 logs/account.log(A)와 logs/account-1.log(B). 1이면 기존 경로와 같다.
#   HEAP_SAMPLE=1이면 계좌 JVM의 힙 사용량을 jstat으로 2초마다 OUT/heap.jstat에 남긴다. 거부 로그는 ERROR로 올린다(ladder.sh와 같은 이유).
cd "$(dirname "$0")/../.."
N=${ACCTS:-}; BUYERS=${BUYERS:-}; C=${C:-}; RATE=${RATE:-}; SEC=${SECONDS_STEADY:-}; OUT=${OUT:?}
PRICE=${PRICE:-20000}; BUYER_BALANCE=${BUYER_BALANCE:-10000000000}
ACCOUNT_WORKERS=${ACCOUNT_WORKERS:-1}
if [ "$ACCOUNT_WORKERS" != 1 ] && [ "$ACCOUNT_WORKERS" != 2 ]; then echo "ACCOUNT_WORKERS는 1 또는 2여야 한다: $ACCOUNT_WORKERS" >&2; exit 1; fi
if [ -z "$N" ] || [ -z "$BUYERS" ] || [ -z "$C" ] || [ -z "$RATE" ] || [ -z "$SEC" ]; then echo "ACCTS, BUYERS, C, RATE, SECONDS_STEADY가 필요하다" >&2; exit 1; fi
STOCK_COUNT=${STOCK_COUNT:-3}
if [ $((C % STOCK_COUNT)) -ne 0 ]; then echo "C는 종목 수 ${STOCK_COUNT}의 배수여야 한다: $C" >&2; exit 1; fi
if [ $((RATE * SEC % (2 * STOCK_COUNT))) -ne 0 ] || [ $((RATE * SEC)) -lt $((2 * STOCK_COUNT)) ]; then echo "RATE x SECONDS_STEADY는 $((2 * STOCK_COUNT))의 배수여야 한다: $((RATE * SEC))" >&2; exit 1; fi
PREFILL=$((C * N / STOCK_COUNT))
PAIRS=$((RATE * SEC / (2 * STOCK_COUNT)))
HOLD=$((C / STOCK_COUNT + (PAIRS + N - 1) / N + 5))
TOTAL_TRADES=$((PAIRS * STOCK_COUNT))
TOTAL_ORDERS=$((PREFILL * STOCK_COUNT + PAIRS * STOCK_COUNT * 2))
NEED=$(( (3 * TOTAL_TRADES + BUYERS - 1) / BUYERS * PRICE * 4 / 10 ))
if [ "$BUYER_BALANCE" -lt "$NEED" ]; then echo "BUYER_BALANCE $BUYER_BALANCE 가 필요량 $NEED 보다 작다" >&2; exit 1; fi
CHANNEL="aeron:udp?endpoint=localhost:20040"
[ "$ACCOUNT_WORKERS" = 2 ] && CHANNEL="$CHANNEL,aeron:udp?endpoint=localhost:20041"
BULK_CMD="send-bulk $CHANNEL 90002 SELL $PRICE $PREFILL pre --price-step=0"
PACED_CMD="send-paced $CHANNEL 90002 $N $((90002 + N)) $BUYERS $PRICE $PREFILL $PAIRS $RATE mix"
mkdir -p "$OUT"
printf 'ACCOUNT_WORKERS=%s ACCTS=%s BUYERS=%s C=%s RATE=%s SECONDS_STEADY=%s PRICE=%s PREFILL_PER_STOCK=%s PAIRS_PER_STOCK=%s HOLD=%s BUYER_BALANCE=%s TOTAL_ORDERS=%s TOTAL_TRADES=%s\n' \
  "$ACCOUNT_WORKERS" "$N" "$BUYERS" "$C" "$RATE" "$SEC" "$PRICE" "$PREFILL" "$PAIRS" "$HOLD" "$BUYER_BALANCE" "$TOTAL_ORDERS" "$TOTAL_TRADES" > "$OUT/params.txt"
LOGARG="--logging.level.com.flab.stocktradingengine.account.worker.listener.LoggingAccountResultListener=ERROR"

if [ "${DRY_RUN:-0}" = 1 ]; then
  cat "$OUT/params.txt" | tr ' ' '\n'
  echo "BULK_ACCOUNTS=$N 계좌 수, 로그 $LOGARG"
  echo "쌓기 -> $BULK_CMD <종목 ${STOCK_COUNT}개>"
  echo "유지 -> $PACED_CMD <종목 ${STOCK_COUNT}개>"
  exit 0
fi

PORTS="20020 20040 20060 8010 8011 8012"; [ "$ACCOUNT_WORKERS" = 2 ] && PORTS="20020 20040 20041 20060 20061 8011 8100 8101"
for p in $PORTS; do lsof -nP -iUDP:$p >/dev/null 2>&1 && { echo "PORT_BUSY UDP $p"; exit 5; }; done
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
# 계좌 워커가 둘이면 매칭의 체결 목적지를 정적 슬롯 표(체결 수신 주소)로 준다(loadtest/run-e2e-v2.sh의 shard_args_fill과 같다).
MATCH_EXTRA=(); ACC_SHARD=()
if [ "$ACCOUNT_WORKERS" = 2 ]; then
  MATCH_EXTRA=("--shard-routing.slot-count=256" "--shard-routing.shards[0].endpoint=aeron:udp?endpoint=localhost:20060" "--shard-routing.shards[0].slot-from=0" "--shard-routing.shards[0].slot-to=127" "--shard-routing.shards[1].endpoint=aeron:udp?endpoint=localhost:20061" "--shard-routing.shards[1].slot-from=128" "--shard-routing.shards[1].slot-to=255")
  ACC_SHARD=("--shard-routing.slot-count=256" "--shard-routing.shards[0].endpoint=aeron:udp?endpoint=localhost:20040" "--shard-routing.shards[0].slot-from=0" "--shard-routing.shards[0].slot-to=127" "--shard-routing.shards[1].endpoint=aeron:udp?endpoint=localhost:20041" "--shard-routing.shards[1].slot-from=128" "--shard-routing.shards[1].slot-to=255")
fi
java "${JO[@]}" -Xlog:gc*:file=$LOG/gc-matching.log:time,uptime -jar $MJ --spring.config.additional-location=file:$W/m0.yml $KB ${MATCH_EXTRA[@]+"${MATCH_EXTRA[@]}"} > $LOG/matching-0.log 2>&1 & MP=$!; echo $MP >> $W/pids
for _ in $(seq 1 60); do grep -q "Started MatchingWorkerApplication" $LOG/matching-0.log && break; sleep 2; done
grep -q "Started MatchingWorkerApplication" $LOG/matching-0.log || { echo START_FAIL matching; exit 6; }
sleep 8
# 종목 수가 3이면 기존대로 slot 32 이상 3개. 그 밖이면 slot 전체에서 STOCK_COUNT개(샤드 확장 시험용).
SPLIT_SLOT=32; [ "$STOCK_COUNT" = 3 ] || SPLIT_SLOT=0
STOCKS=$(./gradlew --offline -q :account-worker:u10Tool -Pu10Args="codes-many 64 $SPLIT_SLOT $STOCK_COUNT" 2>&1 | grep '^HIGH=' | tail -1 | sed 's/^HIGH=//' | tr ',' ' ')
echo "종목: $STOCKS"
./gradlew --offline -q :account-worker:u10Tool -Pu10Args="slot-of 64 $STOCKS" 2>&1 | grep '^SLOT' > "$OUT/slots.txt"
export BULK_ACCOUNTS=$N
BUYER_ACCOUNTS=$BUYERS BUYER_BALANCE=$BUYER_BALANCE python3 -I loadtest/s0b/make-seed.py "$W/seed.yml" "$N" "$HOLD" $STOCKS || exit 1
AP_LIST=()
start_account() { # 번호(0부터)
  local i=$1 suffix="" extra=()
  if [ "$ACCOUNT_WORKERS" = 2 ]; then
    [ "$i" = 1 ] && suffix="-1"
    if [ "$i" = 0 ]; then extra=("--transport.account-intake.channel=aeron:udp?endpoint=localhost:20040" "--transport.fill.channel=aeron:udp?endpoint=localhost:20060" "--account.worker.archive.control-channel=aeron:udp?endpoint=localhost:8100" "--account.worker.archive-dir=$W/account-0")
    else extra=("--transport.account-intake.channel=aeron:udp?endpoint=localhost:20041" "--transport.fill.channel=aeron:udp?endpoint=localhost:20061" "--account.worker.archive.control-channel=aeron:udp?endpoint=localhost:8101" "--account.worker.archive-dir=$W/account-1" "--snowflake.node-id=5"); fi
  fi
  java "${JO[@]}" -Xlog:gc*:file=$LOG/gc-account$suffix.log:time,uptime -jar $AJ --spring.config.additional-location=file:$W/acc.yml,file:$W/seed.yml $KB \
    --account-worker.metrics.enabled=true $LOGARG ${ACC_SHARD[@]+"${ACC_SHARD[@]}"} ${extra[@]+"${extra[@]}"} > $LOG/account$suffix.log 2>&1 & AP_LIST+=($!); echo $! >> $W/pids
  for _ in $(seq 1 ${START_WAIT_TRIES:-60}); do grep -q "Started AccountWorkerApplication" $LOG/account$suffix.log && break; sleep 2; done
  grep -q "Started AccountWorkerApplication" $LOG/account$suffix.log || { echo START_FAIL account$suffix; exit 6; }
}
for i in $(seq 0 $((ACCOUNT_WORKERS - 1))); do start_account $i; done
AP=${AP_LIST[0]}
if [ "$ACCOUNT_WORKERS" = 2 ]; then
  for _ in $(seq 1 30); do n=0; for p in 20040 20041 20060 20061; do lsof -nP -iUDP:$p >/dev/null 2>&1 && n=$((n+1)); done; [ $n = 4 ] && break; sleep 1; done
  [ "$n" = 4 ] || { echo "START_FAIL UDP 포트 4개가 열리지 않았다"; exit 6; }
  ./gradlew --offline -q :account-worker:u10Tool -Pu10Args="shard-counts 2 90002 $N $((90002 + N)) $BUYERS" 2>&1 | grep '^SHARD_COUNTS' > "$OUT/shard-counts.txt"
  echo "ACCOUNT_SHARD_COUNTS $(tr '\n' ';' < "$OUT/shard-counts.txt")"
fi
if [ "${HEAP_SAMPLE:-0}" = 1 ]; then
  jstat -gc $AP 2000 > "$OUT/heap.jstat" 2>&1 & echo $! >> $W/pids
  [ "$ACCOUNT_WORKERS" = 2 ] && { jstat -gc ${AP_LIST[1]} 2000 > "$OUT/heap-w1.jstat" 2>&1 & echo $! >> $W/pids; }
fi
if [ "${CPU_SAMPLE:-0}" = 1 ]; then
  : > "$OUT/cpu.log"
  if [ "$ACCOUNT_WORKERS" = 2 ]; then ROLES=(account-0=$AP account-1=${AP_LIST[1]} matching=$MP); else ROLES=(account=$AP matching=$MP); fi
  loadtest/s0b/cpu_sample.sh "$OUT/cpu.log" "${ROLES[@]}" & echo $! >> $W/pids
fi
accepted_total() { local sum=0 f v; for f in $LOG/account.log $LOG/account-1.log; do [ -f $f ] || continue; v=$(grep -o "acceptedTotal=[0-9]*" $f | tail -1 | sed 's/acceptedTotal=//'); sum=$((sum + ${v:-0})); done; echo $sum; }
rejected_total() { cat $LOG/account.log $( [ -f $LOG/account-1.log ] && echo $LOG/account-1.log ) | grep "metrics accepted=" | sed 's/.*rejected=\([0-9]*\) .*/\1/' | awk '{s+=$1} END{print s+0}'; }
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
if [ "${THREAD_CPU:-0}" = 1 ]; then
  ( sleep $((SEC / 3)); jcmd $AP Thread.print > "$OUT/threads-1.txt" 2>&1; [ "$ACCOUNT_WORKERS" = 2 ] && jcmd ${AP_LIST[1]} Thread.print > "$OUT/threads-1-w1.txt" 2>&1; sleep 10; jcmd $AP Thread.print > "$OUT/threads-2.txt" 2>&1; [ "$ACCOUNT_WORKERS" = 2 ] && jcmd ${AP_LIST[1]} Thread.print > "$OUT/threads-2-w1.txt" 2>&1 ) & echo $! >> $W/pids
fi
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
