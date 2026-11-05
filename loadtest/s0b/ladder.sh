#!/usr/bin/env bash
set -uo pipefail
# 계좌당 미체결 C를 단계별로 올려 가며 각 단계에서 접수 속도를 재는 사다리 실행(2026-11-01, ec).
# 사용(시험 잠금 안에서): ACCTS=<N> LADDER=<C 목록, 쉼표, 종목 수 3의 배수> [ACCEPT_PROBES=<단계:종목당 계좌당 건수,...>] OUT=<결과 폴더> loadtest/s0b/ladder.sh
#   단계마다 PREFILL(수량 1 매도로 C까지 쌓기) -> REJECT(가용 보유보다 1 큰 수량의 매도 = INSUFFICIENT_HOLDING 거부, C 고정) -> (ACCEPT: 접수 검증).
#   주문 계획은 ladder_plan.py가 만든다. 결과 폴더에 plan.tsv, params.txt, logs/가 남고 probe_rates.py가 읽는다.
#   DRY_RUN=1이면 인프라와 앱을 띄우지 않고 단계별 명령만 출력한다(시험용).
#   ACCOUNT_JAR·MATCHING_JAR로 jar를 고른다. SKIP_BUILD=1이면 빌드를 건너뛴다. ACCEPT_WAIT_SECS(기본 180)는 단계마다 처리 끝을 기다리는 최대 초.
#   거부마다 LoggingAccountResultListener가 WARN 로그를 한 줄씩 남기므로 로그 I/O가 속도에 섞이지 않게 그 로거를 ERROR로 올린다(운영에서는 이 로그가 남는다).
cd "$(dirname "$0")/../.."
N=${ACCTS:-}; LADDER=${LADDER:-}; ACCEPT=${ACCEPT_PROBES:-}; OUT=${OUT:?}
if [ -z "$N" ] || [ -z "$LADDER" ]; then echo "ACCTS와 LADDER가 필요하다" >&2; exit 1; fi
STOCK_COUNT=3
HERE="$(pwd)/loadtest/s0b"
mkdir -p "$OUT"
PLAN_FILE="$OUT/plan.tsv"
python3 -I "$HERE/ladder_plan.py" "$N" "$LADDER" "$ACCEPT" > "$PLAN_FILE" || { echo "주문 계획을 만들지 못했다(C는 3의 배수, 늘어나는 순서)" >&2; exit 1; }
HOLD=$(awk -F'\t' '$1=="HOLD"{print $2}' "$PLAN_FILE")
printf 'ACCTS=%s PREFILL_PER_STOCK=0 PER_STOCK=0 ORDER_LOG=off HOLD=%s LADDER=%s ACCEPT=%s\n' "$N" "$HOLD" "$LADDER" "${ACCEPT:-none}" > "$OUT/params.txt"
LOGARG="--logging.level.com.flab.stocktradingengine.account.worker.listener.LoggingAccountResultListener=ERROR"

if [ "${DRY_RUN:-0}" = 1 ]; then
  echo "HOLD=$HOLD"
  echo "BULK_ACCOUNTS=$N 계좌 수, 로그 $LOGARG"
  while IFS=$'\t' read -r kind rung per_stock quantity label; do
    [ "$kind" = HOLD ] && continue
    echo "$kind $rung per_stock=$per_stock quantity=$quantity $label -> send-bulk SELL --quantity=$quantity $per_stock"
  done < "$PLAN_FILE"
  exit 0
fi

for p in 20020 20040 20060 8010 8011 8012; do lsof -nP -iUDP:$p >/dev/null 2>&1 && { echo "PORT_BUSY UDP $p"; exit 5; }; done
lsof -nP -iTCP:19092 -sTCP:LISTEN >/dev/null 2>&1 && { echo "PORT_BUSY TCP 19092"; exit 5; }
W=/tmp/s0b; LOG=$W/logs; rm -rf $W; mkdir -p $LOG; : > $W/pids
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
  rm -rf "$OUT/logs"; cp -R $LOG "$OUT/logs" 2>/dev/null; cp $W/steps.log "$OUT/steps.log" 2>/dev/null; }
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
python3 -I "$HERE/make-seed.py" "$W/seed.yml" "$N" "$HOLD" $STOCKS || exit 1
java "${JO[@]}" -Xlog:gc*:file=$LOG/gc-account.log:time,uptime -jar $AJ --spring.config.additional-location=file:$W/acc.yml,file:$W/seed.yml $KB \
  --account-worker.metrics.enabled=true $LOGARG > $LOG/account.log 2>&1 & echo $! >> $W/pids
for _ in $(seq 1 60); do grep -q "Started AccountWorkerApplication" $LOG/account.log && break; sleep 2; done
grep -q "Started AccountWorkerApplication" $LOG/account.log || { echo START_FAIL account; exit 6; }
accepted_total() { grep -o "acceptedTotal=[0-9]*" $LOG/account.log | tail -1 | sed 's/acceptedTotal=//'; }
rejected_total() { grep "metrics accepted=" $LOG/account.log | sed 's/.*rejected=\([0-9]*\) .*/\1/' | awk '{s+=$1} END{print s+0}'; }
send() { # <종목당 보낼 수> <접두사> <수량> -> 송신 위치 도달을 확인한다
  local out; out=$(./gradlew --offline -q :account-worker:u10Tool -Pu10Args="send-bulk aeron:udp?endpoint=localhost:20040 90002 SELL 20000 $1 $2 --quantity=$3 $STOCKS" 2>&1)
  echo "$out" | grep -E "SENT_BULK|OFFER_FAILED|NOT_CONNECTED|SENDER_POSITION"
  grep -q SENDER_POSITION_REACHED <<<"$out"
}
wait_for() { # <함수 이름> <기대 누적> — 최대 ACCEPT_WAIT_SECS초
  for _ in $(seq 1 "${ACCEPT_WAIT_SECS:-180}"); do [ "$($1)" = "$2" ] && return 0; sleep 1; done; return 1
}
EXPECT_A=0; EXPECT_R=0; : > $W/steps.log
while IFS=$'\t' read -r kind rung per_stock quantity label; do
  [ "$kind" = HOLD ] && continue
  echo "$(date +%s.%N) START $kind $rung $label per_stock=$per_stock quantity=$quantity" >> $W/steps.log
  send "$per_stock" "$(echo "$kind" | cut -c1-2)$rung" "$quantity" || { echo SEND_NOT_CONFIRMED; exit 3; }
  if [ "$kind" = REJECT ]; then
    EXPECT_R=$((EXPECT_R + per_stock * STOCK_COUNT))
    wait_for rejected_total $EXPECT_R || { echo "REJECTED_MISMATCH 기대 $EXPECT_R 실제 $(rejected_total)"; exit 4; }
  else
    EXPECT_A=$((EXPECT_A + per_stock * STOCK_COUNT))
    wait_for accepted_total $EXPECT_A || { echo "ACCEPTED_MISMATCH 기대 $EXPECT_A 실제 $(accepted_total)"; exit 4; }
  fi
  echo "$(date +%s.%N) END $kind $rung $label" >> $W/steps.log
  sleep 3
done < "$PLAN_FILE"
echo "VALID accepted=$EXPECT_A rejected=$EXPECT_R"
