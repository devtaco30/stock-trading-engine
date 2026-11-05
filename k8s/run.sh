#!/usr/bin/env bash
set -uo pipefail
# 쿠버네티스(로컬 kind) 체결 섞기 실행(작업 4.1~4.4). 맥 시험 loadtest/s0b/mix.sh의 기본 경로(계좌 서버 1개, 매칭 서버 1개, 종목 3개)를 파드로 옮긴 것이다.
# 사용: k8s/run.sh <수명 분> <결과 폴더>
#   수명(분)은 필수다. 없거나 양의 정수가 아니면 아무것도 만들지 않고 종료 코드 1.
#   시작 전 검사: 매니페스트의 모든 컨테이너에 CPU·메모리 requests/limits가 있는지(없으면 컨테이너 이름을 내고 종료 코드 2),
#   이미지가 그 파드가 놓일 노드(ste-role 레이블)에 있는지(없으면 MISSING_IMAGE <이미지>를 내고 종료 코드 3). 둘 다 네임스페이스를 만들기 전에 본다.
#   네임스페이스를 만든 직후 수명 만료 삭제(k8s/reaper.sh)를 백그라운드로 띄운다. 시험이 정상으로 끝나면 회수 뒤 바로 지우고(DELETED_BY=run-end) 삭제 프로세스를 끝낸다.
# 환경 변수(기본값): ACCTS(300) BUYERS(30) C(300) RATE(1000) SECONDS_STEADY(60) PRICE(20000) BUYER_BALANCE(10000000000) — 뜻은 mix.sh와 같다.
#   NS(ste-run) 네임스페이스, KCTX(kind-ste) kubectl 컨텍스트, MANIFESTS_DIR(k8s) 매니페스트 폴더,
#   SETUP_ONLY=1 네임스페이스 생성과 삭제 예약까지만 하고 끝낸다(시험용), KEEP_NAMESPACE=1 끝나도 지우지 않고 수명 만료에 맡긴다,
#   KILL_MATCHING_AT=<초> 유지 단계 시작 뒤 그 초에 매칭 파드를 지운다(실패 경로 확인, 작업 4.4),
#   ACCEPT_WAIT_SECS(180) FILL_WAIT_SECS(300) — mix.sh와 같다.
# 결과(<결과 폴더>): namespace-created.txt, namespace-deleted.txt, collect/(판정·로그·한도·이벤트), collect-<시각>.tar.gz, run.out(진행 줄 사본)
#   collect/check.json이 판정기(loadtest/s0b/balance_check.py) 결과다. Aeron 녹화 파일(archive emptyDir)은 회수하지 않는다.
# 종료 코드: 0 유효, 1 인자 오류, 2 자원 한도 누락, 3 송신 위치 미확인 또는 이미지 없음, 4 접수·체결·거부 어긋남(무효), 6 기동 실패, 그 밖은 실패. mix.sh와 같다.
cd "$(dirname "$0")/.."
LIFETIME=${1:-}; OUT=${2:-}
usage() { echo "사용: k8s/run.sh <수명 분, 양의 정수> <결과 폴더>" >&2; }
case "$LIFETIME" in ''|*[!0-9]*) usage; exit 1 ;; esac
[ "$LIFETIME" -gt 0 ] || { usage; exit 1; }
[ -n "$OUT" ] || { usage; exit 1; }
NS=${NS:-ste-run}; KCTX=${KCTX:-kind-ste}; M=${MANIFESTS_DIR:-k8s}
K=(kubectl --context "$KCTX" -n "$NS")
MANIFESTS=("$M/kafka.yaml" "$M/matching-statefulset.yaml" "$M/account-statefulset.yaml" "$M/sender-job.yaml")

# 1) 자원 한도 누락 검사(작업 3.6)
LIMITS_OUT=$(python3 -I k8s/manifest_check.py limits "${MANIFESTS[@]}"); LIMITS_RC=$?
if [ "$LIMITS_RC" != 0 ]; then echo "$LIMITS_OUT"; echo "STOP 자원 한도가 빠진 컨테이너가 있다(위 줄: 파일 종류/이름 컨테이너 빠진 항목)"; exit 2; fi

# 2) 이미지 존재 검사. 로컬 kind 전용: 파드가 놓일 노드(nodeSelector ste-role)마다 containerd에 이미지가 있는지 crictl로 본다.
MISSING=0
while IFS=$'\t' read -r image role; do
  [ -n "$image" ] || continue
  if [ "$role" = "-" ]; then nodes=$(kubectl --context "$KCTX" get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{.items[*].metadata.name}')
  else nodes=$(kubectl --context "$KCTX" get nodes -l "ste-role=$role" -o jsonpath='{.items[*].metadata.name}'); fi
  [ -n "$nodes" ] || { echo "MISSING_NODE ste-role=$role (이미지 $image)"; MISSING=1; continue; }
  for node in $nodes; do
    docker exec "$node" crictl inspecti -q "$image" >/dev/null 2>&1 || { echo "MISSING_IMAGE $image node=$node"; MISSING=1; }
  done
done <<EOF
$(python3 -I k8s/manifest_check.py images "${MANIFESTS[@]}")
EOF
[ "$MISSING" = 0 ] || { echo "STOP 노드에 없는 이미지가 있다. 이미지를 만들고 kind load docker-image --name ste --nodes <노드> <이미지>로 넣는다"; exit 3; }

# 3) 네임스페이스와 수명 만료 삭제
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
if kubectl --context "$KCTX" get ns "$NS" >/dev/null 2>&1; then echo "STOP 네임스페이스 $NS가 이미 있다(이전 실행이 남았다)"; exit 1; fi
kubectl --context "$KCTX" create ns "$NS" >/dev/null || { echo "STOP 네임스페이스를 만들지 못했다"; exit 1; }
printf 'ns=%s lifetime_min=%s created_at=%s created_at_iso=%s expires_at=%s\n' "$NS" "$LIFETIME" "$(date +%s)" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(( $(date +%s) + LIFETIME * 60 ))" > "$OUT/namespace-created.txt"
nohup bash k8s/reaper.sh "$KCTX" "$NS" "$LIFETIME" "$OUT" > /dev/null 2>&1 &
REAPER=$!
echo "$REAPER" > "$OUT/reaper.pid"
echo "NAMESPACE $NS 수명 ${LIFETIME}분 삭제 프로세스 pid=$REAPER"
[ "${SETUP_ONLY:-0}" = 1 ] && exit 0

# 4) 실행 계획(mix.sh 기본 경로와 같은 식)
N=${ACCTS:-300}; BUYERS=${BUYERS:-30}; C=${C:-300}; RATE=${RATE:-1000}; SEC=${SECONDS_STEADY:-60}
PRICE=${PRICE:-20000}; BUYER_BALANCE=${BUYER_BALANCE:-10000000000}; STOCK_COUNT=3
C_DIR="$OUT/collect"; LOG="$C_DIR/logs"; mkdir -p "$LOG"
: > "$C_DIR/steps.log"; : > "$OUT/run.out"
say() { echo "$*" | tee -a "$OUT/run.out"; }
step() { echo "$(date +%s) $*" >> "$C_DIR/steps.log"; }
if [ $((C % STOCK_COUNT)) -ne 0 ] || [ $((RATE * SEC % (2 * STOCK_COUNT))) -ne 0 ]; then say "STOP C는 3의 배수, RATE x SECONDS_STEADY는 6의 배수여야 한다"; exit 1; fi
PREFILL=$((C * N / STOCK_COUNT)); PAIRS=$((RATE * SEC / (2 * STOCK_COUNT)))
HOLD=$((C / STOCK_COUNT + (PAIRS + N - 1) / N + 5)); TOTAL_TRADES=$((PAIRS * STOCK_COUNT))
TOTAL_ORDERS=$((PREFILL * STOCK_COUNT + PAIRS * STOCK_COUNT * 2))
printf 'GROUP_BY_SHARD=0 PER_ACCOUNT_PER_STOCK=0 ACCOUNT_WORKERS=1 ACCTS=%s BUYERS=%s C=%s RATE=%s SECONDS_STEADY=%s PRICE=%s PREFILL_PER_STOCK=%s PAIRS_PER_STOCK=%s HOLD=%s BUYER_BALANCE=%s TOTAL_ORDERS=%s TOTAL_TRADES=%s NS=%s LIFETIME_MIN=%s KILL_MATCHING_AT=%s\n' \
  "$N" "$BUYERS" "$C" "$RATE" "$SEC" "$PRICE" "$PREFILL" "$PAIRS" "$HOLD" "$BUYER_BALANCE" "$TOTAL_ORDERS" "$TOTAL_TRADES" "$NS" "$LIFETIME" "${KILL_MATCHING_AT:-}" > "$C_DIR/params.txt"
CHANNEL="aeron:udp?endpoint=account-0.account:20040"

BG=()   # 이 스크립트가 띄운 백그라운드 프로세스(표본기 등). 끝날 때 이 pid만 끝낸다.
COLLECTED=0
collect() { # 성공·실패와 상관없이 한 번만 돈다(작업 4.3, 4.4)
  [ "$COLLECTED" = 1 ] && return; COLLECTED=1
  for p in ${BG[@]+"${BG[@]}"}; do kill "$p" 2>/dev/null; wait "$p" 2>/dev/null; done
  say "COLLECT 시작"
  "${K[@]}" get pods -o wide > "$C_DIR/pods.txt" 2>&1
  "${K[@]}" get events --sort-by=.lastTimestamp -o wide > "$C_DIR/events.txt" 2>&1
  "${K[@]}" get pods -o json > "$C_DIR/pods.json" 2>/dev/null
  python3 -I k8s/pod_report.py "$C_DIR/pods.json" "$C_DIR/resources.tsv" "$C_DIR/restarts.tsv" 2>> "$C_DIR/collect.err"
  for pod in account-0 matching-0 kafka-0; do
    case $pod in account-0) c=account-worker; f=account.log;; matching-0) c=matching-worker; f=matching-0.log;; kafka-0) c=kafka; f=kafka-0.log;; esac
    "${K[@]}" logs "$pod" -c "$c" > "$LOG/$f" 2>> "$C_DIR/collect.err"
    restarts=$("${K[@]}" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null)
    [ "${restarts:-0}" -gt 0 ] 2>/dev/null && "${K[@]}" logs "$pod" -c "$c" --previous > "$LOG/${f%.log}.previous.log" 2>> "$C_DIR/collect.err"
  done
  "${K[@]}" cp -c account-worker account-0:/logs/gc-account.log "$LOG/gc-account.log" > /dev/null 2>> "$C_DIR/collect.err"
  "${K[@]}" cp -c matching-worker matching-0:/logs/gc-matching.log "$LOG/gc-matching.log" > /dev/null 2>> "$C_DIR/collect.err"
  "${K[@]}" exec kafka-0 -c kafka -- sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe; echo; /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type brokers --entity-name 1 --all | grep -E "log.retention|log.segment.bytes"; echo; du -sm /var/lib/kafka/data' > "$C_DIR/kafka-topics.txt" 2>> "$C_DIR/collect.err"
  if [ -f "$C_DIR/paced.out" ] && [ -s "$LOG/account.log" ]; then
    python3 -I loadtest/s0b/balance_check.py "$C_DIR" > "$C_DIR/check.json" 2> "$C_DIR/check.err"
  else
    echo '{"status": "not_run", "reason": "paced.out 또는 계좌 로그가 없다"}' > "$C_DIR/check.json"
  fi
  cp "$OUT/run.out" "$C_DIR/run.out" 2>/dev/null
  TS=$(date +%Y%m%d-%H%M%S)
  tar czf "$OUT/collect-$TS.tar.gz" -C "$OUT" collect && say "COLLECTED $OUT/collect-$TS.tar.gz"
  if [ "${KEEP_NAMESPACE:-0}" != 1 ]; then
    kubectl --context "$KCTX" delete ns "$NS" --wait=true --timeout=300s > /dev/null 2>&1; RC=$?
    printf 'DELETED_BY=run-end ns=%s deleted_at=%s deleted_at_iso=%s delete_rc=%s\n' "$NS" "$(date +%s)" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RC" >> "$OUT/namespace-deleted.txt"
    kill "$REAPER" 2>/dev/null
  fi
}
trap collect EXIT

render_job() { # <이름> <BULK_ACCOUNTS> <인자...> -> 표준 출력에 Job 매니페스트
  python3 -I - "$M/sender-job.yaml" "$@" <<'PY'
import json, sys
template, name, bulk = open(sys.argv[1], encoding="utf-8").read(), sys.argv[2], sys.argv[3]
print(template.replace("__NAME__", name).replace("__BULK_ACCOUNTS__", bulk).replace("__ARGS__", json.dumps(sys.argv[4:])))
PY
}
run_job() { # <이름> <BULK_ACCOUNTS> <최대 초> <인자...> -> 로그를 collect/<이름>.out에 남기고 성공하면 0
  local name=$1 bulk=$2 limit=$3; shift 3
  render_job "$name" "$bulk" "$@" | "${K[@]}" apply -f - > /dev/null || return 1
  local s f
  for _ in $(seq 1 "$limit"); do
    s=$("${K[@]}" get job "$name" -o jsonpath='{.status.succeeded}' 2>/dev/null); f=$("${K[@]}" get job "$name" -o jsonpath='{.status.failed}' 2>/dev/null)
    [ "${s:-0}" -ge 1 ] 2>/dev/null && break
    [ "${f:-0}" -ge 1 ] 2>/dev/null && break
    sleep 1
  done
  "${K[@]}" logs "job/$name" > "$C_DIR/$name.out" 2>&1
  [ "${s:-0}" -ge 1 ] 2>/dev/null
}
account_log() { "${K[@]}" logs account-0 -c account-worker 2>/dev/null; }
accepted_total() { local v; v=$(account_log | grep -o "acceptedTotal=[0-9]*" | tail -1 | sed 's/acceptedTotal=//'); echo "${v:-0}"; }
fills_total() { account_log | grep "metrics accepted=" | sed 's/.*fill=\([0-9]*\) dup.*/\1/' | awk '{s+=$1} END{print s+0}'; }
rejected_total() { account_log | grep "metrics accepted=" | sed 's/.*rejected=\([0-9]*\) .*/\1/' | awk '{s+=$1} END{print s+0}'; }
START_TS=$(date +%s)
disk_avail_gb() { # 서버 노드(ste-role=server)의 kubelet 통계에서 남은 디스크
  local node; node=$(kubectl --context "$KCTX" get nodes -l ste-role=server -o jsonpath='{.items[0].metadata.name}')
  kubectl --context "$KCTX" get --raw "/api/v1/nodes/$node/proxy/stats/summary" 2>/dev/null | python3 -I -c 'import json,sys; print("%.1f" % (json.load(sys.stdin)["node"]["fs"]["availableBytes"] / 1e9))' 2>/dev/null || echo "?"
}
progress() { say "PROGRESS elapsed=$(( $(date +%s) - START_TS ))s accepted=$(accepted_total) disk_avail_gb=$(disk_avail_gb) $*"; }
wait_accepted() { local i; for i in $(seq 1 "${ACCEPT_WAIT_SECS:-180}"); do [ "$(accepted_total)" = "$1" ] && return 0; [ $((i % 10)) = 0 ] && progress "기대 $1"; sleep 1; done; return 1; }
wait_log() { # <파드> <컨테이너> <문구> <최대 초>
  # 로그를 변수에 먼저 받는다. kubectl logs | grep -q로 쓰면 grep이 먼저 끝나 kubectl이 SIGPIPE를 받고, pipefail 때문에 찾았어도 실패로 나온다.
  local i text; for i in $(seq 1 "$4"); do text=$("${K[@]}" logs "$1" -c "$2" 2>/dev/null); grep -q "$3" <<<"$text" && return 0; sleep 1; done; return 1
}
sample_usage() { # 10초마다 파드 메모리(cgroup memory.current), /dev/shm(Aeron 디렉터리: 실제 쓴 크기와 파일 크기 합), /data(archive) 크기. 한도 값 근거용
  while true; do
    for pc in account-0:account-worker matching-0:matching-worker kafka-0:kafka; do
      pod=${pc%%:*}; c=${pc#*:}
      v=$("${K[@]}" exec "$pod" -c "$c" -- sh -c 'echo mem_bytes=$(cat /sys/fs/cgroup/memory.current) shm_mb=$(du -sm /dev/shm 2>/dev/null | cut -f1) shm_apparent_mb=$(du -sm --apparent-size /dev/shm 2>/dev/null | cut -f1) data_mb=$(du -sm /data /var/lib/kafka/data 2>/dev/null | cut -f1 | head -1)' 2>/dev/null)
      echo "$(date +%s) $pod $v"
    done >> "$C_DIR/pod-usage.log"
    sleep 10
  done
}

# 5) Kafka
say "STAGE kafka"
"${K[@]}" apply -f "$M/kafka.yaml" > /dev/null || exit 6
"${K[@]}" rollout status sts/kafka --timeout=300s > /dev/null || { say "START_FAIL kafka"; exit 6; }
"${K[@]}" exec kafka-0 -c kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null || { say "START_FAIL kafka 토픽 목록 조회"; exit 6; }
step "KAFKA_READY"

# 6) 종목과 시드(mix.sh:103~113). 계산만 하는 하위 명령이라 Kafka·서버 없이 돈다
run_job codes 1 120 codes-many 64 32 "$STOCK_COUNT" || { say "START_FAIL codes-many"; exit 6; }
STOCKS=$(grep '^HIGH=' "$C_DIR/codes.out" | tail -1 | sed 's/^HIGH=//' | tr ',' ' ')
say "종목: $STOCKS"
run_job slots 1 120 slot-of 64 $STOCKS && grep '^SLOT' "$C_DIR/slots.out" > "$C_DIR/slots.txt"
BUYER_ACCOUNTS=$BUYERS BUYER_BALANCE=$BUYER_BALANCE python3 -I loadtest/s0b/make-seed.py "$C_DIR/seed.yml" "$N" "$HOLD" $STOCKS || exit 1
"${K[@]}" create configmap account-config --from-file=account.yml="$M/config/account.yml" --from-file=seed.yml="$C_DIR/seed.yml" > /dev/null || exit 1

# 7) 매칭·계좌 서버. 순서는 mix.sh:97~122와 같다(매칭 기동 로그 → 8초 → 계좌).
#    계좌 파드를 먼저 만들어 DNS 이름을 생기게 하고(매칭 서버가 기다린다) 앱은 init 컨테이너 wait-start-gate에서 멈춰 둔다.
#    매칭 기동 로그를 본 뒤 8초 기다리고 /gate/go를 만들어 계좌 앱을 띄운다(매니페스트 주석).
say "STAGE servers"
"${K[@]}" apply -f "$M/matching-statefulset.yaml" -f "$M/account-statefulset.yaml" > /dev/null || exit 6
sample_usage & BG+=($!)
wait_log matching-0 matching-worker "Started MatchingWorkerApplication" 300 || { say "START_FAIL matching"; exit 6; }
step "MATCHING_STARTED"
sleep 8
GATE_OK=0
for _ in $(seq 1 60); do "${K[@]}" exec account-0 -c wait-start-gate -- touch /gate/go > /dev/null 2>&1 && { GATE_OK=1; break; }; sleep 1; done
[ "$GATE_OK" = 1 ] || { say "START_FAIL 계좌 기동 문을 열지 못했다"; exit 6; }
step "ACCOUNT_GATE_OPENED"
wait_log account-0 account-worker "Started AccountWorkerApplication" 300 || { say "START_FAIL account"; exit 6; }
step "SERVERS_STARTED"
run_job assigned 1 120 assigned kafka-0.kafka:9092 64
ASSIGNED=$(grep '^ASSIGNED' "$C_DIR/assigned.out" | tail -1); say "$ASSIGNED"
case "$ASSIGNED" in "ASSIGNED 64/64 MOVING 0 ENDPOINTS {aeron:udp?endpoint=matching-0.matching:20020=64}") ;; *) say "START_FAIL 배정 맵이 매칭 0번 64칸이 아니다"; exit 6;; esac

# 8) 쌓기
step "START PREFILL per_stock=$PREFILL"
run_job bulk "$N" 300 send-bulk "$CHANNEL" 90002 SELL "$PRICE" "$PREFILL" pre --price-step=0 $STOCKS
grep -E "SENT_BULK|OFFER_FAILED|NOT_CONNECTED|SENDER_POSITION" "$C_DIR/bulk.out" | tee -a "$OUT/run.out"
[ "$(grep -c SENDER_POSITION_REACHED "$C_DIR/bulk.out")" = 1 ] || { say "SEND_NOT_CONFIRMED 쌓기"; exit 3; }
EXPECT=$((PREFILL * STOCK_COUNT))
wait_accepted $EXPECT || { say "ACCEPTED_MISMATCH 쌓기 기대 $EXPECT 실제 $(accepted_total)"; exit 4; }
step "END PREFILL"
sleep 3

# 9) 유지(초당 RATE건). KILL_MATCHING_AT이 있으면 그 초에 매칭 파드를 지운다
step "START STEADY rate=$RATE seconds=$SEC pairs_per_stock=$PAIRS"
if [ -n "${KILL_MATCHING_AT:-}" ]; then
  ( sleep "$KILL_MATCHING_AT"; echo "$(date +%s) KILL matching-0" >> "$C_DIR/steps.log"; "${K[@]}" delete pod matching-0 --wait=false >> "$C_DIR/steps.log" 2>&1 ) & BG+=($!)
fi
render_job paced "$N" send-paced "$CHANNEL" 90002 "$N" $((90002 + N)) "$BUYERS" "$PRICE" "$PREFILL" "$PAIRS" "$RATE" mix $STOCKS | "${K[@]}" apply -f - > /dev/null
PACED_OK=0
for i in $(seq 1 $((SEC + 300))); do
  s=$("${K[@]}" get job paced -o jsonpath='{.status.succeeded}' 2>/dev/null); f=$("${K[@]}" get job paced -o jsonpath='{.status.failed}' 2>/dev/null)
  [ "${s:-0}" -ge 1 ] 2>/dev/null && { PACED_OK=1; break; }
  [ "${f:-0}" -ge 1 ] 2>/dev/null && break
  [ $((i % 10)) = 0 ] && progress "유지 단계"
  sleep 1
done
"${K[@]}" logs job/paced > "$C_DIR/paced.out" 2>&1
grep -E "PACED_START|PACED_DONE|OFFER_FAILED|NOT_CONNECTED|SENDER_POSITION" "$C_DIR/paced.out" | tee -a "$OUT/run.out"
[ "$PACED_OK" = 1 ] || { say "SENDER_JOB_FAILED paced"; exit 3; }
[ "$(grep -c SENDER_POSITION_REACHED "$C_DIR/paced.out")" = 1 ] || { say "SEND_NOT_CONFIRMED 유지"; exit 3; }
EXPECT=$((EXPECT + PAIRS * STOCK_COUNT * 2))
wait_accepted $EXPECT || { say "ACCEPTED_MISMATCH 유지 기대 $EXPECT 실제 $(accepted_total)"; exit 4; }
step "END STEADY"

# 10) 체결 반영 대기와 거부 확인(mix.sh:168~187과 같다. 계좌 서버가 1개라 NOT_OWNED는 0이어야 한다)
FILL_EXPECT=$((2 * TOTAL_TRADES)); FILLS_OK=0; waited=0
for _ in $(seq 1 "${FILL_WAIT_SECS:-300}"); do
  [ "$(fills_total)" = "$FILL_EXPECT" ] && { FILLS_OK=1; break; }
  sleep 1; waited=$((waited + 1))
done
sleep 3
if [ "$FILLS_OK" = 1 ]; then say "FILLS_COMPLETE fills=$FILL_EXPECT waited=${waited}s"; else say "FILLS_INCOMPLETE have=$(fills_total) expect=$FILL_EXPECT after=${waited}s"; fi
REJ=$(rejected_total)
NOT_OWNED=$(account_log | grep -c "담당 슬롯이 아닌 계좌로 이벤트가 왔습니다")
say "NOT_OWNED_FANOUT $NOT_OWNED"
REJ=$((REJ - NOT_OWNED))
[ "$REJ" = 0 ] || { say "REJECTED_NONZERO rejected=$REJ"; exit 4; }
[ "$FILLS_OK" = 1 ] || { say "FILLS_INCOMPLETE 체결 반영이 한도 안에 끝나지 않았다 (무효)"; exit 4; }
say "VALID accepted=$EXPECT rejected=$REJ"
exit 0
