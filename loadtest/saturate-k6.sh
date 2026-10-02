#!/usr/bin/env bash
set -uo pipefail

# k6 단독 포화점 리허설(설계서 6-3a, measure-protocol-c.md U7b). 앱·Docker 없이 더미 주문 서버(DummyOrderServer.java)에
# k6를 붙여 속도마다 k6가 예정 속도를 지키는지 본다. 서버는 202를 즉시 돌려주므로 포화가 나오면 k6(와 이 노트북)의 한계다.
#
# 사용:
#   loadtest/saturate-k6.sh --dry-run          계획(속도별 N, 예상 소요, 결과 폴더)만 출력하고 아무것도 띄우지 않는다
#   loadtest/saturate-k6.sh [--rates "192 500 1000"]   더미 서버를 띄우고 속도마다 k6를 한 번씩 돌린 뒤 서버를 멈춘다
#
# 환경 변수 (기본값). ★는 이 스크립트에서 새로 정한 이름이다.
#   ★RATES="192 500 1000 2000 4000 6000"   시험할 속도(건/초). 필요하면 "... 8000"을 더한다. --rates 인자가 우선한다
#   ★ARRIVAL_SEC=20       속도마다 N = 속도 x 이 값(입력 파일 줄 수로 제한). 도착 시간이 같아야 속도끼리 비교된다
#   ★DUMMY_PORT=9790      더미 서버 포트
#   ★SETTLE_SEC=5, GAP_SEC=5   k6 시작 전 대기, 속도 사이 대기 초
#   ★INPUT_FILE=loadtest/results/e2e-input.jsonl   replay-e2e.js 와 같은 입력 파일
#   ★RUN_ID=k6-solo-<UTC yyyymmdd-HHMM>   결과 폴더 이름 (loadtest/results/<RUN_ID>/)
# 결과 폴더: k6-<속도>.json(k6 요약) k6-<속도>.out cpu-<속도>.csv(1초 간격) server-<속도>.json(서버가 센 도착) table.md env.txt server.log
# 부하를 거는 스크립트다. Jack 의 노트북 점유가 필요하므로 시작 신호 없이 실행하지 않는다.

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

ARRIVAL_SEC="${ARRIVAL_SEC:-20}"
DUMMY_PORT="${DUMMY_PORT:-9790}"
SETTLE_SEC="${SETTLE_SEC:-5}"
GAP_SEC="${GAP_SEC:-5}"
TEARDOWN_SEC=3
SERVER_START_SEC=3
INPUT_FILE="${INPUT_FILE:-loadtest/results/e2e-input.jsonl}"
RATES_TEXT="${RATES:-192 500 1000 2000 4000 6000}"
RUN_ID="${RUN_ID:-k6-solo-$(date -u +%Y%m%d-%H%M)}"
RUN_DIR="loadtest/results/${RUN_ID}"
DRY_RUN=0
SERVER_PID=""
SAMPLER_PID=""

usage() {
    cat >&2 <<'USAGE'
사용: loadtest/saturate-k6.sh [--dry-run] [--rates "192 500 1000"]
  --dry-run   계획만 출력한다 (서버·k6를 띄우지 않는다)
  --rates     시험할 속도 목록 (환경 변수 RATES 보다 우선)
USAGE
}

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --dry-run) DRY_RUN=1 ;;
            --rates)
                [ $# -ge 2 ] || { usage; exit 2; }
                RATES_TEXT="$2"
                shift
                ;;
            -h | --help) usage; exit 0 ;;
            *) echo "모르는 인자: $1" >&2; usage; exit 2 ;;
        esac
        shift
    done
}

input_lines() {
    if [ -f "$INPUT_FILE" ]; then
        wc -l < "$INPUT_FILE" | tr -d ' '
    else
        echo 100000
    fi
}

plan_args() {
    python3 loadtest/solo_saturation.py plan --rates "$RATES_TEXT" --arrival-sec "$ARRIVAL_SEC" \
        --input-lines "$(input_lines)" --settle-sec "$SETTLE_SEC" --teardown-sec "$TEARDOWN_SEC" --gap-sec "$GAP_SEC" \
        --server-start-sec "$SERVER_START_SEC"
}

cleanup() {
    [ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2> /dev/null
    [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2> /dev/null
    wait 2> /dev/null
    return 0
}

# 시작 전 점검: 다른 k6·앱이 떠 있거나 포트가 쓰이고 있으면 측정값이 오염되므로 시작하지 않는다
preflight() {
    command -v k6 > /dev/null || { echo "k6 가 없다" >&2; return 1; }
    command -v java > /dev/null || { echo "java 가 없다" >&2; return 1; }
    [ -f "$INPUT_FILE" ] || { echo "입력 파일이 없다: ${INPUT_FILE} (python3 loadtest/generate-e2e-input.py)" >&2; return 1; }
    if pgrep -f 'k6 run' > /dev/null; then
        echo "다른 k6 가 돌고 있다" >&2
        return 1
    fi
    if pgrep -f 'bootRun|StockTradingEngineApplication|OrderEngineApplication|AccountWorkerApplication|MatchingWorkerApplication' > /dev/null; then
        echo "앱 JVM 이 떠 있다 — 단독 포화점은 앱 없이 잰다" >&2
        return 1
    fi
    if lsof -nP -iTCP:"$DUMMY_PORT" -sTCP:LISTEN > /dev/null 2>&1; then
        echo "포트 ${DUMMY_PORT} 가 이미 쓰이고 있다" >&2
        return 1
    fi
}

start_server() {
    echo "더미 서버 기동 (포트 ${DUMMY_PORT})"
    java -XX:+UseSerialGC loadtest/DummyOrderServer.java "$DUMMY_PORT" > "${RUN_DIR}/server.log" 2>&1 &
    SERVER_PID=$!
    local waited=0
    until curl -fsS "http://127.0.0.1:${DUMMY_PORT}/__health" > /dev/null 2>&1; do
        sleep 0.5
        waited=$((waited + 1))
        if [ "$waited" -ge 40 ]; then
            echo "더미 서버가 20초 안에 열리지 않았다 (${RUN_DIR}/server.log)" >&2
            return 1
        fi
    done
}

run_rate() {
    local rate="$1" n="$2"
    echo "--- ${rate}/초 N=${n}"
    curl -fsS -X POST "http://127.0.0.1:${DUMMY_PORT}/__reset" > /dev/null
    sleep "$SETTLE_SEC"
    RATE="$rate" N="$n" VERSION=v1 PASS=solo INPUT_FILE="$INPUT_FILE" BASE="http://127.0.0.1:${DUMMY_PORT}" \
        k6 run --summary-export="${RUN_DIR}/k6-${rate}.json" loadtest/replay-e2e.js > "${RUN_DIR}/k6-${rate}.out" 2>&1 &
    local k6_pid=$!
    python3 loadtest/solo_saturation.py sample --out "${RUN_DIR}/cpu-${rate}.csv" --interval 1 \
        --pid "k6=${k6_pid}" --pid "server=${SERVER_PID}" &
    SAMPLER_PID=$!
    wait "$k6_pid"
    kill "$SAMPLER_PID" 2> /dev/null
    wait "$SAMPLER_PID" 2> /dev/null
    SAMPLER_PID=""
    curl -fsS "http://127.0.0.1:${DUMMY_PORT}/__stats" > "${RUN_DIR}/server-${rate}.json"
    python3 loadtest/solo_saturation.py table "$RUN_DIR" --rates "$rate" --arrival-sec "$ARRIVAL_SEC" \
        --input-lines "$(input_lines)" | sed -n '/^| 속도/,/^$/p'
}

main() {
    parse_args "$@"
    if ! python3 loadtest/solo_saturation.py plan --rates "$RATES_TEXT" --arrival-sec "$ARRIVAL_SEC" \
        --input-lines "$(input_lines)" > /dev/null 2> /tmp/saturate-k6-args.$$; then
        cat /tmp/saturate-k6-args.$$ >&2
        rm -f /tmp/saturate-k6-args.$$
        echo "속도 목록이 잘못됐다: ${RATES_TEXT}" >&2
        exit 1
    fi
    rm -f /tmp/saturate-k6-args.$$
    if [ "$DRY_RUN" = "1" ]; then
        echo "시험할 속도: ${RATES_TEXT}"
        plan_args
        echo "결과 폴더: loadtest/results/${RUN_ID}/"
        exit 0
    fi

    preflight || exit 1
    mkdir -p "$RUN_DIR"
    {
        echo "run_id=${RUN_ID}"
        echo "start=$(date +%s) $(date)"
        echo "HEAD=$(git rev-parse HEAD)"
        echo "RATES=${RATES_TEXT} ARRIVAL_SEC=${ARRIVAL_SEC} DUMMY_PORT=${DUMMY_PORT} SETTLE_SEC=${SETTLE_SEC} GAP_SEC=${GAP_SEC}"
        k6 version
        java -version 2>&1 | head -1
        sysctl -n machdep.cpu.brand_string hw.ncpu hw.memsize
        pmset -g batt | head -1
        pmset -g therm
        echo "input_lines=$(input_lines) sha256=$(shasum -a 256 "$INPUT_FILE" | awk '{print $1}')"
    } > "${RUN_DIR}/env.txt" 2>&1
    trap 'cleanup' EXIT
    trap 'exit 130' INT TERM
    start_server || exit 1

    local rate n lines
    lines="$(input_lines)"
    for rate in $RATES_TEXT; do
        n=$(python3 -c "print(min($rate * $ARRIVAL_SEC, $lines))")
        run_rate "$rate" "$n"
        sleep "$GAP_SEC"
    done
    python3 loadtest/solo_saturation.py table "$RUN_DIR" --rates "$RATES_TEXT" --arrival-sec "$ARRIVAL_SEC" \
        --input-lines "$lines" --out "${RUN_DIR}/table.md"
    echo "완료 — ${RUN_DIR}/table.md"
}

main "$@"
