#!/usr/bin/env bash
set -uo pipefail

# 39 설계 전 과정(e2e) 측정 오케스트레이터.
#
# 버전(v1/v2)마다 프로세스를 한 번만 기동해 워밍업+측정 3패스를 재기동 없이 연속 재생한다.
# 재기동은 쓰지 않는다 — 계좌 워커(스냅샷+저널 복구)도 v1(Postgres)도 재기동해봤자 마지막
# 상태를 그대로 복구할 뿐이라 "패스 초기화" 효과가 없고, JIT 워밍업만 날아간다(2b 지시,
# 2026-09-16 — 처음엔 패스마다 재기동하려 했다가 이 이유로 뒤집음).
#
# 패스 경계마다 a4의 히스토그램 훅에 파일 트리거로 "지금까지 걸 스냅샷으로 꺼내고 리셋"을
# 요청해 4개(워밍업+측정3)의 분리된 분포를 얻는다. 같은 입력을 재생하므로 requestId는
# replay-e2e.js가 패스별로 접미사를 붙여 멱등 재전송으로 오인되지 않게 한다.
#
# 전제(ec·a4 산출물, 없으면 경고만 내고 계속):
#   loadtest/results/e2e-input.jsonl                  (ec — 결정론 페어 입력, 4패스 소화량 시드 필요)
#   측정 훅 트리거 파일 폴링(latency-trigger-{v1,v2}) (a4)
cd "$(dirname "$0")/.."

RESULTS_DIR="loadtest/results"
mkdir -p "$RESULTS_DIR"

INPUT_FILE="$(pwd)/${RESULTS_DIR}/e2e-input.jsonl"
if [ ! -f "$INPUT_FILE" ]; then
    echo "경고: ${INPUT_FILE} 없음 — ec의 입력 생성기를 먼저 돌려야 한다" >&2
fi

start_version() {
    if [ "$1" = "v1" ]; then ./loadtest/run-e2e-v1.sh; else ./loadtest/run-e2e-v2.sh; fi
}

stop_version() {
    if [ "$1" = "v1" ]; then ./loadtest/stop-v1.sh; else ./loadtest/stop-v2.sh; fi
}

sample_cpu() {
    local out_file="$1"
    : > "$out_file"
    while true; do
        ts=$(date +%s)
        overall=$(top -l 1 -n 0 | grep "CPU usage")
        k6_pid=$(pgrep -x k6 | head -1)
        k6_cpu="0.0"
        if [ -n "${k6_pid:-}" ]; then
            k6_cpu=$(ps -o %cpu= -p "$k6_pid" 2>/dev/null | tr -d ' ')
        fi
        echo "${ts} ${overall} | k6_cpu=${k6_cpu}%" >> "$out_file"
        sleep 2
    done
}

run_replay() {
    local version="$1" label="$2"
    echo "--- ${version} ${label}: replay 시작 ---"
    sample_cpu "${RESULTS_DIR}/cpu-${version}-${label}.log" &
    local sampler_pid=$!
    VERSION="$version" VUS=50 PASS="$label" INPUT_FILE="$INPUT_FILE" \
        k6 run --summary-export="${RESULTS_DIR}/e2e-${version}-${label}.json" loadtest/replay-e2e.js
    kill "$sampler_pid" 2>/dev/null
    wait "$sampler_pid" 2>/dev/null
}

# a4의 히스토그램 훅에 "지금까지 쌓인 걸 스냅샷으로 꺼내고 리셋"을 요청한다. 트리거 파일에
# 원하는 출력 경로를 한 줄로 적으면, 훅이 그 경로에 JSON을 덤프하고 트리거 파일을 지운다
# (지워짐 = 처리 완료 신호).
request_latency_snapshot() {
    local version="$1" label="$2"
    local trigger_path
    if [ "$version" = "v1" ]; then trigger_path="${RESULTS_DIR}/latency-trigger-v1"
    else trigger_path="${RESULTS_DIR}/latency-trigger-v2"
    fi
    local output_path="${RESULTS_DIR}/latency-${version}-${label}.json"

    echo "$output_path" > "$trigger_path"
    local waited=0
    while [ -f "$trigger_path" ] && [ "$waited" -lt 10 ]; do
        sleep 0.5
        waited=$((waited + 1))
    done
    if [ -f "$trigger_path" ]; then
        echo "경고: ${version} 히스토그램 스냅샷 트리거가 5초 안에 처리 안 됨 — a4 훅 미배선/미동작 가능성" >&2
        rm -f "$trigger_path"
    elif [ -f "$output_path" ]; then
        echo "히스토그램 스냅샷 저장: ${output_path}"
    else
        echo "경고: 트리거는 지워졌는데 ${output_path}가 없음" >&2
    fi
}

for version in v1 v2; do
    echo "=== ${version} 기동 ==="
    start_version "$version"

    echo "=== ${version}: 워밍업 패스(분포는 버리되, 리셋은 필요) ==="
    run_replay "$version" "warmup"
    request_latency_snapshot "$version" "warmup"

    for pass in 1 2 3; do
        echo "=== ${version}: 측정 패스 ${pass} ==="
        run_replay "$version" "pass${pass}"
        request_latency_snapshot "$version" "pass${pass}"
    done

    stop_version "$version"
    sleep 3
done

echo "완료 — ${RESULTS_DIR}/e2e-{v1,v2}-{warmup,pass1,pass2,pass3}.json (k6 요약, HTTP 접수 처리량),"
echo "       ${RESULTS_DIR}/latency-{v1,v2}-{warmup,pass1,pass2,pass3}.json (접수·예약 지연 백분위, warmup은 참고용),"
echo "       ${RESULTS_DIR}/cpu-{v1,v2}-*.log (CPU 포화 확인용) 확인"
