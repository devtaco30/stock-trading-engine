#!/usr/bin/env bash
set -uo pipefail

# 39 설계 전 과정(e2e) 측정 오케스트레이터.
# 버전(v1/v2)마다: 기동 → 워밍업 1패스(버림) → 재기동 → 측정 3패스(각각 latency json 보존) → 정지.
# 재기동으로 패스를 나누는 이유 — a4의 히스토그램 훅은 "종료 시 1회 덤프"만 하므로, 패스별로
# 분리하려면 그 사이에 프로세스를 새로 띄우는 게 가장 단순하다(2b 지시: 워밍업 1회+측정 3회,
# 회차 늘리기보다 패스 간 편차 확인이 목적).
#
# 전제(ec·a4 산출물, 없으면 경고만 내고 계속):
#   loadtest/results/e2e-input.jsonl                       (ec — 결정론 페어 입력)
#   loadtest/results/latency-v1-order-engine.json          (a4 — v1 접수 지연 덤프, 종료 시 1회)
#   loadtest/results/latency-v2-account-worker.json        (a4 — v2 접수 지연 덤프, 종료 시 1회)
cd "$(dirname "$0")/.."

RESULTS_DIR="loadtest/results"
mkdir -p "$RESULTS_DIR"

INPUT_FILE="$(pwd)/${RESULTS_DIR}/e2e-input.jsonl"
if [ ! -f "$INPUT_FILE" ]; then
    echo "경고: ${INPUT_FILE} 없음 — ec의 입력 생성기를 먼저 돌려야 한다" >&2
fi

latency_source_for() {
    if [ "$1" = "v1" ]; then echo "${RESULTS_DIR}/latency-v1-order-engine.json"
    else echo "${RESULTS_DIR}/latency-v2-account-worker.json"
    fi
}

start_version() {
    if [ "$1" = "v1" ]; then ./loadtest/run-e2e-v1.sh; else ./loadtest/run-e2e-v2.sh; fi
}

stop_version() {
    if [ "$1" = "v1" ]; then ./loadtest/stop-v1.sh; else ./loadtest/stop-v2.sh; fi
}

# macOS 기본 bash(3.2)는 연관 배열을 못 쓴다(c7-load-test-harness.md 기존 발견) — case문으로 대체.
sample_cpu() {
    local out_file="$1"
    : > "$out_file"
    while true; do
        ts=$(date +%s)
        overall=$(top -l 1 -n 0 | grep "CPU usage")
        k6_pid=$(pgrep -x k6 | head -1)
        k6_cpu="0.0%"
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
    VERSION="$version" VUS=50 INPUT_FILE="$INPUT_FILE" \
        k6 run --summary-export="${RESULTS_DIR}/e2e-${version}-${label}.json" loadtest/replay-e2e.js
    kill "$sampler_pid" 2>/dev/null
    wait "$sampler_pid" 2>/dev/null
}

for version in v1 v2; do
    echo "=== ${version}: 워밍업 패스(버림) ==="
    start_version "$version"
    run_replay "$version" "warmup"
    stop_version "$version"
    sleep 3

    for pass in 1 2 3; do
        echo "=== ${version}: 측정 패스 ${pass} ==="
        start_version "$version"
        run_replay "$version" "pass${pass}"
        latency_src=$(latency_source_for "$version")
        if [ -f "$latency_src" ]; then
            cp "$latency_src" "${RESULTS_DIR}/latency-${version}-pass${pass}.json"
        else
            echo "경고: ${latency_src} 없음 — a4 히스토그램 훅이 아직 안 배선됐거나 게이트가 꺼져 있을 수 있다" >&2
        fi
        stop_version "$version"
        sleep 3
    done
done

echo "완료 — ${RESULTS_DIR}/e2e-{v1,v2}-{warmup,pass1,pass2,pass3}.json (k6 요약),"
echo "       ${RESULTS_DIR}/latency-{v1,v2}-pass{1,2,3}.json (접수 지연 백분위),"
echo "       ${RESULTS_DIR}/cpu-{v1,v2}-*.log (CPU 포화 확인용) 확인"
