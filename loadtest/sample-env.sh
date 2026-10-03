#!/usr/bin/env bash
set -uo pipefail

# 기준선 재측정(묶음 B 단계 1) 환경 샘플러. 설계서 2-5절.
#
# 사용: loadtest/sample-env.sh <출력 csv>      (백그라운드로 띄우고 kill 로 멈춘다)
#
# 샘플 하나당 한 줄을 기록한다. 열:
#   ts, k6_cpu, jvm_<이름>..., pg_cpu, kafka_cpu, idle, user, sys, top5, batt, therm_limit
#   - k6, JVM, docker 컨테이너 CPU: 1코어 대비 %. 직전 샘플과의 누적 CPU 시간 차이를 경과 시간으로 나눈 값이다.
#     첫 샘플은 비교할 직전 값이 없어 비어 있다. 이전 샘플러가 쓴 `top -l 1`의 첫 샘플과 `ps -o %cpu=`는 쓰지 않는다.
#   - idle, user, sys: `top -l 2 -s 1` 의 두 번째 샘플 "CPU usage" 줄
#   - top5: 두 번째 샘플의 상위 5개 프로세스를 `이름:CPU%|이름:CPU%|...` 한 문자열로 합친 것
#   - batt: AC 또는 Battery, therm_limit: `pmset -g therm` 의 CPU_Speed_Limit (없으면 NA)
#   - rss_<이름>: JVM 프로세스의 RSS(실제로 올라간 물리 메모리) MB. `ps -o rss=` 합계. 힙 크기가 아니다
#   - pg_mem, kafka_mem: 컨테이너 메모리 사용량 MB (`docker stats` 의 MemUsage). 기존 열 뒤에 붙여 앞 열의 위치가 바뀌지 않는다
#
# 환경 변수 (이 스크립트에서 새로 정한 이름과 기본값)
#   SAMPLER_COND        v1 | v2. JVM 열 목록을 정한다 (기본 v1)
#   SAMPLER_JVMS        "이름=메인클래스 이름=메인클래스 ..." 로 JVM 목록을 직접 지정 (SAMPLER_COND 보다 우선)
#   SAMPLER_PAUSE_SEC   샘플 수집이 끝난 뒤 쉬는 시간 초 (기본 2). 수집에 2~3초가 걸려 주기가 약 5초가 된다
#   SAMPLER_PG          postgres 컨테이너 이름 (기본 stock-trading-postgres)
#   SAMPLER_KAFKA       kafka 컨테이너 이름 (기본 stock-trading-kafka)
#   SAMPLER_ONCE        1 이면 샘플을 두 번만 쓰고 끝낸다 (동작 확인용)

OUT_FILE="${1:?사용: sample-env.sh <출력 csv>}"
COND="${SAMPLER_COND:-v1}"
PAUSE_SEC="${SAMPLER_PAUSE_SEC:-2}"
PG_CONTAINER="${SAMPLER_PG:-stock-trading-postgres}"
KAFKA_CONTAINER="${SAMPLER_KAFKA:-stock-trading-kafka}"

if [ -z "${SAMPLER_JVMS:-}" ]; then
    case "$COND" in
        v1) SAMPLER_JVMS="api=com.flab.stocktradingengine.api.StockTradingEngineApplication order-engine=com.flab.stocktradingengine.OrderEngineApplication matching-engine=com.flab.stocktradingengine.matching.MatchingApplication settlement-engine=com.flab.stocktradingengine.settlement.engine.SettlementEngineApplication" ;;
        v2) SAMPLER_JVMS="api=com.flab.stocktradingengine.api.StockTradingEngineApplication account-worker=com.flab.stocktradingengine.account.worker.AccountWorkerApplication matching-worker=com.flab.stocktradingengine.matching.worker.MatchingWorkerApplication" ;;
        *) echo "SAMPLER_COND 는 v1 또는 v2 여야 한다 (받은 값: ${COND})" >&2; exit 1 ;;
    esac
fi

STATE_DIR="$(mktemp -d)"
trap 'rm -rf "$STATE_DIR"; exit 0' TERM INT

now_sec() {
    perl -MTime::HiRes=time -e 'printf "%.3f\n", time'
}

# ps 의 누적 CPU 시간("M:SS.cc" 또는 "H:MM:SS")을 초로 바꾼다.
cpu_seconds() {
    ps -o time= -p "$1" 2>/dev/null | awk 'NF { n = split($1, a, ":"); s = 0; for (i = 1; i <= n; i++) s = s * 60 + a[i]; print s }'
}

# cpu_pct <라벨> <pid...> : 라벨별로 직전 샘플을 STATE_DIR 에 두고 1코어 대비 % 를 출력한다. 비교할 값이 없으면 빈 문자열.
cpu_pct() {
    local label="$1"
    shift
    local now pid seconds
    now="$(now_sec)"
    local previous_file="${STATE_DIR}/${label}"
    local current_file="${STATE_DIR}/${label}.next"
    : > "$current_file"
    for pid in "$@"; do
        seconds="$(cpu_seconds "$pid")"
        if [ -n "$seconds" ]; then
            echo "${pid} ${seconds}" >> "$current_file"
        fi
    done
    local pct=""
    if [ -f "$previous_file" ]; then
        pct="$(awk -v now="$now" '
            NR == FNR { if (FNR == 1) { previous_wall = $1 } else { previous[$1] = $2 } next }
            { if ($1 in previous) { delta += $2 - previous[$1]; matched = 1 } }
            END {
                wall = now - previous_wall
                if (matched && wall > 0) printf "%.1f", delta / wall * 100
            }' "$previous_file" "$current_file")"
    fi
    { echo "$now"; cat "$current_file"; } > "$previous_file"
    echo "$pct"
}

# top 한 번 호출에서 두 번째 샘플의 CPU usage 줄과 상위 5개 프로세스를 읽는다. 출력: "idle,user,sys,top5"
read_top() {
    top -l 2 -s 1 -n 5 -o cpu -stats pid,command,cpu 2>/dev/null | awk '
        /^Processes:/ { block++ }
        block == 2 && /^CPU usage:/ {
            line = $0
            gsub(/CPU usage: /, "", line)
            n = split(line, parts, ", ")
            for (i = 1; i <= n; i++) {
                value = parts[i]; sub(/%.*/, "", value)
                if (parts[i] ~ /user/) user = value
                else if (parts[i] ~ /sys/) sys = value
                else if (parts[i] ~ /idle/) idle = value
            }
        }
        block == 2 && /^PID/ { in_table = 1; next }
        block == 2 && in_table && NF >= 3 && count < 5 {
            name = $2
            for (i = 3; i < NF; i++) name = name "_" $i
            gsub(/,/, "", name)
            entry = name ":" $NF
            top5 = (count == 0) ? entry : top5 "|" entry
            count++
        }
        END { printf "%s,%s,%s,%s", idle, user, sys, top5 }'
}

read_docker_cpu() {
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' "$PG_CONTAINER" "$KAFKA_CONTAINER" 2>/dev/null | tr -d '%'
}

# "123.4MiB" 같은 docker 크기 표기를 MB(소수 첫째 자리)로 바꾼다. 빈 값은 빈 값
to_mb() {
    echo "$1" | awk 'NF {
        value = $1 + 0
        unit = $1; sub(/^[0-9.]+/, "", unit)
        if (unit == "GiB" || unit == "GB") value = value * 1024
        else if (unit == "KiB" || unit == "kB" || unit == "KB") value = value / 1024
        else if (unit == "B") value = value / 1048576
        printf "%.1f\n", value
    }'
}

# docker stats 출력에서 컨테이너 메모리 사용량(MB). "이름,CPU,사용량 / 한도" 형식의 세 번째 칸 앞부분을 쓴다
container_mem() {
    local raw
    raw="$(echo "$1" | awk -F, -v name="$2" '$1 == name { split($3, parts, " "); print parts[1] }')"
    to_mb "$raw"
}

# 프로세스 id 들의 RSS 합계(MB). id 가 없으면 빈 값
rss_mb() {
    [ "$#" -gt 0 ] || return 0
    local kb
    kb="$(ps -o rss= -p "$(echo "$@" | tr ' ' ',')" 2>/dev/null | awk '{ s += $1 } END { if (NR > 0) print s }')"
    [ -n "$kb" ] && awk -v kb="$kb" 'BEGIN { printf "%.1f\n", kb / 1024 }'
}

container_cpu() {
    echo "$1" | awk -F, -v name="$2" '$1 == name { print $2 }'
}

read_batt() {
    if pmset -g batt 2>/dev/null | head -1 | grep -q "AC Power"; then echo "AC"; else echo "Battery"; fi
}

read_therm() {
    local limit
    limit="$(pmset -g therm 2>/dev/null | awk -F= '/CPU_Speed_Limit/ { gsub(/ /, "", $2); print $2 }')"
    echo "${limit:-NA}"
}

write_header() {
    local header="ts,k6_cpu"
    local entry
    for entry in $SAMPLER_JVMS; do
        header="${header},jvm_${entry%%=*}"
    done
    local memory_header=""
    for entry in $SAMPLER_JVMS; do
        memory_header="${memory_header},rss_${entry%%=*}"
    done
    echo "${header},pg_cpu,kafka_cpu,idle,user,sys,top5,batt,therm_limit${memory_header},pg_mem,kafka_mem" > "$OUT_FILE"
}

sample_once() {
    local ts k6_pids k6_cpu line entry label pattern jvm_cpus="" jvm_rss="" docker_out pg_cpu kafka_cpu pg_mem kafka_mem top_out
    ts="$(date +%s)"

    k6_pids="$(pgrep -x k6 | head -1)"
    # shellcheck disable=SC2086
    k6_cpu="$(cpu_pct k6 $k6_pids)"

    for entry in $SAMPLER_JVMS; do
        label="${entry%%=*}"
        pattern="${entry#*=}"
        # shellcheck disable=SC2046
        jvm_cpus="${jvm_cpus},$(cpu_pct "jvm_${label}" $(pgrep -f "$pattern"))"
        # shellcheck disable=SC2046
        jvm_rss="${jvm_rss},$(rss_mb $(pgrep -f "$pattern"))"
    done

    docker_out="$(read_docker_cpu)"
    pg_cpu="$(container_cpu "$docker_out" "$PG_CONTAINER")"
    kafka_cpu="$(container_cpu "$docker_out" "$KAFKA_CONTAINER")"
    pg_mem="$(container_mem "$docker_out" "$PG_CONTAINER")"
    kafka_mem="$(container_mem "$docker_out" "$KAFKA_CONTAINER")"

    top_out="$(read_top)"
    line="${ts},${k6_cpu}${jvm_cpus},${pg_cpu},${kafka_cpu},${top_out},$(read_batt),$(read_therm)${jvm_rss},${pg_mem},${kafka_mem}"
    echo "$line" >> "$OUT_FILE"
}

# 테스트가 함수만 불러올 수 있게, SAMPLER_SOURCE_ONLY=1 이면 샘플링 루프를 돌지 않는다
if [ "${SAMPLER_SOURCE_ONLY:-0}" != "1" ]; then
    write_header
    count=0
    while true; do
        sample_once
        count=$((count + 1))
        if [ "${SAMPLER_ONCE:-0}" = "1" ] && [ "$count" -ge 2 ]; then
            break
        fi
        sleep "$PAUSE_SEC"
    done
    rm -rf "$STATE_DIR"
fi
