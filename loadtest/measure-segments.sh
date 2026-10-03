#!/usr/bin/env bash
set -uo pipefail

# 구간 분해 측정(묶음 C) 오케스트레이터. 선언 문서는 loadtest/measure-protocol-c.md 이고, 임계값·제외 규칙·포화 규칙은
# 이 스크립트가 아니라 그 문서와 analyze-*.py 에 있다. 이 스크립트는 점검·기동·회차 실행·덤프 수집·포화 계산을 맡는다.
# 점검, 인덱스, DB 상태, 환경 샘플러 같은 공용 함수는 measure-baseline.sh 를 source 해서 재사용한다.
#
# 사용:
#   PREFLIGHT_ONLY=1 loadtest/measure-segments.sh   점검 1~8과 시작 검사(허용 목록, 선언 문서)를 읽기 전용으로 실행하고 끝낸다
#   loadtest/measure-segments.sh                    점검 통과 후 SEGMENT_SETS 의 세트를 순서대로 측정한다
#
# 세트 표기: 단계:조건[:계측]
#   단계  S1(192/초, N 10,000, 1+5회) S3(1,000/초, 20,000, 1+4) S5(4,000/초, 40,000, 1+3)
#         S1X(S1 속도, 11회: 계측 켬/끔 영향 확인용) S1R(S1 속도, 3회: 리허설 1+2) I<속도>(보간 단계: N=속도x20, 1+3, 예 I450 I2000)
#         S5X(4,000/초 재측정, N 40,000, 기본 1+5회, S5X_ROUNDS 로 바꾼다. 시드가 40,000건 회차를 여섯 번까지만 받는다)
#         L(포화점 탐색 계단: 회차마다 속도를 올리고 처음 포화 신호가 나온 회차에서 멈춘다. 선언 문서 4-4절)
#   ★LADDER_RATES="192 300 400 500 700 1000 1500 2000 3000 4000"   계단 속도(첫 값이 워밍업 회차). ★LADDER_ARRIVAL_SEC=20, ★LADDER_N_CAP=40000
#   조건  v1idx(v1, 인덱스 있음, INDEX_MODE=1) v2   (인덱스 없는 v1은 측정 조건이 아니다)
#   계측  1(기본) 단계 계측을 켠다 / 0 끈다
#
# 환경 변수 (기본값). ★는 이 스크립트에서 새로 정한 이름이다.
#   ★SEGMENT_SETS      기본 계획(default_set_plan): S1 v1idx, v2, v2, v1idx 교차 → S3 v1idx, v2 → S5 v1idx, v2
#   ★MEASURE_STAGES    세트마다 이 스크립트가 정해서 run-e2e-*.sh 에 넘긴다(세트 표기의 계측 값). 직접 줄 필요 없다
#   ★RUN_ID            결과 폴더 이름 (기본 c-<UTC yyyymmdd-HHMM>). 세트마다 <NN>-<단계>-<조건>-st<계측> 하위 폴더를 만든다
#   ★ALLOWLIST_FILE    src/main 허용 목록 (기본 loadtest/measure-allowlist-c.txt)
#   ★STAGE_DUMP_TIMEOUT=15   단계 덤프 트리거를 지운 것을 기다리는 최대 초
#   그 밖의 변수(ROUND_GAP_SEC, INCLUDE_KAFKA_UI, V1_LISTENER_CONCURRENCY, DRAIN_*, QUIESCE_*, CAFFEINATE 등)는 measure-baseline.sh 와 같다.
#   RATE, N, ROUNDS, INDEX_MODE 는 세트 표기가 정하므로 밖에서 주지 않는다(세트마다 덮어쓴다). ROUND_CLEAR 는 쓰지 않는다(쌓이는 조건).

cd "$(dirname "$0")/.."

SEG_RUN_ID_GIVEN="${RUN_ID:-}"
SEG_SOURCE_ONLY="${MEASURE_SOURCE_ONLY:-0}"
export MEASURE_SOURCE_ONLY=1
# shellcheck disable=SC1091
source loadtest/measure-baseline.sh
MEASURE_SOURCE_ONLY="$SEG_SOURCE_ONLY"
# shellcheck disable=SC1091
source loadtest/stage-args.sh

if [ -n "$SEG_RUN_ID_GIVEN" ]; then
    RUN_ID="$SEG_RUN_ID_GIVEN"
else
    RUN_ID="c-$(date -u +%Y%m%d-%H%M)"
fi
RUN_DIR="${RESULTS_DIR}/${RUN_ID}"
SEGMENT_ROOT="$RUN_DIR"
ALLOWLIST_FILE="${ALLOWLIST_FILE:-loadtest/measure-allowlist-c.txt}"
STAGE_DUMP_TIMEOUT="${STAGE_DUMP_TIMEOUT:-15}"
PROTOCOL_FILES="loadtest/measure-protocol-b0.md loadtest/measure-protocol-b1.md loadtest/measure-protocol-c.md"
ROUND_CLEAR=0
SEGMENT_SETS="${SEGMENT_SETS:-}"

# ---------------------------------------------------------------- 단계 구성과 보간 (measure-protocol-c.md 4절)

# k6 가상 사용자(VU) 수. 필요한 동시 요청 수는 도착률 x 응답 시간(Little의 법칙)이다. replay-e2e.js 의 기본 maxVUs 300 은 응답 5ms 를 가정한
# 값이라(필요 약 20개), 실측 응답 80~97ms 의 4,000건/초(필요 약 320~400개)에서 모자랐다. 선언 문서 4-5절
K6_PRE_ALLOCATED_VUS="${K6_PRE_ALLOCATED_VUS:-500}"
K6_MAX_VUS="${K6_MAX_VUS:-1500}"

# "속도 N 회차수(워밍업 1 포함)" 를 출력한다
step_config() {
    case "$1" in
        S1) echo "192 10000 6" ;;
        S3) echo "1000 20000 5" ;;
        S5) echo "4000 40000 4" ;;
        S5X) echo "4000 40000 ${S5X_ROUNDS:-6}" ;;
        S1X) echo "192 10000 11" ;;
        S1R) echo "192 10000 3" ;;
        L)
            local rates first_rate
            rates="$(ladder_rates)"
            first_rate="${rates%% *}"
            echo "${first_rate} $(ladder_n "$first_rate") $(echo "$rates" | wc -w | tr -d ' ')"
            ;;
        I[0-9]*)
            local rate="${1#I}"
            echo "${rate} $((rate * 20)) 4"
            ;;
        *)
            echo "모르는 단계: $1" >&2
            return 1
            ;;
    esac
}

round_to_50() {
    awk -v x="$1" 'BEGIN { printf "%d\n", int((x + 25) / 50) * 50 }'
}

# 두 속도의 기하평균을 50의 배수로 반올림한 값 (4-2절)
interp_rate() {
    awk -v a="$1" -v b="$2" 'BEGIN { printf "%d\n", int((sqrt(a * b) + 25) / 50) * 50 }'
}

# 단계의 유효 측정 회차 중 과반이 포화면 포화한 단계 (4-1절). 인자: 포화 회차 수, 측정 회차 수(워밍업 제외)
step_is_saturated() {
    if [ $(($1 * 2)) -gt "$2" ]; then echo yes; else echo no; fi
}

# 낮은 단계는 포화가 아니고 높은 단계는 포화일 때만 사이에 단계를 하나 넣는다 (4-2절)
needs_interpolation() {
    if [ "$1" = "no" ] && [ "$2" = "yes" ]; then echo yes; else echo no; fi
}

# ---------------------------------------------------------------- 포화 판정 (4-1절 표)

# 인자: dropped http_failed hist_count n lag first_p50 last_p50 drain_sec arrival_sec
# NA 인 입력은 그 지표만 건너뛴다. 해당하는 이유를 쉼표로 이어 출력하고, 없으면 none.
saturation_reasons() {
    local dropped="$1" failed="$2" hist="$3" n="$4" lag="$5" first="$6" last="$7" drain="$8" arrival="$9"
    local reasons=()
    is_number "$dropped" && [ "$dropped" -ge 1 ] && reasons+=(k6_dropped)
    is_number "$failed" && [ "$failed" -ge 1 ] && reasons+=(http_failed)
    is_number "$hist" && is_number "$n" && [ "$hist" -lt "$n" ] && reasons+=(hist_count)
    is_number "$lag" && [ "$lag" -gt 0 ] && reasons+=(lag)
    if is_number "$first" && is_number "$last" && awk -v f="$first" -v l="$last" 'BEGIN { exit !(f > 0 && l >= 1.5 * f) }'; then
        reasons+=(latency_growth)
    fi
    if is_number "$drain" && is_number "$arrival" && awk -v d="$drain" -v a="$arrival" 'BEGIN { exit !(d > 0.5 * a) }'; then
        reasons+=(drain)
    fi
    if [ "${#reasons[@]}" -eq 0 ]; then
        echo none
    else
        (IFS=,; echo "${reasons[*]}")
    fi
}

is_number() {
    case "$1" in
        '' | *[!0-9.]*) return 1 ;;
        *) return 0 ;;
    esac
}

# 시계열 CSV(epochSecond,count,p50Micros,p99Micros)에서 창 [start,end] 안 count>0 행의 처음 20%와 마지막 20% p50 평균.
# 행이 없으면 "NA NA"
series_edges() {
    local file="$1" start="$2" end="$3"
    if [ ! -f "$file" ]; then
        echo "NA NA"
        return
    fi
    awk -F, -v s="$start" -v e="$end" '
        NR > 1 && $1 >= s && $1 <= e && $2 > 0 { n++; v[n] = $3 }
        END {
            if (n == 0) { print "NA NA"; exit }
            k = int(n * 0.2); if (k < 1) k = 1
            for (i = 1; i <= k; i++) first += v[i]
            for (i = n - k + 1; i <= n; i++) last += v[i]
            printf "%d %d\n", first / k + 0.5, last / k + 0.5
        }' "$file"
}

# rounds-<cond>.csv 에서 회차의 열 값. 없으면 NA
round_fact() {
    local file="$1" round="$2" column="$3"
    awk -F, -v r="$round" -v c="$column" '
        NR == 1 { for (i = 1; i <= NF; i++) if ($i == c) col = i; next }
        $1 == r && col > 0 { print $col; found = 1 }
        END { if (!found) print "NA" }' "$file" 2> /dev/null || echo NA
}

# ---------------------------------------------------------------- 단계 덤프 수집

stage_trigger_targets() {
    case "$1" in
        v1) echo "api order-engine matching-engine settlement-engine" ;;
        v2) echo "api account-worker matching-worker" ;;
    esac
}

# 앱이 트리거를 지우면 덤프가 써진 것이다(StageDumpRunner 가 쓰고 나서 지운다). 시간 안에 안 지워지면 트리거를 치우고 실패
trigger_stage_dump() {
    local trigger="$1" dest="$2" timeout_sec="$3" waited=0
    mkdir -p "$(dirname "$dest")"
    printf '%s\n' "$dest" > "$trigger"
    while [ -f "$trigger" ]; do
        if [ "$waited" -ge "$timeout_sec" ]; then
            rm -f "$trigger"
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
    return 0
}

collect_stage_dumps() {
    local cond="$1" round="$2" cond_dir="$3" label status=0
    for label in $(stage_trigger_targets "$cond"); do
        if ! trigger_stage_dump "$(stage_trigger_path "$cond" "$label")" \
            "${cond_dir}/stages-${cond}-${label}-${round}.json" "$STAGE_DUMP_TIMEOUT"; then
            echo "단계 덤프 실패: ${cond} ${label} ${round}" >&2
            echo "${round},${label}" >> "${cond_dir}/stage-dump-failures-${cond}.csv"
            status=1
        fi
    done
    return $status
}

copy_series_files() {
    local cond="$1" cond_dir="$2" label file
    for label in $(stage_trigger_targets "$cond"); do
        file="${STAGE_RESULTS_ABS}/series-${cond}-${label}.csv"
        [ -f "$file" ] && cp "$file" "${cond_dir}/series-${cond}-${label}.csv"
    done
    return 0
}

# 포화 판정에 쓰는 시계열은 대표 구간 B (v1 order-engine, v2 account-worker)
series_label_for() {
    case "$1" in
        v1) echo order-engine ;;
        v2) echo account-worker ;;
    esac
}

# ---------------------------------------------------------------- 시작 검사

# 변경된 src/main 파일 이름(표준입력)에서 허용 목록(파일 인자)에 없는 것을 출력한다. 목록 파일이 없으면 실패
allowlist_violations() {
    local list="$1"
    [ -f "$list" ] || { echo "허용 목록이 없다: ${list}" >&2; return 1; }
    grep -vxFf "$list" || true
}

# 이 브랜치의 src/main 변경이 허용 목록 안인지 본다. b1 의 assert_src_main_unchanged 를 대신한다 (측정 브랜치는 계측 변경이 있다)
check_src_main_allowlist() {
    local changed violations
    changed="$(git diff --name-only "$B1_BASE_SHA" -- '*/src/main/*')"
    violations="$(printf '%s\n' "$changed" | sed '/^$/d' | allowlist_violations "${ALLOWLIST_FILE}")" || return 1
    if [ -n "$violations" ]; then
        echo "허용 목록 밖의 src/main 변경이 있다 — 시작하지 않는다:" >&2
        echo "$violations" >&2
        return 1
    fi
    echo "src/main 허용 목록 대조: 목록 밖 변경 없음 (변경 $(printf '%s\n' "$changed" | sed '/^$/d' | wc -l | tr -d ' ')개, 목록 ${ALLOWLIST_FILE} sha256 $(shasum -a 256 "${ALLOWLIST_FILE}" | awk '{print $1}'))"
}

commit_precedes() {
    case "$1" in '' | *[!0-9]*) echo no; return ;; esac
    if [ "$1" -lt "$2" ]; then echo yes; else echo no; fi
}

# 선언 문서 커밋 시각이 이 실행의 시작 시각보다 앞인지 (첫 측정 로그보다 앞서야 한다)
check_protocol_precedes_start() {
    local start_epoch="$1" file commit_epoch status=0
    for file in $PROTOCOL_FILES; do
        commit_epoch="$(git log -1 --format=%ct -- "$file")"
        if [ "$(commit_precedes "$commit_epoch" "$start_epoch")" != "yes" ]; then
            echo "${file} 커밋 시각(${commit_epoch})이 시작 시각(${start_epoch})보다 앞이 아니다 — 시작하지 않는다" >&2
            status=1
        else
            echo "선언 문서 커밋 시각 확인: ${file} commit=${commit_epoch} < start=${start_epoch}"
        fi
    done
    return $status
}

# ---------------------------------------------------------------- 세트 표기와 계획

# "단계 조건 인덱스(0|1) 계측(0|1)" 을 출력한다
parse_set_token() {
    local token="$1" step cond stages
    IFS=: read -r step cond stages <<< "$token"
    stages="${stages:-1}"
    case "$cond" in
        v1idx) echo "${step} v1 1 ${stages}" ;;
        v2) echo "${step} v2 0 ${stages}" ;;
        *)
            echo "모르는 조건: ${cond} (token=${token})" >&2
            return 1
            ;;
    esac
}

# 4절의 실행 순서: S1에서만 v1, v2, v2, v1 으로 교차하고 S3·S5 는 v1, v2 순으로 한 번씩
default_set_plan() {
    echo "S1:v1idx:1 S1:v2:1 S1:v2:1 S1:v1idx:1 S3:v1idx:1 S3:v2:1 S5:v1idx:1 S5:v2:1"
}

# ---------------------------------------------------------------- 포화점 탐색(계단, 세트 L)

# 계단 속도(건/초). 첫 값은 워밍업 회차(r01)의 속도다. 탐색용이라 임의의 값이고 선언 문서 4-4절에 적는다
ladder_rates() {
    echo "${LADDER_RATES:-192 300 400 500 700 1000 1500 2000 3000 4000}"
}

# 회차의 주문 수: 속도 x 도착 시간(LADDER_ARRIVAL_SEC, 기본 20초). 시드 용량 때문에 LADDER_N_CAP(기본 4만)에서 자른다
ladder_n() {
    local n=$(($1 * ${LADDER_ARRIVAL_SEC:-20})) cap="${LADDER_N_CAP:-40000}"
    [ "$n" -gt "$cap" ] && n="$cap"
    echo "$n"
}

# 회차별 건수 목록(시드 확인에 쓴다)
ladder_counts() {
    local rate counts=""
    for rate in $(ladder_rates); do
        counts="${counts:+$counts }$(ladder_n "$rate")"
    done
    echo "$counts"
}

# 계단 한 세트의 회차를 돈다. 회차마다 속도와 N 이 바뀐다. 첫 회차(워밍업)는 포화 판정에서 뺀다.
# 처음으로 포화 신호가 나온 회차에서 멈추고(그 뒤 회차는 밀린 처리분이 섞인다) 그 속도를 ladder-first-saturated.txt 에 남긴다. 없으면 none
run_ladder_rounds() {
    local cond="$1" cond_dir="$2" number=0 rate round reasons first_saturated="none" status=0
    echo "round,rate,n" > "${cond_dir}/rates-${cond}.csv"
    for rate in $(ladder_rates); do
        number=$((number + 1))
        round="$(printf 'r%02d' "$number")"
        RATE="$rate"
        N="$(ladder_n "$rate")"
        set_analyzer_env "$N"
        echo "${round},${RATE},${N}" >> "${cond_dir}/rates-${cond}.csv"
        echo "=== 계단 ${round}: ${RATE}건/초, N=${N}"
        run_round_segments "$cond" "$round" "$cond_dir"
        status=$?
        if [ "$status" -eq 2 ]; then
            break
        fi
        reasons="$(awk -F, -v r="$round" '$1 == r { print $2 }' "${cond_dir}/saturation-${cond}.csv" 2> /dev/null | tail -n 1)"
        if [ "$number" -gt 1 ] && [ -n "$reasons" ] && [ "$reasons" != "none" ]; then
            first_saturated="$rate"
            echo "포화 신호가 처음 나온 속도: ${rate}건/초 (${reasons}) — 계단을 멈춘다"
            break
        fi
        sleep "${ROUND_GAP_SEC:-0}"
    done
    echo "$first_saturated" > "${cond_dir}/ladder-first-saturated.txt"
    return "$status"
}

# ---------------------------------------------------------------- 회차와 세트

# measure-baseline.sh 의 run_round 와 같은 절차에 단계 덤프 수집과 포화 계산을 더한 것. 비움(ROUND_CLEAR)은 쓰지 않는다.
# 포화 계산은 saturation-<cond>.csv 에 한 줄 쓴다: round,reasons,dropped,http_fail,hist_count,n,lag,first_p50_us,last_p50_us,drain_sec,arrival_sec
SATURATION_HEADER="round,reasons,dropped,http_fail,hist_count,n,lag,first_p50_us,last_p50_us,drain_sec,arrival_sec"

run_round_segments() {
    local cond="$1" round="$2" cond_dir="$3"
    echo "--- ${cond} ${round}"
    local db_before="" db_after="" db_delta=""
    local order_lines match_lines
    if [ "$cond" = "v1" ]; then db_before="$(orders_count)"; fi
    order_lines="$(log_line_count loadtest/logs/order-engine.log)"
    match_lines="$(log_line_count loadtest/logs/matching-engine.log)"

    if [ "$cond" = "v1" ] && [ "$INDEX_MODE" = "1" ]; then
        record_dbstats "$cond_dir" "$cond" "$round" start
    fi

    SAMPLER_COND="$cond" ./loadtest/sample-env.sh "${cond_dir}/cpu-${cond}-${round}.csv" &
    local sampler_pid=$!
    sleep 5

    local k6_start k6_end k6_exit
    k6_start="$(date +%s)"
    # 계측을 켠 세트는 예정 도착 시각 헤더(X-Sched-At)를 실어 api 필터가 G 를 기록하게 한다. 끈 세트는 b0·b1 과 같은 요청이다
    SCHED_HEADER="$([ "${MEASURE_STAGES:-0}" = "1" ] && echo 1 || echo 0)" \
        PRE_ALLOCATED_VUS="$K6_PRE_ALLOCATED_VUS" MAX_VUS="$K6_MAX_VUS" \
        RATE="$RATE" N="$N" VERSION="$cond" PASS="$round" INPUT_FILE="$INPUT_FILE" \
        k6 run --summary-export="${cond_dir}/k6-${cond}-${round}.json" loadtest/replay-e2e.js \
        > "${cond_dir}/logs/k6-${cond}-${round}.out" 2>&1
    k6_exit=$?
    k6_end="$(date +%s)"

    wait_for_drain "$cond" "${db_before:-0}"

    # 후속 처리 완료까지의 시간(포화 판정의 배수 시간). v1 은 FILLED 개수가 멈추고 컨슈머 lag 가 0이 될 때까지.
    # v2 는 하네스가 후속 처리 완료를 볼 수단이 없어 NA 로 둔다(고정 대기만 한다).
    local drain_sec="NA" lag_total="NA"
    if [ "$cond" = "v1" ]; then
        if await_quiescent > /dev/null; then
            drain_sec=$(($(date +%s) - k6_end))
        else
            drain_sec=$(($(date +%s) - k6_end))
            echo "후속 처리 대기 시간 초과 — 배수 시간 ${drain_sec}초로 기록" >&2
        fi
        lag_total="$(sum_lags "$(group_lag "$(v1_consumer_group)")" "$(group_lag matching-engine)" "$(group_lag settlement-engine)")"
    fi

    rm -f "${RESULTS_DIR}/latency-${cond}-${round}.json"
    request_latency_snapshot "$cond" "$round"
    if [ -f "${RESULTS_DIR}/latency-${cond}-${round}.json" ]; then
        mv "${RESULTS_DIR}/latency-${cond}-${round}.json" "${cond_dir}/latency-${cond}-${round}.json"
    fi
    if [ "${MEASURE_STAGES:-0}" = "1" ]; then
        collect_stage_dumps "$cond" "$round" "$cond_dir" || true
    fi

    kill "$sampler_pid" 2> /dev/null
    wait "$sampler_pid" 2> /dev/null

    if [ "$cond" = "v1" ]; then
        db_after="$(orders_count)"
        db_delta=$((db_after - db_before))
    fi

    local e6=0 e7=0
    e6="$(detect_rebalance "$cond" "$order_lines" "$match_lines")"
    all_apps_alive "$cond" || e7=1

    local args=(judge --dir "$cond_dir" --cond "$cond" --round "$round" --k6-start "$k6_start" --k6-end "$k6_end"
        --k6-exit "$k6_exit")
    [ -n "$db_delta" ] && args+=(--db-delta "$db_delta")
    [ "$e6" = "1" ] && args+=(--e6)
    [ "$e7" = "1" ] && args+=(--e7)
    python3 loadtest/analyze-baseline.py "${args[@]}"

    record_saturation "$cond" "$round" "$cond_dir" "$k6_start" "$k6_end" "$lag_total" "$drain_sec"

    if [ "$e7" = "1" ]; then
        echo "E7: 앱이 죽었다 — ${cond} 조건을 중단한다. 처음부터 다시 돌린다 (파일은 지우지 않는다)" >&2
        return 2
    fi
}

# 컨슈머 그룹 lag 합. NA 가 하나라도 있으면 NA
sum_lags() {
    local total=0 value
    for value in "$@"; do
        is_number "$value" || { echo NA; return; }
        total=$((total + value))
    done
    echo "$total"
}

# saturation csv 한 줄. 이유에 쉼표가 있을 때(이유가 둘 이상)만 큰따옴표로 감싸 열이 밀리지 않게 한다. 이유가 하나이면 따옴표가 없어
# awk -F, 로 두 번째 칸이 none 인지 보는 기존 코드가 그대로 동작한다
saturation_csv_line() {
    local round="$1" reasons="$2"
    shift 2
    case "$reasons" in
        *,*) reasons="\"${reasons}\"" ;;
    esac
    echo "${round},${reasons},$1,$2,$3,$4,$5,$6,$7,$8,$9"
}

record_saturation() {
    local cond="$1" round="$2" cond_dir="$3" k6_start="$4" k6_end="$5" lag="$6" drain="$7"
    local rounds_file="${cond_dir}/rounds-${cond}.csv" file="${cond_dir}/saturation-${cond}.csv"
    local dropped failed hist first last arrival reasons
    dropped="$(round_fact "$rounds_file" "$round" dropped)"
    failed="$(round_fact "$rounds_file" "$round" http_fail)"
    hist="$(round_fact "$rounds_file" "$round" hist_count)"
    read -r first last <<< "$(series_edges "${STAGE_RESULTS_ABS}/series-${cond}-$(series_label_for "$cond").csv" "$k6_start" "$k6_end")"
    arrival=$(((N + RATE - 1) / RATE))
    reasons="$(saturation_reasons "$dropped" "$failed" "$hist" "$N" "$lag" "$first" "$last" "$drain" "$arrival")"
    [ -f "$file" ] || echo "$SATURATION_HEADER" > "$file"
    saturation_csv_line "$round" "$reasons" "$dropped" "$failed" "$hist" "$N" "$lag" "$first" "$last" "$drain" "$arrival" >> "$file"
    echo "포화 판정 ${round}: ${reasons}"
}

# 계단(L)과 4,000건/초(S5X)에서는 E1·E5 가 포화의 증상이라 제외 사유가 아니라 기록만 남긴다(선언 문서 4-4절, 4-5절). 다른 단계는 비워 둔다
record_only_for_step() {
    case "$1" in
        L | S5X) echo "E1 E5" ;;
        *) echo "" ;;
    esac
}

# 묶음 C 의 judge 환경: E2(k6 CPU)는 회차를 제외하지 않고 기록만 하고(measure-protocol-c.md 첫 문단), 기대 건수는 세트의 N 이다.
# 환경 변수가 없을 때 analyze-baseline.py 의 b0·b1 동작은 그대로다.
set_analyzer_env() {
    export ANALYZE_E2=0
    export ANALYZE_EXPECTED_COUNT="$1"
}

# 세트 하나: 앱 기동부터 ROUNDS 회까지. 결과 폴더는 RUN_DIR 을 세트 폴더로 바꿔서 기존 함수들의 경로(${RUN_DIR}/${cond}/...)를 그대로 쓴다.
# 끝나면 SET_STATUS(0 정상, 1 준비 실패, 2 E7)와 SET_SATURATED(yes|no), SET_MEASURED 를 남긴다.
run_set() {
    local index="$1" token="$2" step cond index_mode stages
    read -r step cond index_mode stages <<< "$(parse_set_token "$token")" || return 1
    read -r RATE N ROUNDS <<< "$(step_config "$step")" || return 1
    LADDER_ACTIVE=0
    [ "$step" = "L" ] && LADDER_ACTIVE=1
    INDEX_MODE="$index_mode"
    CONDITIONS="$cond"
    set_analyzer_env "$N"
    export ANALYZE_RECORD_ONLY="$(record_only_for_step "$step")"
    export MEASURE_STAGES="$stages"
    local set_name set_cond_label
    set_cond_label="$cond"
    [ "$index_mode" = "1" ] && set_cond_label="v1idx"
    set_name="$(printf '%02d' "$index")-${step}-${set_cond_label}-st${stages}"
    RUN_DIR="${SEGMENT_ROOT}/${set_name}"
    local cond_dir="${RUN_DIR}/${cond}"
    mkdir -p "${cond_dir}/logs"
    echo "=== 세트 ${set_name}: RATE=${RATE} N=${N} 회차=${ROUNDS} INDEX_MODE=${INDEX_MODE} MEASURE_STAGES=${MEASURE_STAGES}"
    echo "set=${set_name} start=$(date +%s)" > "${RUN_DIR}/set.txt"
    rm -f "${STAGE_RESULTS_ABS}"/stage-trigger-* "${STAGE_RESULTS_ABS}"/series-*.csv

    SET_STATUS=0
    SET_SATURATED=no
    SET_MEASURED=0
    if ! preflight_seed; then
        echo "시드가 모자라 세트를 시작하지 않는다" >&2
        SET_STATUS=1
        RUN_DIR="$SEGMENT_ROOT"
        return 1
    fi
    if ! prepare_condition "$cond"; then
        SET_STATUS=1
    else
        record_autovacuum before "$cond_dir" "$cond"
        local number round status
        if [ "$LADDER_ACTIVE" = "1" ]; then
            run_ladder_rounds "$cond" "$cond_dir"
            status=$?
            if [ "$status" -eq 2 ]; then SET_STATUS=2; fi
        else
            for number in $(seq 1 "$ROUNDS"); do
                round="$(printf 'r%02d' "$number")"
                run_round_segments "$cond" "$round" "$cond_dir"
                status=$?
                if [ "$status" -eq 2 ]; then SET_STATUS=2; break; fi
                if [ "$number" -lt "$ROUNDS" ]; then sleep "$ROUND_GAP_SEC"; fi
            done
        fi
        record_autovacuum after "$cond_dir" "$cond"
        copy_series_files "$cond" "$cond_dir"
        if [ "$INDEX_MODE" = "1" ] && [ "$cond" = "v1" ]; then
            record_schema "${cond_dir}/schema-after.txt"
        fi
        cp loadtest/logs/*.log "${cond_dir}/logs/" 2> /dev/null || true
        ./loadtest/stop-"${cond}".sh
        saturated_round_count "$cond_dir" "$cond"
    fi
    b1_cleanup
    RUN_DIR="$SEGMENT_ROOT"
    return "$SET_STATUS"
}

# saturation-<cond>.csv 의 워밍업(r01) 뒤 회차 중 포화 회차 수로 SET_SATURATED 와 SET_MEASURED 를 정한다
saturated_round_count() {
    local cond_dir="$1" cond="$2" file saturated measured
    file="${cond_dir}/saturation-${cond}.csv"
    [ -f "$file" ] || return 0
    measured="$(awk -F, 'NR > 1 && $1 != "r01" { n++ } END { print n + 0 }' "$file")"
    saturated="$(awk -F, 'NR > 1 && $1 != "r01" && $2 != "none" { n++ } END { print n + 0 }' "$file")"
    SET_MEASURED="$measured"
    SET_SATURATED="$(step_is_saturated "$saturated" "$measured")"
}

# 계획의 세트를 순서대로 돌린다. S1 이 포화한 단계로 나오면 중단한다 (4-1절). 모든 세트가 끝나면 단계 사이 보간 단계를 하나씩 더한다 (4-2절)
run_plan() {
    local plan="$1" index=0 token step cond_key stale
    # bash 3.2(macOS 기본)에는 연관 배열이 없어, 단계와 조건을 이름에 넣은 변수 SEGSAT_<단계>_<조건> 에 포화 여부를 담는다
    for stale in $(compgen -v SEGSAT_); do unset "$stale"; done
    for token in $plan; do
        index=$((index + 1))
        read -r step _ _ _ <<< "$(parse_set_token "$token")" || return 1
        cond_key="$(echo "$token" | cut -d: -f2)"
        run_set "$index" "$token" || echo "세트 ${token} 가 끝까지 가지 못했다 (상태 ${SET_STATUS})" >&2
        printf -v "SEGSAT_${step}_${cond_key}" '%s' "$SET_SATURATED"
        if [ "$step" = "S1" ] && [ "$SET_SATURATED" = "yes" ]; then
            echo "S1 이 포화한 단계로 나왔다 — 측정을 중단하고 보고한다 (세트 ${token})" >&2
            return 4
        fi
    done
    local low high pair rate low_var high_var cond low_rate high_rate
    for pair in "S1 S3 192 1000" "S3 S5 1000 4000"; do
        read -r low high low_rate high_rate <<< "$pair"
        for cond in v1idx v2; do
            low_var="SEGSAT_${low}_${cond}"
            high_var="SEGSAT_${high}_${cond}"
            if [ "$(needs_interpolation "${!low_var:-no}" "${!high_var:-no}")" = "yes" ]; then
                rate="$(interp_rate "$low_rate" "$high_rate")"
                index=$((index + 1))
                echo "보간 단계 추가: ${low}~${high} 사이 I${rate} (${cond})"
                run_set "$index" "I${rate}:${cond}:1" || true
            fi
        done
    done
    return 0
}

main() {
    local start_epoch
    start_epoch="$(date +%s)"
    local plan="${SEGMENT_SETS:-$(default_set_plan)}"

    if [ "$PREFLIGHT_ONLY" = "1" ]; then
        read -r RATE N ROUNDS <<< "$(step_config S1X)"
        run_preflight
        local preflight_status=$?
        section "시작 검사: src/main 허용 목록"
        check_src_main_allowlist || preflight_status=1
        section "시작 검사: 선언 문서 커밋 시각"
        check_protocol_precedes_start "$start_epoch" || preflight_status=1
        echo "실행 계획: ${plan}"
        exit $preflight_status
    fi

    mkdir -p "$RUN_DIR"
    read -r RATE N ROUNDS <<< "$(step_config S1X)"
    run_preflight 2>&1 | tee "${RUN_DIR}/preflight.txt"
    if [ "${PIPESTATUS[0]}" -ne 0 ]; then
        exit 1
    fi
    {
        preflight_env_record
        echo "run_id=${RUN_ID}"
        echo "start=${start_epoch} $(date)"
        echo "HEAD=$(git rev-parse HEAD)"
        echo "PLAN=${plan}"
        echo "ROUND_GAP_SEC=${ROUND_GAP_SEC} INCLUDE_KAFKA_UI=${INCLUDE_KAFKA_UI} V1_LISTENER_CONCURRENCY=${V1_LISTENER_CONCURRENCY} V1_PARTITIONS=${V1_PARTITIONS}"
        check_src_main_allowlist || echo "ALLOWLIST_CHECK_FAILED"
        git diff --name-only "$B1_BASE_SHA" -- '*/src/main/*' | sed 's/^/허용 목록 대조 대상: /'
        check_protocol_precedes_start "$start_epoch" || echo "PROTOCOL_ORDER_CHECK_FAILED"
    } > "${RUN_DIR}/env.txt" 2>&1
    if grep -q "ALLOWLIST_CHECK_FAILED\|PROTOCOL_ORDER_CHECK_FAILED" "${RUN_DIR}/env.txt"; then
        echo "시작 검사에 걸렸다 — env.txt 를 본다" >&2
        exit 1
    fi
    fix_protocol >> "${RUN_DIR}/env.txt" || exit 1
    echo "$plan" > "${RUN_DIR}/plan.txt"

    trap 'b1_cleanup' EXIT
    trap 'exit 130' INT TERM
    if [ "$CAFFEINATE" = "1" ]; then
        caffeinate -i -w $$ &
    fi
    load_snapshot_function

    run_plan "$plan"
    local status=$?
    echo "완료(상태 ${status}) — ${RUN_DIR}"
    exit $status
}

# 테스트가 함수만 불러올 수 있게, MEASURE_SOURCE_ONLY=1 이면 main 을 실행하지 않는다
if [ "${MEASURE_SOURCE_ONLY:-0}" != "1" ]; then
    main
fi
