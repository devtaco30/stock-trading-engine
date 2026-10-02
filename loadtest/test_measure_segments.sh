#!/usr/bin/env bash
set -uo pipefail

# measure-segments.sh 와 stage-args.sh 의 순수 함수(단계 구성, 포화 판정, 허용 목록 대조, 트리거 대상, 기동 인자)를
# 확인하는 테스트. 앱·Docker·부하 없이 돈다.
# 실행: bash loadtest/test_measure_segments.sh   (저장소 루트에서)

cd "$(dirname "$0")/.."
MEASURE_SOURCE_ONLY=1 source loadtest/measure-segments.sh
set +e

PASSES=0
FAILS=0

assert_eq() {
    local name="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        PASSES=$((PASSES + 1))
    else
        FAILS=$((FAILS + 1))
        echo "FAIL ${name}: 기대=[${expected}] 실제=[${actual}]"
    fi
}

assert_contains() {
    local name="$1" haystack="$2" needle="$3"
    if printf '%s' "$haystack" | grep -qF -- "$needle"; then
        PASSES=$((PASSES + 1))
    else
        FAILS=$((FAILS + 1))
        echo "FAIL ${name}: [${needle}] 가 없다"
    fi
}

assert_not_contains() {
    local name="$1" haystack="$2" needle="$3"
    if printf '%s' "$haystack" | grep -qF -- "$needle"; then
        FAILS=$((FAILS + 1))
        echo "FAIL ${name}: [${needle}] 가 있으면 안 된다"
    else
        PASSES=$((PASSES + 1))
    fi
}

assert_fails() {
    local name="$1"
    shift
    if "$@" > /dev/null 2>&1; then
        FAILS=$((FAILS + 1))
        echo "FAIL ${name}: 실패해야 하는데 성공했다"
    else
        PASSES=$((PASSES + 1))
    fi
}

test_step_config() {
    assert_eq "S1" "192 10000 6" "$(step_config S1)"
    assert_eq "S3" "1000 20000 5" "$(step_config S3)"
    assert_eq "S5" "4000 40000 4" "$(step_config S5)"
    assert_eq "S1X(계측 영향 확인용 11회)" "192 10000 11" "$(step_config S1X)"
    assert_eq "보간 단계 I450" "450 9000 4" "$(step_config I450)"
    assert_eq "보간 단계 I2000" "2000 40000 4" "$(step_config I2000)"
    assert_fails "모르는 단계는 실패" step_config S2
}

test_interpolation_rate() {
    assert_eq "192와 1000 사이" "450" "$(interp_rate 192 1000)"
    assert_eq "1000과 4000 사이" "2000" "$(interp_rate 1000 4000)"
    assert_eq "50 단위 반올림" "50" "$(round_to_50 52)"
    assert_eq "50 단위 반올림(올림)" "100" "$(round_to_50 75)"
}

test_saturation_reasons() {
    # 인자: dropped http_failed hist_count n lag first_p50 last_p50 drain_sec arrival_sec
    assert_eq "모두 정상" "none" "$(saturation_reasons 0 0 10000 10000 0 100 110 5 52)"
    assert_eq "dropped 1" "k6_dropped" "$(saturation_reasons 1 0 10000 10000 0 100 110 5 52)"
    assert_eq "http 실패" "http_failed" "$(saturation_reasons 0 2 10000 10000 0 100 110 5 52)"
    assert_eq "히스토그램 건수 부족" "hist_count" "$(saturation_reasons 0 0 9990 10000 0 100 110 5 52)"
    assert_eq "lag 가 남음" "lag" "$(saturation_reasons 0 0 10000 10000 7 100 110 5 52)"
    assert_eq "마지막 20% p50 이 1.5배" "latency_growth" "$(saturation_reasons 0 0 10000 10000 0 100 150 5 52)"
    assert_eq "1.5배 미만은 정상" "none" "$(saturation_reasons 0 0 10000 10000 0 100 149 5 52)"
    assert_eq "배수가 도착 시간의 50% 초과" "drain" "$(saturation_reasons 0 0 10000 10000 0 100 110 27 52)"
    assert_eq "배수가 정확히 50%면 정상" "none" "$(saturation_reasons 0 0 10000 10000 0 100 110 26 52)"
    assert_eq "여러 이유는 쉼표로" "k6_dropped,lag" "$(saturation_reasons 3 0 10000 10000 5 100 110 5 52)"
    assert_eq "시계열을 못 읽으면(NA) 증가 판정은 건너뛴다" "none" "$(saturation_reasons 0 0 10000 10000 0 NA NA 5 52)"
    assert_eq "lag 를 못 읽으면(NA) 건너뛴다" "none" "$(saturation_reasons 0 0 10000 10000 NA 100 110 5 52)"
}

test_analyzer_env() {
    # 묶음 C 는 judge 를 E2 끄고(기록만) 기대 건수를 세트의 N 으로 부른다. b0·b1 기본 동작은 환경 변수가 없을 때 그대로다
    unset ANALYZE_E2 ANALYZE_EXPECTED_COUNT
    set_analyzer_env 20000
    assert_eq "ANALYZE_E2=0" "0" "${ANALYZE_E2:-unset}"
    assert_eq "기대 건수는 N" "20000" "${ANALYZE_EXPECTED_COUNT:-unset}"
    unset ANALYZE_E2 ANALYZE_EXPECTED_COUNT
    assert_eq "run_set 이 set_analyzer_env 를 부른다" "1" "$(declare -f run_set | grep -c 'set_analyzer_env')"
}

test_saturation_has_no_cpu_signal() {
    # k6 CPU 는 기록만 한다(protocol-c 4-3절). 포화 판정 함수와 그 호출에 CPU 입력이 없어야 한다
    assert_eq "saturation_reasons 에 cpu 가 없다" "0" "$(declare -f saturation_reasons | grep -ci 'cpu')"
    assert_eq "record_saturation 에 cpu 가 없다" "0" "$(declare -f record_saturation | grep -ci 'cpu')"
}

test_step_saturation() {
    assert_eq "측정 5회 중 3회면 포화한 단계" "yes" "$(step_is_saturated 3 5)"
    assert_eq "측정 5회 중 2회면 아니다" "no" "$(step_is_saturated 2 5)"
    assert_eq "측정 4회 중 2회는 과반이 아니다" "no" "$(step_is_saturated 2 4)"
    assert_eq "측정 4회 중 3회" "yes" "$(step_is_saturated 3 4)"
    assert_eq "낮은 단계 정상, 높은 단계 포화면 보간" "yes" "$(needs_interpolation no yes)"
    assert_eq "둘 다 정상이면 보간 없음" "no" "$(needs_interpolation no no)"
    assert_eq "낮은 단계가 포화면 보간 없음" "no" "$(needs_interpolation yes yes)"
}

test_series_edges() {
    local tmp
    tmp="$(mktemp)"
    {
        echo "epochSecond,count,p50Micros,p99Micros"
        for second in 100 101 102 103 104 105 106 107 108 109; do
            echo "${second},50,$((1000 + (second - 100) * 100)),9999"
        done
        echo "110,0,0,0"
    } > "$tmp"
    # 창 100~109(10행): 처음 20%=2행(1000,1100)→1050, 마지막 20%=2행(1800,1900)→1850
    assert_eq "처음·마지막 20% 평균" "1050 1850" "$(series_edges "$tmp" 100 109)"
    assert_eq "창 밖 행은 쓰지 않는다" "1050 1850" "$(series_edges "$tmp" 100 110)"
    assert_eq "표본이 없으면 NA" "NA NA" "$(series_edges "$tmp" 200 300)"
    assert_eq "파일이 없으면 NA" "NA NA" "$(series_edges /no/such/file 100 109)"
    rm -f "$tmp"
}

test_trigger_targets() {
    assert_eq "v1 대상" "order-engine matching-engine settlement-engine" "$(stage_trigger_targets v1)"
    assert_eq "v2 대상" "api account-worker matching-worker" "$(stage_trigger_targets v2)"
    assert_eq "트리거 경로" "${RESULTS_DIR}/stage-trigger-v2-account-worker" "$(stage_trigger_path v2 account-worker)"
}

test_trigger_stage_dump() {
    local tmp trigger dest
    tmp="$(mktemp -d)"
    trigger="${tmp}/trigger"
    dest="${tmp}/dump.json"
    # 앱을 흉내 내는 소비자: 트리거를 읽어 목적지에 쓰고 트리거를 지운다
    (
        for _ in $(seq 1 50); do
            if [ -f "$trigger" ]; then
                cat "$trigger" > /dev/null
                echo '{"A":{"count":1}}' > "$dest"
                rm -f "$trigger"
                exit 0
            fi
            sleep 0.1
        done
    ) &
    trigger_stage_dump "$trigger" "$dest" 5
    assert_eq "소비자가 있으면 성공" "0" "$?"
    assert_eq "트리거가 지워졌다" "no" "$([ -f "$trigger" ] && echo yes || echo no)"
    assert_contains "목적지가 써졌다" "$(cat "$dest" 2> /dev/null)" '"A"'
    wait
    assert_fails "소비자가 없으면 시간 초과로 실패" trigger_stage_dump "${tmp}/never" "${tmp}/never.json" 1
    assert_eq "시간 초과 뒤 트리거를 치운다" "no" "$([ -f "${tmp}/never" ] && echo yes || echo no)"
    rm -rf "$tmp"
}

test_allowlist() {
    local tmp
    tmp="$(mktemp)"
    printf '%s\n' "a/src/main/A.java" "b/src/main/B.java" > "$tmp"
    assert_eq "목록 안 변경만 있으면 비어 있다" "" "$(printf '%s\n' "a/src/main/A.java" | allowlist_violations "$tmp")"
    assert_eq "목록 밖 파일을 돌려준다" "c/src/main/C.java" "$(printf '%s\n' "a/src/main/A.java" "c/src/main/C.java" | allowlist_violations "$tmp")"
    assert_eq "변경이 없으면 비어 있다" "" "$(printf '' | allowlist_violations "$tmp")"
    assert_fails "목록 파일이 없으면 실패" bash -c 'MEASURE_SOURCE_ONLY=1 source loadtest/measure-segments.sh; printf "x\n" | allowlist_violations /no/such/list'
    rm -f "$tmp"
}

test_allowlist_current_branch() {
    local output
    output="$(check_src_main_allowlist 2>&1)"
    assert_eq "이 브랜치의 현재 src/main 변경은 허용 목록 안이다" "0" "$?"
    assert_contains "env 기록 문구" "$output" "src/main 허용 목록 대조: 목록 밖 변경 없음"
}

test_commit_precedes() {
    assert_eq "커밋이 시작보다 앞" "yes" "$(commit_precedes 1000 2000)"
    assert_eq "같은 초는 앞이 아니다" "no" "$(commit_precedes 2000 2000)"
    assert_eq "커밋이 시작보다 뒤" "no" "$(commit_precedes 3000 2000)"
    assert_eq "숫자가 아니면 no" "no" "$(commit_precedes "" 2000)"
}

test_set_token() {
    assert_eq "v1 인덱스 있음" "S3 v1 1 1" "$(parse_set_token S3:v1idx:1)"
    assert_eq "v1 인덱스 없음" "S1 v1 0 1" "$(parse_set_token S1:v1:1)"
    assert_eq "v2 계측 끔" "S1X v2 0 0" "$(parse_set_token S1X:v2:0)"
    assert_eq "단계 표시가 없으면 기본 켬" "S5 v2 0 1" "$(parse_set_token S5:v2)"
    assert_fails "모르는 조건은 실패" parse_set_token S1:v3:1
    assert_eq "기본 계획 첫 네 세트(S1 교차)" "S1:v1idx:1 S1:v2:1 S1:v2:1 S1:v1idx:1" "$(default_set_plan | awk '{ print $1, $2, $3, $4 }')"
    assert_contains "기본 계획에 S3·S5" "$(default_set_plan)" "S5:v2:1"
}

test_stage_args() {
    # run-e2e-*.sh 가 쓰는 기동 인자. 끈 상태는 빈 문자열이어야 기존 경로가 바이트 단위로 같다
    assert_eq "끔이면 빈 문자열" "" "$(MEASURE_STAGES=0 stage_args v1 order-engine)"
    assert_eq "변수가 없어도 빈 문자열" "" "$(unset MEASURE_STAGES; stage_args v1 order-engine)"
    local on
    on="$(MEASURE_STAGES=1 stage_args v2 account-worker)"
    assert_contains "켜짐 키" "$on" "--measure.stages.enabled=true"
    assert_contains "트리거 경로" "$on" "--measure.stages.snapshot-trigger-path=${RESULTS_DIR}/stage-trigger-v2-account-worker"
    assert_contains "종료 안전망 경로" "$on" "--measure.stages.output-path=${RESULTS_DIR}/stages-v2-account-worker-final.json"
    assert_contains "시계열 경로" "$on" "--measure.stages.series-path=${RESULTS_DIR}/series-v2-account-worker.csv"
    assert_eq "api 는 시계열을 쓰지 않는다" "" "$(MEASURE_STAGES=1 stage_args v2 api | grep -o 'series-path=[^ ]*')"
}

test_round_fact() {
    local tmp
    tmp="$(mktemp)"
    printf '%s\n' "round,k6_start,k6_end,k6_exit,checks_fail,http_fail,dropped,hist_count,db_delta,flags,valid" \
        "r01,100,152,0,0,0,0,10000,10000,WARMUP,0" "r02,200,252,0,0,3,7,9990,9990,,1" > "$tmp"
    assert_eq "r02 dropped" "7" "$(round_fact "$tmp" r02 dropped)"
    assert_eq "r02 http_fail" "3" "$(round_fact "$tmp" r02 http_fail)"
    assert_eq "r01 hist_count" "10000" "$(round_fact "$tmp" r01 hist_count)"
    assert_eq "없는 회차는 NA" "NA" "$(round_fact "$tmp" r09 dropped)"
    assert_eq "없는 열은 NA" "NA" "$(round_fact "$tmp" r01 nothing)"
    rm -f "$tmp"
}

test_step_config
test_interpolation_rate
test_saturation_reasons
test_analyzer_env
test_saturation_has_no_cpu_signal
test_step_saturation
test_series_edges
test_trigger_targets
test_trigger_stage_dump
test_allowlist
test_allowlist_current_branch
test_commit_precedes
test_set_token
test_stage_args
test_round_fact

echo "통과 ${PASSES}, 실패 ${FAILS}"
[ "$FAILS" -eq 0 ]
