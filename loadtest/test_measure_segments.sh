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
    assert_eq "S1R(리허설 1+2회)" "192 10000 3" "$(step_config S1R)"
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
    assert_eq "v1 대상(api 의 G 포함)" "api order-engine matching-engine settlement-engine" "$(stage_trigger_targets v1)"
    assert_eq "v2 대상" "api account-worker matching-worker account-projection-worker" "$(stage_trigger_targets v2)"
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

test_experiment_env_record() {
    assert_eq "EXPERIMENT_IDLE 이 없으면 기록하지 않는다" "" "$(unset EXPERIMENT_IDLE; experiment_env_record)"
    assert_eq "빈 값이면 기록하지 않는다" "" "$(EXPERIMENT_IDLE= experiment_env_record)"
    assert_eq "threads 를 기록한다" "EXPERIMENT_IDLE=threads" "$(EXPERIMENT_IDLE=threads experiment_env_record)"
    assert_eq "driver 를 기록한다" "EXPERIMENT_IDLE=driver" "$(EXPERIMENT_IDLE=driver experiment_env_record)"
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
    assert_fails "인덱스 없는 v1 조건은 받지 않는다" parse_set_token S1:v1:1
    assert_eq "인덱스 없는 v1 조건 거부 메시지" "1" "$(parse_set_token S1:v1:1 2>&1 | grep -c '모르는 조건: v1 ')"
    assert_not_contains "기본 계획에 인덱스 없는 v1 세트가 없다" "$(default_set_plan)" ":v1:"
    assert_contains "선언 문서가 E3의 전원 부분을 기록 전용으로 밝힌다" "$(cat loadtest/measure-protocol-c.md)" "측정은 배터리 전원에서 수행했다. E3의 전원 부분은 기록 전용으로 바꿨고, 이 변경은 이 실행의 분석 전에 정했다"
    assert_contains "선언 문서의 E3 설명이 CPU 속도 제한만 남긴다" "$(cat loadtest/measure-protocol-c.md)" "E3(CPU 속도 제한"
    local protocol_text
    protocol_text="$(cat loadtest/measure-protocol-c.md)"
    assert_contains "선언 문서가 L·S5X의 E1·E5 기록 전용을 밝힌다" "$protocol_text" "계단(L)과 4,000건/초(S5X) 단계에서는 E1과 E5를 기록 전용으로 둔다(제외 사유로 쓰지 않는다)"
    assert_contains "선언 문서가 변경 시점과 이유를 숨기지 않는다" "$protocol_text" "이 변경은 L·S5X 결과를 본 뒤에 정했다. 이유: 포화 회차에서 E1·E5는 포화가 일으키는 증상이고 환경 오류를 뜻하지 않는다"
    assert_contains "선언 문서가 포화 회차 처리를 그대로 둔다" "$protocol_text" "포화 회차는 4-1절대로 표에 싣고 대표값에서 뺀다"
    assert_contains "선언 문서 1절에 재측정 전 수정 네 가지가 있다" "$protocol_text" "재측정 전에 v2를 다섯 가지 바꿨다"
    assert_contains "선언 문서가 즉시 202를 밝힌다" "$protocol_text" "전달 큐에 넣고 곧바로 202를 준다"
    assert_contains "선언 문서가 v1 api는 바꾸지 않았음을 밝힌다" "$protocol_text" "v1 api는 바꾸지 않는다"
    assert_contains "선언 문서가 LTP 기록을 밝힌다" "$protocol_text" "체결가를 Redis 최근 체결가에 기록한다"
    assert_contains "선언 문서가 호가창 스냅샷 기록을 밝힌다" "$protocol_text" "호가창 상위 10단계 스냅샷도 같은 방식으로 Redis에 기록한다"
    assert_contains "선언 문서의 호가창 한계 문구" "$protocol_text" "호가창 스냅샷은 v1이 이벤트마다 쓰고 v2는 종목별 마지막 상태를 100ms마다 쓴다"
    assert_contains "선언 문서가 재측정 전 수정이 다섯 가지임을 밝힌다" "$protocol_text" "재측정 전에 v2를 다섯 가지 바꿨다"
    assert_contains "선언 문서가 로그 WARN을 밝힌다" "$protocol_text" "우리 코드의 로그를 WARN으로 맞춘다"
    assert_contains "선언 문서가 이전 결과의 코드 상태를 밝힌다" "$protocol_text" "이전 S1X·S5X 결과는 이 변경 전 코드의 값이다"
    assert_contains "선언 문서의 한계 문구" "$protocol_text" "클라이언트가 전달 실패를 읽는 방법은 두 구현 모두 없다. v2는 실패 기록이 api 서버의 Redis에 남는다"
    assert_contains "선언 문서가 큐 대기가 구간 A에 들어감을 밝힌다" "$protocol_text" "전달 큐 대기 시간이 포함된다"
    assert_contains "W-P0 줄 인용이 새 위치다" "$protocol_text" 'W-P0 `OrderV2ApiService.java:60,72`'
    assert_not_contains "옛 W-P0 줄 인용이 남지 않는다" "$protocol_text" "AeronAccountOrderSender.java:95"
    assert_contains "선언 문서에 포화점 탐색 절이 있다" "$protocol_text" "### 4-4. 포화점 탐색"
    assert_contains "선언 문서의 계단 속도가 코드 기본값과 같다" "$protocol_text" "192(워밍업 회차), 300, 400, 500, 700, 1,000, 1,500, 2,000, 3,000, 4,000건/초"
    assert_contains "선언 문서가 새 임계값을 만들지 않는다고 밝힌다" "$protocol_text" "새 임계값을 만들지 않고"
    assert_contains "선언 문서에 G p99 신호가 있다" "$protocol_text" "G p99 ≥ 같은 회차 F p50"
    assert_contains "선언 문서에 4,000건/초 재측정 절이 있다" "$protocol_text" "### 4-5. 4,000건/초 재측정"
    assert_contains "선언 문서에 재측정 근거 문장이 있다" "$protocol_text" "초당 4만건 이상의 거래 체결을 지원"
    assert_contains "선언 문서가 비율이 임의임을 밝힌다" "$protocol_text" "비율은 임의이고"
    assert_contains "선언 문서의 보고 항목에 메모리가 있다" "$protocol_text" "프로세스별 CPU 코어 수와 메모리"
    assert_contains "선언 문서가 RSS가 힙이 아님을 밝힌다" "$protocol_text" "힙 크기가 아니다"
    assert_not_contains "선언 문서에 인덱스 없는 v1 조건이 없다" "$(cat loadtest/measure-protocol-c.md)" "인덱스 없"
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

test_g_wiring() {
    # G(예정 도착 시각과 api 처리 시작 시각의 차이): v1·v2 api 모두 계측 인자를 받고 k6 에 헤더를 켜 준다
    assert_contains "v1 기동 스크립트가 api 에 계측 인자를 붙인다" "$(cat loadtest/run-e2e-v1.sh)" 'stage_args v1 api'
    assert_contains "v2 기동 스크립트가 api 에 계측 인자를 붙인다" "$(cat loadtest/run-e2e-v2.sh)" 'stage_args v2 api'
    assert_contains "계측 켠 세트의 k6 에 SCHED_HEADER 를 넘긴다" "$(declare -f run_round_segments)" 'SCHED_HEADER'
    assert_eq "api 는 시계열을 쓰지 않는다(v1)" "" "$(MEASURE_STAGES=1 stage_args v1 api | grep -o 'series-path=[^ ]*')"
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

# run_plan 은 단계|조건 별 포화 결과를 기억했다가 보간 단계를 넣는다. macOS 기본 bash 3.2 에서도 돌아야 한다
# (연관 배열 declare -A 는 bash 4+ 라 3.2 에서 "S1R: unbound variable" 로 중단된 적이 있다).
# run_set 은 앱·부하를 띄우므로 기록만 하는 가짜로 바꾼다. run_plan 은 서브셸에서 돌린다 — 3.2 에서 변수 오류가 나면 테스트 셸째로 조용히 끝나기 때문이다.
test_run_plan() {
    local calls_file out status original_run_set
    original_run_set="$(declare -f run_set)"
    calls_file="$(mktemp)"
    run_set() {
        echo "$2" >> "$calls_file"
        case "$2" in
            S5:v1idx:1) SET_SATURATED=yes ;;
            S1:v2:1) SET_SATURATED=${FAKE_S1_V2_SATURATED:-no} ;;
            *) SET_SATURATED=no ;;
        esac
        return 0
    }

    ( run_plan "S1R:v1idx:1 S1R:v2:0" ) > /dev/null 2>&1
    status=$?
    assert_eq "S1R 계획이 끝까지 간다" "0" "$status"
    assert_eq "S1R 계획의 호출 수" "2" "$(wc -l < "$calls_file" | tr -d ' ')"

    : > "$calls_file"
    ( run_plan "S1:v1idx:1 S1:v2:1 S3:v1idx:1 S3:v2:1 S5:v1idx:1 S5:v2:1" ) > /dev/null 2>&1
    status=$?
    assert_eq "6세트 계획이 끝까지 간다" "0" "$status"
    assert_eq "S3 낮음 S5 포화이면 보간 단계가 하나 붙는다" "I2000:v1idx:1" "$(tail -n 1 "$calls_file")"
    assert_eq "보간은 v1idx 에만 붙는다" "7" "$(wc -l < "$calls_file" | tr -d ' ')"

    : > "$calls_file"
    ( FAKE_S1_V2_SATURATED=yes run_plan "S1:v1idx:1 S1:v2:1 S3:v1idx:1" ) > /dev/null 2>&1
    status=$?
    assert_eq "S1 이 포화하면 4 로 중단한다" "4" "$status"
    assert_eq "S1 포화 뒤 세트는 돌지 않는다" "2" "$(wc -l < "$calls_file" | tr -d ' ')"

    eval "$original_run_set"
    rm -f "$calls_file"
}

# 계단(L)과 4,000건/초(S5X)에서는 E1·E5 가 포화의 증상이라 기록 전용이다. 다른 단계는 그대로 제외 사유다
test_record_only_steps() {
    assert_eq "계단은 E1 E5 기록 전용" "E1 E5" "$(record_only_for_step L)"
    assert_eq "4,000건/초 재측정은 E1 E5 기록 전용" "E1 E5" "$(record_only_for_step S5X)"
    assert_eq "S1X 는 그대로" "" "$(record_only_for_step S1X)"
    assert_eq "S5 는 그대로" "" "$(record_only_for_step S5)"
    assert_eq "보간 단계도 그대로" "" "$(record_only_for_step I450)"
}

# saturation csv 는 이유가 둘 이상(쉼표 포함)이어도 열이 밀리지 않아야 한다
test_saturation_csv_line() {
    local line columns reasons
    line="$(saturation_csv_line r02 "k6_dropped,drain" 14384 0 40000 40000 0 NA NA 129 10)"
    columns="$(python3 -c 'import csv,sys; r=next(csv.reader([sys.argv[1]])); print(len(r), r[1], r[4], r[9])' "$line")"
    assert_eq "이유가 둘이어도 열이 11개이고 값이 제자리에 있다" "11 k6_dropped,drain 40000 129" "$columns"
    line="$(saturation_csv_line r03 none 0 0 40000 40000 0 100 120 5 10)"
    columns="$(python3 -c 'import csv,sys; r=next(csv.reader([sys.argv[1]])); print(len(r), r[1], r[9])' "$line")"
    assert_eq "이유가 none 이어도 같다" "11 none 5" "$columns"
}

# 우리 코드의 로거만 WARN으로 올린다(주문당 INFO 로그 제거). 루트를 올리면 하네스가 읽는 프레임워크 INFO 줄이 사라진다
test_app_log_level() {
    assert_eq "하네스 기본은 WARN" "WARN" "$APP_LOG_LEVEL"
    assert_eq "설정하지 않으면 인자가 없다(기존 b0·b1 기동 인자와 같다)" "" "$(APP_LOG_LEVEL="" app_log_args)"
    assert_eq "WARN이면 우리 패키지 인자 하나가 붙는다" " --logging.level.com.flab.stocktradingengine=WARN" "$(APP_LOG_LEVEL=WARN app_log_args)"
    assert_not_contains "루트 로거는 건드리지 않는다" "$(APP_LOG_LEVEL=WARN app_log_args)" "logging.level.root"
    assert_contains "v1 기동 스크립트가 인자를 붙인다" "$(cat loadtest/run-e2e-v1.sh)" 'app_log_args'
    assert_contains "v2 기동 스크립트가 인자를 붙인다" "$(cat loadtest/run-e2e-v2.sh)" 'app_log_args'
    assert_not_contains "기동 스크립트는 선언 해시 대상이 아니다" "$PROTOCOL_FILES" "run-e2e"
    assert_not_contains "stage-args.sh도 해시 대상이 아니다" "$PROTOCOL_FILES" "stage-args"
}

# k6 가상 사용자(VU) 수: 필요한 동시 요청 수는 도착률 x 응답 시간(Little의 법칙)이다. 기본 maxVUs 300은 응답 5ms 가정이라 4,000건/초에서 모자랐다
test_k6_vus() {
    assert_eq "미리 만들어 둘 VU 기본값" "500" "$K6_PRE_ALLOCATED_VUS"
    assert_eq "최대 VU 기본값" "1500" "$K6_MAX_VUS"
    local body
    body="$(declare -f run_round_segments)"
    assert_contains "회차 실행이 k6에 PRE_ALLOCATED_VUS를 넘긴다" "$body" 'PRE_ALLOCATED_VUS="$K6_PRE_ALLOCATED_VUS"'
    assert_contains "회차 실행이 k6에 MAX_VUS를 넘긴다" "$body" 'MAX_VUS="$K6_MAX_VUS"'
    local protocol_text
    protocol_text="$(cat loadtest/measure-protocol-c.md)"
    assert_contains "선언 문서에 VU 설정과 근거가 있다" "$protocol_text" "PRE_ALLOCATED_VUS=500, MAX_VUS=1500"
    assert_contains "선언 문서가 Little의 법칙으로 근거를 적는다" "$protocol_text" "Little의 법칙"
    assert_contains "선언 문서가 설정값 오류로 적는다" "$protocol_text" "설정값 오류(응답 시간 가정이 틀림)"
}

# 포화점 탐색(계단) 세트 L: 회차마다 속도가 오른다. 첫 회차는 워밍업이고, 포화 신호가 처음 나온 회차에서 멈춘다
test_ladder() {
    assert_eq "기본 계단 속도" "192 300 400 500 700 1000 1500 2000 3000 4000" "$(ladder_rates)"
    assert_eq "속도를 환경 변수로 바꾼다" "192 1000 2000" "$(LADDER_RATES="192 1000 2000" ladder_rates)"
    assert_eq "N 은 속도 x 도착 20초" "6000" "$(ladder_n 300)"
    assert_eq "N 은 4만으로 자른다(시드 용량)" "40000" "$(ladder_n 3000)"
    assert_eq "4000/초도 4만" "40000" "$(ladder_n 4000)"
    assert_eq "회차별 건수 목록" "3840 6000 8000 10000 14000 20000 30000 40000 40000 40000" "$(ladder_counts)"
    assert_eq "4,000건/초 재측정 단계는 기본 1+5회" "4000 40000 6" "$(step_config S5X)"
    assert_eq "4,000건/초 재측정 회차 수를 바꾼다" "4000 40000 4" "$(S5X_ROUNDS=4 step_config S5X)"
    assert_eq "4,000건/초 재측정도 세트 표기로 받는다" "S5X v2 0 1" "$(parse_set_token S5X:v2:1)"
    assert_eq "계단 설정은 첫 속도, 첫 건수, 회차 수" "192 3840 10" "$(step_config L)"
    assert_eq "계단 세트도 세트 표기로 받는다" "L v1 1 1" "$(parse_set_token L:v1idx:1)"

    assert_eq "계단 세트는 회차별 건수로 시드를 확인한다" "--counts 3840 6000 8000 10000 14000 20000 30000 40000 40000 40000" "$(LADDER_ACTIVE=1 seed_check_args | tr '\n' ' ' | sed 's/ $//')"
    local saved_n="${N:-}" saved_rounds="${ROUNDS:-}"
    N=10000
    ROUNDS=3
    assert_eq "일반 세트는 건수 x 회차" "--count 10000 --rounds 3" "$(LADDER_ACTIVE=0 seed_check_args | tr '\n' ' ' | sed 's/ $//')"
    N="$saved_n"
    ROUNDS="$saved_rounds"

    local dir calls original_round
    dir="$(mktemp -d)"
    calls="$(mktemp)"
    original_round="$(declare -f run_round_segments)"
    run_round_segments() {
        echo "${2}:${RATE}:${N}" >> "$calls"
        [ -f "${3}/saturation-v1.csv" ] || echo "$SATURATION_HEADER" > "${3}/saturation-v1.csv"
        local reasons=none
        [ "$RATE" -ge 1000 ] && reasons=k6_dropped
        echo "${2},${reasons},0,0,${N},${N},0,NA,NA,NA,20" >> "${3}/saturation-v1.csv"
        return 0
    }
    ROUND_GAP_SEC=0 LADDER_RATES="192 300 700 1000 1500" run_ladder_rounds v1 "$dir" > /dev/null 2>&1
    assert_eq "포화 신호가 처음 나온 1000에서 멈춘다" "r01:192:3840 r02:300:6000 r03:700:14000 r04:1000:20000" "$(tr '\n' ' ' < "$calls" | sed 's/ $//')"
    assert_eq "속도 기록 파일" "round,rate,n r01,192,3840 r02,300,6000 r03,700,14000 r04,1000,20000" "$(tr '\n' ' ' < "$dir/rates-v1.csv" | sed 's/ $//')"
    assert_eq "처음 포화한 속도를 남긴다" "1000" "$(cat "$dir/ladder-first-saturated.txt")"

    : > "$calls"
    rm -f "$dir"/*.csv "$dir"/ladder-first-saturated.txt
    ROUND_GAP_SEC=0 LADDER_RATES="192 300 700" run_ladder_rounds v1 "$dir" > /dev/null 2>&1
    assert_eq "포화가 없으면 끝까지 간다" "3" "$(wc -l < "$calls" | tr -d ' ')"
    assert_eq "포화가 없으면 포화 속도 없음" "none" "$(cat "$dir/ladder-first-saturated.txt")"
    eval "$original_round"
    rm -rf "$dir" "$calls"
}

test_k6_vus
test_app_log_level
test_record_only_steps
test_saturation_csv_line
test_ladder
test_step_config
test_interpolation_rate
test_run_plan
test_saturation_reasons
test_analyzer_env
test_saturation_has_no_cpu_signal
test_step_saturation
test_series_edges
test_trigger_targets
test_trigger_stage_dump
test_allowlist
test_allowlist_current_branch
test_experiment_env_record
test_commit_precedes
test_set_token
test_stage_args
test_g_wiring
test_round_fact

echo "통과 ${PASSES}, 실패 ${FAILS}"
[ "$FAILS" -eq 0 ]
