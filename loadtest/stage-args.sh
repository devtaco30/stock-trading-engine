#!/usr/bin/env bash
# 구간 분해 측정(묶음 C, measure/segments 브랜치 한정)의 단계 계측 기동 인자.
# run-e2e-v1.sh / run-e2e-v2.sh 와 measure-segments.sh 가 source 한다. 단독으로 실행하지 않는다.
#
# 환경 변수 (기본값)
#   MEASURE_STAGES=0   1이면 각 프로세스에 --measure.stages.* 인자를 붙인다. 0이거나 없으면 인자를 붙이지 않는다
#                      (기존 b0·b1 기동 경로가 바이트 단위로 같다).
#
# 경로는 모두 절대경로다. gradlew bootRun 의 JVM 작업 디렉터리가 모듈 디렉터리라 상대경로는 쓸 수 없다
# (run-e2e-v1.sh 의 같은 주석 참고).

STAGE_RESULTS_ABS="${STAGE_RESULTS_ABS:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/loadtest/results}"

# 단계 덤프 트리거 파일. 앱의 StageDumpRunner 가 이 파일을 보고 첫 줄의 경로에 덤프를 쓰고 파일을 지운다
stage_trigger_path() {
    echo "${STAGE_RESULTS_ABS}/stage-trigger-$1-$2"
}

# 시계열을 쓰는 프로세스. order-engine(B), settlement-engine(F), account-worker(B)
stage_has_series() {
    case "$2" in
        order-engine | settlement-engine | account-worker) return 0 ;;
        *) return 1 ;;
    esac
}

# $1=v1|v2, $2=프로세스 이름. 끄면 아무것도 출력하지 않는다
stage_args() {
    [ "${MEASURE_STAGES:-0}" = "1" ] || return 0
    local cond="$1" label="$2"
    printf ' --measure.stages.enabled=true --measure.stages.snapshot-trigger-path=%s --measure.stages.output-path=%s/stages-%s-%s-final.json' \
        "$(stage_trigger_path "$cond" "$label")" "$STAGE_RESULTS_ABS" "$cond" "$label"
    if stage_has_series "$cond" "$label"; then
        printf ' --measure.stages.series-path=%s/series-%s-%s.csv' "$STAGE_RESULTS_ABS" "$cond" "$label"
    fi
}

# 우리 코드의 로거(com.flab.stocktradingengine 패키지)만 APP_LOG_LEVEL 로 맞춘다. 비어 있으면 아무것도 출력하지 않아 기동 인자가 그대로다.
# 루트 로거는 건드리지 않는다: 하네스가 프레임워크 INFO 줄(Kafka 컨슈머 그룹, 파티션 재배정)을 로그에서 읽기 때문이다
app_log_args() {
    [ -n "${APP_LOG_LEVEL:-}" ] || return 0
    printf ' --logging.level.com.flab.stocktradingengine=%s' "$APP_LOG_LEVEL"
}
