#!/usr/bin/env bash
set -uo pipefail

# sample-env.sh 의 메모리 열(JVM RSS, 컨테이너 메모리) 변환과 헤더를 확인하는 테스트. 앱·Docker·부하 없이 돈다.
# 실행: bash loadtest/test_sample_env.sh   (저장소 루트에서)

cd "$(dirname "$0")/.."
OUT="$(mktemp)"
SAMPLER_SOURCE_ONLY=1 SAMPLER_COND=v2 source loadtest/sample-env.sh "$OUT"
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

test_to_mb() {
    assert_eq "MiB 는 그대로" "123.4" "$(to_mb 123.4MiB)"
    assert_eq "GiB 는 1024 배" "2048.0" "$(to_mb 2GiB)"
    assert_eq "KiB 는 1024 로 나눈다" "1.0" "$(to_mb 1024KiB)"
    assert_eq "B 단위" "0.0" "$(to_mb 12B)"
    assert_eq "빈 값은 빈 값" "" "$(to_mb "")"
}

test_container_mem() {
    local docker_out
    docker_out=$'stock-trading-postgres,0.52,45.3MiB / 7.653GiB\nstock-trading-kafka,2.1,1.2GiB / 7.653GiB'
    assert_eq "postgres 메모리" "45.3" "$(container_mem "$docker_out" stock-trading-postgres)"
    assert_eq "kafka 메모리" "1228.8" "$(container_mem "$docker_out" stock-trading-kafka)"
    assert_eq "없는 컨테이너는 빈 값" "" "$(container_mem "$docker_out" nothing)"
    assert_eq "CPU 변환은 그대로 동작한다" "2.1" "$(container_cpu "$docker_out" stock-trading-kafka)"
}

test_rss_mb() {
    assert_eq "pid 가 없으면 빈 값" "" "$(rss_mb)"
    local value
    value="$(rss_mb $$)"
    assert_eq "자기 프로세스는 양수" "yes" "$(awk -v v="$value" 'BEGIN { print (v + 0 > 0) ? "yes" : "no" }')"
}

test_header() {
    : > "$OUT"
    write_header
    assert_eq "헤더 끝에 메모리 열이 붙는다" \
        "ts,k6_cpu,jvm_api,jvm_account-worker,jvm_matching-worker,pg_cpu,kafka_cpu,idle,user,sys,top5,batt,therm_limit,rss_api,rss_account-worker,rss_matching-worker,pg_mem,kafka_mem" \
        "$(cat "$OUT")"
}

test_to_mb
test_container_mem
test_rss_mb
test_header
rm -f "$OUT"

echo "통과 ${PASSES}, 실패 ${FAILS}"
[ "$FAILS" -eq 0 ]
