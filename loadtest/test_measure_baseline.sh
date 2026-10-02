#!/usr/bin/env bash
set -uo pipefail

# measure-baseline.sh 의 순수 함수(SQL 문자열, 출력 파싱, 판정)와 비움·인덱스 SQL 의 동작을 확인하는 테스트.
# 실행: bash loadtest/test_measure_baseline.sh   (저장소 루트에서)
# Docker postgres(stock-trading-postgres)가 떠 있으면 임시 스키마 claude_tmp_test 에서 SQL 동작도 확인하고,
# 떠 있지 않으면 그 부분만 건너뛴다. 앱 테이블(public)은 건드리지 않는다.

cd "$(dirname "$0")/.."
MEASURE_SOURCE_ONLY=1 source loadtest/measure-baseline.sh
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

DESCRIBE_SAMPLE='
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                     HOST            CLIENT-ID
matching-engine orders          0          1200            1200            0               consumer-matching-engine-1-abc                  /172.18.0.1     consumer-matching-engine-1
matching-engine orders          1          900             905             5               consumer-matching-engine-1-abc                  /172.18.0.1     consumer-matching-engine-1
matching-engine orders          2          -               0               -               -                                               -               -
'

test_lag_total_from_describe() {
    assert_eq "lag 합" "5" "$(printf '%s' "$DESCRIBE_SAMPLE" | lag_total_from_describe)"
    assert_eq "lag 전부 0" "0" "$(printf '%s\n' 'GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID' 'g t 0 10 10 0 c h x' | lag_total_from_describe)"
    assert_eq "행 없음은 NA" "NA" "$(printf '%s\n' 'Error: Group g not found' | lag_total_from_describe)"
    assert_eq "빈 입력은 NA" "NA" "$(printf '' | lag_total_from_describe)"
}

test_quiescent_step() {
    assert_eq "모두 0이고 같은 개수" "yes" "$(quiescent_step 9500 9500 0 0)"
    assert_eq "개수가 늘고 있으면 아직" "no" "$(quiescent_step 9400 9500 0 0)"
    assert_eq "매칭 lag 가 남음" "no" "$(quiescent_step 9500 9500 3 0)"
    assert_eq "정산 lag 가 남음" "no" "$(quiescent_step 9500 9500 0 1)"
    assert_eq "lag 를 못 읽으면 아직" "no" "$(quiescent_step 9500 9500 NA 0)"
    assert_eq "이전 값이 없으면 아직" "no" "$(quiescent_step "" 9500 0 0)"
}

EXPLAIN_SAMPLE='                                                    QUERY PLAN
----------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=113.85..113.86 rows=1 width=32) (actual time=0.169..0.169 rows=1 loops=1)
   ->  Bitmap Heap Scan on orders  (cost=4.68..113.78 rows=30 width=5) (actual time=0.027..0.158 rows=48 loops=1)
         ->  Bitmap Index Scan on ix_b1_orders_acct_status_side_stock  (cost=0.00..4.67 rows=30 width=0) (actual time=0.019..0.019 rows=48 loops=1)
 Planning Time: 0.440 ms
 Execution Time: 0.201 ms
(13 rows)'

test_explain_parsing() {
    assert_eq "실행 시간" "0.201" "$(printf '%s\n' "$EXPLAIN_SAMPLE" | explain_ms)"
    assert_eq "계획 요약" "Bitmap Heap Scan / Bitmap Index Scan" "$(printf '%s\n' "$EXPLAIN_SAMPLE" | explain_plan)"
    assert_eq "Seq Scan 요약" "Seq Scan" "$(printf '%s\n' ' Seq Scan on orders  (cost=0.00..1.00 rows=1 width=1) (actual time=0.1..0.1 rows=1 loops=1)' ' Execution Time: 8.4 ms' | explain_plan)"
    assert_eq "시간 없으면 빈 값" "" "$(printf '%s\n' 'ERROR: x' | explain_ms)"
}

test_index_sql() {
    local create drop
    create="$(B1_SCHEMA=public index_create_sql)"
    drop="$(B1_SCHEMA=public index_drop_sql)"
    assert_contains "orders 인덱스 열" "$create" "ON public.orders (account_id, status, side, stock_code)"
    assert_contains "holdings 인덱스 열" "$create" "ON public.holdings (account_id, stock_code)"
    assert_contains "이름 접두사 orders" "$create" "ix_b1_orders_acct_status_side_stock"
    assert_contains "이름 접두사 holdings" "$create" "ix_b1_holdings_acct_stock"
    assert_contains "IF NOT EXISTS" "$create" "IF NOT EXISTS"
    assert_contains "ANALYZE" "$create" "ANALYZE public.orders"
    assert_contains "drop 두 개" "$drop" "DROP INDEX IF EXISTS public.ix_b1_orders_acct_status_side_stock"
    assert_contains "drop holdings" "$drop" "DROP INDEX IF EXISTS public.ix_b1_holdings_acct_stock"
    assert_not_contains "다른 인덱스는 건드리지 않는다" "$drop" "DROP TABLE"
}

test_clear_sql() {
    local sql
    sql="$(B1_SCHEMA=public clear_sql)"
    assert_contains "FILLED 만" "$sql" "status = 'FILLED'"
    assert_contains "계좌 범위" "$sql" "account_id BETWEEN 2001 AND 2100"
    assert_not_contains "PENDING 은 지우지 않는다" "$sql" "'PENDING'"
    assert_not_contains "TRUNCATE 금지" "$sql" "TRUNCATE"
    assert_not_contains "VACUUM FULL 금지" "$sql" "FULL"
    local unpaid_pos orders_pos
    unpaid_pos="$(printf '%s' "$sql" | grep -n 'DELETE FROM public.unpaids' | head -1 | cut -d: -f1)"
    orders_pos="$(printf '%s' "$sql" | grep -n 'DELETE FROM public.orders' | head -1 | cut -d: -f1)"
    assert_eq "unpaids 를 먼저 지운다" "yes" "$([ -n "$unpaid_pos" ] && [ -n "$orders_pos" ] && [ "$unpaid_pos" -lt "$orders_pos" ] && echo yes || echo no)"
    assert_not_contains "accounts 는 건드리지 않는다" "$sql" "DELETE FROM public.accounts"
    assert_not_contains "holdings 는 건드리지 않는다" "$sql" "DELETE FROM public.holdings"
    local vacuum
    vacuum="$(B1_SCHEMA=public clear_vacuum_sql)"
    assert_contains "VACUUM orders" "$vacuum" "VACUUM (ANALYZE) public.orders"
    assert_contains "VACUUM unpaids" "$vacuum" "VACUUM (ANALYZE) public.unpaids"
}

test_deleted_count_parsing() {
    local output
    output="$(printf '%s\n' 'unpaids|120' 'orders|9500')"
    assert_eq "지운 orders 행" "9500" "$(printf '%s\n' "$output" | deleted_orders_from_clear_output)"
    assert_eq "출력이 없으면 NA" "NA" "$(printf '' | deleted_orders_from_clear_output)"
}

psql_scratch() {
    docker exec -i stock-trading-postgres psql -X -q -At -U postgres -d stock_trading -v ON_ERROR_STOP=1 "$@"
}

test_sql_against_scratch_schema() {
    if ! docker exec stock-trading-postgres true > /dev/null 2>&1; then
        echo "SKIP: postgres 컨테이너가 없어 임시 스키마 테스트를 건너뛴다"
        return
    fi
    export B1_SCHEMA=claude_tmp_test
    psql_scratch <<'SQL' > /dev/null
DROP SCHEMA IF EXISTS claude_tmp_test CASCADE;
CREATE SCHEMA claude_tmp_test;
CREATE TABLE claude_tmp_test.accounts (LIKE public.accounts INCLUDING ALL);
CREATE TABLE claude_tmp_test.holdings (LIKE public.holdings INCLUDING ALL);
CREATE TABLE claude_tmp_test.orders (LIKE public.orders INCLUDING ALL);
CREATE TABLE claude_tmp_test.unpaids (LIKE public.unpaids INCLUDING ALL);
ALTER TABLE claude_tmp_test.unpaids ADD FOREIGN KEY (order_id) REFERENCES claude_tmp_test.orders(id);
INSERT INTO claude_tmp_test.orders (account_id, filled_quantity, order_at, order_id, order_type, price, quantity, request_id, side, status, stock_code) VALUES
 (2001, 1, now(), 1, 'LIMIT', 100, 1, 'a', 'BUY',  'FILLED',  'A1'),
 (2001, 0, now(), 2, 'LIMIT', 100, 1, 'b', 'BUY',  'PENDING', 'A1'),
 (2051, 1, now(), 3, 'LIMIT', 100, 1, 'c', 'SELL', 'FILLED',  'A1'),
 (2100, 1, now(), 4, 'LIMIT', 100, 1, 'd', 'SELL', 'FILLED',  'A1'),
 (1001, 1, now(), 5, 'LIMIT', 100, 1, 'e', 'BUY',  'FILLED',  'A1'),
 (2101, 1, now(), 6, 'LIMIT', 100, 1, 'f', 'BUY',  'FILLED',  'A1'),
 (2001, 0, now(), 7, 'LIMIT', 100, 1, 'g', 'BUY',  'CANCELLED', 'A1');
INSERT INTO claude_tmp_test.unpaids (amount, settlement_date, status, unpaid_id, account_id, order_id)
 SELECT 10, current_date, 'PENDING', 'u'||o.order_id, 1, o.id FROM claude_tmp_test.orders o WHERE o.order_id IN (1, 2, 5);
SQL
    local output deleted remaining unpaids
    output="$(clear_sql | psql_scratch)"
    deleted="$(printf '%s\n' "$output" | deleted_orders_from_clear_output)"
    assert_eq "지운 orders 행 수(2001~2100의 FILLED 3개)" "3" "$deleted"
    remaining="$(psql_scratch -c "SELECT string_agg(order_id::text, ',' ORDER BY order_id) FROM claude_tmp_test.orders")"
    assert_eq "남은 주문" "2,5,6,7" "$remaining"
    unpaids="$(psql_scratch -c "SELECT string_agg(unpaid_id, ',' ORDER BY unpaid_id) FROM claude_tmp_test.unpaids")"
    assert_eq "남은 unpaids(범위 밖과 PENDING 주문의 것)" "u2,u5" "$unpaids"
    clear_vacuum_sql | psql_scratch > /dev/null
    assert_eq "VACUUM 이 실행된다" "0" "$?"

    index_create_sql | psql_scratch > /dev/null
    local count
    count="$(psql_scratch -c "SELECT count(*) FROM pg_indexes WHERE schemaname='claude_tmp_test' AND indexname LIKE 'ix_b1_%'")"
    assert_eq "인덱스 두 개가 생긴다" "2" "$count"
    index_create_sql | psql_scratch > /dev/null
    assert_eq "두 번 만들어도 실패하지 않는다" "0" "$?"
    index_drop_sql | psql_scratch > /dev/null
    count="$(psql_scratch -c "SELECT count(*) FROM pg_indexes WHERE schemaname='claude_tmp_test' AND indexname LIKE 'ix_b1_%'")"
    assert_eq "원복하면 인덱스가 없다" "0" "$count"
    local pk_count
    pk_count="$(psql_scratch -c "SELECT count(*) FROM pg_indexes WHERE schemaname='claude_tmp_test' AND tablename='orders'")"
    assert_eq "원래 인덱스(pkey, request_id, order_id)는 남는다" "3" "$pk_count"

    local stats
    stats="$(db_state_sql | psql_scratch)"
    assert_eq "상태 행은 7개 값을 가진다" "7" "$(printf '%s' "$stats" | awk -F'|' 'NR==1 { print NF }')"
    index_create_sql | psql_scratch > /dev/null
    local tmpdir rows
    tmpdir="$(mktemp -d)"
    record_dbstats "$tmpdir" v1 r01 start
    record_dbstats "$tmpdir" v1 r01 cleared 340 3
    rows="$(wc -l < "${tmpdir}/dbstats-v1.csv" | tr -d ' ')"
    assert_eq "dbstats 줄 수(머리글 + 두 행)" "3" "$rows"
    assert_eq "dbstats 머리글" "$DBSTATS_HEADER" "$(sed -n 1p "${tmpdir}/dbstats-v1.csv")"
    assert_eq "dbstats 열 수(start)" "17" "$(sed -n 2p "${tmpdir}/dbstats-v1.csv" | awk -F, '{ print NF }')"
    assert_eq "dbstats 열 수(cleared)" "17" "$(sed -n 3p "${tmpdir}/dbstats-v1.csv" | awk -F, '{ print NF }')"
    assert_eq "start 행에 계획이 있다" "yes" "$(sed -n 2p "${tmpdir}/dbstats-v1.csv" | awk -F, '{ print ($13 != "" && $10 != "") ? "yes" : "no" }')"
    assert_eq "cleared 행의 clear_ms" "340" "$(sed -n 3p "${tmpdir}/dbstats-v1.csv" | awk -F, '{ print $16 }')"
    assert_eq "cleared 행의 deleted_orders" "3" "$(sed -n 3p "${tmpdir}/dbstats-v1.csv" | awk -F, '{ print $17 }')"
    check_b1_plans "${tmpdir}/plans.txt" 2> /dev/null
    assert_contains "계획 파일에 q1" "$(cat "${tmpdir}/plans.txt")" "== q1:"
    assert_contains "계획 파일에 qh" "$(cat "${tmpdir}/plans.txt")" "== qh:"
    rm -rf "$tmpdir"
    psql_scratch -c "DROP SCHEMA claude_tmp_test CASCADE" > /dev/null
    unset B1_SCHEMA
}

test_src_main_unchanged() {
    if [ -f loadtest/measure-allowlist-c.txt ]; then
        echo "SKIP: 구간 분해 측정 브랜치는 src/main 계측 변경이 있어 기준 커밋 대조 테스트를 건너뛴다(허용 목록 대조는 test_measure_segments.sh)"
        return
    fi
    local output
    output="$(assert_src_main_unchanged 2>&1)"
    assert_eq "src/main 이 기준 커밋과 같으면 성공" "0" "$?"
    assert_contains "src/main diff 없음 문구" "$output" "src/main diff vs ${B1_BASE_SHA}: 없음"
}

test_lag_total_from_describe
test_src_main_unchanged
test_quiescent_step
test_explain_parsing
test_index_sql
test_clear_sql
test_deleted_count_parsing
test_sql_against_scratch_schema

echo "통과 ${PASSES}, 실패 ${FAILS}"
[ "$FAILS" -eq 0 ]
