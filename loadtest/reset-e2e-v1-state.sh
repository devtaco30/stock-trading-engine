#!/usr/bin/env bash
set -euo pipefail

# v1 e2e 측정 전 완전 초기화. Postgres만 지우면 부족하다 — order-requests 토픽에 쌓인
# 이전 실행의 백로그를 order-engine-e2e 컨슈머 그룹이 그대로 이어받아 소비해, 몇 시간 전
# 메시지의 지연이 새 측정에 섞여 들어간다(실측 발견, 2026-09-16 — Postgres만 지우고 재측정
# 했다가 이전 런 잔재 48만 건을 계속 소비 중인 걸 뒤늦게 발견). group.id별
# --reset-offsets는 그룹이 완전히 사라지지 않은 미묘한 타이밍에 재현이 안 될 수 있어(실측
# 확인 — --list에 안 보였는데도 재기동하니 그 그룹 오프셋을 그대로 이어받음), 토픽 자체를
# 삭제·재생성해 애매함을 없앤다.
PARTITIONS="${1:-3}"

echo "=== order-requests 토픽 삭제·재생성 (partitions=${PARTITIONS}) ==="
docker exec stock-trading-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --topic order-requests 2>&1 | grep -v "^$" || true
sleep 2
docker exec stock-trading-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic order-requests --partitions "$PARTITIONS" --replication-factor 1 \
    --config min.insync.replicas=1

echo "=== Postgres e2e 계좌(2001~2100) 관련 행 정리 ==="
docker exec stock-trading-postgres psql -U postgres -d stock_trading -c \
    "DELETE FROM unpaids WHERE order_id IN (SELECT o.id FROM orders o WHERE o.account_id BETWEEN 2001 AND 2100);"
docker exec stock-trading-postgres psql -U postgres -d stock_trading -c \
    "DELETE FROM orders WHERE account_id BETWEEN 2001 AND 2100;"
docker exec stock-trading-postgres psql -U postgres -d stock_trading -c \
    "DELETE FROM holdings WHERE account_id IN (SELECT id FROM accounts WHERE account_id BETWEEN 2001 AND 2100);"
docker exec stock-trading-postgres psql -U postgres -d stock_trading -c \
    "DELETE FROM accounts WHERE account_id BETWEEN 2001 AND 2100;"

echo "=== 확인 ==="
docker exec stock-trading-postgres psql -U postgres -d stock_trading -c \
    "SELECT (SELECT count(*) FROM accounts WHERE account_id BETWEEN 2001 AND 2100) accounts, (SELECT count(*) FROM orders WHERE account_id BETWEEN 2001 AND 2100) orders;"
docker exec stock-trading-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic order-requests --time -1
