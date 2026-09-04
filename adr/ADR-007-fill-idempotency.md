# ADR-007 체결 반영 멱등성 (tradeId)

## 문제

Kafka `fills.{stockCode}`는 at-least-once 라 같은 `TradeFilledEvent`가 재전달될 수 있다.
`OrderSettlementService`의 부분 체결 반영 가드는 `status != PENDING` 만 검사하는데,
부분 체결된 주문은 여전히 PENDING 이므로 재전달 시 가드를 통과한다.

결과: `Order.addFilled(fillQty)`가 다시 실행되어 **누적 체결 수량·보유(Holding)·미수금(Unpaid)이 중복 집계**된다.
전량 체결이면 `addFilled`가 남은 수량 초과로 예외를 던져 걸리지만(→ ack 폐기),
**부분 체결은 예외 없이 조용히 틀린다** — 더 위험하다.

## 대안

1. **자연키 `(buyOrderId, sellOrderId)`** — 이벤트 변경 없이 이 쌍을 멱등키로.
   "한 매수·매도 쌍은 이 매칭 엔진에서 정확히 한 번만 체결된다"는 내부 불변식에 의존.
2. **명시적 `tradeId`(Snowflake)** — 체결마다 고유 ID를 발급해 이벤트에 담는다. 매칭 내부 규칙과 독립.
3. **무(無)테이블** — `addFilled` 초과 예외에 기대 자연 차단.

## 트레이드오프

| 대안 | 장점 | 단점 |
|---|---|---|
| 자연키 | 이벤트·프로듀서 변경 없음 | 매칭 엔진 내부 불변식에 결합 → 매칭 로직 변경 시 깨질 위험 |
| tradeId | 매칭과 독립·의미 명확, 표준 idempotent-consumer | 이벤트 필드 + 프로듀서 변경, 마커 테이블 필요 |
| 무테이블 | 추가 인프라 0 | **부분 체결 중복을 못 막음**(초과가 안 나 조용히 통과) → 핵심 케이스 실패 |

## 결정

**대안 2 (tradeId + `processed_fills` 마커 테이블)** 채택.

- `TradeFilledEvent`에 `tradeId`(Snowflake) 추가. `MatchingConsumer`가 체결 발행 시점에 발급.
- `ProcessedFill(tradeId PK, processedAt)` 엔티티로 처리 완료를 기록.
- `fillTradePartially` 진입 시 `existsById(tradeId)` 로 이미 반영했는지 확인 → 있으면 early return.
  처리 후 `ProcessedFill` 저장. 매수·매도 반영 + 마커 저장이 **한 트랜잭션**이라 원자적.
- `fills.{stockCode}`는 종목 단위 파티셔닝 → 동일 종목은 단일 컨슈머 스레드에서 직렬 처리되므로
  `existsById` 확인과 `save` 사이에 동시 중복이 끼어들지 않는다(별도 락 불필요).

## 결과

- 재전달분은 가드에서 컷 → 부분 체결 중복 집계 해소.
- **한계**: tradeId는 settlement 쪽 재전달(같은 메시지 재수신)을 막는다.
  matching-engine 자신이 같은 체결을 **재발행**하면(예: Redis 장애로 orders 메시지 재처리 → 재매칭)
  tradeId가 새로 찍혀 중복이 통과한다. producer 쪽 exactly-once 는 별개 문제로 Phase 3+ 에서 다룬다.
- **DB**: `processed_fills` 테이블 필요. H2 `ddl-auto` 로 dev/test 는 자동 생성.
  PostgreSQL 운영 환경에는 DDL 필요:
  ```sql
  CREATE TABLE processed_fills (
      trade_id     BIGINT    PRIMARY KEY,
      processed_at TIMESTAMP NOT NULL
  );
  ```
