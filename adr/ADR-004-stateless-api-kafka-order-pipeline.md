# ADR-004: API Stateless화 — Kafka 주문 파이프라인

## 문제

api 스케일아웃 시 같은 계좌의 주문이 여러 인스턴스에 분산된다.
Account row 비관적 락 경합이 인스턴스 수에 비례해 증가하고, 일정 수준을 넘으면 커넥션 풀 고갈과 응답 지연이 발생한다.
인스턴스를 늘릴수록 성능이 역전되는 구조적 문제다.

## 대안

### 대안 1: 낙관적 락 + 재시도
동기 흐름 유지, `@Version`으로 충돌 감지 후 재시도.
- 트레이드오프: 동시 주문 폭주 시 재시도 루프 증가, 응답 지연 불확정
- 스케일아웃 한계 미해결

### 대안 2: Redis 분산 락
accountId 기준 Redis 락 획득 후 동기 처리.
- 트레이드오프: Redis가 새 SPOF, TTL/timeout 처리 복잡
- 200 응답 유지 가능하나 Redis 가용성에 전체 주문 접수가 의존

### 대안 3: Kafka accountId 파티셔닝 (채택)
api는 Kafka에 발행만, order-processor가 accountId 파티션 기준으로 직렬 처리.
- 트레이드오프: 클라이언트 UX 변경(200 → 202), 주문 접수 지연 증가, 장애 추적 복잡
- DB 락 구조적 제거, api 완전 stateless

### 대안 4: Sticky 라우팅
accountId 기준 consistent hashing으로 특정 api 인스턴스로 고정 라우팅.
- 트레이드오프: L7 라우팅 인프라 필요, 인스턴스 장애 시 재라우팅 복잡

## 결정

**대안 3 채택. Kafka accountId 파티셔닝 기반 비동기 주문 파이프라인.**

## 아키텍처

```
HTTP → api (stateless)
         ↓ Kafka: order-requests (key=accountId)
       trading / OrderRequestConsumer
         → 잔고·보유 검증
         → 주문 DB 저장
         ↓ Kafka: orders.{stockCode} (key=stockCode)
       trading / MatchingConsumer
         → OrderBook 매칭
         ↓ Kafka: fills.{stockCode}
       settlement / SettlementConsumer
         → T+2 정산
```

### 프로세스 분리

| 프로세스 | 역할 |
|---|---|
| api | HTTP 수신, 입력 검증(가격 제한폭 등), Kafka 발행, 202 응답 |
| trading | OrderRequestConsumer(접수·검증·저장) + MatchingConsumer(매칭) |
| settlement | 정산 처리 |

### 토픽 설계

| 토픽 | 파티션 키 | 발행자 | 소비자 |
|---|---|---|---|
| `order-requests` | accountId | api | trading/OrderRequestConsumer |
| `orders.{stockCode}` | stockCode | trading/OrderRequestConsumer | trading/MatchingConsumer |
| `fills.{stockCode}` | stockCode | trading/MatchingConsumer | settlement/SettlementConsumer |

### accountId 파티셔닝 효과

같은 accountId는 항상 같은 파티션 → 같은 컨슈머 스레드에서 직렬 처리.
잔고 검증과 주문 저장이 단일 스레드에서 순서대로 실행되므로 DB 비관적 락 불필요.

## 결과

- api가 완전 stateless → 인스턴스 추가만으로 수평 확장
- DB 비관적 락 제거 → 커넥션 풀 고갈 위험 해소
- 주문 요청이 Kafka에 영속 → 장애 시 재처리 가능
- 클라이언트는 202 수신 후 폴링 또는 WebSocket으로 결과 확인
- ADR-001(락 최적화), ADR-003(락 순서)은 OrderRequestConsumer 내부로 국한되며 향후 제거 가능
