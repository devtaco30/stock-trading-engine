# ADR-008 주문 접수 멱등키를 requestId로 교체

## 문제

부하테스트(k6 3000건)가 두 가지를 잡았다.

1. 멱등키가 `requestedAt = Instant.now()`(API 수신 서버시각)였다. 버스트 때 서로 다른 정상 주문이 같은 밀리초에 몰리면 `(account_id, requested_at)` UNIQUE가 깨진다. 주식 도메인은 같은 계좌의 연속 주문이 정상인데, 이 키는 그걸 중복으로 오판한다.
2. `OrderCommandService`의 `catch(DataIntegrityViolationException)`가 **롤백된(rollback-only) 트랜잭션 안에서** 복구 조회를 실행했다. Hibernate `AssertionFailure`가 나고, `DefaultErrorHandler`가 9회 재시도한 뒤 주문이 유실됐다.

시각 기반 키를 쓰는 한 1번은 남는다. 시각은 요청을 구분하는 값이 아니라 요청이 도착한 순간일 뿐이라, 같은 계좌의 정상 연속 주문과 재전달 중복을 구별하지 못한다.

## 대안

1. **시각 기반 키 유지 + 복구 조회만 REQUIRES_NEW로 분리** — `AssertionFailure`(2번)는 사라진다. 하지만 정상 주문끼리의 충돌(1번)은 그대로 남는다. 원인이 아니라 증상만 다룬다.
2. **`requestId` 전역 UNIQUE + check-then-act + REQUIRES_NEW 안전망** — 요청마다 고유한 식별자를 키로 삼는다. 저장 전에 조회로 중복을 걸러내고(check-then-act), catch는 동시 삽입 경쟁의 최후 안전망으로만 둔다. 1번·2번을 함께 해결한다.

## 트레이드오프

| 대안 | 장점 | 단점 |
|---|---|---|
| 복구만 분리 | 변경 최소, AssertionFailure 제거 | 정상 주문 충돌(오판) 그대로 — 근본 해결 아님 |
| requestId 전역 UNIQUE | 재전송·이중클릭 멱등, 연속 주문 허용, 표준 idempotent-consumer | 클래스 2개 추가, 정상경로마다 REQUIRES_NEW 조회 1회 |

## 결정

**대안 2** 채택.

- 멱등키 = `requestId`. 클라이언트가 보내면 그 값, 없으면 `OrderApiService`가 `UUID`를 생성한다. API에서 한 번만 찍혀 Kafka 재전달에도 안정적이다.
- `(account_id, requested_at)` UNIQUE 제거, `request_id` 전역 UNIQUE 추가. `requested_at` 컬럼은 감사용으로 남긴다(하류 `OrderPlacedEvent`가 참조).
- `OrderCommandService`를 셋으로 나눈다. 오케스트레이터(트랜잭션 없음)가 `OrderIdempotencyReader`(REQUIRES_NEW 커밋 조회)로 먼저 중복을 확인하고, 없으면 `OrderWriter`(락·검증·저장, 트랜잭션)에 위임한다. 저장이 롤백돼도 오케스트레이터에는 트랜잭션이 없어, 복구 조회가 오염된 트랜잭션을 재사용하지 않는다.
- `order-requests`는 accountId 파티셔닝이라 같은 계좌 요청은 단일 컨슈머 스레드에서 직렬 처리된다. check-then-act만으로 대부분의 중복을 막고, catch는 리밸런싱 등 예외적 경쟁의 안전망으로만 동작한다.

## 결과

- 단위·슬라이스 테스트 119개 통과(`./gradlew test`). 멱등 오케스트레이션 6개, requestId 생성 2개 신규.
- PostgreSQL 운영 반영은 `ddl-auto: update`가 옛 UNIQUE를 자동으로 지우지 않으므로 매뉴얼 DDL이 필요하다:
  ```sql
  ALTER TABLE orders DROP CONSTRAINT uq_orders_account_requested;
  ALTER TABLE orders ADD COLUMN request_id VARCHAR(255);
  -- 기존 행 백필 후
  ALTER TABLE orders ALTER COLUMN request_id SET NOT NULL;
  ALTER TABLE orders ADD CONSTRAINT uq_orders_request_id UNIQUE (request_id);
  ```
- 부하테스트 재측정(before/after 유실 비교)은 별도 단계로 남는다.
