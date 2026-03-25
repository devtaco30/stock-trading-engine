## 문제

`MatchingConsumer.toEntry(Order)`에서 `order.getAccount().getAccountId()`를 호출한다.
`Order.account`는 Lazy Loading이므로, `OrderQueryService.getPendingByStockCodeSortedByTime()`의
JPA 세션이 닫힌 뒤 이 코드가 실행되면 `LazyInitializationException`이 발생한다.

## 대안

**A. JOIN FETCH / @EntityGraph**
리포지토리 쿼리에 `JOIN FETCH o.account`를 추가해 세션 내에서 `account`를 함께 로드한다.
`@Transactional(readOnly = true)`와 함께 사용하면 세션이 열린 상태에서 account가 초기화된다.

**B. Order에 accountId 직접 보유**
`Order` 엔티티에 `accountId` 필드를 추가한다.
`toEntry(Order)`에서 `account` 참조 없이 `order.getAccountId()`로 바로 접근한다.

## 트레이드오프

| | A (JOIN FETCH) | B (accountId 직접 보유) |
|---|---|---|
| JPA 관례 | 자연스러움 | 벗어남 |
| 쿼리 비용 | account 행을 항상 JOIN | 추가 쿼리 없음 |
| Lazy Loading 의존 | 있음 (세션 범위 주의 필요) | 없음 |
| Aggregate 경계 | Order가 Account를 직접 참조 | ID만 참조 (DDD 권장) |
| 사용 목적 | accountId 하나를 위해 Account 전체 로드 | 필요한 값만 보유 |

## 결정

**B안 채택 — `Order` 엔티티에 `accountId` 필드 직접 보유**

`toEntry(Order)`에서 필요한 값은 `accountId` 하나뿐이다.
그 하나를 위해 `Account` 엔티티 전체를 JOIN FETCH하는 것은 과잉이다.
또한 `Order`가 `Account` Aggregate를 직접 참조하지 않고 ID만 갖는 형태는
DDD에서 Aggregate 경계를 명확히 하는 권장 패턴이기도 하다.
Lazy Loading 문제가 구조적으로 제거되어 세션 범위를 고려할 필요가 없어진다.

## 결과

- `Order` 엔티티에 `accountId Long` 필드 추가
- `MatchingConsumer.toEntry(Order)`: `order.getAccount().getAccountId()` → `order.getAccountId()`
- `OrderQueryService.getPendingByStockCodeSortedByTime()`: `@Transactional(readOnly = true)` 불필요
- `Order.account` 연관관계는 계좌 상세 조회 등 다른 용도로 유지
