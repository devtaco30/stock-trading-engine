## 문제

`MatchingConsumer.onPartitionsAssigned()`에서 DB PENDING 주문을 로드해 `OrderBook`에 복원할 때,
`OrderEntry` 생성에 `accountId`가 필요하다.

`Order` 엔티티는 `@ManyToOne(fetch = LAZY) Account account`만 갖고 있어
`order.getAccount().getAccountId()` 호출이 `@Transactional` 없는 컨슈머 컨텍스트에서
`LazyInitializationException`을 발생시킨다.

## 대안

### A. EAGER fetch로 변경
`@ManyToOne(fetch = FetchType.EAGER)`
→ 매 주문 조회마다 Account JOIN. 정산·조회 등 Account 불필요한 경로에서도 항상 로드.

### B. JPQL JOIN FETCH
쿼리마다 `JOIN FETCH o.account` 명시.
→ 복원 쿼리에는 적용 가능하지만, `OrderEntry` 생성이 필요한 모든 경로를 추적·관리해야 함.

### C. `accountId` 컬럼 직접 추가 (비정규화)
FK 컬럼 `account_id`를 scalar 필드로 중복 노출.
`insertable = false, updatable = false` — `@ManyToOne`이 컬럼을 관리한다.

## 트레이드오프

| | A (EAGER) | B (JOIN FETCH) | C (denormalize) |
|---|---|---|---|
| Lazy 해결 | O | O | O |
| 불필요한 Account 로드 | 항상 | 경로별 제어 | 없음 |
| 코드 변경 범위 | entity 1줄 | 쿼리 추가 | entity + 사용처 |
| 정합성 위험 | 없음 | 없음 | FK와 동일 컬럼이므로 없음 |

## 결정

**C를 선택한다.**

`accountId`는 `OrderEntry` 생성 이후 읽기 전용으로만 사용된다.
`insertable = false, updatable = false`이므로 `@ManyToOne`과 충돌 없이 FK 값을 직접 읽는다.
트랜잭션 없이도 안전하게 접근 가능하고, Account 엔티티를 불필요하게 로드하지 않는다.

## 결과

- `Order.accountId`: `@Column(name = "account_id", insertable = false, updatable = false)`
- `MatchingConsumer.toEntry(Order)`: `order.getAccount().getAccountId()` → `order.getAccountId()`
- `OrderSettlementService`: `getAccountId()` 호출로 통일 (Account가 필요한 `marginRate` 접근은 유지)
