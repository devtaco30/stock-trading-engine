# ADR-009 매도 주문 가용 수량 검증 (over-sell 방지)

## 문제

매수 접수는 주문 시점에 예약증거금을 합산해 over-commit을 막는다(`sumReservedMarginByAccountId`). 매도 접수에는 대칭 방어가 없었다.

- `OrderWriter.writeSellOrder`가 실제 보유 수량(`holding.getQuantity()`)만 검증하고, **이미 접수된 PENDING 매도 주문 수량을 합산하지 않았다**. 보유는 체결(정산) 시점에만 `decreaseHolding`으로 차감되므로, 100주 보유로 100주 매도주문을 여러 건 접수할 수 있었다.
- 체결 차감 `Holding.subtractQuantity`에 음수 가드가 없었다. `int` 컬럼에 DB 제약도 없어, 두 매도가 모두 체결되면 `0 − 100 = −100`이 저장됐다.

## 대안

1. **체결 시점 방어만.** 접수는 그대로 두고 정산에서 걸러낸다. 접수 단계 over-sell을 허용하고 실패를 체결까지 미룬다(늦은 실패).
2. **Holding에 예약수량 컬럼 추가.** 주문 시 증가, 취소·체결 시 감소하는 상태값으로 관리한다.
3. **주문 시점 PENDING 매도 잔량 합산 검증.** `SUM(quantity − filledQuantity)`로 미체결 매도 약정을 집계해 가용 수량을 검증한다. 주문 상태가 진실의 원천이고, 별도 상태 동기화가 없다.

## 트레이드오프

| 대안 | 장점 | 단점 |
|---|---|---|
| 체결만 방어 | 구현 최소 | 근본 아님, 접수 over-sell 허용, 늦은 실패 |
| 예약수량 컬럼 | 조회 시 집계 불필요 | 취소·부분체결·복원마다 갱신해야 함(누락 시 어긋남), 스키마 변경 |
| 잔량 합산 검증 | 매수 증거금 검증과 대칭, 상태 동기화 불필요 | 락 보유 중 집계 쿼리 1회 추가 |

## 결정

**대안 3** 채택. 매수 `sumReservedMarginByAccountId`와 대칭 구조로 맞춘다.

- `OrderRepository.sumPendingSellQuantity(accountId, stockCode)` = `SUM(quantity − filledQuantity)` where PENDING & SELL. 부분 체결분은 이미 보유에서 차감됐으므로 잔량만 미체결 약정으로 집계한다.
- `OrderWriter.writeSellOrder`에서 `가용 = 보유 − pendingSellSum`을 계산해 `가용 < 요청수량`이면 `InsufficientResourceException`. 이 검증은 `getHoldingByAccountIdForUpdate`의 Holding 행 락 안에서 실행된다. 같은 계좌·종목 매도는 이 락으로 직렬화되고, order-requests가 accountId로 파티셔닝되어 컨슈머 스레드도 단일이라, 합산과 검증 사이에 다른 매도가 끼어들지 않는다.
- 안전망으로 `Holding.subtractQuantity`에 가드를 둔다. `subQuantity > quantity`면 `IllegalStateException`으로 실패를 표면화한다. 이는 접수 검증이 뚫린 버그 상황의 최후 방어이며, 정상 흐름에서는 발동하지 않는다.

## 결과

- 단위 테스트 126개 통과(`./gradlew test`). `OrderWriterTest` 4개(가용 충분/초과/전량 약정/보유 없음), `HoldingTest` 3개(정상·경계·초과 가드) 신규.
- PostgreSQL 스키마 변경 없음. 파생 집계 쿼리라 컬럼·제약 추가가 없다.
- 매수 멱등키 수정(ADR-008)과 같은 "주문 시점 예약" 계열이다. 두 방어 모두 주문 상태를 진실의 원천으로 삼아 락 안에서 합산·검증한다.
