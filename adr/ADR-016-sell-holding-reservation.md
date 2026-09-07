# ADR-016: 계좌 워커에 매도 보유예약 추가

## 문제

account-disruptor 계좌 워커는 매수만 주문 시점에 예약을 건다(B2 `tryReserve`, 돈을 예약).
매도는 검증 없이 바로 체결 반영(`applySellFill`)만 있어서, 같은 보유 수량으로 매도 주문을
여러 개 넣어도 주문 시점엔 전부 통과한다. 다 체결되면 실제 가진 것보다 많이 팔린 것(over-sell)이
되는 정합성 구멍이다. v1은 이미 `ADR-009`로 이 문제를 막아뒀는데, v2 계좌 워커에는 이 방어가
빠진 채로 B3c(부분 체결)까지 와버렸다 — 검증·예약 층이 매수/매도 비대칭인 상태.

## 대안

1. 매도는 체결 시점에만 보유를 확인하고 부족하면 거부한다.
2. 매도 주문 접수 시점에 v1과 동일하게 "가용 수량 = 보유 − 이미 건 매도 주문 수량 합"을
   계산해 미리 예약을 걸고, 넘으면 거부한다.

## 트레이드오프

1번은 주문 여러 개를 넣는 순간에는 다 통과시키고 체결 때 가서야 막는다 — 사용자 입장에서
"주문은 됐는데 나중에 취소된다"는 경험이 되고, 매칭 엔진 쪽에서 이미 호가창에 올라간 주문을
체결 직전에 되돌려야 하는 복잡함이 생긴다. 2번은 주문 시점에 막아 매칭 엔진이 항상 "이미
검증된 주문"만 받는다는 계좌 워커의 기존 원칙(B2 매수 예약과 동일)을 유지하지만, 계좌 상태에
"매도 예약 장부"를 하나 더 들어야 한다.

## 결정

**2번을 택한다.** `AccountState`에 `sellReservations: Map<Long orderId, SellReservation(stockCode,
remainingQuantity)>`을 매수 예약 장부(`reservations`)와 대칭으로 추가한다.

- `trySellReserve(orderId, stockCode, quantity)`: 가용수량(보유 − 그 종목 매도예약 합)을 넘으면
  `RejectReason.INSUFFICIENT_HOLDING`으로 거부. 반환 타입은 `ReserveResult`를 재사용하지 않고
  `SellReserveResult(accepted, reservedQuantity, reason)`를 새로 둔다 — 매도는 증거금(금액) 개념이
  없어 같은 타입을 쓰면 필드 의미가 헷갈린다.
- `applySellFill`에 `orderId`를 추가해, 체결마다 그 매도 예약 줄의 남은 수량을 줄인다(B3c와 같은
  잔량 기준 재계산 방식). 예약 없는 주문에 대한 체결, 잔량 초과 체결은 B3c와 같은 정책으로
  `IllegalStateException`(fail-fast).
- 부분 매도·전체 매도를 별도 메서드로 나누지 않는다 — B3c에서 이미 확인했듯 "잔량에서 체결분을
  빼고 0이면 지운다"는 방식이 둘 다 처리한다.
- 하네스에도 매수(B2)와 대칭으로 `EventType.SELL`·`AccountEngine.publishSell`·
  `AccountEventHandler.handleSell`·`AccountResultListener.onSellAccepted`를 추가한다.

## 결과

- `AccountStateTest`·`AccountEngineTest`에 매도 예약(통과·거부·러닝예약·부분체결·잔량초과·예약없음)
  케이스 추가, 기존 매도 체결 테스트 2개는 매도 예약을 먼저 거치도록 수정. 전체 30개 통과.
- 기존 `applySellFill(tradeId, stockCode, fillQty)` 호출부(테스트·`AccountEventHandler`)가
  `applySellFill(tradeId, orderId, stockCode, fillQty)`로 전부 바뀜 — 이 커밋 안에서 완결.
- 매수·매도 검증·예약 층이 대칭을 이뤄, B4(Kafka 정산)를 얹기 전 계좌 워커의 주문 전 검증 책임이
  완결됐다. 이 결정은 ADR-009(v1 over-sell 방어)의 v2 인메모리판이다.
- 여전히 격리 상태(호출자가 직접 `publishSell`/`publishSellFill` 호출, 매칭코어 미연결)라 이 경로가
  실제 매도 주문 흐름에서 검증되는 건 Phase C 통합 이후다.
