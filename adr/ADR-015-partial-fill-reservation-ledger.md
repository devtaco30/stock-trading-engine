# ADR-015: 부분 체결 시 예약 장부를 잔량 기준으로 다시 계산

## 문제

B3a까지 `AccountState.reservations`는 `orderId → 예약 증거금 총액`(`BigDecimal`)만 들고 있었다.
`applyBuyFill`은 체결이 오면 그 줄을 무조건 통째로 지웠다 — 전량 체결만 전제했기 때문이다.
그런데 매칭 엔진은 주문 하나를 여러 tradeId로 나눠 체결시킬 수 있다(orderId : tradeId = 1 : N).
총액만 든 지금 구조로는 "이번에 몇 주가 체결됐고 몇 주가 남았는지"를 알 수 없어, 첫 부분 체결만
와도 예약 전체가 사라져 버린다 — 남은 미체결 수량에 대한 담보가 없어지는 정합성 버그다.

## 대안

1. `reservations` 값을 그대로 총액으로 두고, 체결마다 "체결 비율만큼" 총액에서 차감한다.
2. `reservations` 값을 `Reservation(price, remainingQuantity)`로 바꾸고, 예약 증거금은 그때그때
   `price × remainingQuantity × marginRate`로 다시 계산한다.
3. 체결 수량이 예약 잔량을 넘거나 예약 자체가 없는 주문에 체결이 오면: (a) 조용히 무시, (b) 가능한
   만큼만 반영, (c) 예외를 던져 소비자를 멈춘다.

## 트레이드오프

**1 vs 2**: 1번은 "체결 비율 × 총액"으로 차감액을 계산해야 하는데, 총액은 최초 1회만
`setScale(0, DOWN)`으로 반올림된 값이라 부분 체결이 여러 번 이어지면 반올림 오차가 누적된다.
2번은 매번 `price`(고정)와 `remainingQuantity`(그때그때 값)로 새로 계산하므로 반올림이 누적되지
않는다 — 대신 예약 장부 한 줄이 값 하나에서 값 두 개(레코드)로 늘어난다.

**3의 (a)/(b) vs (c)**: 정상 흐름이라면 매칭이 검증 통과한 주문만 체결시키므로 이 경로는
"버그가 아니면 일어나지 않는" 경로다. (a)·(b)는 잘못된 상태를 조용히 넘겨 계좌 잔고가 실제와
어긋난 채로 계속 쌓이게 둔다. matching-disruptor(ADR-012)와 같은 원칙 — 도메인 불변식 위반은
그 이벤트만 조용히 넘기지 않고 fail-fast로 드러낸다 — 을 계좌 워커에도 그대로 적용해 (c)를 택한다.

## 결정

`reservations`를 `Map<Long, Reservation>`(private record, `price`+`remainingQuantity`)으로 바꾼다.
`tryReserve(orderId, price, quantity)`로 시그니처를 바꿔 가격·수량을 그대로 저장하고
(기존 `orderAmount` 단일값 입력은 폐기), `applyBuyFill`은 체결 수량만큼 `remainingQuantity`를
줄이며 0이 되면 그 줄을 지운다. 예약이 없는 orderId, 또는 체결 수량이 잔량을 넘는 경우는
`IllegalStateException`을 던진다.

## 결과

- `AccountStateTest`에 부분 체결(잔량 유지·잔량 소진)·예약 초과·예약 없음 4개 케이스 추가, 기존
  `AccountEngineTest` 하네스 테스트 포함 전체 21개 통과.
- `AccountEventHandler.handleBuy`가 `tryReserve` 호출부에서 `orderAmount`를 미리 계산하던 코드를
  제거하고 `price`·`quantity`를 그대로 넘기도록 단순화됨.
- 매도 쪽(`applySellFill`)은 매수 예약 장부와 무관해 이번 변경 대상이 아니다 — 매도 보유예약 자체가
  아직 없다는 한계(ADR-014)는 그대로 남아 있다.
- 여전히 격리 상태(호출자가 tradeId·체결 수량을 직접 넘김, 매칭코어 미연결)라 이 예외 경로가 실제
  매칭 흐름에서 검증되는 건 Phase C 통합 이후다.
