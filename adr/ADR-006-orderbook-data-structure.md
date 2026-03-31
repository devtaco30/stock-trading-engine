## 문제

인메모리 오더북은 두 가지 핵심 연산을 동시에 효율적으로 지원해야 한다.

1. **최우선 호가 조회**: 매수 최고가·매도 최저가를 매 매칭 시도마다 꺼낸다.
2. **동일 가격 내 FIFO**: 같은 가격의 주문은 먼저 들어온 순서로 체결한다 (시간 우선).
3. **O(1) 취소**: 주문 취소 요청이 orderId로 들어오며, 큐 전체를 탐색하지 않아야 한다.

## 대안

**A. PriorityQueue (Heap)**
- 가격 기준 정렬은 가능하나 동일 가격 내 FIFO를 보장하지 않는다.
- 중간 취소가 O(n) (힙에서 임의 원소 제거).
- 같은 가격 레벨의 잔량 합계 조회가 O(n).

**B. LinkedList + Node 직접 참조**
- orderIndex에 Node 참조를 저장하면 O(1) 중간 삭제 가능.
- 메모리가 비연속적이라 순차 탐색 시 캐시 미스가 증가한다.
- Node 생명주기 관리 코드가 복잡해지고 실수할 여지가 많다.

**C. Skip List**
- TreeMap과 동일한 O(log n) 삽입·삭제·조회.
- 구현 복잡도 대비 이점 없음. Java 표준 라이브러리에 포함되지 않아 직접 구현 필요.

**D. LMAX Disruptor (Ring Buffer)**
- 캐시 라인 패딩·링버퍼로 초저지연(나노초 단위) 달성.
- 학습 비용과 운영 복잡도가 높다.
- Kafka 파티션 기반 Single Writer가 이미 보장된 환경에서 추가 최적화로는 과잉이다.

**E. TreeMap + ArrayDeque (채택)**
- `TreeMap<BigDecimal, Deque<OrderEntry>>`: 가격 기준 자동 정렬, `firstEntry()` O(1) 최우선 호가 조회.
- `ArrayDeque`: 메모리 연속 배열 기반으로 캐시 친화적. `addLast/pollFirst` O(1) FIFO.
- `HashMap<Long, OrderEntry>` orderIndex: orderId → OrderEntry O(1) 취소·멱등성 체크.
- Lazy Removal: 취소 시 마킹만 하고 `match()` 직전 `drainInvalid()`에서 앞부터 정리 → 평균 O(1).

## 트레이드오프

| | TreeMap + ArrayDeque | LinkedList + Node | PriorityQueue | Disruptor |
|---|---|---|---|---|
| 최우선 호가 조회 | O(1) | O(1) | O(1) | O(1) |
| 삽입 | O(log n) | O(1) | O(log n) | O(1) |
| 취소 (Lazy) | O(1) 평균 | O(1) | O(n) | - |
| 캐시 효율 | 높음 | 낮음 | 높음 | 매우 높음 |
| 구현 복잡도 | 낮음 | 높음 | 낮음 | 매우 높음 |

Lazy Removal의 O(1) 근거: 취소 주문이 큐 앞에 연속으로 쌓이는 경우는 드물다.
정상 흐름에서는 유효한 주문이 앞에 있으므로 `drainInvalid()`가 즉시 break된다.
취소가 집중되는 극단적 상황에서는 O(k) (k = 앞에 쌓인 취소 수)이나, k는 전체 큐 크기보다 훨씬 작다.

## 결정

**E안 채택 — TreeMap + ArrayDeque + HashMap orderIndex + Lazy Removal**

취소 주문이 발생할 때 Deque 중간 삭제(O(n))를 피하기 위해 LinkedList + Node를 검토했으나,
캐시 미스 증가와 코드 복잡도가 실익보다 크다고 판단했다.
현재 매칭은 Kafka Single Writer 환경에서 동작하므로 추가 락·동기화 없이도 안전하고,
LMAX Disruptor 수준의 초저지연은 현재 요구사항을 초과한다.

## 결과

- `OrderBook`: TreeMap bids(역순)/asks(오름차순), 가격 레벨당 ArrayDeque로 FIFO 보장
- `cancelOrder()`: orderIndex에서 O(1) 조회 후 cancelled 마킹, Deque 즉시 제거 없음
- `drainInvalid()`: `match()` 호출 시 큐 앞부터 무효 엔트리 정리
- `containsOrder()`: orderIndex + filledOrderIds 로 멱등성 체크 (전량 체결 후에도 재등록 방지)
