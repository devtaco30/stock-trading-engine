# ADR-027 계좌 워커는 Kafka로 받은 이벤트를 journal에 적은 뒤에 ack한다

- 날짜: 2026-09-10
- 상태: 채택. 적용 범위는 정산 컨슈머로 좁아졌다(아래 결과 참조).
- 관련: ADR-014(single-writer 계좌) · ADR-020(전송 반전) · ADR-025(엔진 journal과 replay) · ADR-028(journal을 못 쓸 때) · ADR-035(멱등 장부 상한) · 원문 `decision_records/engine-journal-durability.md`

## 문제

v2 계좌 워커는 주문을 Aeron으로 받는다(ADR-020). 그런데 계좌의 상태를 변경하는 input이 주문만은 아니다. 매칭 워커가 낸 체결과 정산 워커가 낸 미수금 차감도 계좌 잔고를 고친다. 이 둘은 Kafka를 통해 계좌 워커로 들어온다.

이 ADR은 그 Kafka input을 언제 "받았다"고 표시할지를 정한다. 그 표시가 ack이고, ack은 Kafka에게 "이 메시지는 우리가 맡았으니 다시 보내지 않아도 된다"고 말하는 것이다. Kafka는 그 말을 그대로 믿는다. offset이 넘어간 메시지는 계좌 워커를 재기동해도 다시 전달되지 않는다.

그래서 ack하는 순간부터 그 이벤트의 사본은 계좌 워커에만 남는다. 그런데 계좌 워커는 잔고를 메모리에 두고 고치므로(ADR-014) 프로세스가 죽으면 그 메모리가 사라진다. 계좌 워커가 디스크에 남기는 수단은 journal 하나뿐이다. 그러니 journal에 적히기 전에 ack하면, 사본이 Kafka에서는 이미 지워졌고 디스크에는 아직 없는 구간이 생긴다. 그 구간에서 프로세스가 죽으면 그 차감이 사라진다.

### 용어

- **ack · offset** — Kafka 컨슈머가 메시지를 처리했다고 표시하는 것이 ack이고, 그 표시가 저장되는 위치 값이 offset이다. offset이 넘어간 메시지는 재기동해도 다시 전달되지 않는다.
- **링버퍼에 넣기** — 계좌 워커가 Kafka에서 받은 이벤트를 엔진 링버퍼에 넣는 동작. 넣기만 하고 바로 반환하며, journal 기록과 잔고 반영은 그다음에 엔진 스레드가 한다. 메서드 이름이 `AccountEngine.publishSettlement`이지만 Kafka로 무언가를 보내는 것이 아니다. Disruptor에서 링버퍼에 넣는 쪽을 publisher라 부르기 때문에 붙은 이름이고, 이 글에서는 혼동을 피해 "링버퍼에 넣는다"로 쓴다.
- **시퀀스(sequence)** — 링버퍼에 넣은 자리의 번호. 0부터 하나씩 올라간다. 엔진의 각 핸들러가 "여기까지 처리했다"를 이 번호로 표시한다.
- **journal · replay** — journal은 엔진이 받은 입력을 받은 순서대로 적어 둔 기록이고, replay는 그 기록을 다시 적용해 상태를 만드는 복구 방식이다(ADR-025).
- **멱등 장부** — 이미 반영한 체결·정산을 기록해 두고 같은 것이 또 오면 버리는 집합. 계좌마다 `processedTradeIds`·`processedSettlementRefs`를 갖는다(`AccountState.java:61`).
- **유실 창(lost window)** — 어느 쪽에도 기록이 남지 않아 사고가 나면 이벤트가 사라지는 시간 구간.
- **핫패스** — 주문 한 건이 api → 계좌 워커 → 매칭 워커 → 계좌 워커로 지나는 구간. 여기서는 DB도 외부 캐시도 건드리지 않는다(ADR-020).
- **single-writer** — 한 상태를 한 스레드만 고치는 규칙(ADR-014). 락 없이 정합성을 지키는 v2의 토대다.
- **Aeron Archive** — Aeron 스트림을 디스크에 녹화해 두고, 지정한 position부터 되감아 읽어 주는 컴포넌트(ADR-019).
- **position** — 녹화된 스트림 안의 바이트 위치. 복구할 때 "여기부터 읽어라"의 기준점이다.
- **미수금** — 매수 대금이 모자란 채로 체결돼 T+2 정산일까지 갚아야 하는 돈. 정산 워커가 그날이 되면 계좌에서 차감하라고 보낸다.

그때까지 컨슈머는 이벤트를 링버퍼에 넣자마자 ack했다. 넣는 동작은 자리만 잡고 바로 반환하므로, ack하는 그 시점에 이벤트는 아직 journal에 적혀 있지 않다.

계좌 A에서 미수금 5만 원을 차감하라는 정산 이벤트(`SettlementResultEvent`)가 들어왔다고 하자. 지금 코드에서 벌어지는 순서는 이렇다.

| 순서 | 일어나는 일 | 그때 이 이벤트가 남아 있는 곳 |
|---|---|---|
| 1 | 컨슈머가 Kafka에서 차감 이벤트를 받는다 | Kafka(offset 아직 안 넘어감) |
| 2 | 컨슈머가 이벤트를 링버퍼에 넣는다(`publishSettlement`) | Kafka + 계좌 워커 메모리 |
| 3 | **컨슈머가 ack한다 → offset이 넘어간다** | **계좌 워커 메모리뿐** |
| 4 | 엔진 스레드가 journal에 적는다 | 메모리 + 디스크 |
| 5 | 엔진 스레드가 잔고를 5만 원 차감한다 | 메모리 + 디스크 |

3번과 4번 사이가 유실 창이다. 이 구간에서 계좌 워커가 죽으면 그 차감 이벤트는 어디에도 없다. journal에는 아직 안 적혔고, Kafka는 offset이 넘어가서 다시 보내지 않는다. 재기동해서 replay를 돌려도 읽을 기록 자체가 없다.

결과는 계좌 A가 미수금 5만 원을 안 낸 상태로 남는 것이다. 그리고 이 어긋남은 어디에도 로그가 남지 않는다. 정산 워커는 차감을 보냈고 계좌 워커는 ack까지 했으니, 양쪽 다 끝난 일로 보고 있다. 정산 워커 쪽 장부와 계좌 잔고를 따로 맞춰 보기 전에는 드러나지 않는다.

## 대안

1. **링버퍼에 넣자마자 ack.** 그때까지의 코드.
2. **journal에 적힌 것을 확인한 뒤 ack.**
3. **잔고 반영까지 끝난 것을 확인한 뒤 ack.**

## 트레이드오프

| 기준 | 1. 넣자마자 | 2. journal 뒤 | 3. 반영 뒤 |
|---|---|---|---|
| 유실 창 | 있음 | 없음 | 없음 |
| 사고가 났을 때 결과 | 이벤트가 사라짐 | 같은 이벤트가 다시 옴 | 같은 이벤트가 다시 옴 |
| 컨슈머가 붙잡히는 구간 | 없음 | journal append까지 | 비즈니스 처리까지 |
| 엔진 스레드 | 영향 없음 | 영향 없음 | 영향 없음 |

3까지 갈 이유가 없다. journal에 적히고 나면 그 입력은 replay로 다시 적용할 수 있고, 잔고 반영은 그 replay가 재현한다(ADR-025). 반영까지 기다리면 컨슈머만 더 오래 붙잡힌다.

2를 고르면 실패했을 때 나오는 결과가 유실에서 중복으로 바뀐다. journal에 적힌 뒤 ack 전에 죽으면 Kafka가 같은 메시지를 다시 보내고, 그 중복은 멱등 장부가 버린다. 중복은 멱등 장부가 버리면 끝나지만, 유실은 되살릴 기록 자체가 없다.

## 결정

대안 2다.

- 링버퍼에 넣는 메서드(`AccountEngine.publishSettlement`)가 그 자리의 시퀀스를 반환한다.
- `blockUntilJournaled(sequence)`는 journal 핸들러의 시퀀스가 그 값에 도달할 때까지 호출 스레드를 붙잡는다(`AccountEngine.java:502`). 도달했다는 것은 그 자리까지 journal에 적혔다는 뜻이다. 5초 안에 도달하지 못하면 `JournalUnavailableException`을 던진다.
- 컨슈머는 그 뒤에 ack한다(`AccountSettlementConsumer.java:30`).

기다리는 방식은 새로 만든 것이 아니다. `disruptor.getSequenceValueFor(journalHandler)`는 ADR-025가 건 `handleEventsWith(journal).then(business)` 배선이 이미 쓰는 것과 같은 시퀀스다.

## 결과

- 커밋 `a87b3a3`. 엔진에 `blockUntilJournaled`를 넣고 체결·정산 두 컨슈머를 그 뒤에 ack하도록 고쳤다.
- 테스트를 먼저 썼다. 실패하는 테스트는 "ack 시점의 journal 크기가 0"이었다. 지금은 `AccountJournalDurabilityGateTest`가 엔진의 게이트를, `AccountConsumerDurabilityGateTest`가 컨슈머의 게이트를 본다.
- 정상 상태에서 컨슈머가 붙잡히는 시간은 따로 재지 않았다. 정산 컨슈머는 핫패스 밖이라 지연 측정 대상에 넣지 않았다.
- 타임아웃으로 `JournalUnavailableException`이 났을 때 컨슈머가 무엇을 할지는 ADR-028에서 정한다.
- **적용 범위가 그 뒤에 좁아졌다.** 체결 경로는 ADR-032의 첫 실행에서 Kafka를 걷어내고 Aeron으로 옮겼다(`3d6bf59`). Aeron 수신에는 ack가 없으므로 이 결정이 그 경로에서는 의미를 잃었고, 그 구간의 유실 방지는 매칭 워커 쪽 Archive 녹화와 계좌의 position 복구가 맡는다. 지금 `blockUntilJournaled`를 부르는 곳은 정산 컨슈머 하나다.
- Disruptor 4.0.0에서 `getSequenceValueFor`가 `EventHandlerIdentity`를 받도록 바뀌어 있어, 시그니처를 `javap`로 확인하고 썼다.
