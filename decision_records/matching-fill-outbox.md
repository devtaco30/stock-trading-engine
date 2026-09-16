---
feature: matching-fill-outbox
date: 2026-09-16
branch: feat/matching-fill-outbox
commits: [9d063bb, 5b9c81b, efc0037]
feeds: [adr, blog]
---

# 매칭 체결 발신 아웃박스 분리 (I4)

## ADR 네타

### 제목 후보: "체결 발신을 매칭 소비자 스레드에서 분리하고, 목적지별로 아웃박스를 나눈 이유"
- **context(무슨 상황)**: 매칭 엔진은 종목별 호가창을 단일 소비자 스레드로 처리한다(LMAX 스타일). 체결이 나면 그 스레드가 매수·매도 계좌가 각각 속한 샤드 endpoint로 체결을 fan-out 발행한다(`AccountFillPublisher.onFill`, `ShardRoutingTable`로 endpoint 계산).
- **왜(문제·동기)**: 체결은 계좌가 영영 못 받으면 잔고가 틀어지는 값이라 지금까지 `offerNeverDrop`이 매칭 소비자 스레드 위에서 직접 Aeron `offer`를 성공할 때까지 재시도했다. 계좌 샤드 하나가 못 받는 상태가 되면 그 재시도 루프에서 매칭 스레드가 묶이고, 그 스레드가 곧 매칭 자체라 그 노드가 맡은 종목 전체의 매칭이 멈췄다 — 체결 하나를 안 잃으려고 전체 가용성을 내주는 구조.
- **어떻게(대안·결정·트레이드오프)**: 대안 (a) 매칭이 체결을 자기 Archive에 녹화해두고 계좌가 나중에 당겨가는 방식 — I2에서 이미 기각됨(Aeron `replicate`는 Archive 사이 복제이지 원격 당겨오기가 아니고, 그 배선이 레포에 없음). 대안 (b) 아웃박스(큐)+전용 스레드로 발신을 매칭 스레드에서 분리, 목적지 endpoint마다 별도 큐·스레드(공유 큐 하나였다면 A로 가는 재시도가 오래 걸릴 때 A 뒤에 줄 선 B행 체결도 같이 막혔을 것 — head-of-line blocking). 채택 = (b). 결정 넷: D1 목적지별 아웃박스(큐·스레드·인코딩 버퍼 각각 하나), D2 체결은 버리지 않는다(큐 가득 차면 매칭 스레드가 그 자리에서 대기, 계좌가 영구히 안 살아나는 경우까지 닫진 않음 — 대기 프로세스를 두는 I8 몫), D3 인코딩은 전용 스레드에서 하되 tradeId 발급 자리는 매칭 스레드에 그대로 둠(체결 순서와 tradeId 순서가 어긋나지 않게), D4 종료 시 드레인하고 타임아웃 초과분은 ERROR로 남김.
- **무엇을(실제 변경·파일·커밋)**: `FillOutbox` 신규(목적지별 `OneToOneConcurrentArrayQueue<FilledTrade>`+전용 스레드+인코딩 버퍼). `AccountFillPublisher.onFill`은 tradeId 발급 후 큐잉만 하고 즉시 반환. Spring 빈은 `initMethod="start"`, `destroyMethod="close"`. 파일: `matching-worker/src/main/java/com/flab/stocktradingengine/matching/worker/messaging/FillOutbox.java`(신규)·`AccountFillPublisher.java`·`config/MatchingFillPublishConfig.java`. 커밋 `9d063bb`.
- **결과·수치**: 미측정(처리량·지연 벤치마크는 이 트랙 범위 밖). 기능 검증만 — 단위 테스트 GREEN, U2에서 head-of-line blocking 재현(구코드)·해소(신코드) 둘 다 확인.

### 제목 후보2: "복구 불가 스트림은 fail-fast, 일시적 실패는 전용 스레드가 재시도 — 실패 성격을 구분한 이유"
- **context(무슨 상황)**: `FillOutbox`의 전용 스레드가 Aeron `offer` 재시도 중 `CLOSED`·`MAX_POSITION_EXCEEDED`(스트림 복구 불가)를 만날 수 있다. 예전 코드는 이때 매칭 소비자 스레드 자신이 예외를 던져 `MatchingExceptionHandler`가 매칭 전체를 fail-fast로 멈췄다.
- **왜(문제·동기)**: 아웃박스 도입 초안에서는 이 경우도 해당 endpoint 전용 스레드만 조용히 죽게(ERROR 로그만) 짰다 — 가용성 작업의 결을 일관되게 가져간다는 판단이었다. 조정 세션(리뷰)이 반려하며 근거 셋을 댔다: ①`CLOSED`는 "잠깐 안 받는" 일시적 상태가 아니라 성격이 다른 실패다(D2가 벌어주려는 시간은 일시적 상태 몫). ②그대로 둬도 결국 그 목적지 큐가 가득 차 매칭 스레드가 거기서 영구히 대기하게 되므로 매칭은 어차피 멈추는데, ERROR 한 줄만 지나간 뒤라 운영에서는 "왜 멈췄는지 모르는 상태"가 된다(이 프로젝트에서 반복 지적된 "조용히 넘어가는 자리"와 같은 모양). ③죽은 스레드는 D4(종료 시 드레인)도 수행할 수 없어, 그 목적지의 체결이 프로세스 종료 시 그대로 사라진다 — "체결은 버리지 않는다"(D2)와 정면으로 어긋난다.
- **어떻게(대안·결정·트레이드오프)**: 전용 스레드는 Disruptor 핸들러가 아니라 예외를 던져도 매칭 소비자 스레드까지 자동 전파되지 않는다. 그래서 전용 스레드가 치명 상태를 `fatalError`(`AtomicReference<IllegalStateException>`)에 기록하고 죽고(ERROR 로그), `enqueueNeverDrop`이 호출될 때마다 그 기록을 확인해 매칭 소비자 스레드 자신에서 예외를 다시 던지게 했다 — 그래야 `MatchingExceptionHandler`가 예전처럼 매칭 전체를 멈춘다.
- **무엇을(실제 변경·파일·커밋)**: `FillOutbox.fatalError`·`throwIfFatal()`, `publish()`가 CLOSED/MAX_POSITION_EXCEEDED에서 `fatalError.set(error)` 후 던짐. 커밋 `9d063bb`(같은 커밋 안에서 리뷰 반영까지 완결).
- **결과·수치**: 단위 테스트로 검증(`AccountFillPublisherTest#스트림이_복구_불가_상태면_다음_enqueue에서_매칭_스레드로_예외가_전파된다`) — RED(`Expecting code to raise a throwable`) 확인 후 구현, GREEN. 미측정(실제 Aeron 스트림으로 CLOSED를 유발하는 통합테스트는 없음, mock으로만 검증).

### 제목 후보3: "드레인 실패 카운트가 처리 중이던 항목을 빠뜨리던 버그"
- **context(무슨 상황)**: `FillOutbox.close(timeout)`은 종료 시 큐를 드레인하고, 타임아웃 안에 못 끝나면 남은 개수를 ERROR로 남기도록 설계했다(D4).
- **왜(문제·동기)**: U3에서 "타임아웃에 남으면 ERROR로 찍히는지"를 먼저 테스트로 작성해 RED부터 돌렸더니 실패했다 — 발신 재시도에 영원히 갇힌 체결 1건은 `queue.poll()`로 이미 큐에서 빠져나온 상태라 `close()`의 `remaining = queue.size()` 계산이 0이 되고, 드레인이 실패했는데도 "남은 게 없다"고 오판해 ERROR를 아예 안 남기고 있었다. 이 1건은 프로세스가 내려가면 그대로 사라진다 — 이 프로젝트에서 반복된 모양("못 한 일을 없는 일로 치는 자리").
- **어떻게(대안·결정·트레이드오프)**: `close()`가 타임아웃 뒤에도 스레드가 살아있으면(`publishLoop` 종료 조건이 `running=false && queue.isEmpty()`라 안 끝났다는 뜻) `queue.size() + 1`을 남은 개수로 계산하도록 수정. `+1`이 정확히 1인 근거: `publishLoop`가 멈춰 있을 수 있는 자리는 `publish()` 안의 재시도 루프뿐이고, 그 루프는 `poll()`로 큐에서 꺼낸 trade 하나를 손에 든 채로 돌기 때문에 항상 정확히 한 건이다(다른 대기 지점이 없다).
- **무엇을(실제 변경·파일·커밋)**: `FillOutbox.close()`. 커밋 `efc0037`. 테스트 `FillOutboxTest`(Logback `ListAppender`로 ERROR 로그 직접 캡처 — 이 레포에 로그 캡처 테스트 전례가 없어 새로 만듦, 새 의존성 추가 없이 `spring-boot-starter`가 전이로 가져오는 logback-classic만 씀).
- **결과·수치**: RED 메시지 `Expecting any elements of: [] to match given predicate but none did.`(로그가 아예 안 남음) → 수정 후 GREEN.

## 블로그 네타

### "체결 하나를 안 잃으려다 전체를 세운 이야기"
- **훅·핵심 주장**: 유실 방지(never-drop)와 가용성은 공짜로 같이 살 수 없다. 한쪽을 지키는 재시도가 다른 쪽(가용성)의 자원(스레드)을 그대로 쓰면, 지키려던 값이 거꾸로 전체를 멈추는 축이 된다.
- **context**: 매칭 엔진은 단일 스레드로 종목별 호가창을 처리한다(LMAX Disruptor 스타일 — 락 없이 정합성을 지키는 대가로 그 스레드 하나에 모든 게 몰린다). 그 스레드가 체결 발신까지 겸하고 있었다.
- **어떻게(서사·근거)**: "체결을 안 잃는다"는 요구 → 성공할 때까지 재시도하는 루프 → 그 루프가 도는 스레드가 곧 매칭 전체 → 계좌 하나가 느려지면 매칭 전체가 느려진다는 역설이 성립한다. 해법은 "안 잃는다"를 포기하는 게 아니라, **안 잃는 책임(큐)**과 **그 자리에서 기다리는 대가**를 분리하는 것이다 — 목적지마다 별도 대기열을 둬 한 목적지의 대가가 다른 목적지로 전이되지 않게 한다(head-of-line blocking 방지). 그리고 "안 잃는다"에도 예외가 있다는 것 — 복구 불가 상태(CLOSED)는 성격이 다른 실패라, 조용히 견디는 대신 즉시 멈추고 알리는 쪽이 맞다는 판단까지 이어진다.
- **재료(커밋·도식·수치)**: 커밋 `9d063bb`·`5b9c81b`·`efc0037`. U2가 잡은 head-of-line blocking 재현 실패 메시지(`Expecting value to be false but was true` — 매칭 흉내 스레드가 막힌 샤드에 갇혀 다른 샤드로는 끝내 못 감). U3의 `close()` 버그 — 테스트를 먼저 쓰고 RED를 본 것이 실제로 버그를 찾은 사례(로그 캡처 테스트로 "조용히 사라지는 자리"를 잡음).
