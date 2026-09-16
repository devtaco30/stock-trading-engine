---
feature: account-order-forwarding-durability
date: 2026-09-16
branch: feat/account-order-send-durability
commits: [638b94f, 0809a88, dbf215c]
feeds: [adr, blog]
---

# 계좌→매칭 발신 durability (I2 발신측) — 예약은 남는데 주문은 사라지던 문제를 닫기

계좌가 매칭으로 주문을 보내다 실패하면 로그만 남기고 버리던 문제를 닫은 작업이다. matching-worker의 체결 발신 아웃박스(I4)와 대칭인 트랙이고, 세 유닛(U1 안 버림, U2 종료 드레인, U3 통합 증명)으로 나눠 진행했다.

## ADR 네타

### ① 버리지 않고 기다리게 바꾼 이유와 구조(D1·D2·D3)

- **context**: 계좌는 주문을 받으면 먼저 돈을 잡아 둔다(매수면 예약증거금, 매도면 보유 수량 예약) — `AccountEventHandler.handleBuy`/`handleSell`가 `state.tryReserve`/`trySellReserve` 성공 뒤 `matchingOrderSender.forwardPlace(...)`를 부른다. 이 예약은 이미 계좌 저널(Aeron Archive에 durable하게 기록되는 로그)에 남는다 — 직접 확인함. `AeronMatchingOrderSender`(발신을 계좌 단일 쓰기 스레드에서 분리하는 아웃박스+전용 스레드 구조는 이미 있었다)는 발신 큐가 가득 차거나 Aeron `offer`(전송 시도)가 실패하면 `log.warn`만 남기고 그 주문을 버렸다.
- **왜**: 계좌엔 예약이 그대로 남는데 매칭 장부엔 그 주문이 아예 없어서, 체결도 취소도 영영 오지 않는다 — 사용자 입장에서는 돈이 잠긴 채 아무 일도 안 일어난다.
- **어떻게**: matching-worker `FillOutbox`(I4, 체결을 계좌로 보낼 때 같은 문제를 이미 풀어둔 것)를 그대로 미러했다. 다만 목적지가 하나뿐이라(`MatchingOrderSenderConfig`가 `Publication` 하나만 만든다, D5) `FillOutbox`처럼 목적지별로 나누지 않고 `AeronMatchingOrderSender` 하나가 큐·전용 스레드·인코딩 버퍼를 직접 갖는다. 세 결정: **D1** 큐가 가득 차면 `forwardPlace`가 그 자리에서 기다린다 — 기다리는 주체가 계좌의 단일 쓰기 스레드라 그동안 이 샤드가 맡은 **다른 계좌의 주문 접수도 같이 멈춘다.** 이 대가를 받아들이는 근거: 매칭이 주문을 못 받는 상태라면 그 종목의 거래는 어차피 성립하지 않는다 — 접수만 계속 받아 봐야 예약만 쌓이고 체결은 안 난다(matching-worker `FillOutbox`는 목적지가 여럿이라 "하나가 막혀도 나머지를 살리는" 게 핵심이었지만, 여기는 목적지가 하나라 그 구분 자체가 없다). **D2** 전용 스레드는 Aeron `offer`가 성공(반환값 ≥ 0)할 때까지 조용히 재시도한다 — 전용 스레드라 여기서 기다려도 계좌 로직 스레드는 안 막힌다. **D3** `offer`가 `CLOSED`·`MAX_POSITION_EXCEEDED`(스트림이 복구 불가 상태라는 뜻)를 돌려주면 성격이 다르다 — 그 사실을 `fatalError`(`AtomicReference<IllegalStateException>`)에 기록하고 전용 스레드가 그 예외로 죽는다(ERROR 로그). 이 스레드는 Disruptor 핸들러가 아니라 예외를 던져도 계좌 소비자 스레드까지 자동 전파되지 않으므로, `forwardPlace`가 호출될 때마다 그 기록을 확인해 계좌 소비자 스레드 자신에서 다시 던진다 — 그래야 `AccountExceptionHandler`(계좌 쪽 Disruptor 예외 핸들러, 이미 fail-fast 정책)가 계좌 전체를 멈춘다.
- **무엇을**: `AeronMatchingOrderSender.java` 재작성(두 `log.warn` 버리는 자리 삭제). `MatchingOrderSender` 인터페이스의 "절대 블로킹·예외 금지" 계약 javadoc도 이제 틀린 내용이라 같이 고쳤다. 커밋 `638b94f`.
- **결과·수치**: RED — 큐 가득 참 테스트: `Expecting value to be true but was false`(버리지 않고 기다리는지 확인하는 assertion). 복구 불가 전파 테스트: `Expecting code to raise a throwable`. 둘 다 원복 후 GREEN.

### ② 계좌 단독 테스트가 가짜 sender를 쓰게 된 경위와 `@Configuration` 함정

- **context**: 계좌 복구·저널·정산·샤드 격리만 보는 테스트 13개는 매칭 프로세스를 안 띄운다.
- **왜**: ①에서 만든 재시도(D2)가 이 13개에서는 "연결 안 됨"을 계속 재시도하다가, 컨텍스트 종료 시 드레인 타임아웃(5초)을 매번 다 채웠다 — 회귀 시간이 눈에 띄게 늘었다(예: `AccountSnapshotRecoveryIntegrationTest` 7초→46초, `CrossShardFillFanoutIntegrationTest` 7초→41.5초). D2 자체는 의도한 동작이지만 이 테스트들엔 무관한 비용이었다.
- **어떻게**: `NoOpMatchingOrderSender`(테스트 더블, 아무것도 안 함)를 만들어 그 13개에만 명시적으로 등록했다. **처음에 `@Configuration`을 붙였다가 진짜 사고가 났다** — `AccountWorkerApplication`의 컴포넌트 스캔 범위(`com.flab.stocktradingengine.account.worker`)가 테스트 소스까지 포함해서, `@Configuration`이 붙은 이 더블을 명시적으로 `@Import`·`.sources(...)` 안 한 컨텍스트에서도 스캔이 주워 `@Primary`로 등록해버렸다. 그러면 **매칭 전달을 실제로 단언하는 유일한 테스트**(`AccountToMatchingForwardingIntegrationTest`)까지 진짜 sender 대신 이 더블을 쓰게 돼 "5초 안에 매칭 발신 주문을 수신하지 못함"으로 깨졌다 — 처음엔 이걸 "원래 있던 결함"으로 잘못 판단했는데(`git stash`가 untracked 파일은 안 치운다는 걸 놓쳐, 이 더블 클래스가 stash 뒤에도 디스크에 남아 여전히 스캔되고 있었다), 조정 세션이 "혹시 stash가 untracked를 안 치운 것 아니냐"고 짚어줘 `git stash -u`로 다시 격리해 베이스가 실제로는 정상임을 확인했다. 원인은 `@Configuration` 자체 — 이 프로젝트 기존 관례(`RecorderConfig`류: 스테레오타입 애노테이션 없이 `@Bean` 메서드만 두고, 명시적 `@Import`로만 등록)에 맞춰 `@Configuration`을 뗐다.
- **무엇을**: `NoOpMatchingOrderSender.java`·`NoOpMatchingOrderSenderTestConfig.java`(신규, `@Configuration` 없음 — javadoc에 이유 명시). 13개 테스트에 `.sources(...)`(수동 기동 9개) 또는 `@Import(...)`(4개, 기존 `@Import`와 배열로 합침) 등록. 프로덕션 프로퍼티 스위치는 만들지 않았다 — 운영에서 잘못 켜지면 주문이 매칭에 영영 안 가고 아무 에러도 안 나는, 지금 고치는 버그와 같은 결과를 낸다.
- **결과·수치**: `@Configuration` 제거 후 `AccountToMatchingForwardingIntegrationTest` 재확인 PASSED(격리 stash로도 재확인). 13개 시간 회복 — `AccountSnapshotRecoveryIntegrationTest` 46초→2.5초, `CrossShardFillFanoutIntegrationTest` 41.5초→(격리 실행)10초(baseline과 동일).

### ③ 종료 드레인 + 개수 보고(D4)

- **context**: 전용 스레드가 종료될 때 큐에 남은 주문, 그리고 재시도에 갇혀 큐 밖으로 이미 나온 주문을 어떻게 처리할지.
- **왜**: 조용히 사라지는 자리를 만들지 않기 위해 — 드레인이 타임아웃 안에 안 끝나면 몇 건이 유실 가능한지 ERROR로 남겨야 한다.
- **어떻게**: `close()`가 타임아웃까지 `publisherThread.join`한 뒤, 스레드가 아직 살아있으면(=아직 처리 못함) `outbox.size() + 1`을 남은 개수로 로그한다. `+1`이 필요한 이유: 재시도 루프에 갇힌 주문 1건은 이미 `poll()`로 큐 밖에 나와 있어 `outbox.size()`만 세면 빠진다 — matching-worker `FillOutbox.close()`가 I4에서 실제로 겪은 버그와 같은 자리라 그대로 옮겨 심었다.
- **무엇을**: `AeronMatchingOrderSender.close(long)` 오버로드(패키지 가시성, 테스트가 짧은 타임아웃을 준다) + 무인자 `close()`(운영 기본값 5초, Spring destroyMethod). 테스트 2개(전부 드레인 확인, 타임아웃 초과 시 로그 확인 — Logback `ListAppender`로 직접 캡처). 커밋 `0809a88`.
- **결과·수치**: RED — I4의 그 버그를 내 코드에서도 재현(`+1`을 빼고 재실행) → 로그가 "유실 가능한 주문 **0건** 남음"으로 잘못 찍혀 실패(`Expecting any elements of: [...0건...] to match given predicate but none did.`). 원복 후 GREEN.

### ④ U3 — 이 트랙 전체의 증명

- **context**: "매칭이 안 받는 상태에서 주문을 넣고, 매칭이 받기 시작하면 그 주문이 결국 도착하는지"를 실제 Aeron으로 증명해야 한다.
- **왜**: mock으로는 "실제 네트워크 상태가 NOT_CONNECTED일 때 재시도가 진짜로 도는지"를 증명하지 못한다. 그리고 발신 완료를 세는 새 카운터를 만들면 "내부 상태를 훔쳐보는" 관측이 된다(이 프로젝트에서 반복 지적된 함정) — "도착 자체"가 이미 충분한 신호다.
- **어떻게**: `AccountOrderForwardingRetryIntegrationTest` — account-worker 앱을 실제로 띄우되 매칭 인테이크 Subscription을 **일부러 늦게 연다.** 구독이 없는 동안 `engine.publishBuy`로 주문을 접수시키면 accept 콜백은 즉시 오지만(큐잉만 하고 반환, 재시도는 별도 스레드), 실제 Aeron `Publication`엔 구독자가 없어 전용 스레드가 `NOT_CONNECTED`로 계속 재시도한다. 그 뒤에야 Subscription을 열어("매칭이 뒤늦게 받기 시작") poll로 도착을 확인한다.
- **무엇을**: `AccountOrderForwardingRetryIntegrationTest.java`(신규). 커밋 `dbf215c`.
- **결과·수치**: RED(`publish()`의 재시도 루프를 U1 전 1회성 시도로 임시로 되돌림) — `5초 안에 매칭 발신 주문을 수신하지 못함 — 재시도가 도착으로 안 이어졌다`. 원복 후 GREEN. `:account-disruptor:test`·`:account-worker:test` 전체 162 tests, 0 failures.

### ⑤ 범위 경계 — 안 닫은 것

- **context**: D1이 만든 대가(큐가 가득 차면 그 샤드의 다른 계좌 접수도 같이 멈춘다)는 이 트랙이 해소하지 않는다.
- **왜**: 매칭이 영구히 안 살아나는 경우(계좌 프로세스가 다시 안 뜨는 것)까지 버티게 하려면 별도의 대기 프로세스·타임아웃 정책이 필요하고, 그건 I8(가용성 트랙)의 몫이다.
- **어떻게**: 손대지 않음 — javadoc과 결정 기록에 한계로 명시.
- **무엇을**: 코드 변경 없음.
- **결과·수치**: 해당 없음.

## 블로그 네타

### "예약은 남는데 주문은 사라진다 — 발신 실패를 조용히 삼키던 버그"

- **훅·핵심 주장**: 돈을 다루는 시스템에서 "일단 예약해두고 나중 단계가 실패하면 로그만 남긴다"는 설계는, 그 로그를 아무도 안 보는 순간 사용자 돈이 잠긴 채 영원히 안 풀리는 버그가 된다.
- **context**: 계좌는 주문을 받으면 먼저 돈을 잡아 두고 매칭으로 보낸다. 그 발신이 실패하면 예약은 그대로인데 매칭 장부엔 없는 상태가 된다.
- **어떻게(서사·근거)**: matching-worker가 체결 발신에서 이미 같은 문제를 풀어둔 패턴(I4, 목적지별 아웃박스+전용 스레드+재시도)을 그대로 미러했다. 그런데 그 재시도를 계좌 단독 테스트(매칭을 안 띄우는)에 그대로 적용하니 회귀가 느려졌고, 그걸 없애려 만든 테스트 더블에 실수로 `@Configuration`을 붙였다가 정작 "매칭 전달이 실제로 되는지" 증명하는 유일한 테스트를 깨뜨렸다 — 처음엔 이걸 "원래 있던 버그"로 오판할 뻔했는데, `git stash`가 untracked 파일은 안 치운다는 사실 하나로 진단이 뒤집혔다. 마지막 증명(U3)은 새 계측 없이 "매칭 구독을 일부러 늦게 열어 실제 네트워크 재시도를 만들고, 도착 자체를 본다"는 방식으로, mock 없이 실제 Aeron 재시도가 작동함을 보였다.
- **재료(커밋·도식·수치)**: 커밋 `638b94f`·`0809a88`·`dbf215c`. RED 메시지 세 개(각 유닛). `git stash -u`로 뒤집힌 진단 사례. 회귀 51→161→162 tests, 매번 0 failures.
