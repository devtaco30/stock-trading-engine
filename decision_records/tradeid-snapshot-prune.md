---
feature: tradeid-snapshot-prune
date: 2026-09-16
branch: fix/account-tradeid-snapshot-bound
commits: [6a91a2e, 0600384]
feeds: [adr, blog]
---

# tradeId 멱등 장부 무한증가 막기 — 주기 durable 스냅샷 + 세대별 가지치기

앞선 flush(`account-idempotency-cache-bound.md`)는 세 멱등 장부 중 tradeId만 "복구 겹침이 스냅샷 정책과 얽혀 이번 범위서 뺀다"로 남겼다. 이 flush는 그 별도 트랙의 **확정된 설계와 구현**을 기록한다. 설계 세션이 설계·리뷰, 스냅샷 구현 세션이 구현. Jack 설계 확정(2026-09-16).

## ADR 네타

### 주기 스냅샷만으로는 릭을 못 막는다 — 스냅샷은 "가지치기를 안전하게" 만들 뿐
- **context**: `processedTradeIds`가 무상한 HashSet이라 거래마다 무한 증가(OOM). "주기 스냅샷 도입"이 답처럼 보였다.
- **왜(오판 교정)**: 스냅샷은 현재 상태를 디스크에 쓰는 것뿐이다. 인메모리 집합이 다 들고 있으면 주기적으로 찍어도 그 큰 집합을 디스크에도 같이 쓰는 것이지 메모리가 안 준다. 스냅샷이 하는 진짜 일 = **가지치기(prune)를 안전하게 만드는 것**.
- **어떻게(결정)**: 복구는 마지막 스냅샷부터 **앞으로만** replay한다(`AccountEngineConfig`의 restore→저널 replay→fill replay, 각각 스냅샷 위치 이후만). 그래서 durable 스냅샷 경계보다 오래된 tradeId는 복구로도 다시 안 오고, 라이브 재전송 창(ms)도 지났으면 다시 올 길이 없다 → 버려도 안전. 핵심 규칙 두 개: ①**durable-before-prune**(디스크에 확실히 쓰인 뒤에만 그 경계보다 오래된 것 버림 — settlement의 durable-before-ack와 같은 순서 규칙) ②라이브 재전송 창만큼 **꼬리 유지**(현재+직전 세대).
- **무엇을**: 릭 수정 = 주기 durable 스냅샷 + 세대별 가지치기. 스냅샷=필요조건, 가지치기=실제로 메모리 줄이는 손.
- **결과·수치**: 집합 크기 = 처리량 × 스냅샷 주기 한 사이클치로 상한(매 주기 리셋), 무한→유한. N 값은 저널 건수 기준 tunable(초기 임시 10_000, 부하 측정 후 재산정 — 미측정).

### 가지치기 경계는 tradeId 값이 아니라 저널 적용 순번이어야 한다
- **context**: 세대를 durable 경계로 자르려면 "경계"를 무엇으로 잡느냐가 문제.
- **왜**: LMAX·Kafka는 단조 sequence 하나로 dedup을 O(1) 워터마크로 접는다. 그런데 tradeId=Snowflake는 다중 매칭노드에서 값이 단조가 아니라(앞선 flush의 P4 정정) 그 워터마크를 못 쓴다.
- **어떻게**: single-writer 계좌 워커의 **저널 도착순서(적용 순번 appliedSeq)는 단조**다. 그걸 세대 경계로 삼는다. tradeId 값으로 접는 대신 "적용 순번 N배수마다 세대 경계"로 구조에 묶는다. 못 접는 크로스축 중복은 시간/개수 창(세대 keep)으로 bound.
- **무엇을**: 자료구조 = 안 A(세대별 Set, `Deque<TradeIdGeneration>`). startNewGeneration(appliedSeq)/pruneOlderThan(keep=2). 코덱 포맷을 세대 리스트로 변경(옛 스냅샷 비호환, 기존 결정대로). 커밋 `0600384`.
- **결과·수치**: keep=2가 현재+직전 세대로 재전송 창을 덮음. 외부 리서치로 업계 표준(이벤트소싱 N-이벤트 스냅샷 + Flink checkpoint/state TTL, LMAX "스냅샷+sequence 기록→이후 저널만 replay"가 최근접) 대조 확인.

### 러닝 중 일관 스냅샷 — 소비자 스레드 캡처 + off-thread fsync + volatile durableSeq
- **context**: `AccountEngine.snapshot()`은 quiescent(graceful stop) 전제라 러닝 중 타 스레드 호출=데이터 레이스(단일 스레드 전제 깨짐). 주기 스냅샷은 러닝 중 일관 캡처가 필요.
- **왜**: ①`AccountEventType`에 컨트롤 이벤트 없음 → 소비자 스레드에서 순서대로 뜨려면 이벤트 트리거 필요. ②저널 핸들러가 비즈니스 핸들러보다 **먼저** 돈다(`handleEventsWith(journal).then(business)`) → `journal.position()`이 소비자 적용 지점보다 앞서 나감. ③`fillConsumedPosition`이 수신 스레드 값이라 러닝 중엔 소비자 적용 지점과 어긋남 → 그대로 저장 시 복구가 미적용 체결을 스킵(유실).
- **어떻게**: ①체결 이벤트에 수신 위치(`sourcePosition`=Aeron `header.position()`)를 링 슬롯(AccountEvent)에 실어 소비자가 자기 적용 지점을 기록(`lastAppliedFillPosition`, 커밋 `6a91a2e`). 와이어 코덱 무변경(인프로세스 필드). ②snapshot()이 `journal.position()` 대신 `businessHandler.lastJournaledPosition()` 사용. ③소비자가 appliedSeq%N==0에 세대 경계+직렬화+`AccountSnapshotSink.offer`, 전용 쓰기 스레드가 fsync(FileChannel.force) 후에만 volatile `durableSeq` 갱신, 소비자는 durableSeq>0이면 매 이벤트 pruneOlderThan(2). ④복구 시 `seedFillPosition(스냅샷의 fillConsumedPosition)`으로 시드(안 하면 복구 직후 스냅샷이 위치 0 저장→전체 재replay, 치명).
- **무엇을**: AccountSnapshotSink(계약, account-disruptor) / AccountSnapshotWriter(구현·전용스레드·write성공후에만 durableSeq, account-worker) / AccountSnapshotStore에 fsync+원자적 rename 추가 / recordingId 공유 빈. 8-arg 엔진 생성자(기존 7-arg는 NO_OP sink 위임, 테스트 무변경).
- **결과·수치**: 구현 완료·리뷰 승인 대기. 구현 중 버그 하나 잡음 — "durableSeq 값 바뀔 때만 prune"은 쓰기 스레드가 느려 세대가 여러 번 열리면 이미 안전한데도 prune이 미뤄짐 → "durableSeq>0이면 매 이벤트 pruneOlderThan(2)(idempotent)"로 단순화. 미측정: 스냅샷 주기 실비용, 복구 시간.

### 리뷰에서 잡은 설계 리스크(설계 세션)
- **peekFirst NPE 지뢰**: 세대 deque가 비면(pruneOlderThan(0)/빈 스냅샷 restore) NPE → `Math.max(keep,1)` clamp.
- **다중 recording**: 단일 스칼라 fillConsumedPosition은 단일 fill 소스 전제. 다중 매칭노드면 소스(recording/session)별 맵으로 확장 필요. 스파이크로 확인(udp 크로스프로세스는 세션별 recording 분리, ipc는 참조카운트 공유라 거짓양성). 이번 제출은 1샤드=1 recording이라 범위 밖 → fork2 트랙으로.
- **1-4 단독은 릭 미완**: 세대 구조만으론 트리거 없어 세대 하나가 무한. 실제 bounding은 주기 스냅샷 배선(트리거+prune)에서 완성.

## 블로그 네타

### "무한히 자라는 dedup 집합을 스냅샷으로 못 자른다 — 스냅샷이 하는 진짜 일은 '가지치기 허가'다"
- **훅·핵심 주장**: "메모리 릭이니 주기 스냅샷 도입"은 반쪽 오답이다. 스냅샷은 메모리를 안 줄인다. 스냅샷이 하는 일은 "이 경계보다 오래된 건 다시 안 온다"를 durable하게 못 박아 **가지치기를 안전하게** 만드는 것이고, 실제로 메모리를 줄이는 건 가지치기다. 둘은 짝이다.
- **context**: 인메모리 single-writer 계좌 엔진(속도 축)에 at-least-once 체결·복구 replay를 막는 멱등 장부가 붙는데 지우는 규칙이 없어 무한 증가.
- **어떻게(서사·근거)**: ①"스냅샷 도입" 직관 → "스냅샷만으론 안 줄어든다" 반전. ②"복구는 스냅샷부터 앞으로만 재생"이라는 한 문장이 안전성의 뿌리 — 경계 이전은 다시 안 옴. ③durable이 왜 필요한가 = 마지막 스냅샷은 크래시 후 남는 것(디스크)이라야 복구 시작점이 됨 = durable-before-ack와 같은 규칙. ④경계 키가 tradeId 값이면 안 되는 이유(Snowflake 다중노드 비단조) → 저널 적용 순번(단조)로. ⑤러닝 중 일관 캡처의 세 함정(quiescent 전제·저널러 앞서감·수신위치 어긋남)과 소비자-스레드-트리거 해법. ⑥업계 대조(이벤트소싱·Flink·LMAX)로 "새로 지어낸 게 아니라 표준 두 패턴의 결합"임을 보임.
- **재료(커밋·도식·수치)**: 도식 `docs/_tradeid_snapshot_prune.html`(AS-IS/TO-BE·durable 안전성·정상/복구 플로우·외부 관행) / 리서치 `scratchpad/external_dedup_research.md` / 커밋 `6a91a2e`(수신 위치)·`0600384`(세대 자료구조)·주기 스냅샷 배선(승인 대기) / JMH 97만~100만 orders/s(≈50만 체결/s, M1 Pro·JDK21) / 저널·비즈니스 핸들러 순서 = 왜 businessHandler.lastJournaledPosition().
