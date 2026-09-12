---
feature: matching-journal-replay
date: 2026-09-12
branch: feat/disruptor-matching-core
commits: [61ede26, 1076b26]
feeds: [adr, blog]
---

# 매칭 저널·리플레이 — 계좌 durability를 매칭에 미러

## ADR 네타
### ADR-029 매칭 durability = 계좌 2b(ADR-025)를 매칭에 미러 (2c)
- **context(무슨 상황)**: ADR-025로 계좌 엔진은 저널 + 리플레이 복구를 갖췄지만, 매칭 엔진은 인메모리 저널(`InMemoryJournal`, CopyOnWriteArrayList)뿐이라 크래시 시 호가창이 소멸한다. 매칭도 같은 durability가 필요하다.
- **왜(문제·동기)**: 두 엔진 다 같은 LMAX 패턴(입력 저널 + 리플레이)인데 매칭만 비어 있었다. ADR-025의 "각 엔진 = 로직 + 자기 입력 링 저널" 원칙을 매칭에 적용한다.
- **어떻게(대안·결정·트레이드오프)**: `AeronArchiveMatchingJournal`(저널 스트림 2005, never-drop offer, CLOSED/MAX_POSITION만 throw = fail-fast), `MatchingEngine.recover(Iterable<JournaledOrder>)`(no-op MatchListener로 체결 재발행 차단, requireNotStarted, PLACE/CANCEL), `MatchingJournalReplayer`(2006, 다중 녹화 시각 순), matching-worker의 `ArchivingMediaDriver` + `AeronArchive` + 빈 순서(과거 읽기 → startRecording → publication). 매칭이 계좌보다 단순한 세 가지가 이 ADR의 포인트다. ①새 코덱 불필요 — `core.codec.OrderCodec`이 이미 JournaledOrder를 직렬화한다(계좌는 5타입 + null 필드라 `AccountJournalEntryCodec`에 flags 바이트를 신규했다). ②seed 불필요 — 호가창은 주문 리플레이만으로 전부 재구성된다(계좌는 초기 잔고 seed). ③`blockUntilJournaled` 불필요 — 그건 계좌가 Kafka에서 체결·정산을 소비하고 ack하는 경로 때문이었다(ADR-027). 매칭 입력은 Aeron 주문 하나뿐이라 ack할 Kafka 소비가 없고, 저널 게이팅(`handleEventsWith(journal).then(business)`)만으로 "매칭 전 저널 먼저"가 보장된다. tradeId carry-over도 불필요 — 계좌 orderId는 엔진 안에서 발급(카운터 = 엔진 상태, ADR-023)이라 복구 시 이월이 필요했지만, 매칭 tradeId는 엣지(`AccountFillPublisher.onFill`, matching-worker)에서 publish 때만 발급된다. 복구는 출력 no-op이라 발급이 없다. 크래시 전 tradeId는 소비 완료됐고, 복구 후 새 체결은 새 tradeId를 받아 시간순으로 단조 증가해 충돌하지 않는다. 엣지 발급이 코어 결정론을 해치지 않는다.
- **무엇을(실제 변경·파일·커밋)**: `61ede26`(2c-1)·`1076b26`(2c-2) 리뷰 통과. books 맵을 `MatchingEventHandler` 주입으로 바꿔 라이브·복구 핸들러가 공유한다(계좌 accounts 맵 패턴).
- **결과·수치**: 매칭 크래시 → replay → 호가창 복구가 닫혔다. (정성적, 복구 시간·스냅샷 간격 트레이드오프는 ADR-030에서 미측정)

## 블로그 네타
### "같은 durability, 다른 난이도 — 매칭 저널이 계좌보다 쉬운 이유"
- **훅·핵심 주장**: 계좌와 매칭은 같은 LMAX 패턴(입력 저널 + 리플레이)을 쓰는데, 매칭 쪽이 훨씬 단순하다. 새 코덱·seed·`blockUntilJournaled`·tradeId carry-over가 다 불필요하다. 같은 패턴의 복잡도가 입력 다양성과 id 소유에서 나뉜다.
- **context**: ADR-025의 계좌 durability를 매칭에 그대로 미러한 작업(2c). 예상보다 단순했던 이유를 정리.
- **어떻게(서사·근거)**: ①새 코덱 불필요 — `OrderCodec` 재사용(계좌는 5타입 + null 필드라 flags 바이트 신규). ②seed 불필요 — 호가창은 주문 리플레이만으로 재구성(계좌는 초기 잔고 seed). ③`blockUntilJournaled` 불필요 — 매칭 입력은 Aeron 주문 하나뿐, ack할 Kafka 소비 없음(계좌는 체결·정산 Kafka 소비 때문). ④tradeId carry 불필요 — 매칭 tradeId는 엣지에서만 발급, 복구는 출력 no-op(계좌 orderId는 엔진 안 발급이라 이월 필요). 입력이 주문 하나 vs 계좌는 주문 + 체결 + 정산 셋 + 엔진 발급 id.
- **재료(커밋·도식·수치)**: `61ede26`(2c-1)·`1076b26`(2c-2). `AeronArchiveMatchingJournal`(스트림 2005), `MatchingJournalReplayer`(2006).
