---
feature: account-idempotency-cache-bound
date: 2026-09-15
branch: (미정 — 설계만, 구현 미착수)
commits: []
feeds: [adr, blog]
---

# 계좌 멱등 장부 3개의 무한증가(OOM) 막기

v2 계좌 엔진 `AccountState`는 각 계좌의 잔고·보유를 프로세스 메모리에 들고 있고(락 없는 single-writer = 속도 축), 여기에 "이미 처리한 일"을 기억하는 멱등 장부 3개가 붙어 있다 — `processedTradeIds`·`processedSettlementRefs`(둘 다 `Set<Long>`)·`requestIdToOrderId`(`Map<String,Long>`, `AccountState.java:40-42`). 셋 다 `add`/`put`만 있고 지우는 규칙이 없어 계좌 객체가 메모리에 있는 동안 무한 증가하고, 스냅샷에도 통째로 저장돼(`:74-76` restore, `:95-96` capture) 스냅샷 파일·직렬화 비용까지 부풀린다. 워커 세션이 찾은 v2 최고심각 릭. 설계 도식: `docs/_idempotency_cache_solutions.html`.

## ADR 네타

### 멱등 장부 상한은 '메모리 숫자'가 아니라 '정합성 창'이다 — 소스 성격별로 방식이 갈린다
- **context**: 무한 증가를 막으려면 "오래된 건 지우자"가 필요한데, 지운 id가 다시 도착하면 멱등이 뚫려 체결/정산이 이중 반영된다(돈). 그래서 "얼마나 기억할지"는 메모리 튜닝이 아니라 정합성 문제다.
- **왜(문제)**: 같은 레포 `OrderBook.java:63-75`의 LRU(5000)+TTL(5분)을 그대로 복붙하는 게 자연스러워 보이지만, 그건 "체결 완료 주문 보존"이라 의미가 다르다 — 근거 없는 매직넘버가 된다.
- **어떻게(대안·결정·트레이드오프)**: 각 장부의 ①중복 소스가 순서를 보장하나 ②값을 되돌려줘야 하나로 방식이 갈린다.
  - **`processedTradeIds` → 워터마크(long 하나)**. 근거: 체결은 Aeron 스트림 순서 보장 + 계좌가 "먼저 저널 기록 → 그다음 반영"(`AccountJournalEventHandler`, journal-before-apply)이라, 저널이 tradeId 기준 **빠짐없는 앞부분(prefix)** = 단조. 그래서 집합 없이 `max` 하나로 판정(`tradeId ≤ max` 무시, 크면 반영+갱신). 중복은 오직 **재기동 복구** 때 저널 replay ∩ fill replay 두 경로가 같은 체결을 겹쳐 실을 때뿐인데, 두 스트림은 position 공간이 달라 **position 워터마크로는 못 걸러내고 tradeId만 둘을 잇는다** → 그래서 tradeId 기반이 맞다. 라이브에선 순서 보장이라 같은 체결이 두 번 올 일이 없음. **무한 Set → long 하나, 스냅샷도 대폭 축소.**
  - **`processedSettlementRefs` → LRU 상한 집합**. 근거: 정산은 `accountId`를 Kafka 키로 발행해(`PendingSettlementSettler:52` `send(TOPIC, String.valueOf(accountId), event)`) 파티션 안 순서는 보장되고, `settlementRef`는 tradeId를 그대로 쓴다(`SettlementRequestPublisher:23`, 체결 하나당 정산 하나). **그런데도 워터마크는 불가** — 정산 워커가 T+2 만기분을 **스캔해서** 보내고 발송 실패 시 **재시도가 나중에** 껴서, 계좌가 받는 순서가 settlementRef 순서와 어긋날 수 있다(낮은 ref가 늦게). 그게 미처리 신규면 워터마크가 잘못 스킵 = 미수금 차감 누락(돈). → 순서를 못 믿으니 상한 집합. 정산은 미수금당 1회로 빈도가 낮아 N이 작아도 안전. **N=1000/계좌 제안**(레포 `OrderBook` LRU 패턴 미러, 메모리 계좌당 ~8KB).
  - **`requestIdToOrderId` → TTL**. 근거: 이건 "봤나"만 보는 집합이 아니라 **재전송 시 orderId를 되돌려주는 조회맵**(`:110` get, `:120` put)이라 워터마크(값 하나)로는 orderId를 못 돌려준다. 키도 클라 문자열이라 단조 아님. → `requestId → (orderId, 저장시각)` Map에 TTL. **TTL 값 = 클라 재전송 SLA**(우리 SLA 미정 → Stripe 선례 24h 제안, "어떤 현실적 재시도 예산보다 길다").
  - **off-heap(RocksDB/Redis) 기각**: 규모 문제가 아니라, v2가 계좌 상태를 인메모리 single-writer로 두는 게 속도 축인데 dedup만 외부 저장소로 빼면 핫패스에 외부 I/O를 도로 얹어 설계 철학과 충돌.
- **Kafka 이해 정정(설계 중 Jack이 잡음)**: at-least-once 중복은 브로커가 다시 "push"해서가 아니다 — Kafka는 pull(poll)이고, 컨슈머가 처리 후 **offset 커밋 전에 죽거나 리밸런스**되면 되살아나 마지막 커밋 지점부터 다시 poll해 재처리한다. 그래서 "라이브에서도 중복"이라던 내 첫 서술은 과장 — steady-state가 아니라 장애/리밸런스 때만.
- **무엇을(변경 예정)**: `AccountState` 세 필드 + `toSnapshot`/restore 생성자 + `AccountStateSnapshot`(record) + `AccountSnapshotCodec`(바이너리 직렬화). **스냅샷 포맷이 바뀌어 옛 스냅샷은 못 읽음** — 학습 프로젝트라 허용, 단 코덱 write/read 동시 수정 필수.
- **결과·수치**: 미구현. 예상 효과 — tradeId: `Set<Long>`(무한) → `long` 하나 / settlement: `Set`(무한) → ≤1000 / requestId: Map에 시각 추가 + TTL 만료로 상한.

## 블로그 네타

### "무한히 자라는 멱등 장부, 상한을 '몇'으로? — 소스가 답을 정한다"
- **훅·핵심 주장**: dedup 상태를 아무 숫자로 자르면 돈이 틀어진다(정합성 창). 업계는 무한 집합을 안 쓴다 — 순서가 있으면 워터마크로 집합을 아예 없애고, 없으면 TTL/LRU로 시간·개수를 재전송 창에 맞춰 자른다. 한 시스템 안에서도 소스마다 답이 달라진다.
- **context**: 계좌를 accountId로 샤딩해 상태를 인메모리로 들면(속도 축) 멱등 장부가 무한 증가하는 릭이 따라온다. 3개 장부가 각각 다른 소스(Aeron 체결 / Kafka 정산 / 클라 요청)라 답이 셋으로 갈린다.
- **어떻게(서사·근거)**: "먼저 저널 기록 → 그다음 반영" 구조가 저널을 tradeId의 깨끗한 앞부분으로 만들어 워터마크를 가능케 한다(집합 소멸). 정산은 같은 tradeId를 멱등키로 쓰고 accountId로 파티션 순서까지 보장되지만, 만기 스캔·재시도가 도착 순서를 흩뜨려 워터마크가 안 된다 — 순서 보장과 "단조 도착"은 다르다는 게 핵심. requestId는 값을 되돌려주는 조회맵이라 dedup 워터마크로 못 접는다. Kafka 중복이 push가 아니라 offset 커밋 지연(pull 재소비)이라는 정정도 서사에 좋음.
- **재료(커밋·도식·수치)**: `AccountState.java:40-42`(장부 3개)·`157/227/259`(멱등 판정)·`74-76/95-96`(스냅샷) / `AccountJournalEventHandler`(journal-before-apply) / `PendingSettlementSettler:52`(accountId 키)·`SettlementRequestPublisher:23`(settlementRef=tradeId) / 대조군 `OrderBook.java:63-75` / 업계 Kafka Idempotent Producer·Stripe 멱등키 24h·Flink state TTL / 도식 `docs/_idempotency_cache_solutions.html`. 수치는 구현 후 실측.
