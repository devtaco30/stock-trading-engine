---
feature: snapshot
date: 2026-09-12
branch: feat/disruptor-matching-core
commits: [0d98058, 447e902, 4cd85f8]
feeds: [adr, blog]
---

# 자체 스냅샷 구현 — 종료 시 상태 blob + 저널 position 경계

## ADR 네타
### ADR-030 자체 스냅샷 구현 (ADR-019 "자체 스냅샷"의 실제 HOW)
- **context(무슨 상황)**: ADR-019가 "스냅샷은 Archive가 안 줘서 자체 구현한다"까지만 정했고 HOW(언제 찍나·어디 저장·복구 연동)는 미정이었다. 저널이 쌓일수록 복구가 0부터 전부 replay라 느려진다 → 스냅샷으로 "마지막 스냅샷부터"로 bound한다. 매칭은 완료, 계좌는 진행 중.
- **왜(문제·동기)**: 복구 시간이 저널 길이에 비례한다. 주기적으로 상태를 통째로 찍고 그 시점의 저널 position을 같이 저장하면, 복구 = 스냅샷 로드 후 그 position부터만 replay가 된다.
- **어떻게(대안·결정·트레이드오프)**: 결정점 세 개. ①트리거: (a)주기(N건/N초) (b)graceful shutdown만 (c)수동 → (b) graceful shutdown 먼저. drain 후 quiescent(single-writer 스레드가 안 돌 때)에만 books를 안전하게 read할 수 있어서다. 주기/라이브는 링 이벤트로 컨슈머 스레드에서 하는 나중 리파인. "메커니즘 먼저, 정책 나중"(a2 스케줄러와 같은 원칙). ②저장: (a)파일 (b)Aeron 스냅샷 스트림 → (a) 파일. 스냅샷은 이벤트 스트림이 아니라 전체 상태 blob 하나라, archive-dir(= PV)에 `*.dat`로 원자적 temp + rename, 최신 1개만 유지. ③코덱 위치: 엔진-private 모듈(`MatchingSnapshotCodec` → matching-disruptor, `AccountSnapshotCodec` → account-disruptor, core.codec 아님). 근거: OrderCodec/AccountJournalEntryCodec은 account↔matching/디스크 와이어 계약이라 core.codec이지만, 스냅샷 포맷은 한 엔진 안에서만 쓰는 파일 포맷이다.
- **무엇을(실제 변경·파일·커밋)**: 엔진에 `snapshot()`(상태 + `journal.position()`)·`restore()`(start 전). `Journal.position()` 추가(Archive는 publication.position, InMemory는 0). 별도 `*SnapshotLifecycle`(엔진보다 낮은 phase = 엔진 drain 후 마지막에 stop, recordingId는 카탈로그 최신 startTimestamp). `*JournalReplayer.readFrom(recordingId, position)` = 그 recording 이전은 skip, 그것은 position부터, 이후는 전부(length ≤ 0이면 skip). 빈 순서: 스냅샷 파일 읽기(순수 I/O) → recoveredEntries(readFrom/readAll) → startRecording. 엔진은 (seed →)restore → recover(delta) → start. 멱등 캐시·카운터를 스냅샷에 포함한다 — 매칭은 `filledOrderTimestamps`(전량 체결 멱등 TTL 캐시), 계좌는 `processedTradeIds`·`processedSettlementRefs`·`requestIdToOrderId`. 빼면 복구 후 TTL 내 재전송이 신규로 오인돼 중복 처리된다(correctness). 계좌는 추가로 orderId 카운터(`AccountOrderIdGenerator.counter` = 엔진 상태)를 담아 복구 후 이어 발급 = 결정론 유지(ADR-023과 맞물림). graceful shutdown 복구 경로: 다음 run에서 readFrom이 스냅샷 position ≈ 녹화 끝이라 그 recording을 skip + 이후 녹화 없음 → recover(empty) → 상태 = 스냅샷만 = 빠른 복구. 크래시(ungraceful)면 스냅샷을 못 찍어 직전 스냅샷 + 그 뒤 저널 replay로 수용한다.
- **결과·수치**: 매칭 완료 `0d98058`(2d-1a 직렬화·복원)·`447e902`(2d-1b 파일 저장 + replay-from-position). 계좌 진행 `4cd85f8`(2d-2a, 리뷰 중) → 2d-2b(복구 배선). 워커 편차(다 타당): `filledTimestamps` 종목별 중첩(내 LLD의 전역 맵 오류를 교정), RestingOrder stockCode 제외(BookSnapshot 키 중복), SnapshotLifecycle 분리(경량 테스트에서 Archive 목킹 회피), position 직접 검증 테스트(멱등이라 최종 상태만으론 0부터 읽고 수렴했는지 구분 못 함 → 리플레이어를 직접 호출해 반환 엔트리로 검증). 미측정 — ADR-019 열린 리스크 그대로: 복구 시간 vs 스냅샷 간격. graceful-only라 크래시 복구는 여전히 마지막 스냅샷 이후 저널 전체 replay.

## 블로그 네타
### "멱등이 테스트를 속인다 — '최종 상태가 맞다'는 복구 검증이 아니다"
- **훅·핵심 주장**: 매칭·계좌가 다 멱등(중복 무시)이라, 스냅샷 position부터 읽든 0부터 읽든 최종 상태는 같게 수렴한다. 그래서 end-to-end 상태 단언은 readFrom 버그를 못 잡는다. 리플레이어를 직접 불러 반환 엔트리로 검증해야 한다.
- **context**: 스냅샷 + 저널 delta 복구의 테스트 설계. 워커가 스스로 간파한 false-pass 함정.
- **어떻게(서사·근거)**: 멱등이라 position 이전을 잘못 읽어도 최종 상태가 수렴 → 상태 단언 통과 = false pass. 리플레이어를 직접 호출해 반환 엔트리(orderId 리스트)로 "position 이전이 실제로 빠지는지" 검증. ⭐테스트 설계 포인트.
- **재료(커밋·도식·수치)**: ADR-030. position 직접 검증 테스트.

### "스냅샷은 왜 종료할 때만 찍나 — single-writer를 깨지 않고 상태를 읽는 법"
- **훅·핵심 주장**: running 엔진을 다른 스레드에서 스냅샷하면 single-writer가 붕괴한다. graceful shutdown(drain 후 quiescent) 또는 링 이벤트(컨슈머 스레드)만 안전하다. phase 순서로 "엔진 먼저 멈추고 스냅샷 마지막".
- **context**: 스냅샷 트리거 결정(ADR-030). (a)주기 (b)graceful만 (c)수동 중 (b) 먼저.
- **어떻게(서사·근거)**: 다른 스레드에서 books read = single-writer 붕괴. drain 후 quiescent에만 안전 read. 주기/라이브는 링 이벤트(컨슈머 스레드)로 나중에. "메커니즘 먼저, 정책 나중". SnapshotLifecycle의 낮은 phase.
- **재료(커밋·도식·수치)**: ADR-030 트리거 결정.

### "스냅샷 + 저널 델타 — 다중 recording에서 position부터 읽기"
- **훅·핵심 주장**: 재시작마다 새 recording이 생겨 저널이 여러 recording에 걸친다. 스냅샷 position은 특정 recording 안의 위치다. 복구는 그 recording을 position부터, 이후는 전부 읽는다. 파일 스냅샷이 {recordingId, position}을 들고 Archive replay 경계를 가리킨다.
- **context**: 스냅샷 복구의 하드 파트(ADR-030). 다중 recording 경계 처리.
- **어떻게(서사·근거)**: 재시작 = 새 recording → 저널이 여러 recording에 걸침. 스냅샷 position은 특정 recording 안 위치. `readFrom(recordingId, position)` = 그 recording 이전 skip / 그것은 position부터 / 이후 전부(length ≤ 0 skip). 파일 스냅샷이 경계 포인터.
- **재료(커밋·도식·수치)**: `447e902`(2d-1b replay-from-position). `*JournalReplayer.readFrom(recordingId, position)`.

### "코덱을 어디 두나 — 공유 와이어 계약 vs 엔진-private 파일 포맷"
- **훅·핵심 주장**: 같은 "직렬화"라도 경계가 다르다. OrderCodec은 account↔matching 와이어라 core.codec에, 스냅샷 코덱은 한 엔진 안 파일 포맷이라 엔진 모듈에 둔다.
- **context**: 스냅샷 코덱 위치 결정(ADR-030 ③). core.codec에 둘지 엔진 모듈에 둘지.
- **어떻게(서사·근거)**: OrderCodec/AccountJournalEntryCodec = account↔matching/디스크 와이어 계약 → core.codec. 스냅샷 포맷 = 한 엔진 안에서만 쓰는 파일 포맷 → 엔진-private 모듈(`MatchingSnapshotCodec` → matching-disruptor, `AccountSnapshotCodec` → account-disruptor). 직렬화의 공유 범위가 모듈 위치를 정한다.
- **재료(커밋·도식·수치)**: ADR-029/030.
