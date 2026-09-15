---
feature: account-state-projection-impl
date: 2026-09-15
branch: feat/account-state-projection
commits: [139b8b2, 2de9235, 72e189d, 5c3e722]
feeds: [adr, blog]
---

# 계좌 상태 프로젝션 — 구현 (ADR-021 설계의 실구현)

2026-09-08 `account-state-persistence.md`(ADR-021 네타)에서 설계만 한 "full-state → 프로젝션 컨슈머 → DB read model"을 이 세션에 실제로 구현했다. fork1 3b(로컬 3-JVM 라이브 왕복)를 설계하다가, 체결 결과를 관찰할 방법이 account-worker 로그뿐이고 그게 사실 이 프로젝션 미구현을 우회한 것임을 Jack이 짚어 트랙을 앞당겼다.

## ADR 네타

### ADR: 계좌 상태 프로젝션 구현 — full-state 발행 + 별도 컨슈머 + 배치 coalesce
- **context(무슨 상황)**: v2 계좌 엔진(account-worker)은 잔고·보유를 인메모리 single-writer로만 들고 있다. 체결이 반영돼도 DB에 안 남아 유저 조회도, durable read model도 없었다. ADR-021이 설계한 CQRS read-model 프로젝션을 실제로 붙이는 자리.
- **왜(문제·동기)**: fork1 3b에서 "체결이 됐는지"를 확인할 수단이 로그 grep뿐이었다. 그리고 인메모리 상태가 조회 가능한 형태로 어디에도 없었다. Jack: "체결 완료되고 비동기로 인메모리 스냅샷을 DB로 업데이트 치면서 영속화 시켜둬야 되는 거 아니냐."
- **어떻게(대안·결정·트레이드오프)**:
  - **발행 전송 = Kafka off-path**. 하이브리드 원칙(핫패스=Aeron, off-path=Kafka)대로. read model은 eventual consistency라 Kafka ms 지연 무방. 기존 `SettlementRequestPublisher` 패턴 미러.
  - **★핵심 갈림 = write-behind 스레드(같은 엔진 프로세스에서 DB 쓰기) vs 별도 프로젝션 컨슈머(별도 프로세스)**. account-state-persistence.md가 이 둘을 대안 i/ii로 대비하고 ii를 택했으나 근거("LMAX: I/O를 로직 스레드에서 분리")는 둘 다 만족해서 사실 구분을 못 했다. Jack이 "엔진 서버 안 DB 전용 스레드로 하면 안 되냐, 그 원칙 근거가 뭐냐, 비용은 생각했냐"로 재검토를 요구. **결정 기준 = 상황 가정**: v2는 목표 LMAX+분산 CQRS 아키텍처를 실제 모양대로 만드는 학습(가정 A)이므로, 엔진(인메모리 authority)/프로젝션(별도)의 CQRS 분리 자체가 학습 대상. write-behind는 그 분리를 한 프로세스로 도로 합치는 지름길이라 A 가정선 기각. JVM 하나 더 드는 비용은 학습 프로젝트에선 결정 축이 아니다.
  - **seq = 계좌별 단조 카운터**. single-writer라 순차 발급. 상태 실변경마다 +1(거부·멱등무시 제외). stale-guard 키. 스냅샷+디스크 코덱에 영속해 재기동 후 재현.
  - **full-state(delta 아님)**: 비동기 순서 뒤집힘/유실에 delta는 돈이 새거나 무한대기, full-state+seq는 자가치유(ADR-021 163/161 시나리오).
  - **DB 부하 = 컨슈머 배치 coalesce로**. per-event 발행은 단순하게 두고(Kafka는 append 로그라 쌈), 컨슈머가 poll 배치를 accountId별 최신 seq 하나로 합쳐 계좌당 upsert 1회. DB 쓰기가 체결 수가 아니라 "배치 속 계좌 수"에 비례. Kafka가 발행 빈도↔DB 쓰기 빈도를 분리하는 완충.
  - **read model 테이블 = v2 전용 신설**. v1 `accounts`/`holdings`는 v1 정산·입출금이 write 중이라 동시 write 충돌 + `holdings.avgPrice` 원천이 v2엔 없음. 그래서 별도 `account_projection`.
- **무엇을(실제 변경·파일·커밋)**:
  - U1 `139b8b2`: `AccountState.seq` + `AccountStateSnapshot`·`AccountSnapshotCodec`(바이너리 디스크 코덱)에 seq 영속. 코덱까지 안 고치면 재기동 후 seq=0 리셋되는 걸 잡음.
  - U2 `2de9235`: `AccountResultListener.onStateChanged` default 콜백, `AccountEventHandler`가 apply 실반영 시 엔진 스레드에서 캡처→`AccountStatePublisher`(SPSC 큐+전용 스레드, `AeronMatchingOrderSender` 미러)→Kafka `account-state`. `AccountStateEvent`=core/kafka/event.
  - U3-i `72e189d`: 신규 `account-projection-worker` 모듈, `account_projection`(PK=accountId)+`account_projection_holding`(@IdClass 복합키), `AccountProjectionUpserter`(read-then-conditional `applyIfNewer` stale-guard, 보유 전체교체, @Transactional 커밋 후 ack).
  - U3-ii `5c3e722`: 컨슈머 배치 리스너 + `upsertBatch`(accountId별 max-seq만 남겨 upsert). self-invocation 프록시 함정 회피 위해 공통 `applyOne` non-transactional 추출.
- **결과·수치**: **미측정**(부하 테스트 안 함). e2e(임베디드 Kafka+H2, 레포 최초)로 U2 발행→U3 역직렬화→upsert / stale skip / 재도착 멱등 / 배치 수렴 검증 — 첫 실행 통과. U4(조회 API) 미구현(보류).

## 블로그 네타

### "락 없는 엔진의 상태를 DB로 — write-behind 스레드 대신 별도 컨슈머를 고른 진짜 이유"
- **훅·핵심 주장**: 같은 결과를 내는 두 방법(엔진 프로세스 안 DB 스레드 vs 별도 프로젝션 프로세스) 사이에서 선택을 가르는 건 비용이 아니라 '상황 가정'이다. 가정을 안 박고 비용부터 저울질하면 틀린 축으로 결정한다.
- **context**: single-writer 계좌 엔진의 인메모리 상태를 DB read model로 영속·조회.
- **어떻게(서사·근거)**: 처음엔 "JVM 하나 더 드는 비용" vs "격리·CQRS" 로 저울질. Jack이 "모든 건 상황 가정이 뭐냐에 따라 거기 맞는 선택을 했느냐"로 되돌림. 두 방법 다 LMAX I/O 분리는 만족 → 구분점은 CQRS 분리(엔진=authority/프로젝션=별도)를 유지하느냐. 가정 A(목표 아키텍처를 실제로 만드는 학습)에선 그 분리가 목적 자체라 별도 컨슈머. 곁들여: "account-worker는 DB-free"를 ADR-018 근거로 인용했다가, 원문을 열어보니 ADR-018은 호스트 프레임워크(Spring vs Quarkus) 결정이지 DB-free 조항이 없던 것 — 근거는 확인하고 인용해야 한다는 서사.
- **재료(커밋·도식·수치)**: 커밋 4개, LLD `docs/_account_projection_lld.md`, [[v2 계좌 상태 영속 모델]]. 수치 미측정.

### "full-state + seq — 순서를 포기하고 자가치유를 얻다, 그리고 DB 부하는 소비에서 잡는다"
- **훅·핵심 주장**: 비동기로 상태를 반영할 때 delta는 순서가 뒤집히면 돈이 새고, full-state+seq는 최신만 이기게 두면 자가치유한다. 그리고 per-event 발행의 DB 부하는 발행을 줄여서가 아니라 컨슈머가 배치로 합쳐서 잡는다.
- **context**: account-worker → Kafka `account-state` → 프로젝션 컨슈머 → `account_projection` upsert.
- **어떻게(서사·근거)**: seq stale-guard(`incoming > stored`만 반영, 신규계좌는 seq=0 빈 엔티티로 같은 경로). 보유는 delta 아닌 전체교체(매도로 사라진 종목이 옛 값으로 안 남게). "체결마다 DB 쓰면 부하 크지 않냐"(Jack)에 대한 답 = Kafka가 완충, 컨슈머가 poll 배치를 accountId별 max-seq로 합쳐 계좌당 upsert 1회. DB 쓰기 ∝ 배치 속 계좌 수.
- **재료(커밋·도식·수치)**: 커밋 2de9235·72e189d·5c3e722. seq 재기동 재현 테스트(mid-stream 스냅샷+저널 replay). 배치 coalesce mock 테스트(save 1회·max seq).
