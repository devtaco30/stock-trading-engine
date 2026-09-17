---
feature: order-result-record-track
date: 2026-09-17
branch: feat/order-result-record
commits: [93a7c72, f9dfef8, abecb8d, c23bc23]
feeds: [adr, blog]
---

# 주문 결과 기록 트랙 (U1~U3-b)

## ADR 네타

### "결과 스트림을 저널과 분리한 이유"

- **context(무슨 상황)**: v2는 주문을 받으면 202를 돌려주고 계좌 워커가 뒤에서 검증·예약을
  처리한다. `AccountResultListener` 구현체는 로깅·지표·정산 요청 발행·잔고 발행 넷인데, 그중
  누구도 판정(수락·거부·중복)을 클라이언트가 볼 수 있는 곳으로 보내지 않는다.
- **왜(문제·동기)**: 계좌 워커가 거부해도 그 사실이 로그와 지표에만 남는다. 클라이언트는
  202를 받은 뒤 자기 주문이 실제로 어떻게 됐는지 알 방법이 없다.
- **어떻게(대안·결정·트레이드오프)**:
  - 대안 A(기각) — 저널(4005)에 판정도 같이 싣는다. 기각 이유: 저널은 계좌 상태 복구의 유일한
    근거라 포맷을 건드리면 리플레이 신뢰가 깨진다. 목적도 다르다(저널=복구용 내부 기록,
    이 트랙=클라이언트 알림용 외부 공개 기록).
  - 채택 — 별도 Aeron Archive 스트림(core `AeronStreamIds.ORDER_RESULT`=4007)에 판정만 담아
    기록하고, 별도 스레드가 Kafka `order-results` 토픽으로 옮긴다.
  - 저장 순서는 "Archive에 먼저 쓰고 별도 스레드가 Kafka로 옮긴다"로 확정(Kafka 브로커가
    느릴 때 주문 접수가 멈추면 안 된다는 게 이 트랙의 1번 결정 — ADR-020·033의 "Kafka를
    핫패스 밖에 둔다" 전제를 그대로 따름).
- **무엇을(실제 변경·파일·커밋)**:
  - U1(93a7c72) — `core/codec/OrderVerdict`·`OrderResultEntry`·`OrderResultCodec`,
    `AeronStreamIds.ORDER_RESULT=4007`, `KafkaTopics.orderResults()`. `RejectReason`(account-disruptor)을
    core가 참조 못 해(의존 순서: core → account-disruptor) enum name() 문자열로 싣는다.
  - U2(f9dfef8) — `account-worker/messaging/OrderResultRecorder`(`AccountResultListener` 구현,
    `onAccepted`·`onSellAccepted`·`onRejected`·`onDuplicateRequest`만 담음),
    `account-worker/config/OrderResultArchiveConfig`(recording+`ExclusivePublication` 배선).
    offer 실패 정책은 `AeronArchiveAccountJournal`(저널)과 동일 — 성공(≥0)할 때까지
    `BackoffIdleStrategy`로 재시도, `CLOSED`·`MAX_POSITION_EXCEEDED`면 예외를 던져 계좌 워커
    소비자를 fail-fast로 멈춘다. 부수 발견 — 새 `ExclusivePublication` 빈 때문에 기존 통합테스트
    (`AccountJournalReplayerPoisonIntegrationTest`)의 타입 기반 `getBean(ExclusivePublication.class)`
    조회가 모호해짐 → 그 테스트 줄만 빈 이름 지정으로 수정(프로덕션 배선은 안 건드림).
  - U3-a(abecb8d) — 원안(같은 스트림에 라이브 Subscription을 하나 더 열기)을 재현으로 기각.
  - U3-b(c23bc23) — `OrderResultCatchUpReplayer`(`AccountJournalReplayer.readFrom`과 같은 구조),
    `OrderResultForwardPositionStore`(원자적 파일 쓰기, `AccountSnapshotStore`와 같은 결).
    기동 순서: 캐치업 읽기 → 이번 실행 recording 시작 → 발행 스트림·recordingId 조회 → 캐치업분
    Kafka 발행 + position을 (recordingId, 0)으로 마킹 → 라이브 replay 시작.
- **결과·수치**:
  - **재현 1(느린 구독자 → offer 막힘)**: `aeron:ipc?term-length=65536` 채널에 발행자 하나 +
    구독자 둘(하나는 계속 poll, 하나는 전혀 poll 안 함), 128바이트 메시지를 반복 offer →
    **205번째 offer에서 `BACK_PRESSURED(-2)`**. 재현 코드 전문은
    `_investigation_order_result_backpressure.md`(워크트리 루트, git 미커밋).
  - **재현 2(진행 중인 recording도 라이브로 따라감)**: `AeronArchive` 1.48.0 소스 javadoc —
    `replay(recordingId, position, length, ...)`의 length에 `Long.MAX_VALUE`를 주면 "follow a
    live recording". `ArchivingMediaDriver`로 실 Archive를 띄우고 recording을 멈추지 않은 채로
    `replay(recordingId, 0, Long.MAX_VALUE, ...)`를 열어 ①이미 기록된 메시지 1건 수신 ②그
    상태로 추가 발행한 메시지 2번째도 같은 replay subscription으로 도착 — 확인 완료.
  - **재시작 중복 창**: position을 100건마다 저장(`OrderResultForwarder.POSITION_SAVE_INTERVAL`)
    → 재시작 시 최대 **99건**이 중복 발행될 수 있다(진짜 중복, 유실 아님). 주기를 줄이면 창은
    작아지고 디스크 쓰기(fsync 포함)는 늘어난다 — 트레이드오프를 클래스 주석에 수치로 남김.

### "판정 캐치업을 저널 복구와 같은 구조로 만든 이유"

- **context**: U3-a는 "이번 실행에서 시작한 recording"만 봐서, 재시작하면 이전 실행에 남은
  미전송 판정이 영영 Kafka로 안 간다(유실 — d0가 처음엔 "중복"으로 잘못 짚었다가 정정, 근거는
  `AccountJournalReplayer`가 이전 recording을 전부 이어 읽는 것과 같은 구조의 문제라는 점).
- **왜**: recording은 프로세스 실행 1회당 1개 생긴다. Archive replay는 recordingId 하나만
  본다. 그래서 재시작 넘어 이어보려면 "어느 recording의 어디까지 봤나"를 저장해야 한다.
- **어떻게**: `AccountJournalReplayer.readFrom`을 그대로 본떴다 — 저장된 recordingId보다 먼저
  시작한 recording은 건너뛰고, 그 recordingId는 저장된 position부터, 그 뒤 recording은 전부
  처음부터 읽는다. 아직 stop 안 된(진행 중인) recording은 `stopPosition()`이 미확정(NULL)이라
  길이 계산이 0 이하가 돼 자동으로 제외된다 — 저널 리플레이어와 같은 성질을 그대로 재사용.
- **무엇을**: U3-b(c23bc23) 전체.
- **결과**: `OrderResultCatchUpReplayerTest` 2건 — 진행 중인 recording 제외, 저장된 position
  이전 엔트리 skip + 다음 recording까지 이어 읽기. 둘 다 실 `ArchivingMediaDriver`로 검증(PASSED).

## 블로그 네타

### "느린 소비자가 프로듀서를 막는다 — Aeron IPC에서 본 백프레셔"

- **훅·핵심 주장**: 큐 기반 시스템에서 익숙한 "소비자가 느리면 큐만 쌓인다"는 감각이 Aeron
  IPC에서는 안 통한다. 로그 버퍼가 공유 메모리 하나라, 느린 구독자 하나가 발행 자체를 막는다.
  205번째 메시지에서 막히는 걸 직접 재현해 보기 전까진 "그냥 라이브 구독 하나 더 열면 되지
  않나"라고 생각했다.
- **context**: 계좌 워커가 주문 판정을 클라이언트에 알리려고 결과를 Kafka로 옮기는 트랙. Kafka가
  느려지면 그 지연이 계좌 엔진(주문 접수 자체)까지 전파되면 안 된다는 게 전제였는데, 처음
  설계(라이브 구독)는 그 전제를 몰래 어기고 있었다.
- **어떻게(서사·근거)**: 조정 세션(d0)이 "Aeron은 가장 느린 구독자에 맞춰 발행을 제한한다"고
  지적 → 확신 없이 넘기지 않고 재현 테스트로 직접 확인(205번째 offer에서 BACK_PRESSURED) →
  대안(Archive replay, length=Long.MAX_VALUE로 진행 중인 recording도 라이브 추종)도 공식
  javadoc과 재현으로 먼저 확인한 뒤에야 설계를 바꿈. 재시작 시 유실(중복 아님) 문제를 다시
  d0가 잡아, 저널 리플레이어와 같은 구조로 캐치업을 만들어 닫음.
- **재료(커밋·도식·수치)**: `_investigation_order_result_backpressure.md`(재현 코드 전문),
  커밋 abecb8d(설계 전환)·c23bc23(캐치업), 수치(205번째 BACK_PRESSURED, 재시작 중복 창 99건).
