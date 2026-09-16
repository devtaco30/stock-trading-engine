---
feature: account-fill-multi-recording-replay
date: 2026-09-16
branch: fix/account-fill-replay-multi-source
commits: [4fa568d, 7f6a4a8]
feeds: [adr, blog]
---

# 매칭 프로세스가 둘 이상일 때 계좌 재기동 복구에서 체결이 빠지는 문제 (I1 U1~U3)

전 단계(U0, 체결 반영이 적용 순서와 무관함을 검증)는 `decision_records/account-fill-order-independence.md`. 이 문서는 그 전제를 딛고 실제 수정(U1~U3)을 다룬다.

## ADR 네타

### ① 문제의 정체 — Archive는 연결마다 recording을 따로 만든다

- **context(무슨 상황)**: 계좌 워커는 매칭이 낸 체결을 받는 즉시 자기 Aeron Archive에
  REMOTE로 녹화한다(`AccountFillIntakeConfig`, `startRecording(fillChannel, FILL,
  SourceLocation.REMOTE)`). 재기동 시 "라이브로 못 받은 체결"을 이 녹화에서 되읽어
  복구한다(`AccountFillReplayer`).

- **왜(문제·동기)**: Aeron Archive는 recording을 채널·스트림 단위가 아니라 **연결(세션)
  단위**로 만든다 — 매칭 프로세스가 둘 이상이거나, 하나뿐인 매칭이 재시작해 새 연결로 다시
  붙어도 recording이 늘어난다. 그런데 `AccountFillReplayer.findRecording()`은
  `listRecordingsForUri`로 찾은 recording 중 **마지막 하나만**(`found.get(found.size()-1)`)
  읽었다. 매칭이 하나뿐이고 재시작을 한 번도 안 하면 recording도 하나뿐이라 이 문제가
  드러나지 않았을 뿐이다 — 매칭이 둘이 되거나 한 번이라도 재시작하면, 나머지 recording에
  디스크로 durable하게 남아 있는 체결이 조용히 버려져 잔고·보유 수량이 틀어진다(돈).

- **어떻게 실측했나**: `AccountFillMultiSessionRecordingIntegrationTest`(이 트랙보다 먼저
  존재하던 "미확인 전제" 실측 테스트, 1-5)가 udp로 별도 `MediaDriver` 두 개에서 발행하면
  recording이 실제로 2개 생기는 것을 이미 확인해뒀다. 같은 Aeron 클라이언트에서
  `aeron:ipc`로 두 번 발행하면 세션을 참조카운트로 공유해 sessionId가 같아지는 거짓양성을
  먼저 겪고서 udp+별도 드라이버로 바꾼 경위도 그 테스트에 남아 있다. 이번 U3 테스트도 같은
  전제(별도 `MediaDriver`)를 그대로 썼다.

### ② 왜 replayer 하나만 고쳐서 안 됐나 — "어디까지 읽었나"를 값 하나로 들고 있는 자리가 다섯 곳

| 자리 | 고치기 전 | 파일 |
|---|---|---|
| 수신기가 보는 image | `subscription.imageAtIndex(0)` 하나만 | `AccountFillReceiver.updateConsumedPosition` |
| 엔진이 기억하는 위치 | `private volatile long lastAppliedFillPosition` | `AccountEventHandler` |
| 스냅샷 싱크 계약 | `offer(byte[], long fillPosition, long appliedSeq)` | `AccountSnapshotSink` |
| 스냅샷 파일 | `[recordingId:8][fillConsumedPosition:8][스냅샷…]` | `AccountSnapshotStore` |
| **graceful stop 스냅샷 경로** | `fillReceiver.consumedPosition()`을 그대로 씀 | `AccountSnapshotLifecycle.stop()` |
| 되읽기 | `readFrom(channel, streamId, fromPosition)` → recording 하나 | `AccountFillReplayer` |

다섯 번째 자리(`AccountSnapshotLifecycle`)는 처음 LLD를 쓸 때는 "수신기가 보는 image"
한 줄로만 다뤄졌는데, 실제로는 그 값이 **두 개의 서로 다른 스냅샷 경로**(러닝 중 스냅샷은
엔진이 반영한 위치를 쓰고, graceful stop 스냅샷은 수신기가 도착시킨 위치를 씀)에 각각
다르게 쓰이고 있었다 — 코드를 실제로 읽지 않고 LLD 표만 보고 진행했다면 U1에서
`AccountFillReceiver.updateConsumedPosition()`만 맵으로 바꾸고 이 경로를 놓쳤을 것이다.
이 지점은 인수인계 세션(a4)이 다음 세션에 넘기는 메모를 쓰며, 그리고 이 세션이 코드를
읽으며 각자 독립적으로 찾아 조정 세션에 보고했다.

수신기가 도착시킨 위치(아직 처리 안 된 것 포함할 수 있음)와 엔진이 반영한 위치(실제로
처리된 것만) 중 무엇을 graceful stop 스냅샷의 기준으로 쓸지도 결정이 필요했다 — graceful
stop은 링을 비우고(drain) 멈추므로 이론상 둘이 같아지지만, **기준은 항상 반영한 쪽**으로
잡았다(사고 시 두 값이 어긋나는 경로가 생기면 반영 안 된 체결을 반영됐다고 스냅샷에 담는
쪽이 더 위험하다). 결과적으로 `AccountFillReceiver.consumedPosition()`을 쓰는 곳이 하나도
안 남아 그 메서드 자체와, 그 메서드만 검증하던 `AccountFillReceiverPositionIntegrationTest`를
삭제했다.

### ③ 결정 셋과 근거

**D1. 위치를 Aeron `sessionId`(int)로 키잉**. recordingId가 아니라 sessionId를 쓴 이유는
수신 시점(`header.sessionId()`)에 바로 얻을 수 있는 값이기 때문이다 — recordingId는 그
자리에서 모르는 값이라 조회(카탈로그 매핑)가 하나 더 필요해진다. Archive 카탈로그도
`listRecordingsForUri` 콜백 인자에 sessionId를 주므로, 복구 때 "이 sessionId의 recording"을
찾아 시작 위치와 맞출 수 있다.

**D2. 스냅샷 파일에 형식 번호를 두고, 다르면 예외로 기동을 멈춘다** — `AccountSnapshotFormatException`.
"못 읽었으니 스냅샷이 없는 셈 치고 저널을 전부 재생한다"로 조용히 넘기지 않는다. 이유:
파일에는 잔고·보유·예약이 그대로 들어 있는데 못 읽는 것이고, 조용히 넘기면 복구가 왜
오래 걸렸는지(전체 저널 재생) 아무도 모른 채 지나간다. 예외 메시지에 파일 경로·읽은 버전·
기대 버전을 넣어 사람이 판단하게 한다. 새 레이아웃:
`[version:1=2][journalRecordingId:8][fillSourceCount:4][(sessionId:4)(position:8)]×N[스냅샷바이트]`.
구 형식(버전 필드 자체가 없음)을 새 리더로 읽으면 첫 바이트가 옛 recordingId의 최상위
바이트로 읽혀 2와 다르게 나온다 — 별도 마이그레이션 코드 없이 그 자체로 감지된다.

**D3. 여러 recording을 읽는 순서는 복원하지 않는다** — U0에서 미리 검증(`AccountFillOrderIndependenceTest`).
체결 반영이 tradeId 멱등 + 그 순간의 남은 예약액 기준 차감(텔레스코핑)이라 어떤 순서로
적용해도 최종 상태가 같다. 이 전제가 없었다면 recording 여러 개를 "어느 순서로 읽어야
하는가"가 별도 설계 과제가 됐을 것이다.

### ④ 조용히 넘어가는 코드를 두 번 만들었다 지웠다

이 트랙에서 "값을 하나로 뭉개서 조용히 넘기지 않는다"는 원칙(D2)을 두 번 실제로 적용했다.

1. **스냅샷 형식(D2 본체)** — 위에서 설명한 버전 예외.
2. **U1+U2와 U3 사이의 임시 다리** — U1+U2 커밋에서 `StoredAccountSnapshot.fillConsumedPosition()`
   타입이 `long`→`Map<Integer,Long>`으로 바뀌면서, U3가 아직 안 끝난 상태에서
   `AccountFillIntakeConfig.accountFillReplayedEntries`(재기동 시 놓친 체결 replay, U4b)의
   호출부가 깨졌다. `AccountFillReplayer.readFrom`은 U3 전까지 여전히 recording 하나만
   읽었으므로, 맵에서 값 하나를 뽑아 넘기는 다리를 놓았다. 처음엔 그 추출을 주석 하나로만
   남겼는데, 조정 세션이 "맵 항목이 둘 이상이면 조용히 하나를 버리는 게 지금 고치는 버그와
   같은 모양"이라고 지적해 **항목이 2개 이상이면 WARNING 로그**(발행자 수·전체 맵 내용
   포함)를 추가했다. U3에서 `readFrom`이 맵을 직접 받게 되면서 이 다리·로그 전부 삭제.

## 검증

- **단위·통합 회귀**: `./gradlew :account-disruptor:test :account-worker:test --rerun-tasks`
  — account-disruptor 108개, account-worker 44개(U3 신규 통합테스트 1개 포함) 전부
  PASSED(failures=0, errors=0, skipped=0). account-worker 쪽에는 실제 Aeron Archive를
  쓰는 통합테스트(`AccountSnapshotRecoveryIntegrationTest`·`AccountFillReplayRecoveryIntegrationTest`·
  신규 `AccountFillMultiSourceReplayRecoveryIntegrationTest`)가 포함돼 있다.

- **RED 확인(이 테스트가 실제로 버그를 잡는지)**: U3의 두 main 코드 파일
  (`AccountFillReplayer.java`·`AccountFillIntakeConfig.java`)만 `git stash`로 빼고(신규
  통합테스트는 그대로 두고) 돌렸다 — `AssertionFailedError: [매칭A·매칭B 체결이 전부
  반영된 잔고여야 한다] expected: 0 but was: 1`로 실패(잔고가 기대값 960,000이 아니라
  매칭A의 fill#1만 반영된 값으로 남음). `git stash pop`으로 즉시 복원해 GREEN 재확인했다.
  통과만 보고 넘어가지 않고, 고치기 전 코드에서 실제로 깨지는 것까지 확인한 것.

- **플레이키 여부**: 신규 통합테스트를 연속 3회 실행, 전부 PASSED(각 13~14초). Aeron
  통합테스트가 이 레포에서 타이밍 문제로 흔들린 전례(`project_flaky_replay_recovery_test`)가
  있어 별도로 확인했다.

- **새 통합테스트 설계 메모**: 매칭 프로세스 둘을 각각 별도 `MediaDriver`+`Aeron` 클라이언트로
  띄워 udp로 발행 — 같은 클라이언트에서 `aeron:ipc`로 두 번 발행하면 세션을 공유해
  sessionId가 같아지는 함정을 피하려고. 시나리오 안에 D1·U3의 두 분기를 모두 넣었다:
  매칭A는 스냅샷에 저장된 위치부터 이어 읽고, 매칭B(스냅샷 이후 새로 연결)는 위치 맵에
  없어 recording 시작 위치부터 읽는다.

- **처음 설계 실수 하나**: `@DynamicPropertySource`로 udp 포트를 배선하려다
  `endpoint has port=0` 에러로 실패했다 — 이 테스트는 `@SpringBootTest`가 아니라 수동
  `SpringApplicationBuilder`(`AccountFillReplayRecoveryIntegrationTest`와 같은 패턴)를
  쓰는데, `@DynamicPropertySource`는 Spring TestContext Framework(`@SpringBootTest` 계열)가
  관리하는 컨텍스트에만 적용된다 — 아무도 그 static 메서드를 호출해주지 않아 포트 필드가
  기본값 0으로 남았다. 포트를 테스트 메서드 안에서 직접 할당해 `launch(fillChannel)`로
  넘기는 방식으로 고쳤다.

## 블로그 네타

### "받는 쪽이 녹음하면, 발행자마다 트랙이 갈린다 — Aeron Archive REMOTE 녹화의 세션 단위"

- **훅·핵심 주장**: "녹화를 어느 쪽이 하는가"를 정하고 나면 끝난 게 아니다 — **녹화가
  무슨 단위로 쪼개지는가**까지 알아야 한다. 받는 쪽(계좌)이 REMOTE로 녹화하면, 보낸 쪽이
  누구든 하나로 합쳐 녹음될 거라 생각하기 쉬운데, 실제로는 **연결(세션)마다** 별도
  녹화가 생긴다. 발행자가 하나뿐이거나 재시작을 안 하면 이 사실이 코드에 묻혀 안 보인다.

- **context**: 계좌 워커가 매칭에서 오는 체결을 재기동 복구용으로 자기 Aeron Archive에
  REMOTE로 녹화하는데, 그 되읽는 코드가 "마지막 recording 하나만" 읽고 있었다 — 매칭이
  하나뿐이고 재시작을 안 하는 지금까지는 안 터졌다.

- **어떻게(서사·근거)**: recording이 여럿일 수 있다는 걸 알고 나면 그다음 질문은
  "그럼 여러 recording을 어떤 순서로 읽어야 하는가"인데, 답은 "순서를 신경 쓸 필요가
  없다"였다(U0, 체결 반영의 텔레스코핑 성질). 순서를 안 따져도 되는 성질을 먼저
  테스트로 증명해두고서야, "recording을 전부 읽되 순서는 무시한다"는 단순한 설계가
  안전하다고 확신하고 갈 수 있었다. 그리고 "값 하나를 여러 발행자에 걸쳐 뭉개면 안 된다"는
  원칙을 스냅샷 파일 형식(구버전 감지)과 코드 안의 임시 다리(경고 로그) 두 곳에 똑같이
  적용한 경험 — 조용히 넘어가는 코드는 나중에 반드시 같은 모양의 버그로 되돌아온다.

- **재료(커밋·도식·수치)**: 커밋 `4fa568d`(U1+U2, 19 files)·`7f6a4a8`(U3, 3 files).
  회귀 108+44 PASSED. RED 확인 로그(`expected: 0 but was: 1`). 도식 후보: Archive
  recording이 세션마다 분리되는 그림(매칭A·매칭B가 각자 recording을 만들고, 계좌가 재기동할
  때 두 recording을 각자 sessionId로 이어 읽는 흐름).
