---
feature: account-archive-segment-purge
date: 2026-09-16
branch: fix/account-archive-segment-purge
commits: [0b27914, d09f0a6, 3f24bef]
feeds: [adr, blog]
---

# 계좌 Archive 세그먼트 회수 (I5)

## ADR 네타

### "계좌 프로세스의 Aeron Archive 저널·체결 기록을 어떻게 디스크에서 회수하는가"

- **context(무슨 상황)**: 계좌 프로세스는 저널(스트림 4005, 처리한 이벤트 순서대로)과 받은 체결 원본(스트림 6001, 엔진에 반영하기 전 크래시 대비)을 Aeron Archive에 계속 기록한다. 프로세스를 오래 켜둘수록 디스크에 쌓이는 세그먼트 파일이 무한히 늘어난다.
- **왜(문제·동기)**: 레포에 `purgeSegments`·`truncateRecording`·`deleteRecording`·`detachSegments` 사용처가 0건이었다 — 버리는 코드 자체가 없었다. 복구 시간과는 무관하다(스냅샷에 적힌 위치부터만 replay하므로 쌓인 양과 복구 시간이 비례하지 않는다) — 이 작업이 닫는 것은 디스크 용량 하나뿐이다.
- **어떻게(대안·결정·트레이드오프)**:
  - **경계 = "한 세대 여유"(D1)**: 이번에 찍은 스냅샷 경계가 아니라 직전에 찍은 경계까지만 지운다. 스냅샷 파일이 하나만 유지되므로(rename으로 덮어씀) 최신 스냅샷 경계까지 지웠는데 그 파일이 손상되면 그 앞 기록도 사라져 되살릴 방법이 없다. 메모리에서 tradeId 세대를 현재+직전 남기는 규칙(`AccountState.pruneOlderThan`)과 같은 결을 디스크에 적용한 것.
  - **회수 시점(D2)**: 스냅샷이 디스크에 확정(fsync 완료)된 직후. `AccountSnapshotWriter.writeTask()`의 `snapshotStore.write(...)`가 예외 없이 반환한 시점.
  - **전용 Archive 연결(D3)**: 기동 배선이 쓰는 `AeronArchive` 빈을 런타임에 같이 쓰지 않는다. 같은 제어 세션을 여러 스레드가 동시에 쓰면 안 되므로, 회수 담당 클래스가 `start()`에서 전용 연결을 열고 `close()`에서 닫는다.
  - **실패 처리(D4)**: 회수 실패는 엔진을 멈추지 않는다. 대상 recordingId·목표 위치·이유를 경고 로그로 남기고 다음 스냅샷 주기가 재시도한다.
  - **클래스 분리(SRP)**: 처음엔 회수 로직을 `AccountSnapshotWriter` 안에 넣었으나, 조정 세션이 SRP 위반(스냅샷 쓰기와 세그먼트 회수는 서로 다른 이유로 바뀐다)을 지적해 `AccountArchiveSegmentPurger`로 분리했다. U2(체결 회수) 설계 때 "저널 회수와 체결 회수를 같은 클래스에 둘지"도 같은 기준으로 판단 — 이 둘은 "같은 정책(Archive 세그먼트 회수)이 같이 바뀌는" 하나의 책임이자 같은 전용 연결·같은 스레드 순차 실행 제약을 공유하므로, 하나의 클래스(고수준 메서드 두 개: `purgeJournalUpTo`·`purgeFillsUpTo`)로 유지했다. "무엇을 회수할지"(체결의 sessionId→recordingId 매핑)를 writer가 갖게 하면 writer가 다시 정책을 갖게 돼 분리 취지가 무너진다고 판단.
  - **동시성 제약**: 같은 recording에 삭제 계열 작업(purge/detach/delete)이 겹치면 서버가 "another delete operation in progress" 에러를 낸다(`ArchiveConductor.isDeleteAllowed`) → 저널·체결 회수를 같은 스레드(스냅샷 쓰기 스레드)에서 순차 실행해 애초에 안 겹치게 했다.
- **무엇을(실제 변경·파일·커밋)**:
  - `0b27914`(U1, 저널): `AccountArchiveSegmentPurger` 신규, `AccountSnapshotSink.offer` 시그니처에 `journalPosition` 파라미터 추가(구현·대역 5곳: `AccountEngine.NO_OP_SNAPSHOT_SINK`·`AccountSnapshotWriter`·`AccountEventHandler`·`AccountEngineSnapshotTriggerTest`·`AccountEngineLatencyHistogramTest`·`AccountEngineConfigTest`).
  - `d09f0a6`(U2, 체결): Purger를 recordingId 인자화(`Map<Long,Long> previousPositionByRecording`), `purgeJournalUpTo(long)`·`purgeFillsUpTo(Map<Integer,Long>)` 고수준 메서드 추가. `AccountSnapshotConfig`에 fillChannel·fillStreamId 배선.
  - `3f24bef`(U3, 검증): `AccountOrderIntakeConfig.archivingMediaDriver`에 세그먼트·term 길이 선택적 property(`account.worker.archive.segment-file-length`·`account.worker.archive.ipc-term-buffer-length`, 비우면 운영값 그대로) 추가. `AccountArchivePurgeRecoveryIntegrationTest` 신규.
- **결과·수치**: U1~U3 각 커밋 시점 `:account-worker:test` 전체 GREEN(46→47→48개, 실패 0건, 3회 실측). U3에서 20,000건 재전송으로 스냅샷·회수 2회 트리거 실측 확인(정확한 세그먼트 파일 감소량은 기록 안 함 — "회수 전 대비 감소했다"는 부등호 어서션만 검증).

## 블로그 네타

### "Aeron Archive javadoc만 보고 설계를 뒤집을 뻔한 순간 — 서버 소스까지 읽어야 했던 이유"

- **훅·핵심 주장**: `AeronArchive.purgeSegments`의 공식 javadoc은 "활성 기록 중인 recording은 detach 불가"라고 읽힌다. 이 문구만 믿었으면 "계좌 프로세스가 살아있는 동안 저널을 회수한다"는 설계 전체를 갈아엎어야 했다. 실제로는 서버 구현(`ArchiveConductor.isValidDetach`)을 열어보니 "지금 쓰고 있는 세그먼트 자체만 못 지운다"는 훨씬 관대한 조건이었다.
- **context**: 계좌 프로세스가 Aeron Archive에 무한히 쌓는 저널·체결 기록을 회수하는 기능(I5)을 만드는 중이었다. 설계(LLD)가 "착수 전에 공식 javadoc으로 직접 확인하고, 어긋나면 구현하지 말고 보고하라"고 못박아 둔 지점이었다.
- **어떻게(서사·근거)**: javadoc 한 줄 때문에 멈추지 않고, aeron-archive 소스 jar를 풀어 `ArchiveConductor.isValidDetach()`를 직접 읽었다. `newStartPosition`이 "지금까지 실제로 기록된 위치"(recording 진행 중이면 `recordedPosition()`)의 세그먼트 경계 이하이기만 하면 통과한다는 걸 확인했다 — javadoc의 "active for recording" 금지는 실제로는 "가장 최근의, 아직 안 끝난 세그먼트"만을 가리켰다. 이 확인 하나로 설계 전체를 지킬 수 있었다.
- **재료(커밋·도식·수치)**: `0b27914` 커밋 메시지, LLD 4절("착수 전 확인 완료"로 개정한 섹션). 이후 트랙에서 같은 패턴(관찰 신호가 틀렸는데 구현을 의심할 뻔한 순간)이 U2·U3에서 두 번 더 나왔다 — 아래 두 번째 포스팅 소재.

### "겉보기 진행과 실제로 기다려야 할 값이 다를 때 — Aeron 테스트가 세 번 흔들린 이유"

- **훅·핵심 주장**: 같은 트랙에서 관찰 신호를 잘못 짚어 테스트가 막힌 사건이 세 번 있었다. 매번 "구현이 잘못됐나"를 먼저 의심했지만, 세 번 다 진짜 문제는 "내가 보고 있는 값이 내가 기다려야 할 값이 아니었다"는 것이었다.
- **context**: I5의 U2(체결 회수 테스트)·U3(회수 뒤 복구 검증 테스트)를 만드는 중, 그리고 이전 세션(I1)의 복구 테스트 flaky 사건까지 — 전부 "Aeron/디스크 상태가 비동기로 바뀌는데 동기적 값을 보고 판단했다"는 같은 모양이었다.
- **어떻게(서사·근거)**:
  1. U2: 발행자(publication)가 목표 위치에 도달한 시점에 바로 세그먼트 파일 수를 쟀다가 flaky하게 실패(기대 8, 실제 9). REMOTE 녹화는 네트워크를 거쳐 비동기로 디스크에 쓰이므로, `publication.position()`과 `testArchive.getRecordingPosition(recordingId)` 사이에 시차가 있었다.
  2. U3: 같은 requestId를 20,000번 재전송한 뒤 `AccountState.seq()`가 오르길 기다렸다가 30초 타임아웃. 로그를 보니 "요청 재전송 무시"가 정확히 20,000번 찍혀 있었다 — 처리는 멀쩡히 되고 있었다. `AccountState.seq()`는 실제 상태 변경(tryReserve 등)이 일어날 때만 오르고, dedup되는 재전송은 저널엔 남지만 이 값을 안 태운다는 게 원인이었다. 세그먼트·term 길이를 의심해 1MB까지 올려봤지만 그대로 재현돼, 그쪽은 원인이 아니라는 것도 같이 확인했다(엉뚱한 곳을 판 시간 자체가 이 함정의 크기를 보여준다). `AccountSnapshotWriter.durableSeq()`(handler의 전역 appliedSeq를 반영)로 바꿔서 해결했다.
  3. (이 트랙 이전, I1) 복구 테스트가 flaky했던 사건도 같은 계열이었다 — `accountState()`를 라이브 상태 캡처에 쓴 것이 테스트의 race였다(별도 기록: `project_flaky_replay_recovery_test`).
- **재료(커밋·도식·수치)**: `d09f0a6`·`3f24bef` 커밋 메시지에 각 사건을 그대로 적어뒀다. "무엇을 기다려야 하는가"라는 질문 하나로 정리되는 세 가지 사례 — 발행 위치 vs 기록 위치, 상태 카운터 vs 적용 순번 카운터, 라이브 캡처 vs 확정된 값.
