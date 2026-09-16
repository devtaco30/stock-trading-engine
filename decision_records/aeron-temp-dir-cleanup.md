---
feature: aeron-temp-dir-cleanup
date: 2026-09-16
branch: fix/aeron-temp-dir-cleanup
commits: [72f68b2]
feeds: [adr, blog]
---

# Aeron 드라이버 임시 디렉터리 정리 (I10) — 디스크를 두 번 채운 뒤에 고친 것

Aeron 드라이버가 만드는 임시 디렉터리를 종료 시 지우게 한 작업. 코드 자체는 작지만(설정 두 줄, 17군데), 발견 경위와 "무엇을 절대 지우면 안 되는지"의 구분이 이 기록의 값이다.

## ADR 네타

### Aeron 드라이버 디렉터리와 Archive 디렉터리를 구분해 한쪽만 지운 이유

- **context**: Aeron(프로세스 사이 메시지를 저지연으로 나르는 전송 라이브러리)은 드라이버(통신 버퍼를 관리하는 프로세스 내부 컴포넌트)를 띄울 때마다 임시 디렉터리(`aeron-<사용자>-<임의문자열>`, 각 수십MB)를 만든다. 이 레포는 계좌·매칭 워커, api, 그리고 다수의 테스트가 매번 새 임베디드 드라이버를 띄우는데, 그 디렉터리를 지우라는 설정(`dirDeleteOnStart`·`dirDeleteOnShutdown`)을 한 번도 걸지 않았다 — 레포 전체에 사용처 0건.
- **왜**: 회귀 한 번 돌릴 때마다 디스크가 수십~수백GB씩 찼다. 이날만 작업이 두 번 멈췄다 — 한 번은 여유 공간이 바닥나 Kafka 브로커가 `sync failed`로 죽었다. Jack이 직접 발견해 지시했다.
- **어떻게**: `MediaDriver.Context`(드라이버 자신의 통신용 디렉터리를 관리하는 설정 객체)에 `dirDeleteOnStart(true)`(뜨기 전 낡은 디렉터리가 있으면 지움, 이 레포는 매번 랜덤 이름을 쓰므로 방어적 차원)와 `dirDeleteOnShutdown(true)`(정상 종료 시 그 실행이 만든 디렉터리를 지움)를 건다. **Archive.Context(저널·스냅샷이 들어 있는 별도 디렉터리, `archiveDir` 프로퍼티)는 일부러 손대지 않았다** — 이 프로젝트의 재기동 복구 테스트들은 run1(프로세스를 한 번 띄워 상태를 만들고 정상 종료)과 run2(같은 archiveDir로 다시 띄워 복원 확인)의 2단계 구조다. Archive 디렉터리를 종료 시 지우게 만들면 run2가 읽을 게 없어져 그 테스트들이 전부 깨지거나, 더 나쁘게는 "복구되는 것처럼 보이는데 실제로는 아무것도 안 읽는" 상태(빈 상태로 시작해도 어차피 초기 seed 값과 같아 보여 겉보기로는 통과하는 경우)가 된다.
- **무엇을**: 프로덕션 3곳 — `AccountOrderIntakeConfig`(111행, ArchivingMediaDriver), `AccountOrderPublishConfig`(41행, plain MediaDriver, Archive 없음), `MatchingOrderIntakeConfig`(107행, ArchivingMediaDriver). 테스트 9개 파일 14곳 — 임베디드 드라이버를 직접 띄우는 자리(`AeronAccountEndToEndTest`·`AccountFillMultiSessionRecordingIntegrationTest`·`AccountFillMultiSourceReplayRecoveryIntegrationTest`·`CrossShardFillFanoutIntegrationTest`·`AccountArchiveFillSegmentPurgerTest`·`AccountArchiveSegmentPurgerTest`·`AeronAccountOrderSenderIntegrationTest`·`AeronMatchingEndToEndTest`·`AeronIpcRoundtripTest`). 대부분은 각 파일에 `cleanEmbeddedMediaDriverContext()` 헬퍼 메서드를 추가해 `MediaDriver.launchEmbedded()`가 그걸 쓰게 했고, 이미 명시적 `Context`를 쓰던 두 곳(`AccountArchiveFillSegmentPurgerTest`·`AccountArchiveSegmentPurgerTest`)은 그 자리에 직접 플래그를 추가했다. 커밋 `72f68b2`.
- **결과·수치**: `AeronIpcRoundtripTest`(Archive 없는 순수 Aeron 테스트)를 골라 실측 — 고치기 전 1회 실행당 `aeron-*` 폴더 +17개(612→629), 고친 뒤 +0개(629→629). 전체 회귀: matching-disruptor·account-disruptor·api(Aeron 발신 테스트)·account-worker·matching-worker 전부 GREEN(account-worker+matching-worker 합쳐 83 tests, 0 failures). 특히 재기동 복구 테스트(`AccountArchivePurgeRecoveryIntegrationTest`·`AccountFillMultiSourceReplayRecoveryIntegrationTest`·`MatchingOrderIntakeRecoveryIntegrationTest`·`MatchingPeriodicSnapshotRecoveryIntegrationTest` 등, run1이 쓰고 run2가 읽는 구조)가 전부 통과해 Archive 디렉터리가 실제로 안 건드려졌음을 확인했다.

## 블로그 네타

### "지울 걸 지우고, 지우면 안 될 걸 안 지운 이야기 — 폴더가 두 종류였다"

- **훅·핵심 주장**: "테스트가 임시 파일을 안 치운다"는 흔한 문제처럼 보이지만, 이 레포엔 이름이 비슷한 디렉터리가 두 종류(통신 버퍼 vs 저널·스냅샷) 있었고, 구분 없이 "다 지우자"로 갔으면 디스크 문제 대신 데이터 유실 문제를 만들었을 것이다.
- **context**: Aeron 드라이버는 뜰 때마다 임시 디렉터리를 만든다. 이 레포엔 그 디렉터리를 지우는 설정이 한 번도 없어 회귀를 돌릴 때마다 디스크가 찼고, 하루에 두 번 작업을 멈추게 했다.
- **어떻게(서사·근거)**: 원인(`dirDeleteOnStart`·`dirDeleteOnShutdown` 사용처 0건)을 확인한 뒤 가장 먼저 한 일은 "지울 대상이 정확히 뭔가"를 가르는 것이었다 — 같은 프로세스가 두 종류의 Aeron 디렉터리를 만든다: 드라이버 자신의 통신 버퍼(프로세스가 죽으면 의미 없음)와 Archive의 저널·스냅샷 디렉터리(재시작 넘어 보존해야 함, 재기동 복구 테스트가 실제로 그걸 읽는다). 종료 시 지우는 설정을 드라이버 쪽에만 걸고 Archive 쪽은 코드에서 물리적으로 건드리지 않은 뒤, 재기동 복구 테스트 전부를 이름까지 확인하며 통과시켜 "저널 디렉터리는 그대로다"를 증명했다.
- **재료(커밋·도식·수치)**: 커밋 `72f68b2`. 실측 +17개→+0개(612→629→629). 재기동 복구 테스트 목록과 전부 PASSED.
