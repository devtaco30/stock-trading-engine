---
feature: aeron-test-parallel-isolation
date: 2026-09-16
branch: feat/c7-load-harness
feeds: [adr]
---

# account-worker Aeron Archive 통합 테스트는 같은 머신에서 병렬 실행할 수 없다

멀티세션(여러 워크트리가 같은 머신에서 동시에 gradle 테스트)·부하 측정 프로세스 상시 가동 환경에서, account-worker의 Aeron Archive 통합 테스트가 다른 JVM과 겹쳐 돌면 대거 실패한다. 증상은 아카이브의 "recording exists for streamId=4005"와 제어 포트 충돌이다. a4가 git stash로 자기 변경 유무와 무관하게 재현, ec도 독립 확인했다 — 코드 버그가 아니라 테스트가 머신 전역 자원(고정 포트)을 잡는 구조라 병렬이 안 되는 것이다. 고치는 건 별도 트랙이고, 이 기록은 제약과 운영 규약, 격리에 필요한 것을 남긴다.

## 문제 — 무엇을 공유해서 충돌하나 (코드로 확인, 2b 검증)

**공유되지 않는 것(격리 이미 됨):**
- **Aeron media-driver 디렉터리(aeron.dir)**: JVM마다 임의 디렉터리를 만든다 — `CommonContext.generateRandomDirName()` (`AccountOrderIntakeConfig.java:86`·`MatchingOrderIntakeConfig.java:80`·`AccountOrderPublishConfig.java:40`). 테스트도 자기 embedded driver의 `driver.aeronDirectoryName()`을 받아 쓴다(17곳 전부).
- **Archive 디렉터리**: 설정이 비어 있으면 `Files.createTempDirectory("...-archive-")`로 매번 새로 만든다(`MatchingOrderIntakeConfig` archive-dir 해석부). 고정 경로를 주는 프로파일은 main 리소스에 없다.

**진짜 공유 자원: Archive 제어 채널의 고정 UDP 포트 `localhost:8010`.**
- `AccountOrderIntakeConfig.java:60` — `DEFAULT_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010"`.
- `MatchingOrderIntakeConfig.java:55` — 같은 기본값 `localhost:8010`.
- 둘 다 속성으로 덮을 수 있지만(`account.worker.archive.control-channel`·`matching.worker.archive.control-channel`) **기본값이 8010으로 고정**이라, 이 채널을 오버라이드하지 않는 JVM 여러 개가 같은 머신에서 뜨면 이 포트 하나를 두고 부딪힌다.

**"recording exists for streamId=4005"의 기제:** 4005는 계좌 저널 발행 스트림이다(`AccountJournalArchiveConfig.java:39` `JOURNAL_STREAM_ID = 4005`). aeron.dir·archive 디렉터리가 JVM마다 분리돼 있어 스트림 id가 같은 것 자체로는 안 부딪힌다 — 그런데 두 JVM의 archive 클라이언트가 **같은 제어 포트(8010)** 로 붙으면서, 한 JVM의 클라이언트가 다른 JVM의 아카이브(또는 포트 바인드 충돌)에 닿아 그쪽이 이미 녹화 중인 저널 스트림 4005의 recording 등록과 겹친다. 즉 **스트림 id 고정은 부수적 증상이고, 겹침을 가능케 한 원인은 고정 제어 포트다.**

## 격리에 필요한 것 (fix — 별도 트랙, 지금 범위 밖)

- **핵심: JVM(=테스트)마다 고유한 Archive 제어 포트.** `*.archive.control-channel`을 임의(ephemeral) 포트로 오버라이드한다. 일부 테스트는 이미 `freePort()`로 control 포트를 잡는다(`CrossShardFillFanoutIntegrationTest`·`AccountFillReplayRecoveryIntegrationTest`) — 실패하는 건 이 오버라이드 없이 기본 8010을 그대로 쓰는 테스트다. 모든 Aeron 통합 테스트가 control(요청·응답) 채널을 임의 포트로 잡게 하면 된다.
- **aeron.dir·archive 디렉터리는 이미 고유화돼 있다** — 손댈 필요 없다(위 확인).
- **스트림 id(4004/2002/4005/6001…)는 안 바꿔도 된다** — 드라이버·아카이브·포트가 JVM마다 분리되면 같은 스트림 id를 써도 안 겹친다. 제품 상수를 흔들 이유가 없다.

## 운영 규약 (지금 적용 — 2b 채택)

- **dc가 부하 측정 프로세스를 띄워둔 동안 아무도 account-worker Aeron 스위트를 돌리지 않는다.**
- 돌려야 하면 dc에게 "지금 측정 중이냐" 먼저 확인하고, 아니라는 답을 받은 뒤 **단독으로** 돌린다.
- 그 사이 신뢰 가능한 신호는 `core`·`account-disruptor`·`order-engine`·`api`와 비-Aeron 테스트다. **account-worker Aeron 결과가 빨갛다고 코드 문제로 단정하지 않는다** — 먼저 이 병렬 경합(제어 포트 8010 충돌)인지 의심한다.

## 결정

account-worker Aeron Archive 통합 테스트의 **병렬 실행을 금지**하고(운영 규약), 테스트 자원 격리(제어 포트 고유화)는 **별도 fix 트랙**으로 둔다. 이 제약은 archive 제어 채널 기본 포트가 8010으로 고정이라 생기는 것으로, 격리를 완성하기 전까지 반복해서 밟는다 — 그래서 규약으로 못 박는다.

## 결과

- 멀티세션 작업 중 account-worker Aeron 스위트는 단독 실행으로만 신뢰한다. 나머지 모듈·비-Aeron 테스트가 상시 신뢰 신호다.
- fix 트랙 착수 시 할 일: 기본 8010을 그대로 쓰는 Aeron 통합 테스트를 찾아 control 채널을 임의 포트로 오버라이드(이미 `freePort()`를 쓰는 두 테스트가 참고 패턴). aeron.dir·archive 디렉터리·스트림 id는 그대로 둔다.
- 정정 이력: 최초 이 기록은 공유 자원을 "media-driver aeron.dir 기본값(미확인 추정)"으로 적었으나, 2b가 코드로 확인해 **aeron.dir·archive 디렉터리는 이미 격리, 실제 공유 자원은 고정 제어 포트 8010**으로 정정했다(위 파일·줄번호). 추정을 사실로 대체함.
- 관련: [[v1-v2-e2e-measurement]]·[[measurement-instrumentation-permanent]](이 경합을 처음 만난 부하 측정 트랙).
