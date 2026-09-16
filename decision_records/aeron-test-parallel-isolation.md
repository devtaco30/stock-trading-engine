---
feature: aeron-test-parallel-isolation
date: 2026-09-16
branch: feat/c7-load-harness
feeds: [adr]
---

# account-worker Aeron Archive 통합 테스트는 같은 머신에서 병렬 실행할 수 없다

멀티세션(여러 워크트리가 같은 머신에서 동시에 gradle 테스트)·부하 측정 프로세스 상시 가동 환경에서, account-worker의 Aeron Archive 통합 테스트가 다른 JVM과 겹쳐 돌면 대거 실패한다. 증상은 아카이브 카탈로그의 "recording exists for streamId=..."(관측된 값 4005 계열)와 제어 포트 충돌이다. a4가 git stash로 자기 변경 유무와 무관하게 재현, ec도 독립 확인했다 — 코드 버그가 아니라 테스트가 머신 전역 자원을 잡는 구조라 병렬이 안 되는 것이다. 고치는 건 별도 트랙이고, 이 기록은 제약과 운영 규약, 그리고 격리에 필요한 것을 남긴다.

## 문제 — 무엇을 공유해서 충돌하나

**확인된 것: Aeron 스트림 id는 고정 컴파일 상수다.** `AeronStreamIds`(core)에 `ACCOUNT_INTAKE=4004`·`MATCHING_INTAKE=2002`·`FILL=6001`이 상수로 박혀 있고 제품·테스트가 그대로 쓴다. 여러 JVM이 같은 채널·같은 스트림 id로 붙으면 그 자원을 두고 겹친다.

**확인된 것: 일부 테스트는 이미 격리한다.** `CrossShardFillFanoutIntegrationTest`·`AccountFillReplayRecoveryIntegrationTest`는 `@TempDir`로 테스트마다 아카이브 디렉터리를 따로 잡고, `freePort()`로 fill·control 포트를 임의 포트로 잡는다. 즉 아카이브 디렉터리와 포트는 이미 테스트별로 분리돼 있다.

**추정(미확인): 남은 공유 자원은 Aeron media-driver 디렉터리(aeron.dir)로 보인다.** 아카이브 디렉터리와 포트가 테스트별로 분리돼 있는데도 "recording exists for streamId" 충돌이 나는 건, 임베디드 media driver가 JVM마다 고유한 `aeronDirectoryName`을 안 받고 사용자 기본 디렉터리(예: `/dev/shm` 또는 `/tmp`의 per-user aeron 디렉터리)를 공유하기 때문일 가능성이 크다 — 그러면 두 JVM이 같은 드라이버의 CnC·스트림 등록을 공유해 같은 채널·스트림의 recording 등록이 겹친다. **어느 테스트·어느 설정이 aeron.dir를 고유화하지 않는지는 아직 코드로 못 박지 않았다(fix 트랙에서 확정).**

## 격리에 필요한 것 (fix — 별도 트랙, 지금 범위 밖)

테스트를 병렬 안전하게 만들려면 JVM(=테스트)마다 아래를 전부 고유화해야 한다.

- **aeron.dir(media-driver 디렉터리)**: 테스트마다 고유 `aeronDirectoryName`(예: `@TempDir` 또는 `randomUUID` 경로). ← 지금 빠진 것으로 추정되는 핵심.
- **아카이브 디렉터리**: 이미 `@TempDir`로 분리됨(유지).
- **제어·복제·fill 포트**: 이미 `freePort()`로 임의 포트(유지). archive control·replication 채널도 전부 임의 포트인지 확인 필요.
- **스트림 id는 안 바꿔도 된다.** 스트림 id는 한 드라이버의 채널 안에서만 의미가 있어, 위처럼 드라이버·아카이브·포트가 JVM마다 분리되면 같은 스트림 id를 써도 서로 안 겹친다. 스트림 id 상수를 테스트용으로 바꾸는 건 불필요하고 제품 상수를 흔들 이유가 없다.

## 운영 규약 (지금 적용 — 2b 채택)

- **dc가 부하 측정 프로세스를 띄워둔 동안 아무도 account-worker Aeron 스위트를 돌리지 않는다.**
- 돌려야 하면 dc에게 "지금 측정 중이냐" 먼저 확인하고, 아니라는 답을 받은 뒤 **단독으로** 돌린다.
- 그 사이 신뢰 가능한 신호는 `core`·`account-disruptor`·`order-engine`·`api`와 비-Aeron 테스트다. **account-worker Aeron 결과가 빨갛다고 코드 문제로 단정하지 않는다** — 먼저 이 병렬 경합인지 의심한다.

## 결정

account-worker Aeron Archive 통합 테스트의 **병렬 실행을 금지**하고(운영 규약), 테스트 자원 격리(aeron.dir 고유화 중심)는 **별도 fix 트랙**으로 둔다. 이 제약은 테스트가 머신 전역 자원(고정 스트림 id + 공유 추정 aeron.dir + 드라이버)을 잡는 구조에서 나오는 설계상의 제약이라, 격리를 완성하기 전까지 반복해서 밟는다 — 그래서 규약으로 못 박는다.

## 결과

- 멀티세션 작업 중 account-worker Aeron 스위트는 단독 실행으로만 신뢰한다. 나머지 모듈·비-Aeron 테스트가 상시 신뢰 신호다.
- fix 트랙 착수 시 첫 할 일: 어느 테스트·설정이 aeron.dir를 고유화하지 않는지 코드로 확정(위 추정 검증) → 고유 `aeronDirectoryName` 주입. 그러면 스트림 id·포트를 안 건드리고도 병렬 안전해질 것으로 본다(미검증 가설).
- 관련: [[v1-v2-e2e-measurement]]·[[measurement-instrumentation-permanent]](이 경합을 처음 만난 부하 측정 트랙).
