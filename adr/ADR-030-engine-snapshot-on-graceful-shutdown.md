# ADR-030 v2 엔진의 스냅샷은 정상 종료 때 상태 전체와 journal position을 파일 하나로 남기고, 복구는 그 position부터 replay한다

- 날짜: 2026-09-11(매칭) · 2026-09-12(계좌)
- 상태: 채택. 트리거는 ADR-034가 주기 스냅샷으로 넓혔고, 계좌 스냅샷에 담는 멱등 장부는 ADR-035가 바꿨다.
- 관련: ADR-014(single-writer 계좌) · ADR-019(매칭 내구성 수단) · ADR-023(발급 재현) · ADR-025(엔진 journal과 replay) · ADR-034(주기 스냅샷) · ADR-035(멱등 장부 상한) · 원문 `decision_records/snapshot.md`

## 문제

ADR-025로 v2의 복구는 journal replay가 됐다. 엔진이 받은 입력을 처음부터 다시 적용해 상태를 만드는 방식이다. 그런데 journal은 계속 쌓이기만 한다. 사흘을 돌린 프로세스를 되살리려면 사흘치 입력을 전부 다시 적용해야 한다. 복구 시간이 가동 시간에 비례해 늘어난다.

해법의 이름은 ADR-019에서 이미 정했다. 주기적으로 상태를 통째로 저장해 두고, 복구할 때는 그 시점 이후의 입력만 replay하는 스냅샷이다. ADR-019는 Aeron Archive가 스냅샷을 제공하지 않으므로 직접 만든다는 것까지만 정했다. 언제 찍을지, 어디에 저장할지, 복구에 어떻게 물릴지는 비워 뒀다. 이 ADR이 그 셋을 정한다.

### 용어

- **스냅샷(snapshot)** — 어느 한 시점의 엔진 상태 전체를 통째로 직렬화한 것. 계좌 엔진이면 계좌 상태, 매칭 엔진이면 종목별 호가창이다.
- **journal position** — 그 스냅샷을 찍은 시점까지 journal에 적힌 위치(ADR-025). 복구할 때 "여기부터 읽어라"의 기준이 된다.
- **recording** — Aeron Archive가 녹화 한 번마다 따로 만드는 기록 단위. 프로세스를 다시 띄우면 새 recording이 생기므로, 오래 돌린 시스템의 journal은 여러 recording에 걸쳐 있다.
- **정상 종료(graceful shutdown)** — 링버퍼에 남은 입력을 다 처리하고(drain) 엔진 스레드를 세운 뒤 내려가는 종료. 반대는 프로세스가 갑자기 죽는 것이다.
- **single-writer** — 한 상태를 한 스레드만 건드리는 규칙(ADR-014). 다른 스레드가 그 상태를 읽는 것도 이 규칙을 깬다.
- **멱등 캐시** — 이미 처리한 체결·정산·요청을 기억해 두고 같은 것이 또 오면 버리는 자료구조.
- **journal · replay** — journal은 엔진이 받은 입력을 받은 순서대로 적어 둔 기록이고, replay는 그 기록을 처음부터 다시 적용해 상태를 만드는 복구 방식이다(ADR-025).
- **스트림 id** — 같은 Aeron 미디어 드라이버 위에서 용도별로 채널을 구분하는 번호.

정할 것은 둘이다.

1. **언제 찍는가.** 엔진이 도는 중에 상태를 읽으면 single-writer가 깨지므로 아무 때나 찍을 수 없다.
2. **어디에 저장하는가.** journal은 Aeron Archive에 녹화하는데, 스냅샷도 같은 자리에 둘지는 별개 문제다.

## 대안

**언제 찍는가**

- (a) 주기적으로. 이벤트 N건마다 또는 N초마다.
- (b) 정상 종료할 때만.
- (c) 사람이 시킬 때만.

**어디에 저장하는가**

- (a) 파일 하나로 디스크에.
- (b) Aeron 스냅샷 스트림에 넣고 Archive가 녹화하게 한다.

## 트레이드오프

트리거를 정하는 기준은 single-writer다. 엔진이 도는 중에 다른 스레드가 호가창이나 계좌 맵을 읽으면 그 규칙이 깨진다. 안전한 순간은 둘뿐이다. 하나는 drain이 끝나 엔진 스레드가 멈춘 시점이고, 다른 하나는 스냅샷을 찍으라는 이벤트를 링버퍼에 넣어 엔진 스레드가 직접 찍게 하는 것이다.

| 기준 | (a) 주기 | (b) 정상 종료 | (c) 수동 |
|---|---|---|---|
| single-writer를 지키려면 | 링 이벤트 배선이 필요 | drain 뒤라 그냥 안전 | 링 이벤트 배선이 필요 |
| 크래시 복구 시간 | 마지막 주기 이후만 replay | 마지막 정상 종료 이후 전부 replay | 사람이 찍은 뒤부터 |
| 지금 필요한 것 | 정책(간격 정하기)까지 같이 정해야 함 | 메커니즘만 있으면 됨 | 운영용 |

(b)를 먼저 고르면 스냅샷을 찍고 되살리는 메커니즘 전체를 배선 없이 확인할 수 있다. 간격을 얼마로 할지는 그다음 문제다.

저장 위치는 스냅샷이 어떤 데이터인지를 보면 정해진다. journal은 입력이 계속 붙는 스트림이지만 스냅샷은 상태 blob 하나이고, 필요한 것은 언제나 최신 하나다. 파일에 쓰고 원자적으로 이름을 바꾸면 그것으로 끝난다. Aeron 스트림에 넣으면 녹화된 여러 스냅샷 중 최신 하나를 찾아 읽는 일이 새로 생긴다.


## 결정

**트리거는 (b) 정상 종료다.** 스냅샷을 저장하는 빈의 phase를 엔진보다 낮게 둔다(`MatchingSnapshotLifecycle.java:32`, PHASE = -1). Spring의 `SmartLifecycle`은 높은 phase부터 멈추므로, 엔진(phase 0)과 수신 스레드(phase 1)가 먼저 멈춘 뒤에 이 빈이 멈춘다. 그때는 소비자 스레드가 이미 조용하므로 다른 스레드에서 상태를 읽어도 single-writer가 깨지지 않는다.

**저장은 (a) 파일이다.** Archive 디렉터리에 `matching-snapshot.dat`·`account-snapshot.dat`로 남기고 최신 하나만 유지한다. 쓰는 도중 프로세스가 죽어 반쪽 파일이 남는 것을 막으려고, 임시 파일에 다 쓴 뒤 `ATOMIC_MOVE`로 이름을 바꾼다(`MatchingSnapshotStore.java:98`).

**스냅샷에 담는 것은 상태만이 아니다.**

- `journalPosition` — 복구할 때 replay를 시작할 위치(`AccountSnapshot.java:23`).
- 멱등 캐시 — 매칭은 종목별 `filledOrderTimestampsEpochMillis`(`BookSnapshot.java:14`), 계좌는 `processedSettlementRefs`·`processedRequestIds`(`AccountStateSnapshot.java:32`). 이걸 빼면 복구 직후에 재전송이 처음 보는 요청으로 판정돼 중복 처리된다.
- orderId 카운터 — 계좌만 갖는 `generatorCounter`. ADR-023의 발급 재현이 복구 뒤에도 이어지려면 이 값이 있어야 한다.

**복구 순서**는 스냅샷 파일 읽기 → journal replay → 엔진 `restore()` → 그 뒤 delta를 `recover()` → `start()`다. journal replay는 `readFrom(channel, streamId, fromRecordingId, fromPosition)`으로 읽는다(`AccountJournalReplayer.java:73`). 스냅샷이 가리키는 recording 이전은 건너뛰고, 그 recording은 position부터 읽고, 그 뒤 recording은 전부 읽는다.

## 결과

- 매칭은 `0d98058`(상태 직렬화·복원 왕복)과 `447e902`(파일 저장 + position부터 replay)로 들어갔다. 계좌는 `4cd85f8`(직렬화·복원 왕복)이 먼저 들어갔다.
- 테스트에 함정이 하나 있었다. 매칭과 계좌가 둘 다 멱등이라, 복구할 때 position을 무시하고 0부터 읽어도 최종 상태는 같은 값으로 수렴한다. 그래서 "복구 뒤 상태가 맞다"는 단언은 `readFrom`이 position을 제대로 쓰는지 검증하지 못한다. replay어를 직접 호출해 돌아온 엔트리 목록으로 "position 이전 것이 실제로 빠졌는지"를 본다.
- 정상 종료로 내려간 다음 run에서는 스냅샷 position이 그 recording의 끝과 거의 같고 그 뒤 녹화가 없으므로, replay가 빈 목록을 돌려주고 상태는 스냅샷만으로 선다.
- 이 결정의 한계는 크래시다. 갑자기 죽으면 스냅샷을 찍지 못하므로 마지막 정상 종료 이후의 journal을 전부 replay해야 한다. 복구 시간과 스냅샷 간격의 관계는 재지 않았다. 이 한계를 09-15에 ADR-034가 주기 스냅샷으로 다뤘다.
