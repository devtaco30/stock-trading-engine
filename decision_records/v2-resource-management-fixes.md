---
feature: v2-resource-management-fixes
date: 2026-09-16
branch: fix/v2-resource-management (병합됨, main 2544421)
commits: [9b76720, f264073, 92e70ac, 4764b82]
feeds: [adr, blog]
---

# v2 Aeron/워커 자원 관리 4건

## ADR 네타

### ADR 후보: 매칭 발신 인코딩 버퍼는 필드로 재사용한다(단일 writer 전제)

- **context(무슨 상황)**: Cursor의 1차 코드 리뷰가 `AeronMatchingOrderSender.publish()`를 지적했다 — 주문을 매칭으로 보낼 때마다 `new UnsafeBuffer(ByteBuffer.allocateDirect(256))`로 off-heap(자바 힙 밖에 잡는 네이티브 메모리) 버퍼를 새로 할당하고 있었다.
- **왜(문제·동기)**: off-heap 메모리는 GC가 즉시 회수하지 못하고 `Cleaner`(참조 카운트가 0이 될 때 네이티브 메모리를 정리하는 JDK 내부 메커니즘)에 걸려 있다. 고빈도 주문에서 매 호출 할당은 네이티브 메모리 누적으로 이어질 수 있다. 같은 클래스군(저널 발행 `AeronArchiveAccountJournal`, 체결 발행 `AccountFillPublisher`)은 이미 필드 하나를 재사용하는 패턴이었는데 이 클래스만 예외였다.
- **어떻게(대안·결정·트레이드오프)**: `publish()`는 `publisherThread`라는 전용 스레드 하나만 호출한다(outbox 큐 + 전용 스레드 구조로 이미 단일 writer가 보장돼 있었다). 그래서 버퍼를 인스턴스 필드로 승격해도 동시성 문제가 없다. 대안(스레드마다 버퍼를 두는 ThreadLocal)은 애초에 호출자가 하나뿐이라 불필요한 복잡도였다.
- **무엇을(실제 변경·파일·커밋)**: `AeronMatchingOrderSender.java` — `UnsafeBuffer buffer`를 로컬 변수에서 `private final UnsafeBuffer encodeBuffer` 필드로. 커밋 `9b76720`. 테스트(`AeronMatchingOrderSenderTest`)는 연속 두 번 발행이 같은 버퍼 인스턴스(`isSameAs`)를 쓰는지 mock Publication으로 검증.
- **결과·수치**: 미측정(실제 네이티브 메모리 사용량을 프로파일러로 재지는 않았다 — 코드 패턴 통일과 리뷰 지적 해소가 목적).

### ADR 후보: 자원 close 중 하나가 예외를 던져도 나머지는 계속 닫는다

- **context**: `MatchingFillPublications.close()`가 `byEndpoint.values().forEach(ExclusivePublication::close)`로 여러 Aeron Publication을 한 번에 닫고 있었다.
- **왜**: `forEach`는 처리 중 예외가 나면 그 지점에서 멈추고 전파된다. 즉 목록 중간의 Publication 하나가 `close()`에서 예외를 던지면 그 뒤 나머지는 `close()` 자체가 호출되지 않고 네이티브 자원이 열린 채로 남는다.
- **어떻게**: 개별 `try/catch`로 감싸 전부 시도하고, 실패는 `log.warn`으로 남긴다(빈 catch 금지 — 실패 자체를 숨기지 않는다). 대안(전체를 하나의 try로 감싸 첫 실패에서 예외를 던지고 끝냄)은 지금 코드가 이미 하던 것과 본질적으로 같아 기각.
- **무엇을**: `MatchingFillPublications.java`. 커밋 `f264073`. 테스트는 3개 mock Publication 중 가운데 하나가 `close()`에서 예외를 던지게 해두고, 세 개 전부 `close()`가 호출됐는지 검증(RED: 세 번째가 안 불려 실패 → GREEN: 개별 try/catch 후 전부 호출).
- **결과·수치**: 미측정(단위 테스트로만 검증, 실제 운영에서 이 경로가 발동한 사례는 없음).

### ADR 후보: interrupt() 추가를 검증 후 철회한 사례 — 근거 없는 "완화"를 코드에 넣지 않는다

- **context**: `AccountOrderReceiver`/`AccountFillReceiver`의 `close()`가 `running.set(false)` 후 `pollThread.join(1000ms)`만 하고, 타임아웃을 넘겨도 아무 신호·로그가 없었다. poll 스레드가 안 멈춘 채로 곧 Spring이 `destroyMethod`로 Subscription을 닫으면, 살아있는 poll 스레드가 닫힌 자원을 poll하는 race가 생길 수 있다는 게 Cursor 리뷰 지적이었다.
- **왜**: "타임아웃 초과를 감지·로그하는 코드가 없다"가 확인된 진짜 문제였다.
- **어떻게(대안·결정·트레이드오프 — 이 항목의 핵심)**: 처음 제안한 수정은 `pollThread.interrupt()`를 join 전에 호출해 "레이스 창을 좁힌다"는 것이었다. 그런데 리뷰(39)가 "poll 루프가 실제로 interrupt를 어떻게 처리하는지 확인했냐"고 물었고, Agrona `BackoffIdleStrategy`(poll 스레드가 할 일이 없을 때 쓰는 대기 전략) 소스를 직접 열어 확인한 결과: PARKING 단계는 `LockSupport.parkNanos(parkPeriodNs)`이고 `parkPeriodNs`는 최소 1000ns에서 시작해 두 배씩 늘다가 최대 1,000,000ns(1ms)에서 멈춘다. 즉 **interrupt를 안 보내도 이 코드는 최대 1ms마다 스스로 깨어나 `running` 플래그를 재확인한다** — join(1000ms) 타임아웃까지 갈 일이 애초에 거의 없다. 남은 실패 모드 두 가지(GC 장기 정지, subscription.poll()이나 fragmentHandler 안에서 실제로 오래 걸리는 CPU-bound 코드)는 둘 다 interrupt로 못 줄인다 — GC stop-the-world 구간엔 close()를 부르는 스레드도 같이 멈춰 신호 자체가 전달·처리될 수 없고, CPU-bound 코드에서 `Thread.interrupt()`는 플래그만 세울 뿐 강제로 끊지 못한다. **"레이스를 좁힌다"는 주장을 근거로 대지 못해 interrupt() 호출을 코드에서 뺐다.**
- **무엇을**: 최종 변경은 `pollThread.isAlive()` 체크 + 타임아웃 초과 시 `log.log(Level.WARNING, ...)` 한 줄뿐이다(동작 변화 없음, 순수 관측성). 로깅은 이 모듈(`account-disruptor`, 프레임워크 없는 순수 라이브러리)의 기존 관례인 `java.lang.System.Logger`(JDK 내장, 의존성 0)를 따랐다 — 처음엔 lombok `@Slf4j`를 붙였다가 이 모듈에 없던 `slf4j-api` 의존성을 새로 끌어들이고 기존 `System.Logger`(`AccountEventHandler`·`AccountExceptionHandler`가 이미 쓰던 패턴)와 로깅 API가 이중화되는 문제가 있어 되돌렸다. 커밋 `92e70ac`.
- **결과·수치**: 미측정(로그가 실제로 찍히는 케이스는 아직 관측된 적 없음 — "로그가 울리면 그때 구조적 수정(poll 스레드가 자기 Subscription을 루프 종료 후 직접 닫기)을 검토한다"가 다음 단계로 남음).

### ADR 후보: SmartLifecycle의 running 플래그는 AtomicBoolean으로 통일한다

- **context**: `MatchingSnapshotLifecycle.running`이 plain `boolean`이었다. 같은 패키지의 나머지 Lifecycle들(`AccountOrderReceiverLifecycle` 등)은 전부 `AtomicBoolean`을 쓰는데 이 클래스만 예외였다.
- **왜**: `start()`/`stop()`/`isRunning()`을 서로 다른 스레드가 호출할 수 있는 구조(Spring의 `SmartLifecycle` 처리)에서 plain boolean은 JMM(Java Memory Model) 상 가시성 보장이 없다.
- **어떻게**: 리뷰(39)가 확인한 바로는 Spring의 `SmartLifecycle` 처리 자체가 내부적으로 `CountDownLatch`로 동기화해 지금 경로에서 실제 위험은 낮다 — 그럼에도 형제 클래스들과 패턴을 통일하는 게 churn(불필요한 광범위 변경)보다 낫다고 판단해 `AtomicBoolean`으로 교체했다(형제 전부를 `volatile boolean`으로 바꾸는 대안은 기각).
- **무엇을**: `MatchingSnapshotLifecycle.java` — `private boolean running` → `private final AtomicBoolean running`, `start()`=`set(true)`, `stop()`=쓰기 후 `set(false)`, `isRunning()`=`get()`. 커밋 `4764b82`. 새 테스트는 추가하지 않았다 — 순수 타입 교체로 관측 가능한 동작이 그대로이고, JMM 가시성 버그는 두 스레드 레이스를 결정론적으로 재현할 방법이 없어 단위 테스트로 "고쳐졌다"를 증명할 수 없다는 게 이 프로젝트의 판단 기준(mock/트리비아 테스트보다 실제 로직 테스트를 우선)과 일치했다.
- **결과·수치**: 해당 없음(순수 리팩터).

## 블로그 네타

### "레이스를 좁힌다고 믿었던 코드 한 줄을 지운 이야기"

- **훅·핵심 주장**: 코드 리뷰가 지적한 문제를 고치려고 그럴듯한 수정(`interrupt()` 호출)을 먼저 짰다가, 라이브러리 소스를 직접 읽고 나서 그 수정이 아무것도 안 한다는 걸 발견하고 뺐다 — "고쳤다"고 말하기 전에 실제로 뭘 고쳤는지 코드로 확인하는 습관의 값어치.
- **context**: v2 계좌 워커의 Aeron 수신 스레드가 종료될 때 안전하게 멈추는지가 리뷰 대상이었다. `join(1000ms)`이 타임아웃 나도 아무 신호가 없다는 게 확인된 문제.
- **어떻게(서사·근거)**: ①처음엔 "interrupt를 보내면 대기 중인 스레드를 더 빨리 깨울 수 있다"는 직관으로 `pollThread.interrupt()`를 추가했다. ②리뷰어가 "그 스레드가 실제로 어디서, 어떻게 대기하는지 확인했냐"고 물었다. ③Agrona `BackoffIdleStrategy`(널리 쓰이는 오픈소스 저지연 라이브러리의 대기 전략 구현) 소스를 열어보니, 최대 대기 시간이 1밀리초로 하드코딩돼 있었다 — interrupt 없이도 이미 1ms마다 스스로 깨어나 종료 신호를 확인한다. ④진짜 위험한 두 시나리오(GC 정지, CPU에 묶인 무거운 콜백)는 둘 다 interrupt가 손댈 수 없는 영역이었다. ⑤결국 "레이스를 줄인다"는 주장을 뺀 채로, "적어도 문제가 생기면 로그는 남긴다"는 훨씬 겸손한 수정만 남겼다.
- **재료(커밋·도식·수치)**: 커밋 `92e70ac`. Agrona 2.2.1 `BackoffIdleStrategy.java` 소스 인용(`DEFAULT_MAX_PARK_PERIOD_NS = 1_000_000L`). "정상 케이스는 1ms 안에 자기 기상 / GC stop-the-world엔 close 호출자도 같이 멈춤 / CPU-bound엔 인터럽트 플래그만 세워짐" 세 갈래 논증.
