# ADR-028 v2 계좌 워커가 journal에 적지 못하는 동안에는 정산 Kafka 리스너가 멈추고, offset은 넘어가지 않는다

- 날짜: 2026-09-10
- 상태: 채택. 적용 범위는 ADR-027과 같이 정산 컨슈머다.
- 관련: ADR-025(엔진 journal과 replay) · ADR-027(ack 경계) · 원문 `decision_records/engine-journal-durability.md`

## 문제

ADR-027이 계좌 워커의 Kafka 컨슈머에 `blockUntilJournaled`를 넣었다. 이벤트가 journal에 적힌 것을 확인한 뒤에 ack하는 것이다. 그 메서드는 5초 안에 적히지 않으면 `JournalUnavailableException`을 던진다. 이 ADR은 그 예외가 났을 때 컨슈머가 무엇을 할지 정한다.

### 용어

- **리스너 컨테이너(listener container)** — Spring Kafka가 `@KafkaListener` 메서드 하나마다 붙여 주는 실행 단위. 브로커에서 메시지를 당겨 와 그 메서드를 부르고 offset을 관리한다. 이걸 멈추면 그 토픽 소비가 멈춘다.
- **`DefaultErrorHandler`** — 리스너에서 예외가 났을 때 Spring Boot가 아무 설정 없이 쓰는 기본 처리기. 몇 번 재시도한 뒤 로그를 남기고 offset을 넘긴다.
- **DLQ(dead letter queue)** — 처리에 실패한 메시지를 따로 모아 두는 토픽. 나중에 사람이 보고 처리한다.
- **backpressure** — 뒤쪽이 못 받으면 앞쪽을 멈춰 밀리게 두는 것. 버리거나 건너뛰지 않는다.
- **poison 메시지** — 내용 자체가 잘못돼서 몇 번을 다시 처리해도 같은 자리에서 또 실패하는 메시지.
- **changelog** — Kafka Streams가 로컬 상태 저장소의 변경을 기록해 두는 토픽. 인스턴스가 죽으면 이걸 다시 읽어 상태를 복원한다. 이 프로젝트의 journal과 replay가 하는 일과 같다.
- **링버퍼(LMAX Disruptor)** — 고정 크기 배열 하나를 돌려 쓰는 큐. 넣은 순서대로 한 스레드가 꺼내 처리한다. 엔진의 입구다.
- **시퀀스(sequence)** — 링버퍼에 넣은 자리의 번호. 0부터 하나씩 올라간다. 각 핸들러가 "여기까지 처리했다"를 이 번호로 표시한다.
- **Aeron Archive** — Aeron 스트림을 디스크에 녹화해 두고, 지정한 position부터 되감아 읽어 주는 컴포넌트(ADR-019).
- **publication** — Aeron에서 바이트를 내보내는 쪽 핸들. `offer()`로 내보내고, 받는 쪽이 밀리면 음수를 돌려준다.

아무 설정도 하지 않으면 이 예외는 Spring Boot 기본 처리기인 `DefaultErrorHandler`로 간다. 그때 벌어지는 순서는 이렇다.

1. `blockUntilJournaled`가 5초를 기다리다 `JournalUnavailableException`을 던진다. ack하는 줄까지 가지 못한다.
2. 리스너 메서드가 예외로 끝나고 `DefaultErrorHandler`가 그 예외를 받는다.
3. 기본 백오프는 `FixedBackOff(0, 9)`다(`SeekUtils.DEFAULT_BACK_OFF`, spring-kafka 3.3.4). 간격 없이 아홉 번 다시 시도하므로 같은 이벤트를 모두 열 번 처리한다. 시도마다 리스너 메서드를 처음부터 다시 부르니 `blockUntilJournaled`도 매번 5초를 새로 기다린다. 여기서만 50초가 지나고, 그동안 같은 이벤트가 링버퍼에 열 번 들어간다(링은 1024칸이라 이 열 건으로는 차지 않는다).
4. 열 번이 끝나면 기본 recoverer가 로그 한 줄을 남기고 offset을 넘긴다.
5. 컨슈머는 다음 메시지를 받는다.

3번에서 생긴 중복 열 건은 잔고를 틀리게 만들지 않는다. journal이 다시 살아나 비즈니스 핸들러가 그 열 건을 처리하면 `AccountState.applySettlement`가 `processedSettlementRefs`로 걸러 첫 건만 반영한다(`AccountState.java:355`). 다만 journal 핸들러가 비즈니스 핸들러보다 앞에 있으므로 journal에는 열 건이 그대로 적히고, 나중에 replay할 때도 열 건을 재적용한 뒤 아홉 건을 버리게 된다.

문제는 4번이다. offset이 넘어갔으니 그 이벤트는 다시 전달되지 않고, journal에도 적힌 적이 없다. ADR-027이 막은 유실이 자리만 옮긴 셈이다.

그리고 journal을 못 쓰는 상황은 한 건으로 끝나지 않는다. Aeron Archive가 응답하지 않는 동안 들어온 차감이 줄줄이 같은 길을 지나 줄줄이 건너뛰어진다.

## 대안

1. **Spring 기본값.** 재시도한 뒤 건너뛰고 계속 소비한다.
2. **프로세스를 즉시 죽인다.** 예외가 나면 워커를 내린다.
3. **무한 재시도.** 성공할 때까지 같은 메시지를 다시 처리한다.
4. **DLQ로 보낸다.** 실패한 메시지를 별도 토픽에 모으고 계속 소비한다.
5. **일정 시간 pause하고 재시도하다가, 그래도 안 되면 죽인다.**
6. **리스너 컨테이너를 멈춘다.** 예외를 받은 Spring Kafka 에러 처리기가 그 컨테이너를 세워, offset을 넘기지 않은 채로 소비를 중단한다.

## 트레이드오프

| 안 | 유실 | journal이 살아나나 | 프로세스 | 멈춘 것을 어떻게 아나 |
|---|---|---|---|---|
| 1. 기본 스킵 | 있음 | 아니오 | 살아 있음 | 로그에만 남음 |
| 2. 즉시 kill | 없음 | 아니오 | 죽음 | 프로세스가 사라짐 |
| 3. 무한 재시도 | 없음 | 아니오 | 살아 있음 | 로그만 쌓임 |
| 4. DLQ | 없음(옮김) | 아니오 | 살아 있음 | DLQ 토픽 |
| 5. pause 후 kill | 없음 | 아니오 | 결국 죽음 | 프로세스가 사라짐 |
| 6. 컨테이너 정지 | 없음 | 아니오 | 살아 있음 | 컨테이너 상태 |

1과 4는 메시지를 문제로 보는 처리다. 그런데 이 실패의 원인은 저장소에 있다. 메시지 내용에는 문제가 없다. 그래서 메시지를 버리거나 DLQ로 옮겨도 Aeron Archive는 그대로 못 쓰는 상태이고, 다음 메시지도 같은 자리에서 실패한다.

3은 유실이 없다는 점에서 6과 결과가 같다. 다른 것은 상태가 밖에서 보이는지다. 재시도가 도는 동안 컨슈머는 정상으로 보이고 로그만 쌓인다.

2와 5는 처음에 기울었던 안이다. "예외 한 건에 서버를 죽이느냐"는 지적을 받고 다시 봤다. 이 예외가 오는 경우는 journal이 죽었을 때뿐이다. 정상적인 주문 거절은 `RejectReason`으로 빠지므로 여기까지 오지 않는다. 그렇더라도 프로세스를 통째로 내리면 Kafka 소비만 멈추면 될 일에 계좌 워커가 들고 있던 것 전부가 같이 내려간다.

업계 대조가 6을 뒷받침한다. Kafka Streams는 changelog 기록 실패를 치명적으로 보고 인스턴스를 멈춘 뒤, 재기동할 때 changelog를 다시 읽어 상태를 복원한다(KIP-572의 `task.timeout.ms`가 그 정체 시간의 상한이다). 이 프로젝트의 journal이 changelog이고 replay가 복원이므로 같은 모양이다.

## 결정

대안 6이다. 다만 멈추는 조건을 좁힌다. journal이 죽었을 때만 멈추고 나머지 예외는 그대로 둔다.

- journal 실패는 전용 예외 `JournalUnavailableException`으로 던진다(`AccountEngine.java:506`).
- Kafka 에러 처리기를 `CommonDelegatingErrorHandler`로 두고, 기본 위임 대상은 `DefaultErrorHandler`로 둔다. 역직렬화 실패 같은 poison 메시지는 예전처럼 건너뛰고 워커는 계속 돈다.
- 그 위에 `JournalUnavailableException`만 `CommonContainerStoppingErrorHandler`로 위임한다. 이 처리기가 예외를 보고한 리스너 컨테이너의 `stop()`을 부른다. 멈추는 대상은 `account-settlements` 리스너 컨테이너 하나다. 계좌 워커 프로세스는 그대로 떠 있다. `setStopContainerAbnormally(true)`를 줘서 비정상 종료로 표시한다(`AccountKafkaErrorHandlerConfig.java:34`).
- `setCauseChainTraversing(true)`가 필요하다. 리스너에서 난 예외는 `ListenerExecutionFailedException`에 감싸여 오기 때문에, 원인 체인을 따라가야 위임 대상을 고를 수 있다.

## 결과

- 커밋 `870e5ab`. 전용 예외 클래스와 에러 처리기 설정이 들어갔고, 정지가 실제로 불리는지는 `AccountKafkaErrorHandlerConfigTest`가 본다.
- Spring Boot 3.4.0이 `CommonErrorHandler` 빈 하나를 리스너 컨테이너 팩토리에 자동으로 넣는다. `ConcurrentKafkaListenerContainerFactoryConfigurer.setCommonErrorHandler`를 `javap`로 확인하고 이 방식을 골랐다.
- 이 정지가 Kafka 소비만 멈추고 나머지는 평소대로 도는 상태를 뜻하지는 않는다. 5초가 지나도록 시퀀스가 올라오지 않았다는 것은 journal 핸들러가 그 자리에서 나아가지 못했다는 뜻이고, 그 상태는 두 가지다. `append()`는 Aeron publication의 `offer()`가 음수를 돌려주면 `idle()`하며 계속 다시 시도하고, 스트림이 닫혔거나 최대 position을 넘긴 경우에만 예외를 던진다(`AeronArchiveAccountJournal.java:56`). 앞쪽이면 핸들러 스레드가 `append` 안에서 돌고 있고, 뒤쪽이면 `AccountExceptionHandler`가 예외를 다시 던져 그 소비자를 멈춘다.
- 우리가 고른 방식에는 재시도가 없다. `CommonContainerStoppingErrorHandler`는 예외를 받으면 바로 컨테이너를 세우므로, 대안 1에서 생기던 중복 열 건이 여기서는 생기지 않는다.
- 어느 쪽이든 journal 핸들러의 시퀀스가 멈춰 있고, 비즈니스 핸들러는 그 뒤에 게이팅돼 있으므로(ADR-025) Aeron으로 들어오는 주문도 링버퍼에 쌓인 채 처리되지 않는다. 링은 1024칸이고(`AccountEngineConfig.java:38`) 다 차면 링버퍼에 넣는 동작 자체가 막힌다. 프로세스는 살아 있지만 일은 못 하는 상태다. 이 ADR이 보장하는 범위는 "정산 이벤트의 offset을 넘기지 않아 그 이벤트를 잃지 않는다"까지다.
- 멈춘 컨테이너를 다시 띄우는 일은 코드가 하지 않는다. 배포 계층(K8s의 재시작 정책, docker의 restart 옵션)의 몫인데, 이 레포에는 아직 그 배포 설정이 없다. 지금은 사람이 다시 띄워야 하고, 다시 뜨면 ADR-025의 replay가 상태를 맞춘다.
- 이 결정을 쓸 당시 남겨 둔 구멍이 하나 있었다. 내용이 잘못된 이벤트가 엔진 스레드를 죽이면 재시작해서 replay를 돌려도 같은 자리에서 또 죽는다. 이건 나중에 계좌 핸들러가 도메인 예외를 잡아 그 이벤트만 버리도록 고쳤다(`43d91cd`, `AccountEventHandler.java:206`).
