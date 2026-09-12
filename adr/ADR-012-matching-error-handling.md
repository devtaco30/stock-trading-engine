# ADR-012 Disruptor 매칭 코어의 오류 처리 정책

## 문제

`matching-disruptor` 는 링버퍼(RingBuffer) 소비자 체인 `handleEventsWith(journal).then(matcher)` 로 주문을 저널에 기록한 뒤 매칭한다. 이 소비자들은 별도 스레드에서 무한 루프로 이벤트를 처리하는데, 처리 중 예외가 나면 두 종류로 성격이 다르다.

1. **도메인 불변식 위반** — 수량 초과 취소, 잘못된 상태 전이 등 `OrderBook`·`OrderEntry` 가 `IllegalArgumentException`·`IllegalStateException` 으로 거부하는 경우. 같은 입력을 재시도해도 결과가 같다.
2. **예상 못한 오류** — `NullPointerException` 등 버그이거나, 이미 어딘가에서 상태가 오염됐다는 신호. 재시도해도 왜 났는지 모르는 채로 같은 자리에서 또 날 수 있다.

Disruptor 는 소비자 핸들러가 예외를 던지면 등록된 `ExceptionHandler` 로 넘긴다. 기본 처리기(`FatalExceptionHandler`)에 맡기면 예외가 잡히지 않는 한 로그도 없이 스레드가 죽을 수 있어, 두 종류를 구분해 명시적으로 처리해야 한다.

## 대안

**A. 결정적 비즈니스 오류(1번)**
- 대안 A-1: 그 이벤트만 거부하고 로그 남긴 뒤 계속 처리한다.
- 대안 A-2: 소비자 전체를 멈춘다.

**B. 예상 못한 오류(2번)**
- 대안 B-1: 로그만 남기고 다음 이벤트를 계속 처리한다(가용성 우선).
- 대안 B-2: 로그 남긴 뒤 예외를 다시 던져 소비자를 멈춘다(fail-fast).

## 트레이드오프

| 기준 | 계속 처리(A-1 / B-1) | fail-fast(A-2 / B-2) |
|---|---|---|
| 가용성 | 한 이벤트 문제로 전체가 멈추지 않음 | 오류 하나로 매칭 전체가 정지 |
| 정합성 | 원인 모르는 상태에서 계속 매칭 → 오염이 이후 체결에 누적될 위험 | 오염된 상태 위에서 더 진행하지 않음 |
| 복구 | 별도 개입 없이 계속 동작(문제 원인 파악이 늦어짐) | 재시작 + 저널 리플레이 필요(이후 단위) |

결정적 비즈니스 오류는 원인이 이미 알려져 있고(수량·상태 규칙 위반) 그 주문 하나만의 문제이므로, 재시도해도 항상 같은 이유로 실패한다. 이걸 계속 재처리하게 두면 poison message(계속 실패하는 메시지가 소비자를 막는 상태)가 된다. 반면 예상 못한 오류는 원인이 불명확해서, 계속 처리를 강행하면 이후 이벤트가 오염된 호가창 상태 위에서 처리되어 잘못된 체결을 만들 위험이 더 크다.

## 결정

**A는 A-1(거부 + 로그 + 계속), B는 B-2(로그 + fail-fast)로 간다.**

- `MatchingEventHandler.onEvent` 는 `IllegalArgumentException`·`IllegalStateException` 두 예외만 잡아 로그를 남기고 그 이벤트를 폐기한다. 이 두 예외 외에는 잡지 않고 그대로 밖으로 던진다.
- 신규 `MatchingExceptionHandler`(`com.lmax.disruptor.ExceptionHandler<OrderEvent>` 구현)를 만들어 `handleEventException` 에서 sequence·이벤트 타입·orderId·예외를 `System.Logger` ERROR 레벨로 남긴 뒤 `RuntimeException` 으로 다시 던져 프로세서를 멈춘다.
- `MatchingEngine` 생성자에서 `handleEventsWith` 배선 전에 `disruptor.setDefaultExceptionHandler(new MatchingExceptionHandler())` 를 명시적으로 호출한다. Disruptor 기본 처리기에 맡기지 않는다.
- 예상 못한 오류로 소비자가 멈춘 뒤의 복구는 프로세스 재시작 + 저널 리플레이로 처리한다. 리플레이 자체(재유도·체크포인트)는 이번 단위 범위 밖이며 이후 단위에서 다룬다.

## 결과

- `matching-disruptor/src/main/.../MatchingExceptionHandler.java` 추가, `MatchingEngine` 생성자에서 명시적으로 등록.
- `JournalGatingTest` 로 저널러(`JournalEventHandler`)가 매처(`MatchingEventHandler`)보다 항상 먼저 도는 것(체결 시점엔 이미 저널에 기록돼 있는 것)을 검증했다. 이 게이팅이 fail-fast 정책의 전제다 — 매칭이 멈춰도 그 직전까지 들어온 주문은 저널에 남아 있어야 재시작 후 리플레이로 복구할 수 있다.
- **인메모리 저널의 한계**: `InMemoryJournal` 은 JVM 프로세스 메모리에만 있다. fail-fast 로 소비자를 멈춘 원인이 JVM 크래시라면 저널 자체도 함께 사라지므로, 지금 구조로는 "재시작 + 저널 리플레이"가 실제로 복구 수단이 되지 못한다. 진짜 복구를 하려면 파일 기반(또는 그 이상의 영속) 저널과 주기적 스냅샷이 필요하며, 이는 `Journal` 인터페이스로 이미 분리해 둔 자리에 이후 단위에서 구현한다.
- 향후 확장으로, 결정적 비즈니스 오류로 폐기된 이벤트를 그냥 버리지 않고 별도 기록(dead-letter 저널)에 남겨 사후 분석·재현에 쓰는 방안을 고려할 수 있다. 이번 단위에서는 로그로만 남긴다.
