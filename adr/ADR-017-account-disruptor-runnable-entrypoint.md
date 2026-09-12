> ⚠️ **이 ADR은 ADR-018로 대체됨 (2026-09-08). 여기 결정을 따르지 말 것.**
> 이 문서가 택한 "plain `main()` + raw kafka + 코어(`account-disruptor`) 안에 main + 하드코딩 시드"는 **폐기됐다.** 실제 구현은 **별도 Spring Boot 호스트 모듈 `account-worker`**(+`matching-worker`)로 갔고, 코어는 프레임워크 0 라이브러리로 유지된다(ADR-018). **진실의 원천은 커밋된 코드다** — account-worker/matching-worker가 Spring Boot로 이미 구현·커밋돼 있다. 이 ADR은 이력 보존용이니 새로 작업할 때 여기 결정을 재적용하지 말 것.

## 문제

`account-disruptor`(계좌 축 워커)는 지금까지 `main()`이 없다 — 테스트가 `AccountEngine`을 직접 띄우고 검증까지 대신해왔다. B4(Kafka 체결 수신)를 붙이면서, 테스트 통과만으로 끝내지 않고 실제로 떠서 남아 있는 프로세스로 만들어야 한다는 요구가 나왔다. `matching-disruptor`도 같은 상태(main 없음)지만, 이번 결정 범위는 `account-disruptor` 하나로 한정한다 — 두 엔진을 한 프로세스로 묶는 통합 앱은 이전에 시도했다가(`order-manager` 모듈) 캐논 로드맵에 없는 단계라 전부 되돌린 바 있다.

실행용 진입점을 만들 때 풀어야 할 두 가지 실무 문제가 있다:
- `AccountEngine`의 내부 스레드와 (이번에 추가하는) `KafkaFillReceiver`의 폴링 스레드는 전부 데몬 스레드다. `main()`이 객체를 만들고 시작만 시킨 뒤 리턴하면, 데몬 스레드만 남아 JVM이 바로 종료된다.
- `Ctrl+C`(SIGINT)로 멈출 때 `receiver.close()` + `engine.shutdown()`을 정상 호출해야 한다.

## 대안

1. **plain `main()`** — 프레임워크 없이, `Properties`로 Kafka 컨슈머 설정 + `CountDownLatch`로 메인 스레드 대기 + `Runtime.getRuntime().addShutdownHook(...)`으로 종료 처리.
2. **`account-disruptor` 자체를 Spring Boot 앱으로 전환** — `@KafkaListener`, `SpringApplication.run()`이 대기·종료 훅을 자동 처리.
3. **별도 얇은 앱 모듈(Spring Boot)이 `account-disruptor`를 의존으로 감쌈** — 엔진 코드는 순수 자바로 남기고, 실행 껍데기만 Spring.

## 트레이드오프

- **필요한 보일러플레이트의 실제 크기**: 데몬 스레드 대기 + 종료 훅은 코드로 두 줄(`CountDownLatch`, `addShutdownHook`)이다. Spring Boot(대안 2·3)가 자동 처리해주는 부분이 맞지만, 그 대가로 컨텍스트 부팅 시간·빈 스캔·설정 파일이 붙는다. 객체 세 개(컨슈머·엔진·리시버)를 순서대로 만들고 부르는 정도의 배선에는 프레임워크가 매는 것보다 얻는 게 적다.
- **대안 1 vs 2**: 대안 2는 `account-disruptor`가 Spring 컨텍스트에 올라타야 한다. 이 모듈은 B2 때부터 "DB도 Spring도 없이 순수 인메모리로 동작한다"는 걸 증명하는 게 목적이었고, 여기서 Spring을 끌어오면 그 전제가 깨진다.
- **대안 1 vs 3**: 대안 3은 엔진 코드 자체는 순수 자바로 지키면서 실행 껍데기만 Spring으로 만든다는 점에서 절충안이다. 다만 지금 필요한 껍데기가 20줄 안쪽이라, 그 정도를 위해 새 모듈(빌드 설정·의존성·배포 단위)을 하나 더 만드는 비용이 이득보다 크다.

## 결정

대안 1을 택한다. `account-disruptor` 안에 plain `main()`(`AccountDisruptorApp` 또는 동급 이름)을 추가한다. Kafka 컨슈머 설정은 `Properties`로 직접 구성하고, 계좌 시드는 지금 단계에서는 DB 연결이 없으므로 하드코딩한다. 메인 스레드는 `CountDownLatch`로 대기하고, `Runtime.getRuntime().addShutdownHook(...)`으로 `receiver.close()` → `engine.shutdown()` 순서를 보장한다. Gradle `application` 플러그인으로 `./gradlew :account-disruptor:run` 실행을 배선한다(빌드 도구 기능일 뿐 런타임 의존성은 추가하지 않는다).

`matching-disruptor`의 실행 진입점, 그리고 두 엔진을 한 프로세스로 묶는 통합 앱은 이 결정의 범위 밖이다 — 전자는 필요해지면 같은 패턴(plain `main()`)을 그대로 적용하면 되고, 후자는 캐논 로드맵상 Phase C(다중 프로세스) 몫이다.

## 결과

- `account-disruptor`가 Spring 없는 라이브러리 성격을 유지한 채, 실행 가능한 프로세스로도 동작하게 된다.
- 계좌 시드가 하드코딩이라 재기동할 때마다 같은 계좌 상태로 초기화된다 — 실제 계좌 데이터 연동(DB 또는 스냅샷)은 이후 단계 몫으로 남는다.
- `KafkaFillReceiver`(B4 본체)는 이 ADR과 별개로 구현하고, `AccountDisruptorApp`은 그걸 조립만 한다.
