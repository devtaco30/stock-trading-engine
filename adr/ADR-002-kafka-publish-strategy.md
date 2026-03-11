# ADR-002: Kafka 발행 전략 — OrderApiService 결합도 분리

## 문제

`OrderApiService`가 매수/매도/취소 처리 후 직접 `KafkaTemplate`을 호출해 메시지를 발행한다.
SRP 위반 — 주문 접수 오케스트레이션 책임 외에 "Kafka로 어떻게 발행하는가"까지 알고 있다.
`afterCommit()` 패턴도 `OrderApiService` 내부에 수동으로 관리된다.

## 대안

### 1. 현재 방식 — OrderApiService 직접 발행
```java
registerAfterCommit(() -> kafkaTemplate.send(topic, key, event));
```

### 2. KafkaEventPublisher 레이어 분리
```java
kafkaEventPublisher.publish(new OrderPlacedEvent(...));
```
OrderApiService → KafkaEventPublisher → KafkaTemplate

### 3. ApplicationEventPublisher + @TransactionalEventListener
```java
// OrderApiService
applicationEventPublisher.publishEvent(new OrderPlacedEvent(...));

// KafkaEventListener
@TransactionalEventListener(phase = AFTER_COMMIT)
public void handle(OrderPlacedEvent event) { kafkaTemplate.send(...); }
```

## 트레이드오프

| 기준 | 대안 1 | 대안 2 | 대안 3 |
|---|---|---|---|
| SRP | 위반 | 준수 | 준수 |
| 결합도 | 높음 (KafkaTemplate 직접 의존) | 중간 | 낮음 (Kafka 존재 모름) |
| 흐름 추적 | 직접 호출, 쉬움 | 직접 호출, 쉬움 | 이벤트 구독, 간접적 |
| afterCommit 보장 | 수동 패턴 | 수동 패턴 | 프레임워크(@TransactionalEventListener) |
| 확장성 | 낮음 | 중간 | 높음 (구독자 추가 시 OrderApiService 무변경) |

## 결정

**대안 3 — ApplicationEventPublisher + @TransactionalEventListener** 채택.

이유:
- OCP: 새로운 이벤트 처리(알림, 감사 로그 등) 추가 시 OrderApiService 무변경
- `@TransactionalEventListener(phase = AFTER_COMMIT)`이 수동 afterCommit() 패턴을 대체
- Phase 3에서 이벤트 종류 증가를 고려하면 가장 확장성 있는 구조
- OrderApiService가 Kafka 인프라에서 완전히 분리

## 결과

- `OrderApiService`에서 `KafkaTemplate` 의존성 제거
- `registerAfterCommit()` 수동 패턴 제거
- `OrderKafkaEventListener` 추가 — Kafka 발행 전담
- 이벤트 흐름: OrderApiService → ApplicationEventPublisher → OrderKafkaEventListener → KafkaTemplate
