# 의사결정 기록 (ADR)

기술 결정을 마주칠 때마다 `문제 / 대안 / 트레이드오프 / 결정 / 결과` 형식으로 남긴 문서다. 번호는 결정한 순서이고, 뒤 번호가 앞 번호를 대체하면 비고에 적었다. 제목만 읽어도 무엇을 정했는지 보이게 썼다.

★ = 이 레포를 처음 보는 사람이 먼저 읽을 것.

## v1 — DB 비관적 락 기반 (001~012)

계좌 정합성을 DB 락으로 지킨 구현이다. 락 경합과 트랜잭션 경계가 결정의 중심이다.

| ★ | 번호 | 제목 | 비고 |
|---|---|---|---|
| ★ | 001 | [매수 주문 비관적 락 최적화 전략](ADR-001-buy-order-lock-optimization.md) | v1의 기준선 |
| | 002 | [Kafka 발행 전략 — OrderApiService 결합도 분리](ADR-002-kafka-publish-strategy.md) | |
| | 003 | [비관적 락 획득 순서 — 데드락 방지](ADR-003-lock-ordering.md) | |
| | 004 | [API Stateless화 — Kafka 주문 파이프라인](ADR-004-stateless-api-kafka-order-pipeline.md) | |
| | 005 | [Order가 accountId를 직접 보유](ADR-005-order-accountid-denormalization.md) | ADR-010에서 매핑 정정 |
| | 006 | [호가창 자료구조 — TreeMap + ArrayDeque + orderId 인덱스](ADR-006-orderbook-data-structure.md) | |
| | 007 | [체결 반영 멱등성 (tradeId)](ADR-007-fill-idempotency.md) | |
| | 008 | [주문 접수 멱등키를 requestId로 교체](ADR-008-order-idempotency-request-id.md) | |
| | 009 | [매도 주문 가용 수량 검증 (over-sell 방지)](ADR-009-sell-order-available-quantity.md) | |
| | 010 | [Order.accountId를 Snowflake accountId로 일치](ADR-010-order-accountid-snowflake-fk.md) | |
| | 011 | [종목별 토픽을 단일 파티션 분할 토픽으로 전환](ADR-011-orders-fills-single-partitioned-topic.md) | |
| | 012 | [Disruptor 매칭 코어의 오류 처리 정책](ADR-012-matching-error-handling.md) | |

## v2 — single-writer + Aeron (013~036)

같은 도메인을 락 없이 다시 만든 구현이다. 계좌 상태를 메모리에 두고 한 스레드가 고치며, 전송·내구성·복구를 직접 설계한다.

| ★ | 번호 | 제목 | 비고 |
|---|---|---|---|
| | 013 | [주문 전달을 Kafka에서 Aeron으로](ADR-013-aeron-order-transport.md) | |
| ★ | 014 | [계좌 축 single-writer 인메모리 워커 (DB 비관적 락 제거)](ADR-014-account-disruptor-single-writer-worker.md) | v2의 토대 |
| | 015 | [부분 체결 시 예약 장부를 잔량 기준으로 다시 계산](ADR-015-partial-fill-reservation-ledger.md) | |
| | 016 | [계좌 워커에 매도 보유예약 추가](ADR-016-sell-holding-reservation.md) | |
| | 017 | [계좌 워커 실행 진입점 — plain main](ADR-017-account-disruptor-runnable-entrypoint.md) | ADR-018로 대체, 이력 보존용 |
| | 018 | [v2 엔진 호스트 프레임워크 — Spring Boot + Spring Kafka](ADR-018-v2-engine-host-framework.md) | |
| ★ | 019 | [매칭 내구성 — Disruptor + Aeron Archive journal](ADR-019-matching-durability-strategy.md) | Aeron Cluster 기각 근거 |
| ★ | 020 | [전송 전략 반전 — 핫패스는 Aeron + Archive, Kafka는 off-path](ADR-020-hot-path-aeron-kafka-off-path.md) | |
| | 021 | [계좌 상태 영속 — 인메모리 authority + DB 조회모델 프로젝션](ADR-021-account-state-db-projection.md) | |
| | 023 | [orderId는 계좌 워커가 발급하고, 값은 발급 시각을 빼고 (nodeId, 카운터)로만 만든다](ADR-023-order-id-issuer-and-determinism.md) | |
| ★ | 025 | [v2 엔진의 유실 복구는 엔진 노드마다 Aeron Archive journal을 두고 replay로 한다](ADR-025-engine-input-journal-and-replay.md) | |
| | 027 | [계좌 워커는 Kafka로 받은 이벤트를 journal에 적은 뒤에 ack한다](ADR-027-journal-before-kafka-ack.md) | |
| | 028 | [journal에 적지 못하는 동안에는 정산 Kafka 리스너가 멈추고, offset은 넘어가지 않는다](ADR-028-journal-unavailable-backpressure.md) | |
| | 030 | [스냅샷은 정상 종료 때 상태 전체와 journal position을 파일 하나로 남긴다](ADR-030-engine-snapshot-on-graceful-shutdown.md) | ADR-034가 주기 스냅샷으로 확장 |
| | 031 | [계좌 샤딩·조정 — 상황 가정에서 출발, 고정 슬롯 + 정적 라우팅](ADR-031-account-sharding-fixed-slots.md) | |
| | 032 | [v2 Kafka 완전 제거 — 핫패스와 정산을 Aeron + Archive 단일 스택으로](ADR-032-single-stack-aeron-archive.md) | ADR-033으로 되돌림 |
| | 033 | [하이브리드 전송 복원 — 정산은 Kafka 유지](ADR-033-hybrid-transport-restored.md) | ADR-032를 되돌리고 ADR-020 재확인 |
| | 034 | [주기 스냅샷 — 입력 N건마다 찍고, 저장된 구간의 체결 기록은 버린다](ADR-034-periodic-snapshot-and-tradeid-pruning.md) | |
| | 035 | [체결·정산·주문 접수 기록이 무한히 쌓이는 현상 방어](ADR-035-idempotency-ledger-bounds.md) | |
| ★ | 036 | [주문 하나가 잔고 예약까지 끝난 시점에서 v1과 v2를 비교한다](ADR-036-v1-v2-measurement-design.md) | v1/v2 속도 수치의 근거 |

번호가 빈 자리(022·024·026·029)는 `decision_records/`에만 남은 결정이다. ADR로 올릴 때 그 번호를 쓴다.
