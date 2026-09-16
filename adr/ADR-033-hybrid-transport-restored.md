# ADR-033 하이브리드 전송 복원 — 정산은 Kafka 유지 (ADR-032 되돌림, ADR-020 재확인)

- 날짜: 2026-09-14
- 상태: 채택. Kafka 완전 제거 결정(ADR-032 네타, `decision_records/single-stack-aeron-archive.md`)을 대체한다.
- 관련: ADR-020(전송 반전) · 계좌 발신을 로직 스레드 밖으로(`decision_records/account-forwarding-off-thread.md`)

## 문제

ADR-032는 "Kafka를 완전히 제거하고 단일 Aeron + Archive 스택으로 간다"를 결정했다. 그에 따라 정산 왕복(계좌 ↔ settlement-worker)을 Aeron + Archive로 옮기는 마이그레이션 4유닛을 구현하고 리뷰까지 통과시켰다. 코덱(S1), 계좌 수신(S5), settlement 발행 부트스트랩·발행(S4a), Settler의 durable-before-markSettled 컷오버(S4b)다.

forward 경로(계좌 → 정산 미수금 통지)를 설계하던 중 문제가 드러났다. 미수금 통지는 돈이라 never-drop이어야 하는데, 계좌 엔진의 `recover()`는 `NoOpAccountResultListener`로 돌아서 replay가 미수금 통지를 다시 발화하지 않는다. 주문 포워딩처럼 "drop돼도 재기동 replay가 복구한다"가 통하지 않는다. 그래서 never-drop을 (A) 핸들러가 직접 offer = LMAX single-writer 스레드를 블로킹, 또는 (B) SPSC 큐 + 전용 스레드 + 녹화 = Kafka 프로듀서를 손으로 재구현, 둘 중 하나로 풀어야 했다. 이 딜레마는 Kafka를 뗐기 때문에 생긴 것이었다.

backward 경로(S4b)에서도 markSettled 전 durable 보장을 위해 `getRecordingPosition >= position`을 폴링 대기하고 있었다. 이것은 `KafkaTemplate.send(...).get()`의 손 재구현이다.

## 대안

1. ADR-032 유지: 정산까지 Aeron. forward는 (A) 또는 (B)로 never-drop을 손으로 만든다.
2. 하이브리드 복원: 핫패스(주문·체결·체결 반영)는 Aeron + Archive, off-path 정산 왕복은 Kafka 유지. ADR-020이 그은 선으로 돌아간다.

## 트레이드오프

| 기준 | Kafka 유지 (ADR-020) | Kafka 제거 (ADR-032) |
|---|---|---|
| 비동기·논블로킹 전달 | 프로듀서가 제공 | 손 재구현(forward 옵션 B) |
| durable 보장 | 브로커 + acks | recording position await 손구현(S4b) |
| LMAX single-writer 보호 | 막지 않음(fire-and-forget) | A = 블로킹 위험, B = 복잡 |
| 코드 | a2 정산 왕복(원 단위 정합 검증 완료) 그대로 | 되돌리고 재작성 |
| 속도 | ms이지만 T+2 정산에는 무관 | µs이지만 정산에는 불필요 |
| 대가 | 서드파티 2개(Aeron + Kafka) · docker Kafka 유지 | 단일 스택 |

정산은 off-path의 느린(T+2) 비동기 durable 잡 전달이다. 이것이 Kafka의 니치다. 업계도 같은 구도다. 매칭 핫패스는 Aeron·LMAX Disruptor(브로커 없는 UDP, 핫패스에서 블로킹 I/O 금지), post-trade·정산은 Kafka·NATS 같은 메시지 큐로 비동기 디커플이 표준이다. LMAX 본체도 Disruptor 인메모리(핫패스) + 바깥 큐 구조다.

## 결정

대안 2. 핫패스 = Aeron + Archive, off-path 정산 왕복 = Kafka 유지. ADR-032가 정산까지 통일하려 한 것이 과했고 ADR-020의 선이 옳았다.

## 결과

- `git reset --hard 7ef9bbb`로 정산 Aeron 이관 4커밋을 드롭하고 태그 `settlement-aeron-attempt`로 시도를 보존했다. 체결 마이그레이션 U2~U5(핫패스 Aeron)는 그대로 유지했다.
- 정산은 Kafka로 원상복구(`AccountSettlementConsumer` · `PendingSettlementSettler`).
- 도구 니치가 명확해졌다. Aeron = 핫패스 저지연, Kafka = off-path durable 잡 큐. 대가는 서드파티 둘과 docker Kafka 유지다.
- 교훈: 단위 스펙(S1~S4b)에 들어가기 전에 이전 결정(ADR-020)·목표 아키텍처·업계 관행에 먼저 붙였어야 했다. 결정의 전제("Archive가 내구성을 흡수한다")가 핫패스에는 맞고 off-path에는 맞지 않는다는 것을 4유닛을 구현한 뒤에야 확인했다.
