---
feature: hybrid-transport-restored
date: 2026-09-14
branch: feat/fill-aeron-migration
commits: []
feeds: [adr, blog]
supersedes: single-stack-aeron-archive.md
reaffirms: transport-inversion.md
---

# 하이브리드 전송 복원 — 정산은 Kafka off-path 유지 (ADR-032 되돌림, ADR-020 재확인)

> 설계 결정(코드 되돌림). ADR-032(`single-stack-aeron-archive.md`)의 "Kafka 완전 제거"를 되돌리고,
> ADR-020(`transport-inversion.md`)의 "핫패스=Aeron / Kafka는 off-path 유지"를 다시 확정한다.
> 정산 Aeron 이관 시도(S1·S5·S4a·S4b)는 `git reset --hard 7ef9bbb`로 드롭하고 태그
> `settlement-aeron-attempt`(→877bde8)로 보존했다.

## ADR 네타

### ADR-033 하이브리드 전송 복원 — 정산 Kafka 유지

- **context(무슨 상황)**: ADR-032가 "Kafka 완전 제거 → 단일 Aeron+Archive"를 결정하고, 그에 따라 정산 왕복(계좌↔정산)을 Aeron+Archive로 옮기는 마이그레이션에 착수했다(코덱 S1, 계좌 수신 S5, settlement 발행 부트스트랩·발행 S4a, Settler durable-before-markSettled 컷오버 S4b — 전부 구현·리뷰 통과). forward(계좌→정산 미수금 통지) 설계를 파던 중 되돌렸다.

- **왜(되돌린 이유)**: 정산은 **off-path의 느린(T+2) 비동기 durable 잡 전달**이다 — 이게 정확히 **Kafka의 니치**다. Aeron으로 옮기면 Kafka 프로듀서가 공짜로 주던 것(비동기·논블로킹 send + 브로커 버퍼·재시도·durable)을 **손으로 재구현**해야 한다:
  - **backward(S4b)**: markSettled 전 durable 보장을 위해 `getRecordingPosition >= position` 폴링 대기 = Kafka `KafkaTemplate.send(...).get()`의 손 재구현.
  - **forward**: 미수금 통지는 돈이라 never-drop이어야 하는데, 계좌 엔진 `recover()`가 `NoOpAccountResultListener`로 돌아 **replay가 미수금을 재발화하지 않는다** → 주문 포워딩처럼 "drop돼도 재기동 replay가 복구"가 안 통함. 그래서 never-drop을 (A)핸들러 직접 offer=LMAX single-writer 블로킹 or (B)SPSC큐+전용스레드+녹화=Kafka 프로듀서 손 재구현, 둘 중 하나로 풀어야 하는 딜레마가 생긴다. **이 딜레마 자체가 Kafka를 떼서 만들어진 문제**다.

- **어떻게(이해득실 — 정산 경로 한정)**:

  | 기준 | Kafka 유지 (ADR-020) | Kafka 제거 (ADR-032) |
  |---|---|---|
  | 비동기·논블로킹 전달 | 프로듀서가 공짜로 | 손 재구현(forward 옵션 B) |
  | durable | 브로커 + acks | recording-position await 손구현(S4b) |
  | LMAX single-writer | 안 막음(fire-and-forget) | A=블록위험 / B=복잡 |
  | 코드 | a2 왕복(원 단위 정합) 그대로 | 되돌리고 재작성 |
  | 속도 | ms지만 T+2라 무관 | µs지만 정산엔 불필요 |
  | 대가 | 서드파티 2개·docker Kafka 유지 | 단일 스택 |

  **결정 = 하이브리드**: 핫패스(주문·체결·체결반영) = Aeron+Archive / **off-path 정산 왕복 = Kafka 유지**. ADR-032가 정산까지 Aeron으로 통일하려 한 게 과도했고, ADR-020이 그은 선이 옳았다.

- **근거 2겹(내부 결정 + 업계 관행)**:
  - **내부**: ADR-020(2026-09-08)이 이미 "핫패스에 Kafka 홉 = 속도 죽음(ms vs µs ~1000배) → 핫패스는 Aeron, **Kafka는 off-path(정산·프로젝션) 유지**, 내구성 자가관리 대가는 핫패스에서만 수용"으로 정했다. ADR-032가 이걸 재검증 없이 뒤집었다.
  - **업계**: 매칭 핫패스 = Aeron/LMAX Disruptor(broker-less UDP, HFT 전용, 핫패스에서 블로킹 I/O 금지). post-trade/정산 파이프라인 = 비동기 디커플, Kafka/NATS 메시지 큐가 표준. LMAX 본체도 Disruptor 인메모리(핫패스) + 바깥 큐 구조이고, Disruptor(핫패스)+Kafka(저널/커맨드 로그) 조합은 실제 사례로 존재. 두 도구를 니치별로 쓰는 게 표준이지 하나로 통일하는 게 아니다.
    - 소스: nadcab order-matching-engine-architecture · aeron.io/other/aeron-sequencer-vs-apache-kafka · sanj.dev aeron-vs-kafka · martinfowler.com/articles/lmax.html · github gc-garcol/lmax-disruptor-bank.

- **무엇을(실제 변경)**: `git reset --hard 7ef9bbb`로 정산 4커밋(S1 `40b3033`·S5 `6ecea0d`·S4a `2ed5c0f`·S4b `877bde8`) 드롭. 태그 `settlement-aeron-attempt`(→877bde8)로 시도 보존(참조·학습용). **체결 마이그레이션 U2~U5(핫패스 Aeron)는 그대로 유지**(ADR-020 핫패스 규칙에 맞음). 정산은 Kafka 원상복구(`AccountSettlementConsumer`·`PendingSettlementSettler` kafkaTemplate). ADR-032는 되돌림 배너, ADR-020은 재확인 배너.

- **결과·트레이드오프**: 서드파티 2개(Aeron+Kafka)·docker Kafka 유지가 대가다. 그 대신 정산의 비동기 durable 전달을 손으로 재구현하지 않고, 도구 니치가 명확해지며(Aeron=핫패스 저지연 / Kafka=off-path durable 잡큐), 이미 원 단위 정합 맞춘 정산 코드를 버리지 않는다. 학습 서사도 "단일 스택 순수성"보다 "니치별 도구 선택"이 더 정직하고 강하다.

- **메타 교훈(반복 지적)**: 단위 스펙(S1~S4b)에 들어가기 전에 **이전 결정(ADR-020)·목표 아키텍처·업계 관행에 먼저 붙였어야** 했다([[feedback_anchor_to_situational_assumption]]·[[feedback_verify_canonical_roadmap]]). Jack의 "순서 보장되냐 / 샤딩 걷어낸다 했잖아 / 기업 리서치도 하랬지"가 이 재검토 요구였는데, 좁게 방어만 하고 4유닛을 구현한 뒤에야 되돌렸다. 결정을 떠받치는 전제(ADR-032의 "Archive가 내구성 흡수")가 핫패스엔 맞고 off-path엔 안 맞는다는 걸 스펙 전에 갈랐어야 했다.

## 블로그 네타

### "Kafka를 뗐다가 되돌린 이야기 — 니치 없는 통일은 손 재구현이다"
- **훅·핵심 주장**: 단일 스택 순수성을 노리고 Kafka를 통째로 뗐더니, 느린 정산 경로에서 Kafka 프로듀서(비동기·논블로킹·durable send)를 Aeron 위에 손으로 다시 짓고 있었다. 되돌렸다. 무거운 도구를 다 없애는 게 아니라, 니치가 맞는 자리에 남기는 게 설계다.
- **context**: ADR-032로 Kafka 완전 제거를 결정하고 정산 왕복 4유닛을 구현하던 중, forward 미수금 발행에서 "never-drop인데 LMAX single-writer를 막지 마라"는 딜레마를 만나 재검토.
- **어떻게(서사·근거)**: ①핫패스=Aeron / off-path 정산=Kafka는 ADR-020이 이미 그은 선인데 재검증 없이 뒤집었던 것 ②정산에서 Kafka가 있던 이유는 fan-out이 아니라 비동기 durable 잡 전달이고 Archive가 그걸 대체 못 함 ③`recover()`가 NoOp라 forward drop을 replay로 못 메우는 걸 발견한 게 트리거 ④업계도 매칭=Aeron/정산=Kafka로 가름. 정직한 실패 서사(4유닛 구현 후 되돌림, 태그로 보존).
- **재료**: 이해득실 표, `settlement-aeron-attempt` 태그, 업계 소스, ADR-020/032 대조.
