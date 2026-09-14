---
feature: single-stack-aeron-archive
date: 2026-09-14
branch: main
commits: []
feeds: [adr, blog]
---

# Kafka 완전 제거 — 단일 Aeron+Archive 스택 (ADR-020을 확장, ADR-019 Cluster 재검토)

> 설계 단계 기록(미구현, target). 이 세션(2026-09-14)의 큰 아키텍처 결정.
> ADR-020(transport-inversion, `transport-inversion.md`)이 "Kafka는 off-path 유지"였는데, 이 ADR이 "Kafka 완전 제거"로 더 나간다.
> 앞선 것: durability(ADR-019, `engine-journal-durability.md`·`snapshot.md`) · 샤딩(ADR-031, `account-sharding-coordination.md`).

## ADR 네타

### ADR-032 Kafka 완전 제거, 단일 Aeron+Archive 스택

- **context(무슨 상황)**: v2는 ADR-020(전송 반전)으로 핫패스(주문·체결)를 Aeron으로 옮기고 Kafka는 off-path(정산 왕복·조회모델 프로젝션)로 남겨뒀다. 즉 Aeron+Kafka 두 서드파티를 쓰는 구조였다. 이 세션에서 "두 큰 서드파티를 쓰는 근거가 탄탄한가"를 스트레스 테스트했다.

- **왜(문제·동기)**: 두 무거운 서드파티는 각자 명확한 니치가 있어야 하고, 니치 밖에 쓰면 관리비용만 내는 쓰레기가 된다(Jack 기준). 그리고 "전제가 바뀌면 그 위 결정을 재검증한다"([[feedback_anchor_to_situational_assumption]]) — Aeron Archive를 replay·failover 위해 도입하면서 **Archive가 내구성까지 제공**하게 됐다. 이 전제 변화로 "Kafka가 신뢰성을 뒷받침한다"는 원래 근거가 만료됐다.

- **어떻게(재검증·결정·트레이드오프)**:

  **재검증 — Kafka의 두 정당화가 이 시스템에선 안 선다:**
  - "체결 내구성 때문에 Kafka" → **맨 Aeron만 비내구**고, Archive가 내구성을 준다(엔진이 입력을 Archive 저널에 기록, 복구는 replay). Kafka의 뒷받침 역할을 Archive가 흡수. 근거 만료.
  - "조회 fan-out 때문에 Kafka" → **이 시스템의 조회 소비자는 api→account 하나뿐.** 다중 독립 소비자도, 외부 연동도 없다. Kafka의 fan-out·retention·multi-consumer 강점을 **쓸 데가 없다.** (없는 요구를 가정으로 끌어오는 건 over-engineering)

  **결정 — Kafka 제거, 단일 Aeron+Archive:**
  - **핫패스(주문·체결)** = Aeron + Archive. µs 전송 + Archive 저널로 내구성·복구.
  - **정산(T+2)** = Kafka도 Aeron 스트리밍도 아님. **settlement-worker가 account Archive를 소비**(로그 읽어 미수금 이벤트 처리) + **자체 DB(Postgres)에 pending 보관.** (정산은 느린 크로스서비스 잡큐라 Aeron 저지연 스트리밍의 니치가 아니다 — 억지로 얹으면 니치 밖 쓰레기. Archive를 소수 소비자가 읽는 로그로 쓴다.)
  - **조회모델 프로젝션** = Kafka-free로 설계(아직 미구현이라 rework 없음). 필요시 account Archive replay로 읽기 DB 재구축.

  **순서 보장(Aeron이 UDP 기반인데?)**: Aeron은 raw UDP가 아니라 UDP 위 reliable·ordered 프로토콜 — 한 publication(스트림) 안에서 로그 position 단조 증가로 순서·gap-free 보장, 손실은 NAK 재전송. 그리고 **정본 순서 = 단일 writer 엔진이 처리·저널한 순서**(replay도 그 순서, 벽시계 아님). 뒤처진 구독자의 gap은 Archive replay로 메운다.

  **Aeron Cluster 재검토(ADR-019 기각 근거 하나가 만료돼 다시 봄)**: ADR-019가 Cluster를 기각한 근거 중 "Kafka가 이미 복제 로그 제공(중복)"은 Kafka 제거로 소멸. 그래도 **Cluster 기각 유지(Jack: 가)** — ① Cluster의 ClusteredService가 우리가 배우려고 직접 만든 **LMAX Disruptor+저널+replay(학습 centerpiece)를 대체·은폐** ② Cluster의 최대 가치=무중단 복제 failover인데 ADR-031에서 **"무중단 불필요, 재시작+replay로 충분"**을 상황 가정으로 못 박음 → Cluster의 강점을 안 씀. 결정론 요건은 이미 만족하나(적합) 위 둘이 여전히 서서 Archive 유지가 맞다. (학습 target을 "Cluster로 실제 거래소 HA 매칭"으로 바꾸면 뒤집힘 — Coinbase가 그 방식. 지금 target은 Disruptor 직접 조립.)

- **무엇을(마이그레이션 범위)**: v2 Kafka 3개 토픽만 대상. **v1(order-engine·matching-engine·settlement-engine·api)은 무관**(v1-db-lock 완성본).
  - `account-fills`(체결 반영, matching-worker→account-worker) → Aeron+Archive
  - `settlement-requests`·`account-settlements`(정산 왕복) → account Archive 소비 + settlement DB
  - `AccountKafkaErrorHandlerConfig`(A-3) · docker-compose Kafka/kafka-ui 제거
  - 프로젝션은 미구현이라 설계만 Kafka-free.

- **결과·트레이드오프**:
  - **얻는 것**: 서드파티 하나(관리비용↓), 니치 명확(Aeron=핫패스 저지연·저널 / Archive=내구·복구·로그), 돈 경로에서 Kafka ms 홉 소멸.
  - **버리는 것**: ① 이미 동작하는 Kafka 코드(account-fills·정산 왕복, a2는 부분체결 반올림까지 원 단위 정합 맞춘 것) 폐기+재작성 회귀 위험 ② 체결 전달 내구성을 Archive 위에 직접 구현(매칭이 체결 기록 + 계좌 replay-from-position) = C6(복구)와 겹치는 큰 작업 ③ Archive를 소수 소비자(정산·복구) 로그로 씀 — purpose-built 아님(소수라 OK).
  - **미결(설계 단계로)**: 체결 Aeron+Archive 전달 내구성 상세, 정산의 account-Archive-소비 재설계, 마이그레이션 순서.

- **메타 교훈**: 결정을 떠받치는 전제가 바뀌면(여기선 Archive 도입) 그 위 결정(Kafka 존치)을 즉시 재검증해야 한다. 이번엔 그 재검증이 늦어 Jack이 단계마다 밀어야 드러났다. [[feedback_anchor_to_situational_assumption]].

## 블로그 네타

### "서드파티를 하나 지웠다 — Kafka를 빼기까지의 재검증"
- **훅·핵심 주장**: Aeron+Kafka 두 서드파티로 시작했지만, Archive가 내구성을 흡수하고 조회 소비자가 api→account 하나뿐임을 확인하자 Kafka의 존재 근거가 사라졌다. 무거운 도구는 니치가 없으면 관리비용만 내는 쓰레기다.
- **context**: 전송 반전(ADR-020)으로 Kafka를 off-path로 밀어둔 뒤, "두 서드파티 근거가 탄탄한가" 스트레스 테스트.
- **어떻게(서사·근거)**: Aeron=속도/Kafka=신뢰성으로 시작 → replay·failover 위해 Archive 도입 → Archive가 내구성까지 주니 Kafka 근거 만료 → 조회는 단일 소비자라 fan-out도 불필요 → Kafka 제거. "전제 바뀌면 재검증" 교훈(늦게 깨달아 단계마다 밀린 과정 포함, 정직한 서사). Aeron이 raw UDP 아니라 reliable-ordered라 순서도 보장.
- **재료**: v2 Kafka 3토픽, Archive 저널, `docs/_sharding_decision.html`.

### "Aeron Cluster를 안 쓴 이유 — 상위 도구가 학습 코어를 먹는다"
- **훅·핵심 주장**: Kafka를 빼면 Aeron Cluster(복제 합의)로 갈 법도 한데 안 갔다. Cluster는 우리가 배우려고 직접 만든 LMAX Disruptor를 통째 대체하고, 그 최대 강점(무중단 복제 failover)은 우리 상황 가정에 없는 요구다.
- **context**: Kafka 제거로 ADR-019의 Cluster 기각 근거 하나가 만료돼 재검토.
- **어떻게(서사·근거)**: Aeron 3층(코어/Archive/Cluster). Cluster=production HA 매칭(Coinbase)이지만 학습 target이 "Disruptor 직접 조립"이고 무중단은 불필요(ADR-031)라 과함. 상위 도구가 항상 정답은 아니다 — 학습 target과 상황 가정이 도구를 정한다.
- **재료**: ADR-019 재검토, ADR-031 상황 가정.
