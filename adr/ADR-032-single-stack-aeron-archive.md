# ADR-032 Kafka 완전 제거 — 단일 Aeron + Archive 스택

- 날짜: 2026-09-14
- 상태: 대체됨. ADR-033(하이브리드 전송 복원)이 off-path 정산 부분을 되돌렸다. 핫패스 부분만 유효하다.
- 관련: ADR-019(매칭 내구성) · ADR-020(전송 반전) · ADR-031(샤딩) · ADR-033(되돌림) · 원문 `decision_records/single-stack-aeron-archive.md`

## 문제

ADR-020으로 핫패스(주문 인테이크·매칭·체결 반영)를 Aeron으로 옮기고 Kafka는 off-path에 남겼다. 정산 T+2 왕복과 계좌 조회모델 프로젝션이 그것이다. 그래서 v2는 Aeron과 Kafka라는 무거운 서드파티 둘을 함께 쓴다. 이 세션에서 그 둘을 쓰는 근거가 아직 서는지 스트레스 테스트했다.

Kafka를 남긴 근거를 떠받치던 전제가 그사이 바뀌었기 때문이다. 매칭 엔진의 복구를 위해 Aeron Archive(Aeron 스트림을 디스크에 녹화해 두고 나중에 되감아 읽는 컴포넌트)를 도입했는데, 이 Archive가 내구성까지 제공한다. "Kafka가 신뢰성을 뒷받침한다"는 원래 근거가 이 도입으로 만료됐는지 확인해야 했다.

## 대안

1. **유지**: 핫패스 Aeron + off-path Kafka. ADR-020 그대로 둔다.
2. **Kafka 제거, 단일 Aeron + Archive**: 정산은 settlement-worker가 계좌 Archive를 로그로 읽고 자체 Postgres에 pending을 보관한다. 조회모델 프로젝션은 아직 구현 전이므로 Kafka 없이 설계한다.
3. **Aeron Cluster로 올라가기**: Raft 합의로 복제되는 로그 위에서 엔진을 돌린다(ADR-019가 한 번 기각했다).

## 트레이드오프

Kafka를 남긴 근거 둘을 먼저 재검증했다.

| 남긴 근거 | 재검증 결과 |
|---|---|
| 체결 내구성 | 맨 Aeron은 비내구다. 그러나 엔진이 입력을 Archive 저널에 녹화하고 복구를 replay(녹화된 입력을 처음부터 다시 먹여 상태를 재구성)로 한다. Kafka가 지던 역할을 Archive가 대신한다. 근거 만료. |
| 조회 fan-out | 이 시스템의 조회 소비자는 api → account 하나다. 다중 독립 소비자도 외부 연동도 없어 Kafka의 fan-out·retention을 쓸 데가 없다. 없는 요구를 가정으로 끌어오면 over-engineering이 된다. |

| 기준 | 1. 유지 | 2. Kafka 제거 |
|---|---|---|
| 서드파티 | Aeron + Kafka, docker Kafka 운영 | Aeron 하나 |
| 도구별 쓸 자리 | Kafka가 off-path에만 남아 얇아짐 | Aeron = 저지연 전송, Archive = 내구·복구·로그 |
| 체결 전달 내구성 | 브로커가 제공 | 매칭이 체결을 녹화하고 계좌가 position부터 replay, 자가 관리 |
| 이미 동작하는 코드 | 그대로 | `account-fills`와 정산 왕복을 폐기·재작성, 회귀 위험 |
| 정산 경로 | Kafka 프로듀서의 비동기·durable 전달 | Archive를 소수 소비자가 읽는 로그로 씀 |

순서 보장은 Aeron이 맡는다. Aeron은 UDP를 쓰되 그 위에 재전송과 순서 보장을 더한 프로토콜이라, 한 publication(발행 스트림) 안에서 로그 position이 단조 증가하고, 손실은 NAK 재전송으로 메운다. 정본 순서는 단일 writer 엔진이 처리하고 저널에 적은 순서이고 replay도 그 순서를 따른다. 뒤처진 구독자의 빈 구간은 Archive replay로 메운다.

Aeron Cluster(대안 3)도 다시 봤다. ADR-019가 Cluster를 기각할 때 쓴 근거 중 "Kafka가 이미 복제 로그를 제공하므로 중복"은 Kafka 제거로 소멸한다. 그래도 기각을 유지했다. Cluster의 ClusteredService는 이 프로젝트의 학습 대상인 LMAX Disruptor 링버퍼와 저널·replay를 대체한다. 그리고 Cluster의 가장 큰 값인 무중단 복제 페일오버는 ADR-031이 "무중단은 필요 없고 재시작 + replay로 충분하다"를 상황 가정으로 정해 둬서 쓸 일이 없다.

## 결정

대안 2. Kafka를 제거하고 단일 Aeron + Archive 스택으로 간다.

- 핫패스(주문·체결) = Aeron + Archive. µs 전송에 Archive 저널로 내구성과 복구를 확보한다.
- 정산 T+2 = settlement-worker가 계좌 Archive를 소비하고 pending을 자체 Postgres에 보관한다.
- 조회모델 프로젝션 = Kafka 없이 설계한다. 필요하면 계좌 Archive replay로 읽기 DB를 재구축한다.

대상은 v2의 Kafka 토픽 셋(`account-fills` · `settlement-requests` · `account-settlements`)이다. v1(order-engine · matching-engine · settlement-engine · api)은 손대지 않는다.

## 결과

- 첫 실행은 체결 경로다. `account-fills`를 Kafka에서 Aeron + Archive로 옮겼다(`93e19b5` 체결 코덱, `d8d6866` 매칭 발행, `3d6bf59` 계좌 수신, `7ef9bbb` 죽은 토픽 정리). 이 부분은 지금도 유효하다.
- 정산 왕복 이관은 네 커밋까지 갔다가 되돌렸다(`40b3033` 코덱, `6ecea0d` 계좌 수신, `2ed5c0f` 발행, `877bde8` durable 컷오버). `git reset --hard 7ef9bbb`로 드롭하고 태그 `settlement-aeron-attempt`에 남겼다. 되돌린 이유와 경위는 ADR-033에 있다.
- 되돌림의 핵심은 전제의 적용 범위였다. "Archive가 내구성을 흡수한다"는 이 결정의 전제는 핫패스에만 해당한다. 정산은 느린 비동기 durable 잡 전달이라, Aeron으로 옮기면 Kafka 프로듀서를 손으로 다시 만들게 된다.
- 교훈: 결정을 떠받치는 전제가 바뀌면 그 전제에 기대 내린 결정을 바로 재검증해야 한다. 이 결정에서는 그 재검증이 늦었다. 재검증의 결론을 적용 범위까지 따지지 않고 실행에 넣은 대가가 정산 4커밋 폐기다.
