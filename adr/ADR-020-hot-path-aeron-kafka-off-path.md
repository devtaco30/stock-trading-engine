# ADR-020 전송 전략 반전 — 핫패스는 Aeron + Archive, Kafka는 off-path

- 날짜: 2026-09-08 (2026-09-14 ADR-033이 재확인)
- 상태: 채택
- 관련: ADR-018(호스트 프레임워크) · ADR-019(매칭 내구성) · ADR-033(하이브리드 복원) · ADR-032(Kafka 완전 제거 시도, ADR-033이 되돌림)

## 문제

v2 핫패스는 api → account-worker(검증·예약) → matching-worker(매칭) → account-worker(체결 반영)로 이어진다. 이 경로의 전송 수단을 정해야 한다.

초기 설계는 돈이 오가는 구간(주문 인테이크 `order-requests`, 체결 반영 `account-fills`)을 Kafka로 두고 매칭 입력만 Aeron으로 받았다. 그런데 v2의 목표는 정합성과 속도 둘 다이고, 핫패스에 Kafka 홉이 하나라도 남으면 그 홉이 end-to-end 지연을 지배한다. Kafka 홉은 ms 단위, Aeron은 µs 단위라 대략 1000배 차이가 난다. 한 구간만 Aeron으로 바꿔도 앞뒤 Kafka 홉 때문에 그 Aeron이 전체 지연에서 드러나지 않는다.

라우팅 부담이 새로 생기는 것도 아니었다. 매칭축(stockCode)에서 이미 sender가 목적지를 계산해 보내고 있었으므로, 인테이크에서 accountId로 목적지를 계산하는 것도 같은 메커니즘이다.

## 대안

1. **부분 Aeron**: 돈 구간(인테이크·체결 반영)은 Kafka, 매칭 입력만 Aeron. 초기 설계.
2. **핫패스 전부 Aeron + Archive, Kafka는 off-path만**: 인테이크·매칭·체결 반영을 전부 Aeron으로, 내구성은 각 엔진이 입력 스트림을 Aeron Archive에 녹화해 확보. Kafka는 정산 T+2 왕복과 조회모델 프로젝션에만.
3. **전부 Kafka**: 속도 목표를 포기.

## 트레이드오프

| 기준 | 1. 부분 Aeron | 2. 핫패스 전부 Aeron |
|---|---|---|
| end-to-end 지연 | Kafka 홉(ms)이 지배, Aeron 구간이 드러나지 않음 | µs 차수 유지 |
| 내구성 | 브로커가 제공 | 각 엔진이 Archive 녹화·replay를 직접 관리 |
| 두 경로 정렬(주문 경로와 체결 경로가 같은 계좌를 같은 프로세스로) | 수동 Aeron 라우팅과 Kafka 파티셔닝을 맞춰야 함 | 둘 다 Aeron이고 같은 accountId 라우팅 테이블을 쓰면 자동 성립 |
| 리스크 | 낮음 | 돈이 지나는 경로의 내구성을 브로커에 기대지 못하고 self-managed Archive로 관리 |

대안 2의 대가는 내구성 자가 관리다. 학습 목적에는 이 대가가 재료가 된다.

## 결정

대안 2. 핫패스(api → account → matching → 체결 반영)는 전부 Aeron + Archive 저널로 간다. Kafka는 off-path에만 남긴다. 정산 T+2 왕복(`settlement-requests` · `account-settlements`)과 계좌 조회모델 프로젝션(`account-state`)이 그것이다.

ADR-018·019의 "계좌 = Kafka 복제 로그, Aeron = 전송 전용" 부분은 이 결정이 대체한다.

## 결과

- 이미 만들어 둔 `account-fills`(Kafka) 체결 반영 경로는 Aeron으로 이전했다(`93e19b5` 체결 코덱, `d8d6866` 매칭 발행, `3d6bf59` 계좌 수신, `7ef9bbb` 죽은 토픽 정리).
- 바이너리 손코덱이 이 결정에서 시작됐다(`AccountOrderCodec`, `5f611e8`).
- 2026-09-14 Kafka 완전 제거 결정(ADR-032)이 "정산까지 Aeron으로 통일"을 시도했다가 정산 경로에서 Kafka 프로듀서의 비동기·durable 전달을 손으로 재구현하게 되는 문제를 만나 되돌렸고, ADR-033이 이 ADR의 선(핫패스 Aeron / off-path Kafka)을 재확인했다.
- 실측: 3-JVM(api·account-worker·matching-worker)을 UDP로 띄운 라이브 왕복은 로그로 확인(`4905c7b`). 부하는 v1과 v2에 같은 입력 파일을 먹이고 같은 endpoint(주문을 받아 잔고를 예약한 시점)에서 비교했다. 같은 도착률 초당 192건에서 접수 지연이 v1 p50 12~13ms·p99 50~56ms, v2 p50 1.9~2.0ms·p99 3.9~4.2ms다. 원본 데이터는 `loadtest/evidence/`, 측정 설계와 한계는 `decision_records/v1-v2-e2e-measurement.md`에 있다.
- 미측정: Archive 복구 시간, 리샤딩 프로토콜(ADR-031과 `decision_records/c5-multiprocess-coordination.md`에서 설계).
