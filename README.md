# 주식 거래 및 자산 코어 엔진

증권 계좌 도메인(증거금 검증·T+2 결제·미수금)을 다루는 Java 매매 엔진입니다. 하나의 시나리오를 두 가지 방법론으로 구현했습니다. v1은 DB 비관적 락으로 정합성을 지키는 방식으로 구현했고, v2는 빠른 거래 속도를 위해 그 락을 걷어내고 계좌 상태를 프로세스 메모리에 둔 뒤 단일 스레드에서만 다루도록 했습니다.

같은 조건에서 측정하면 주문 접수 지연이 v1 p50 12~13ms · p99 50~56ms, v2 p50 1.9~2.0ms · p99 3.9~4.2ms입니다(도착률 초당 192건, 측정 설계는 ADR-036).

## 어디서부터 읽나

| 보고 싶은 것 | 위치 |
|---|---|
| 결정 32건과 근거 | [adr/README.md](adr/README.md) — ★ 여섯이 v1·v2의 뼈대입니다 |
| v1과 v2 속도 비교 | [ADR-036](adr/ADR-036-v1-v2-measurement-design.md) · 원본 데이터 [loadtest/evidence/](loadtest/evidence/) |
| 도메인 규칙 | [시나리오](https://devtaco30.github.io/stock-trading-engine/_scenario_walkthrough.html) — 100주 매수를 증거금 계산까지 숫자로 따라갑니다 |
| 작업 중 남긴 기록 | [decision_records/README.md](decision_records/README.md) |

## 핵심 시나리오

### 1. 기본 주문 및 체결
실시간 증거금 체크와 체결 처리

### 2. 미수금 발생과 T+2 차감
체결 시 미수금을 기록하고, 결제일에 정산 워커가 차감

---

## v1 — DB 락 기반 (2026-06~08)

api가 HTTP로 주문을 받아 `order-requests` 토픽으로 넘깁니다. order-engine이 잔고를 검증하고 증거금을 예약해 DB에 저장한 뒤 `orders`로 발행하고, matching-engine이 호가창에서 매칭해 `fills`를 발행하며, settlement-engine이 체결을 받아 미수금과 T+2 정산을 처리합니다. 네 앱은 각각 별도 Spring Boot 프로세스입니다. 같은 계좌에 요청이 동시에 들어오는 상황은 DB 락과 멱등키로 처리했습니다.

**락으로 동시 주문을 직렬화합니다.** 매수는 계좌 행, 매도는 보유 행에 `PESSIMISTIC_WRITE`를 걸고, 락은 언제나 Account → Holding 순서로만 잡아 순환 대기를 막습니다. 예약 증거금 합산은 스트림 대신 DB `SUM` 쿼리로 바꿔 락 보유 시간을 줄였습니다. (ADR-001·003)

**호가창은 TreeMap + ArrayDeque입니다.** 가격은 TreeMap으로 최우선 호가를 O(log n)에 꺼내고, 같은 가격 안은 ArrayDeque로 도착 순서를 지킵니다. 취소는 orderId 인덱스로 찾아 표시해 두고 매칭할 때 걸러내는 lazy removal로 처리합니다. (ADR-006)

그 밖에 접수는 requestId, 체결은 tradeId로 중복 반영을 막고(ADR-007·008), 매도는 미체결 잔량을 `SUM(quantity − filledQuantity)`로 합산해 over-sell을 거부합니다(ADR-009).

한계는 부하에서 드러났습니다(2026-09-04 k6, 단일 머신).

| 매수 접수 | 계좌 50개 | 단일 계좌 |
|---|---|---|
| 처리량 | 초당 479건 | 초당 649건 |
| p99 | 1,465ms | 452ms |

락을 쥔 채 DB와 브로커를 오가는 왕복이 원인입니다.

---

## v2 — LMAX Disruptor 링버퍼로 단일 스레드 처리, Aeron으로 전송 (2026-09)

v2는 v1과 같은 정합성 요구를 락 없이 처리합니다. 네 가지입니다.

- **single-writer**: 계좌 잔고와 종목 호가창을 프로세스 메모리에 두고, 계좌는 accountId·종목은 stockCode로 샤딩해 스레드 하나만 고치게 합니다. DB 락을 쓰지 않습니다. (LMAX Disruptor 링버퍼, ADR-014)
- **핫패스 = Aeron**: 주문 접수 → 계좌 검증·예약 → 매칭 → 체결 반영을 Aeron(UDP, µs 단위)으로 연결하고, 각 엔진은 입력 스트림을 Aeron Archive에 녹화합니다. 복구는 스냅샷 + journal replay입니다. (ADR-019·020, journal·스냅샷 상세는 `decision_records/engine-journal-durability.md` · `snapshot.md`)
- **off-path = Kafka**: T+2 정산 왕복과 계좌 조회모델 프로젝션만 Kafka입니다. 한 번 Kafka를 완전히 뗐다가 정산 경로에서 프로듀서를 손으로 재구현하게 되어 되돌렸습니다. (ADR-033)
- **정합성은 락 대신**: 계좌당 소유자 하나, requestId·tradeId·settlementRef 멱등 집합, seq가 더 작은 갱신을 버리는 DB 프로젝션. (ADR-021, requestId 멱등은 `decision_records/id-idempotency-determinism.md`)

### 모듈 (v2 추가분)

| 모듈 | 역할 |
|---|---|
| `matching-disruptor` | 매칭 코어 라이브러리 (Disruptor, 프레임워크 0) |
| `account-disruptor` | 계좌 코어 라이브러리 (검증·예약·체결 반영·멱등) |
| `matching-worker` | 매칭 호스트 앱 (Spring Boot + Aeron Archive) |
| `account-worker` | 계좌 호스트 앱 (Aeron 인테이크, 체결 수신, Kafka 정산·프로젝션 발행) |
| `settlement-worker` | T+2 정산 (Kafka, PostgreSQL) |
| `account-projection-worker` | 계좌 read model 프로젝션 (Kafka → DB) |

api 모듈의 `/api/v2/orders/*`가 v2 진입점이고 `/api/v1/*`은 그대로 v1입니다.

### 측정

v1과 v2에 같은 입력 파일을 주고, 주문을 받아 잔고를 예약한 지점에서 비교했습니다(`loadtest/`, k6).

| 도착률 초당 192건 | v1 | v2 |
|---|---|---|
| 접수 지연 p50 | 12~13ms | 1.9~2.0ms |
| 접수 지연 p99 | 50~56ms | 3.9~4.2ms |

| 포화 처리량 | v1 | v2 |
|---|---|---|
| 컨슈머 3개 | 초당 227건 | — |
| 컨슈머 9개 | 초당 385건 | — |
| 계좌 워커 1개 | — | 초당 4,000건 이상 |

v2 값은 하한입니다. k6 송신이 먼저 막혀 상한은 측정하지 못했습니다. 측정 설계와 한계(부하 생성기가 측정 대상과 같은 노트북에 있어 높은 도착률에서 꼬리 지연이 오염된 것 포함)는 ADR-036에, 확정 결과 원본은 `loadtest/evidence/`에 있습니다.

매칭 코어만 따로 측정하면(JMH, `./gradlew :matching-disruptor:jmh`, 지연 벤치 `:matching-disruptor:latencyBench`) 처리량이 초당 97만~100만 주문, 저부하 지연 p50이 13~30µs입니다(Apple M1 Pro 10코어, OpenJDK 21.0.3, 대기 전략 3종).

## 실행

### v1 (4개 프로세스, Kafka)

```bash
docker compose up -d   # kafka · redis · postgres
./gradlew :api:bootRun               --args='--spring.profiles.active=e2e'
./gradlew :order-engine:bootRun      --args='--spring.profiles.active=e2e'
./gradlew :matching-engine:bootRun   --args='--spring.profiles.active=e2e'
./gradlew :settlement-engine:bootRun --args='--spring.profiles.active=e2e'
```

`e2e` 프로파일은 PostgreSQL `localhost:9702/stock_trading`을 사용하고, 기동할 때 `stocks.sql`·`quotes.sql`·`e2e-seed.sql`로 종목과 데모 계좌를 넣습니다. 프로파일을 주지 않으면 앱마다 H2 in-memory로 뜹니다.

### v2 (3개 프로세스, Aeron UDP)

```bash
docker compose up -d   # kafka · redis · postgres
./gradlew :account-worker:bootRun  --args='--spring.profiles.active=udp'
./gradlew :matching-worker:bootRun --args='--spring.profiles.active=udp'
./gradlew :api:bootRun             --args='--spring.profiles.active=udp'
```

데모 시드(계좌 90001 매수 / 90002 매도, 종목 A900110)가 api와 account-worker에 같이 들어 있습니다.

---

## 문서

구조와 측정 문서는 GitHub Pages에 올려 두었습니다 → **https://devtaco30.github.io/stock-trading-engine/**

- 먼저 읽을 것 — 도메인 기본 개념 · 시나리오
- v1 — 구조 정본 · 주문에서 체결, 정산까지 · Kafka 내부 동작 · 병목 분석
- v2 — 구조 정본 · 링버퍼 매칭 · 체결 전달 분리 · 스냅샷과 tradeId 삭제 · 정산 왕복
- v1과 v2 비교 — 전환 내용 · 두 구현 비교 · 처리량 측정 · 부하 측정 리포트

레포 안에 있는 것: [API 명세](docs/3_API_명세.md) · [PostgreSQL 선정 근거](docs/4_PostgreSQL_선정_근거.md) · [인증 로그인 정리](docs/5_인증_로그인_정리.md)

---

## 기술 스택

- Java 17 (Gradle toolchain) / Spring Boot 3.x
- PostgreSQL · Kafka (KRaft) · Redis
- v2: LMAX Disruptor · Aeron (+ Archive) · JMH · k6

---


## 라이선스

MIT License
