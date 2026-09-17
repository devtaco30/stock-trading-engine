# 주식 거래 및 자산 코어 엔진

> Java/Spring Boot 기반 증권 계좌 시스템 백엔드

## 프로젝트 개요

주식 거래 시 발생하는 T+2 결제, 증거금, 미수금, 강제 청산 로직을 구현하는 백엔드 엔진입니다.

증권사 계좌 시스템의 핵심 비즈니스 로직을 Java로 정확하게 구현하는 게 목표입니다.

## 핵심 시나리오

### 1. 기본 주문 및 체결
실시간 증거금 체크와 체결 처리

### 2. T+2 시차 활용
매도 예정금액을 담보로 즉시 재매수 (회전매매)

### 3. 미수금 발생 및 연체
결제일 배치로 미수 계좌 상태 전환

### 4. 강제 청산
미수 미납 시 시스템이 자동으로 보유 주식 매도

---

## v2 — 같은 엔진을 락 없이 다시 만들다 (2026-09)

v1은 주문 하나를 처리할 때 계좌 행에 비관적 락(`PESSIMISTIC_WRITE`)을 걸고, 매칭·정산 사이를 Kafka로 이었습니다. 정합성은 지켜졌지만 락을 쥔 채 DB·브로커를 오가는 왕복이 처리 속도의 천장이었습니다(ADR-001. 2026-09-04 k6 실측: 매수 접수 479 req/s(50계좌) / 649 req/s(단일 계좌), p99 1,465 / 452 ms, 단일 머신).

v2는 같은 정합성 요구를 반대 방식으로 만족시킵니다.

- **single-writer**: 계좌 잔고와 종목 호가창을 프로세스 메모리에 두고, 계좌는 accountId·종목은 stockCode로 샤딩해 스레드 하나만 고치게 합니다. 락이 사라집니다. (LMAX Disruptor 링버퍼, ADR-014)
- **핫패스 = Aeron**: 주문 접수 → 계좌 검증·예약 → 매칭 → 체결 반영은 Aeron(UDP, µs 단위)으로 잇고, 각 엔진은 입력 스트림을 Aeron Archive에 녹화합니다. 복구는 스냅샷 + 저널 replay입니다. (ADR-019·020, 저널·스냅샷 상세는 `decision_records/engine-journal-durability.md` · `snapshot.md`)
- **off-path = Kafka**: T+2 정산 왕복과 계좌 조회모델 프로젝션만 Kafka입니다. 한 번 Kafka를 완전히 뗐다가 정산 경로에서 프로듀서를 손으로 재구현하게 되어 되돌렸습니다. (ADR-033)
- **정합성은 락 대신**: 계좌당 소유자 하나, requestId·tradeId·settlementRef 멱등 집합, full-state + seq로 stale 거부하는 DB 프로젝션. (ADR-021, requestId 멱등은 `decision_records/id-idempotency-determinism.md`)

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

### 실행 (3-JVM, UDP)

```bash
docker compose up -d   # kafka · redis · postgres
./gradlew :account-worker:bootRun  --args='--spring.profiles.active=udp'
./gradlew :matching-worker:bootRun --args='--spring.profiles.active=udp'
./gradlew :api:bootRun             --args='--spring.profiles.active=udp'
```

데모 시드(계좌 90001 매수 / 90002 매도, 종목 A900110)가 api와 account-worker에 같이 들어 있습니다. 정산·프로젝션 워커는 `local` 프로파일(PostgreSQL 9702)로 별도 기동합니다.

### 측정

- 매칭 코어 JMH: `./gradlew :matching-disruptor:jmh`, 지연 벤치 `:matching-disruptor:latencyBench` — 처리량 초당 약 97만~100만 주문, 저부하 지연 p50 13~30µs (Apple M1 Pro 10코어, OpenJDK 21.0.3, WaitStrategy 3종)
- 부하: `loadtest/` (k6) — v1과 v2에 같은 입력 파일을 먹이고 같은 끝점(주문을 받아 잔고를 예약한 시점)에서 비교했습니다.
  같은 도착률 초당 192건에서 접수 지연은 **v1 p50 12~13ms · p99 50~56ms**, **v2 p50 1.9~2.0ms · p99 3.9~4.2ms**입니다.
  포화 용량은 v1이 초당 227건(컨슈머 3)에서 385건(컨슈머 9), v2가 초당 4000건 이상입니다 — v2 쪽은 하한입니다. k6 송신이 먼저 막혀 v2의 천장은 재지 못했습니다.
  측정 설계와 한계(부하 생성기가 측정 대상과 같은 노트북에 있어 높은 도착률에서 꼬리 지연이 오염된 것 포함)는 `decision_records/v1-v2-e2e-measurement.md`에, 확정 결과 원본은 `loadtest/evidence/`에 있습니다.

### 결정 기록

설계 결정은 `adr/`에 있습니다. v2 핵심은 ADR-014(single-writer 계좌), 019(매칭 내구성), 020(전송 반전), 021(계좌 영속), 031(샤딩), 033(하이브리드 복원)입니다. 아직 ADR 파일로 옮기지 않은 결정(저널 내구성·스냅샷·orderId 결정론·K8s HA·C5 조정 등)은 `decision_records/`에 원문이 있습니다.

---

## 문서

- [1. 주식 거래 기본 개념](docs/1_주식거래_기본개념.md)
- [2. 4단계 시나리오](docs/2_4단계_시나리오.md)
- [3. API 명세](docs/3_API_명세.md)
- [4. PostgreSQL 선정 근거](docs/4_PostgreSQL_선정_근거.md)

---

## 기술 스택

- Java 17 (Gradle toolchain) / Spring Boot 3.x
- PostgreSQL · Kafka (KRaft) · Redis
- v2: LMAX Disruptor · Aeron (+ Archive) · JMH · k6

---

## 로컬 실행 (Docker + 더미 데이터)

로컬 전용 설정이다. 멘토/동료가 바로 실행해 볼 수 있도록 DB는 Docker로 띄우고, 기동 시 더미 데이터가 자동 생성된다. (운영 배포와는 별개.)

### 1. PostgreSQL 띄우기
```bash
docker compose up -d
```

### 2. 애플리케이션 실행 (프로파일 `local`)
```bash
./gradlew :api:bootRun --args='--spring.profiles.active=local'
```

- **DB**: `localhost:5432`, DB명 `stock_trading`, 사용자/비밀번호 `postgres` / `postgres`
- **스키마**: JPA `ddl-auto: create` 로 테이블 생성
- **stocks**: market 모듈 `stocks.sql` 로 종목 데이터 적재
- **더미 데이터** (profile=local, 최초 1회): `LocalDummyDataInitializer` 가 유저 3명·계좌 3개(잔고 5천만/3천만/2천만, 증거금률 40%)·우량주 보유를 생성. 이미 유저가 있으면 스킵.
- **시세(quotes)**: 기본은 삼성전자 1종목 placeholder. **오늘자 종가 반영**이 필요하면 프로젝트 루트에서 아래 실행 후 앱 재기동.
  ```bash
  pip install -r scripts/requirements.txt
  python scripts/fetch_quotes.py
  ```
  → `market/src/main/resources/quotes.sql` 이 갱신되며, 다음 기동 시 해당 시세가 로드됨.

### 3. DB 중지
```bash
docker compose stop
```

---

## 백엔드 챌린지

- 금액 정합성 (BigDecimal 1원 단위)
- 복잡한 상태 전이 관리
- T+2 결제 시차 처리
- 배치 성능 최적화

---

## 라이선스

MIT License
