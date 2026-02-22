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

## 문서

- [1. 주식 거래 기본 개념](docs/1_주식거래_기본개념.md)
- [2. 4단계 시나리오](docs/2_4단계_시나리오.md)
- [3. API 명세](docs/3_API_명세.md)
- [4. PostgreSQL 선정 근거](docs/4_PostgreSQL_선정_근거.md)

---

## 기술 스택

- Java 17+
- Spring Boot 3.x
- PostgreSQL
- Spring Batch (결제일 배치)

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
