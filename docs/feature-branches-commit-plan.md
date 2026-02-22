# 기능별 브랜치 생성 및 커밋 계획

작업 트리에 있는 변경을 **기능 단위로만** 스테이징해서 브랜치별로 커밋하는 절차입니다.

---

## 0. 기준 커밋

- **옵션 A**: 이미 `git reset 5fd8496`(mixed) 했다면 → 현재 브랜치 HEAD가 `5fd8496`, 작업 트리에 모든 변경 유지.
- **옵션 B**: 아직 안 했다면 → 아래 브랜치는 **현재 HEAD(412eb9e)** 에서 만들어도 됨. 나중에 `feature/api-base`를 5fd8496으로 리셋하고 싶으면 그때 진행.

아래 예시는 **기준 커밋을 `5fd8496`** 로 두고, `git checkout -b feature/xxx 5fd8496` 로 브랜치를 만드는 방식입니다.  
(현재 HEAD가 412eb9e이면 `5fd8496` 대신 `412eb9e` 로 해도 됨.)

---

## 1. 브랜치 순서 (의존성 고려)

| 순서 | 브랜치 이름 | 설명 |
|------|-------------|------|
| 1 | `feature/account` | 계좌 도메인: Account, Holding, AccountStatusHistory, 리포지토리·서비스, User 제거 |
| 2 | `feature/trading` | 주문 도메인: Order 엔티티·리포지토리, command/service/view |
| 3 | `feature/market` | 시세 도메인: Quote, 주식/시세 리포지토리·서비스, stocks/quotes.sql |
| 4 | `feature/settlement` | 정산 도메인: settlement 모듈 |
| 5 | `feature/account-refactor` | 계좌 API: DTO 분리, Facade, Mapper, AccountController/AccountApiService |
| 6 | `feature/auth` | 인증: user 모듈, Interceptor, Resolver, WebMvcConfig |
| 7 | `feature/snowflake` | ID 생성: core SnowflakeIdGenerator, api Snowflake 설정 |
| 8 | `feature/redis-config` | Redis: 수동 설정, application.yml exclude |
| 9 | `feature/api-implementation` | API 레이어: 주문/체결/시세/정산 컨트롤러·서비스·DTO, 더미 제거 |
| 10 | `feature/test` | 테스트: 통합 테스트, integration 패키지, test resources |
| 11 | `feature/local-setup` | 로컬 환경: docker-compose, README, settings(auth 제거), gradle 등 |

---

## 2. 브랜치별 `git add` 경로

각 브랜치에서는 **해당 행만** 선택해서 `git add` 후 한 번씩만 커밋하면 됩니다.

### 2.1 feature/account (계좌 도메인)

```
account/build.gradle.kts
account/src/main/java/com/flab/stocktradingengine/account/entity/Account.java
account/src/main/java/com/flab/stocktradingengine/account/entity/AccountStatus.java
account/src/main/java/com/flab/stocktradingengine/account/entity/AccountStatusHistory.java
account/src/main/java/com/flab/stocktradingengine/account/entity/Holding.java
account/src/main/java/com/flab/stocktradingengine/account/repository/AccountRepository.java
account/src/main/java/com/flab/stocktradingengine/account/repository/AccountStatusHistoryRepository.java
account/src/main/java/com/flab/stocktradingengine/account/repository/HoldingRepository.java
account/src/main/java/com/flab/stocktradingengine/account/service/
account/src/test/
account/src/main/java/com/flab/stocktradingengine/account/entity/User.java
account/src/main/java/com/flab/stocktradingengine/account/repository/UserRepository.java
```

- `User.java`, `UserRepository.java` 는 **삭제**이므로 `git add` 하면 “삭제”로 스테이징됨.

---

### 2.2 feature/trading (주문 도메인)

```
trading/src/main/java/com/flab/stocktradingengine/trading/entity/Order.java
trading/src/main/java/com/flab/stocktradingengine/trading/repository/OrderRepository.java
trading/src/main/java/com/flab/stocktradingengine/trading/command/
trading/src/main/java/com/flab/stocktradingengine/trading/service/
trading/src/main/java/com/flab/stocktradingengine/trading/view/
```

---

### 2.3 feature/market (시세 도메인)

```
market/src/main/java/com/flab/stocktradingengine/market/entity/Quote.java
market/src/main/java/com/flab/stocktradingengine/market/repository/QuoteRepository.java
market/src/main/java/com/flab/stocktradingengine/market/repository/StockRepository.java
market/src/main/java/com/flab/stocktradingengine/market/mapper/
market/src/main/java/com/flab/stocktradingengine/market/service/
market/src/main/java/com/flab/stocktradingengine/market/view/
market/src/main/resources/application.yml
market/src/main/resources/quotes.sql
market/src/main/resources/stocks.sql
market/src/main/resources/data.sql
market/src/test/
```

**한 번에 add (repo 루트에서):**
```bash
git add \
  market/src/main/java/com/flab/stocktradingengine/market/entity/Quote.java \
  market/src/main/java/com/flab/stocktradingengine/market/repository/QuoteRepository.java \
  market/src/main/java/com/flab/stocktradingengine/market/repository/StockRepository.java \
  market/src/main/java/com/flab/stocktradingengine/market/mapper/ \
  market/src/main/java/com/flab/stocktradingengine/market/service/ \
  market/src/main/java/com/flab/stocktradingengine/market/view/ \
  market/src/main/resources/application.yml \
  market/src/main/resources/quotes.sql \
  market/src/main/resources/stocks.sql \
  market/src/main/resources/data.sql \
  market/src/test/
```

- `data.sql` 은 **삭제**로 스테이징.

---

### 2.4 feature/settlement (정산 도메인)

```
settlement/src/
```

---

### 2.5 feature/account-refactor (계좌 API)

```
api/build.gradle.kts
api/src/main/java/com/flab/stocktradingengine/controller/account/AccountController.java
api/src/main/java/com/flab/stocktradingengine/dto/account/AccountDetailData.java
api/src/main/java/com/flab/stocktradingengine/dto/account/request/
api/src/main/java/com/flab/stocktradingengine/dto/account/response/
api/src/main/java/com/flab/stocktradingengine/dto/account/AccountDetailDto.java
api/src/main/java/com/flab/stocktradingengine/dto/account/AccountDto.java
api/src/main/java/com/flab/stocktradingengine/dto/account/DepositRequest.java
api/src/main/java/com/flab/stocktradingengine/dto/account/HoldingDto.java
api/src/main/java/com/flab/stocktradingengine/dto/account/TransactionResponse.java
api/src/main/java/com/flab/stocktradingengine/dto/account/WithdrawRequest.java
api/src/main/java/com/flab/stocktradingengine/exception/ForbiddenException.java
api/src/main/java/com/flab/stocktradingengine/facade/AccountReadFacade.java
api/src/main/java/com/flab/stocktradingengine/facade/AccountCommandFacade.java
api/src/main/java/com/flab/stocktradingengine/mapper/AccountDetailDtoMapper.java
api/src/main/java/com/flab/stocktradingengine/mapper/AccountDtoMapper.java
api/src/main/java/com/flab/stocktradingengine/mapper/HoldingDtoMapper.java
api/src/main/java/com/flab/stocktradingengine/service/AccountApiService.java
```

**한 번에 add (repo 루트에서):**
```bash
git add \
  api/build.gradle.kts \
  api/src/main/java/com/flab/stocktradingengine/controller/account/AccountController.java \
  api/src/main/java/com/flab/stocktradingengine/dto/account/AccountDetailData.java \
  api/src/main/java/com/flab/stocktradingengine/dto/account/request/ \
  api/src/main/java/com/flab/stocktradingengine/dto/account/response/ \
  api/src/main/java/com/flab/stocktradingengine/dto/account/AccountDetailDto.java \
  api/src/main/java/com/flab/stocktradingengine/dto/account/AccountDto.java \
  api/src/main/java/com/flab/stocktradingengine/dto/account/DepositRequest.java \
  api/src/main/java/com/flab/stocktradingengine/dto/account/HoldingDto.java \
  api/src/main/java/com/flab/stocktradingengine/dto/account/TransactionResponse.java \
  api/src/main/java/com/flab/stocktradingengine/dto/account/WithdrawRequest.java \
  api/src/main/java/com/flab/stocktradingengine/exception/ForbiddenException.java \
  api/src/main/java/com/flab/stocktradingengine/facade/AccountReadFacade.java \
  api/src/main/java/com/flab/stocktradingengine/facade/AccountCommandFacade.java \
  api/src/main/java/com/flab/stocktradingengine/mapper/AccountDetailDtoMapper.java \
  api/src/main/java/com/flab/stocktradingengine/mapper/AccountDtoMapper.java \
  api/src/main/java/com/flab/stocktradingengine/mapper/HoldingDtoMapper.java \
  api/src/main/java/com/flab/stocktradingengine/service/AccountApiService.java
```

- 상단 4개는 추가/수정, 그 다음 6개는 **삭제** 스테이징용.

---

### 2.6 feature/auth

```
git add \
   user/ \
   api/src/main/java/com/flab/stocktradingengine/config/WebMvcConfig.java \
   api/src/main/java/com/flab/stocktradingengine/interceptor/AuthenticationInterceptor.java \
   api/src/main/java/com/flab/stocktradingengine/resolver/ \
   api/src/main/java/com/flab/stocktradingengine/exception/AuthenticationException.java \
   settings.gradle.kts 
```

- `api/build.gradle.kts` 는 2.5에서 이미 넣었으면 여기서는 **수정분만** 필요 시 추가. (user 의존성은 보통 api/build.gradle.kts에 있음 → account-refactor에 이미 포함했으면 auth에서는 생략 가능.)

---

### 2.7 feature/snowflake

```
git add \
   core/build.gradle.kts \
   core/src/main/java/com/flab/stocktradingengine/support/ \
   core/src/test/ \
   api/src/main/java/com/flab/stocktradingengine/config/SnowflakeConfig.java \
   api/src/main/java/com/flab/stocktradingengine/config/SnowflakeNodeIdResolver.java
```

---

### 2.8 feature/redis-config

```
git add \
   api/src/main/java/com/flab/stocktradingengine/config/RedisConfig.java \
   api/src/main/resources/application.yml 
```

- `application.yml` 에는 Redis 관련(exclude 등)만 넣고 싶다면, 이 브랜치에서 해당 부분만 수정한 뒤 이 경로만 add.

---

### 2.9 feature/api-implementation

```
api/src/main/java/com/flab/stocktradingengine/config/LocalDummyDataInitializer.java
api/src/main/java/com/flab/stocktradingengine/controller/order/OrderController.java
api/src/main/java/com/flab/stocktradingengine/controller/order/TradeController.java
api/src/main/java/com/flab/stocktradingengine/controller/market/MarketController.java
api/src/main/java/com/flab/stocktradingengine/controller/settlement/SettlementController.java
api/src/main/java/com/flab/stocktradingengine/dto/market/
api/src/main/java/com/flab/stocktradingengine/dto/order/
api/src/main/java/com/flab/stocktradingengine/dto/trade/
api/src/main/java/com/flab/stocktradingengine/dto/settlement/
api/src/main/java/com/flab/stocktradingengine/exception/GlobalExceptionHandler.java
api/src/main/java/com/flab/stocktradingengine/service/OrderApiService.java
api/src/main/java/com/flab/stocktradingengine/service/QuoteApiService.java
api/src/main/java/com/flab/stocktradingengine/service/SettlementApiService.java
api/src/main/java/com/flab/stocktradingengine/service/StockSearchApiService.java
api/src/main/java/com/flab/stocktradingengine/dummy/DummyAccountData.java
api/src/main/java/com/flab/stocktradingengine/dummy/DummyMarketData.java
api/src/main/java/com/flab/stocktradingengine/dummy/DummyOrderData.java
api/src/main/java/com/flab/stocktradingengine/dummy/DummySettlementData.java
api/src/main/java/com/flab/stocktradingengine/dummy/DummyToken.java
api/src/main/java/com/flab/stocktradingengine/dto/transaction/TransactionDto.java
```

**한 번에 add (repo 루트에서):**
```bash
git add \
  api/src/main/java/com/flab/stocktradingengine/config/LocalDummyDataInitializer.java \
  api/src/main/java/com/flab/stocktradingengine/controller/order/OrderController.java \
  api/src/main/java/com/flab/stocktradingengine/controller/order/TradeController.java \
  api/src/main/java/com/flab/stocktradingengine/controller/market/MarketController.java \
  api/src/main/java/com/flab/stocktradingengine/controller/settlement/SettlementController.java \
  api/src/main/java/com/flab/stocktradingengine/dto/market/ \
  api/src/main/java/com/flab/stocktradingengine/dto/order/ \
  api/src/main/java/com/flab/stocktradingengine/dto/trade/ \
  api/src/main/java/com/flab/stocktradingengine/dto/settlement/ \
  api/src/main/java/com/flab/stocktradingengine/exception/GlobalExceptionHandler.java \
  api/src/main/java/com/flab/stocktradingengine/service/OrderApiService.java \
  api/src/main/java/com/flab/stocktradingengine/service/QuoteApiService.java \
  api/src/main/java/com/flab/stocktradingengine/service/SettlementApiService.java \
  api/src/main/java/com/flab/stocktradingengine/service/StockSearchApiService.java \
  api/src/main/java/com/flab/stocktradingengine/dummy/DummyAccountData.java \
  api/src/main/java/com/flab/stocktradingengine/dummy/DummyMarketData.java \
  api/src/main/java/com/flab/stocktradingengine/dummy/DummyOrderData.java \
  api/src/main/java/com/flab/stocktradingengine/dummy/DummySettlementData.java \
  api/src/main/java/com/flab/stocktradingengine/dummy/DummyToken.java \
  api/src/main/java/com/flab/stocktradingengine/dto/transaction/TransactionDto.java
```

- dto/order, dto/market 등은 **수정** 포함. dummy·TransactionDto 는 **삭제**로 add.

---

### 2.10 feature/test

```
api/src/test/java/com/flab/stocktradingengine/ScenarioIntegrationTest.java
api/src/test/java/com/flab/stocktradingengine/integration/
api/src/test/java/com/flab/stocktradingengine/service/
api/src/test/java/com/flab/stocktradingengine/StockTradingEngineApplicationTest.java
api/src/test/resources/
```

**한 번에 add (repo 루트에서, 백슬래시 뒤 공백 없이):**
```bash
git add \
  api/src/test/java/com/flab/stocktradingengine/ScenarioIntegrationTest.java \
  api/src/test/java/com/flab/stocktradingengine/integration/ \
  api/src/test/java/com/flab/stocktradingengine/service/ \
  api/src/test/java/com/flab/stocktradingengine/StockTradingEngineApplicationTest.java \
  api/src/test/resources/
```

- `ScenarioIntegrationTest.java` 는 **삭제**(루트 테스트) + `integration/` 아래로 **이동**이면, 삭제와 새 경로 둘 다 add.

---

### 2.11 feature/local-setup

```
README.md
docker-compose.yml
gradle/libs.versions.toml
settings.gradle.kts
auth/build.gradle.kts
docs/
```

- `auth/build.gradle.kts` 는 **삭제**. `settings.gradle.kts` 는 auth 제거·user 포함 반영(이미 2.6에서 넣었을 수 있음 → 중복 add 해도 최종 내용으로 한 번만 커밋되면 됨).  
- docs는 원하면 이 브랜치에, 아니면 제외해도 됨.

---

## 3. 실행 흐름 (한 브랜치 예시)

1. **기준 확인**  
   - `git log --oneline -1` → 5fd8496 또는 412eb9e 중 사용할 기준 확인.

2. **첫 브랜치**  
   ```bash
   git checkout -b feature/account 5fd8496
   git add <2.1 경로만>
   git status   # 확인
   git commit -m "feat(account): 계좌 도메인 Holding, AccountStatusHistory, User 제거"
   git push -u origin feature/account
   ```

3. **다음 브랜치로 넘어갈 때**  
   - 작업 트리에 아직 남은 변경이 많으므로 **stash** 후 브랜치 전환, 복원 후 해당 경로만 add:
   ```bash
   git stash -u
   git checkout feature/api-base
   git stash pop
   git checkout -b feature/trading 5fd8496
   git add <2.2 경로만>
   git commit -m "feat(trading): 주문 도메인 Order, command/service/view"
   git push -u origin feature/trading
   ```
   - 3~11번 브랜치도 같은 방식으로 반복.

4. **한 파일이 여러 기능에 걸친 경우**  
   - `api/build.gradle.kts`, `settings.gradle.kts`, `application.yml` 등은 **가장 먼저 필요한 브랜치**에서 한 번만 add (위 표에 그렇게 배치해 둠).  
   - 나중 브랜치에서 같은 파일을 다시 수정했다면, 그 브랜치에서 해당 파일을 한 번 더 add 해서 커밋하면 됨.

---

## 4. 참고

- **공유 파일**  
  - `api/build.gradle.kts`: account-refactor(2.5)에서 먼저 add.  
  - `settings.gradle.kts`: auth(2.6)에서 add.  
  - `application.yml`: redis-config(2.8)에서 add. (market(2.3)에서 market 모듈용 application.yml 수정 가능.)  
- **충돌**  
  - 같은 파일을 여러 브랜치에서 수정하면, 나중에 머지할 때 충돌이 날 수 있음. 순서대로 머지하면 해결 가능.  
- **빌드**  
  - 각 브랜치 커밋 후 `./gradlew build` 로 한 번씩 확인하면 안전함.

이대로 진행해 보시고, 브랜치 이름이나 경로를 바꾸고 싶으면 알려주시면 됩니다.
