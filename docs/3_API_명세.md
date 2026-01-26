# API 명세

## 공통 응답 구조

### 성공 응답
```json
{
  "success": true,
  "data": {...},
  "message": "string"  // 선택적 - 성공 메시지
}
```

### 리스트 응답
```json
{
  "success": true,
  "data": {
    "items": [...],
    "totalCount": "int",     // 전체 개수
    "hasNext": "boolean"     // 다음 페이지 존재 여부
  }
}
```

### 에러 응답
```json
{
  "success": false,
  "error": {
    "code": "string",        // 에러 코드 (영문 대문자 + 언더스코어 형식, 예: "AUTH_REQUIRED", "ACCOUNT_NOT_FOUND")
    "message": "string"      // 에러 메시지
  }
}
```

**참고:**
- HTTP Status Code는 여전히 사용 (200: 성공, 400: 잘못된 요청, 401: 인증 실패, 403: 권한 없음, 404: 리소스 없음, 500: 서버 오류)
- 응답 본문의 `error.code`는 내부 에러 코드로, HTTP Status Code와 별개이며 영문 대문자 + 언더스코어 형식 사용
- 응답 본문의 `success` 필드로도 성공/실패를 명확히 구분

---

## 계좌

### 계좌 목록 조회

**Method:** `GET`  
**Path:** `/api/v1/accounts`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Response:**
```json
{
  "success": true,
  "data": {
    "accounts": [                              // 계좌 목록
      {
        "accountId": "string",                 // 계좌 ID
        "balance": "BigDecimal",               // 총 현금 잔액
        "withdrawableBalance": "BigDecimal",   // 출금 가능 잔액 (총 현금 - 홀딩된 증거금)
        "marginRate": "int",                   // 증거금률 (40 | 100)
        "status": "ACTIVE | IN_ARREARS | FROZEN" // 계좌 상태 (ACTIVE: 정상, IN_ARREARS: 미수금 발생, FROZEN: 동결)
      }
    ],
    "totalCount": "int"                        // 전체 계좌 개수
  }
}
```

**처리:**
1. 인증 토큰 검증
2. 인증된 사용자의 모든 계좌 조회
3. 계좌 목록 반환

**예외:**
- 인증 실패: HTTP 401, error.code: "AUTH_REQUIRED", message: "인증이 필요합니다"

---

### 계좌 조회

**Method:** `GET`  
**Path:** `/api/v1/accounts/{accountId}`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Path Parameters:**
- `accountId` (string, 필수) - 계좌 ID

**Response:**
```json
{
  "success": true,
  "data": {
    "accountId": "string",                    // 계좌 ID
    "balance": "BigDecimal",                  // 총 현금 잔액
    "withdrawableBalance": "BigDecimal",      // 출금 가능 잔액 (총 현금 - 홀딩된 증거금)
    "totalAssets": "BigDecimal",              // 총 자산 평가액 (현금 + 보유 주식 평가액)
    "unpaidAmount": "BigDecimal",            // 미결제 금액 (T+2 결제 마감 기한 금액)
    "pendingSellAmount": "BigDecimal",       // 매도 예정금액 (T+2 입금일 금액, 보유종목 매도 시 100% 환원)
    "buyLimit": "BigDecimal",                // 매수 가능 금액 (총 매수력 / (marginRate / 100))
    "marginRate": "int",                      // 증거금률 (40 | 100) - 계좌 설정값
    "status": "ACTIVE | IN_ARREARS | FROZEN"  // 계좌 상태 (ACTIVE: 정상, IN_ARREARS: 미수금 발생, FROZEN: 동결)
  }
}
```

**처리:**
1. 인증 토큰 검증 -> interceptor 에서 처리
2. 인증된 사용자의 계좌 ID와 요청한 accountId 일치 여부 확인
3. 계좌 정보 조회
4. 보유 주식별 현재가 조회 (내부 DB의 시세 테이블에서 조회)하여 총 자산 평가액 계산
5. 대용가 계산 (현재가 × 수량 × 0.7) - 일반 종목 기준 대용가율 70% (한국거래소 규정)
6. 총 매수력 = 출금 가능 현금 + 대용가 + 매도 예정금액
7. 매수 가능 금액 = 총 매수력 / (marginRate / 100) - 계좌 설정된 증거금률 적용
   - marginRate 40%: 총 매수력 / 0.4 (레버리지 가능, 미수 거래 허용)
   - marginRate 100%: 총 매수력 / 1.0 = 총 매수력 (전액 필요, 미수 거래 불가)

**규정 근거:**
- 증거금률 40%: 금융투자협회 신용거래약관 및 신용거래리스크관리모범규준 (일반 종목 기준)
- 대용가율 70%: 한국거래소 증권시장업무규정 (일반 종목 기준)
- 매도예정금액 환원: 금융투자협회 신용거래설명서 (보유종목 매도 시 100% 환원)
- T+2 결제 구조: 자본시장법 제161조, 한국거래소 증권시장업무규정 (표준 결제 주기, 법적으로 강제사항)
- 매도 대금 입금: 항상 T+2일에만 입금됨 (불변의 규칙, 한국예탁결제원 중앙집중결제 시스템)
- 출금 가능 금액: 국내 증권 거래법상 예수금에서 증거금현금을 제외한 금액
- 회전매매(매도대금 담보): 자본시장법 제394조 및 제396조, 금융투자업규정 제4-20조, 금융투자협회 [금융투자회사의 영업 및 업무에 관한 규정]
- 변제 대용 처리: 한국거래소 결제 시스템상 허용된 절차 (결제일과 입금일이 다른 경우 상계 처리)
- 미수금 조기 변제: 결제일(T+2) 전에 현금 입금으로 미수금을 미리 갚을 수 있음 (이자 없음)

**예외:**
- 인증 실패: HTTP 401, error.code: "AUTH_REQUIRED", message: "인증이 필요합니다"
- 권한 없음: HTTP 403, error.code: "FORBIDDEN", message: "본인의 계좌만 조회할 수 있습니다"
- 계좌 없음: HTTP 404, error.code: "ACCOUNT_NOT_FOUND", message: "존재하지 않는 계좌"

---

### 보유 주식 조회

**Method:** `GET`  
**Path:** `/api/v1/accounts/{accountId}/holdings`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Path Parameters:**
- `accountId` (string, 필수) - 계좌 ID

**Response:**
```json
{
  "success": true,
  "data": {
    "holdings": [                            // 보유 주식 목록
      {
        "stockCode": "string",               // 종목 코드
        "stockName": "string",               // 종목명
        "quantity": "int",                   // 보유 수량
        "averagePrice": "BigDecimal",        // 평균 매수가
        "currentPrice": "BigDecimal",        // 현재가
        "evaluationAmount": "BigDecimal",    // 평가 금액 (현재가 × 수량)
        "profit": "BigDecimal",              // 손익 (평가 금액 - 매수 금액)
        "profitRate": "BigDecimal"          // 손익률 (%)
      }
    ],
    "totalCount": "int"                      // 전체 보유 종목 개수
  }
}
```

**처리:**
1. 인증 토큰 검증
2. 인증된 사용자의 계좌 ID와 요청한 accountId 일치 여부 확인
3. 계좌의 보유 주식 조회
4. 각 종목의 현재가 조회 (내부 DB의 시세 테이블에서 조회)
5. 평가 금액 계산
6. 손익 계산

**예외:**
- 인증 실패: HTTP 401, error.code: "AUTH_REQUIRED", message: "인증이 필요합니다"
- 권한 없음: HTTP 403, error.code: "FORBIDDEN", message: "본인의 계좌만 조회할 수 있습니다"

---

### 입금

**Method:** `POST`  
**Path:** `/api/v1/accounts/{accountId}/deposit`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Path Parameters:**
- `accountId` (string, 필수) - 계좌 ID

**Request Body:**
```json
{
  "amount": "BigDecimal"  // 필수, 양수 - 입금액
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "transactionId": "string",           // 거래 ID
    "balance": "BigDecimal",             // 입금 후 현금 잔액
    "transactionAt": "long"              // 거래 일시 (13자리 밀리초 epoch timestamp)
  }
}
```

**처리:**
1. 인증 토큰 검증
2. 인증된 사용자의 계좌 ID와 요청한 accountId 일치 여부 확인
3. 입금액 검증 (양수)
4. **미수금 조기 변제 우선 처리**:
   - 계좌에 미수금이 있고 결제일(T+2)이 아직 오지 않은 경우
   - 입금액을 미수 변제 대금으로 우선 할당
   - 미수금이 모두 상환되면 남은 금액만 현금 잔액 증가
   - 미수금이 없거나 모두 상환된 경우, 입금액 전체를 현금 잔액 증가
5. 입금 내역 기록

**예외:**
- 인증 실패: HTTP 401, error.code: "AUTH_REQUIRED", message: "인증이 필요합니다"
- 권한 없음: HTTP 403, error.code: "FORBIDDEN", message: "본인의 계좌만 입금할 수 있습니다"
- 음수 금액: HTTP 400, error.code: "INVALID_AMOUNT", message: "입금액은 양수여야 합니다"

---

### 출금

**Method:** `POST`  
**Path:** `/api/v1/accounts/{accountId}/withdraw`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Path Parameters:**
- `accountId` (string, 필수) - 계좌 ID

**Request Body:**
```json
{
  "amount": "BigDecimal"  // 필수, 양수 - 출금액
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "transactionId": "string",           // 거래 ID
    "balance": "BigDecimal",             // 출금 후 현금 잔액
    "transactionAt": "long"              // 거래 일시 (13자리 밀리초 epoch timestamp)
  }
}
```

**처리:**
1. 인증 토큰 검증
2. 인증된 사용자의 계좌 ID와 요청한 accountId 일치 여부 확인
3. 출금 가능 금액 계산 (총 현금 - 홀딩된 증거금) - 국내 증권 거래법상 예수금에서 증거금현금을 제외한 금액
4. 출금액 검증
5. 미수금 있으면 출금 불가
6. 현금 잔액 감소
7. 출금 내역 기록

**예외:**
- 인증 실패: HTTP 401, error.code: "AUTH_REQUIRED", message: "인증이 필요합니다"
- 권한 없음: HTTP 403, error.code: "FORBIDDEN", message: "본인의 계좌만 출금할 수 있습니다"
- 잔액 부족: HTTP 400, error.code: "INSUFFICIENT_BALANCE", message: "출금 가능 금액 부족"
- 미수금 있음: HTTP 400, error.code: "ARREARS_EXISTS", message: "미수금이 있어 출금 불가"

---

## 주문

### 매수 주문

**Method:** `POST`  
**Path:** `/api/v1/orders/buy`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Request Body:**
```json
{
  "accountId": "string",              // 필수 - 계좌 ID
  "stockCode": "string",              // 필수 - 종목 코드 (한국거래소 공식 6자리 숫자 코드)
  "orderType": "LIMIT | MARKET",      // 필수 - 주문 유형 (지정가/시장가)
  "price": "BigDecimal",              // 지정가인 경우 필수 - 주문 가격
  "quantity": "int"                   // 필수, 양수 - 주문 수량
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "orderId": "string",                  // 주문 ID
    "status": "PENDING",                  // 주문 상태
    "orderAt": "long",                    // 주문 일시 (13자리 밀리초 epoch timestamp)
    "heldMargin": "BigDecimal"           // 홀딩된 증거금 (필요 금액 × (marginRate / 100))
  }
}
```

**처리:**
1. 인증 토큰 검증
2. 인증된 사용자의 계좌 ID와 요청한 accountId 일치 여부 확인
3. 계좌 상태 체크 (동결 계좌는 주문 불가)
4. 내부 DB의 종목 마스터 테이블에서 종목 존재 여부 확인
5. 현재가 조회 (시장가인 경우, 내부 DB의 시세 테이블에서 조회)
6. 필요 금액 계산
7. 증거금 계산 (필요 금액 × (marginRate / 100)) - 계좌 설정된 증거금률 적용
8. 매수 가능 금액 체크
   - marginRate 40%: 매수 가능 금액 = 총 매수력 / 0.4
   - marginRate 100%: 매수 가능 금액 = 총 매수력 (필요 금액 전체가 총 매수력 이하여야 함)
9. 증거금 홀딩 처리 (국내 증권 거래법상):
   - 출금 가능 현금을 우선 사용
   - 출금 가능 현금이 부족하면 매도 예정금액에서 담보 설정 (보유종목 매도건이므로 100% 재사용 가능)
   - 홀딩된 증거금은 출금 가능 금액에서 제외
10. 주문 생성 (PENDING 상태)

**예외:**
- 인증 실패: HTTP 401, error.code: "AUTH_REQUIRED", message: "인증이 필요합니다"
- 권한 없음: HTTP 403, error.code: "FORBIDDEN", message: "본인의 계좌로만 주문할 수 있습니다"
- 계좌 동결: HTTP 400, error.code: "ACCOUNT_FROZEN", message: "거래 정지된 계좌"
- 종목 없음: HTTP 404, error.code: "STOCK_NOT_FOUND", message: "존재하지 않는 종목"
- 매수력 부족: HTTP 400, error.code: "INSUFFICIENT_BUYING_POWER", message: "매수 가능 금액 부족"
- 음수 수량: HTTP 400, error.code: "INVALID_QUANTITY", message: "수량은 양수여야 합니다"

---

### 매도 주문

**Method:** `POST`  
**Path:** `/api/v1/orders/sell`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Request Body:**
```json
{
  "accountId": "string",              // 필수 - 계좌 ID
  "stockCode": "string",              // 필수 - 종목 코드 (한국거래소 공식 6자리 숫자 코드)
  "orderType": "LIMIT | MARKET",      // 필수 - 주문 유형 (지정가/시장가)
  "price": "BigDecimal",              // 지정가인 경우 필수 - 주문 가격
  "quantity": "int"                   // 필수, 양수 - 주문 수량
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "orderId": "string",              // 주문 ID
    "status": "PENDING",              // 주문 상태
    "orderAt": "long"                 // 주문 일시 (13자리 밀리초 epoch timestamp)
  }
}
```

**처리:**
1. 인증 토큰 검증
2. 인증된 사용자의 계좌 ID와 요청한 accountId 일치 여부 확인
3. 보유 주식 조회
4. 보유 수량 체크
5. 주문 생성 (PENDING 상태)

**예외:**
- 인증 실패: HTTP 401, error.code: "AUTH_REQUIRED", message: "인증이 필요합니다"
- 권한 없음: HTTP 403, error.code: "FORBIDDEN", message: "본인의 계좌로만 주문할 수 있습니다"
- 보유 주식 없음: HTTP 400, error.code: "HOLDING_NOT_FOUND", message: "보유하지 않은 종목"
- 수량 부족: HTTP 400, error.code: "INSUFFICIENT_QUANTITY", message: "보유 수량 부족"

---

### 체결 처리 로직 (내부)

**처리 주기:** 실시간 또는 주기적 (예: 1분마다)

**매수 주문 체결 처리:**
1. PENDING 상태인 매수 주문 조회
2. 현재가 조회 (지정가 주문: 주문 가격과 현재가 비교, 시장가 주문: 즉시 체결)
3. 체결 조건 만족 시:
   - 주문 상태를 FILLED로 변경
   - 체결가 및 체결 수량 기록
   - 보유 주식에 종목 추가 (없으면 신규 생성, 있으면 수량 및 평균 매수가 업데이트)
   - 미결제 금액 생성 (체결 금액 - 홀딩된 증거금, T+2 결제 마감 기한)
   - 홀딩된 증거금은 그대로 유지 (T+2 결제 시 사용)
   - 증거금 홀딩 시 매도 예정금액에서 담보 설정한 경우, 매도 예정금액 차감
4. 체결 실패 시 주문 상태 유지 (PENDING)

**매도 주문 체결 처리:**
1. PENDING 상태인 매도 주문 조회
2. 현재가 조회 (지정가 주문: 주문 가격과 현재가 비교, 시장가 주문: 즉시 체결)
3. 체결 조건 만족 시:
   - 주문 상태를 FILLED로 변경
   - 체결가 및 체결 수량 기록
   - 보유 주식에서 수량 차감 (0이 되면 삭제)
   - 매도 예정금액 생성 (체결 금액, T+2 입금일)
     - 보유종목 매도건이므로 100% 재사용 가능 (매수 가능 금액 계산에 포함)
   - **가상 잔고(buyingPower) 즉시 증가**: 매도 체결 직후 매수 가능 금액 계산에 즉시 반영
   - **실제 현금(actualCash)은 T+2일 배치 작업에서만 업데이트**: 매도 대금은 법적으로 T+2일에만 입금됨
4. 체결 실패 시 주문 상태 유지 (PENDING)

---

### 주문 취소

**Method:** `DELETE`  
**Path:** `/api/v1/orders/{orderId}`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Path Parameters:**
- `orderId` (string, 필수) - 주문 ID

**Response:**
```json
{
  "success": true,
  "data": {
    "orderId": "string",                    // 취소된 주문 ID
    "returnedMargin": "BigDecimal"          // 반환된 증거금 (매수 주문인 경우)
  }
}
```

**처리:**
1. 인증 토큰 검증
2. 주문 조회
3. 인증된 사용자의 계좌 ID와 주문의 계좌 ID 일치 여부 확인
4. 주문 상태 체크 (PENDING만 취소 가능)
5. 매수 주문인 경우 홀딩된 증거금 반환
6. 주문 상태를 CANCELLED로 변경

**예외:**
- 인증 실패: HTTP 401, error.code: "AUTH_REQUIRED", message: "인증이 필요합니다"
- 권한 없음: HTTP 403, error.code: "FORBIDDEN", message: "본인의 주문만 취소할 수 있습니다"
- 주문 없음: HTTP 404, error.code: "ORDER_NOT_FOUND", message: "존재하지 않는 주문"
- 이미 체결: HTTP 400, error.code: "ORDER_ALREADY_FILLED", message: "체결된 주문은 취소 불가"

---

### 주문 내역 조회

**Method:** `GET`  
**Path:** `/api/v1/accounts/{accountId}/orders`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Path Parameters:**
- `accountId` (string, 필수) - 계좌 ID

**Query Parameters:**
- `status` (string, 선택) - PENDING, FILLED, CANCELLED
- `startAt` (long, 선택) - 시작 일시 (13자리 밀리초 epoch timestamp), 미지정 시 최근 1개월 전
- `endAt` (long, 선택) - 종료 일시 (13자리 밀리초 epoch timestamp), 미지정 시 현재 시간

**Response:**
```json
{
  "success": true,
  "data": {
    "orders": [                              // 주문 목록
      {
        "orderId": "string",                 // 주문 ID
        "stockCode": "string",                // 종목 코드
        "stockName": "string",                // 종목명
        "side": "BUY | SELL",                 // 매수/매도 구분
        "orderType": "LIMIT | MARKET",        // 주문 유형 (지정가/시장가)
        "price": "BigDecimal",               // 주문 가격
        "quantity": "int",                    // 주문 수량
        "status": "PENDING | FILLED | CANCELLED", // 주문 상태
        "orderAt": "long",                    // 주문 일시 (13자리 밀리초 epoch timestamp)
        "filledAt": "long"                   // 체결 일시 (13자리 밀리초 epoch timestamp, 체결된 경우)
      }
    ],
    "totalCount": "int",                     // 전체 주문 개수
    "hasNext": "boolean"                     // 다음 페이지 존재 여부
  }
}
```

**처리:**
1. 인증 토큰 검증
2. 인증된 사용자의 계좌 ID와 요청한 accountId 일치 여부 확인
3. 기간 설정: startAt/endAt 미지정 시 최근 1개월로 자동 설정
4. 조건에 맞는 주문 조회
5. 최신순 정렬

**예외:**
- 인증 실패: HTTP 401, error.code: "AUTH_REQUIRED", message: "인증이 필요합니다"
- 권한 없음: HTTP 403, error.code: "FORBIDDEN", message: "본인의 계좌만 조회할 수 있습니다"

---

## 보유 현황

### 거래 내역 조회

**Method:** `GET`  
**Path:** `/api/v1/accounts/{accountId}/transactions`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Path Parameters:**
- `accountId` (string, 필수) - 계좌 ID

**Query Parameters:**
- `startAt` (long, 선택) - 시작 일시 (13자리 밀리초 epoch timestamp), 미지정 시 최근 1개월 전
- `endAt` (long, 선택) - 종료 일시 (13자리 밀리초 epoch timestamp), 미지정 시 현재 시간

**Response:**
```json
{
  "success": true,
  "data": {
    "transactions": [                        // 거래 내역 목록
      {
        "transactionAt": "long",             // 거래 일시 (13자리 밀리초 epoch timestamp)
        "stockCode": "string",               // 종목 코드
        "stockName": "string",               // 종목명
        "side": "BUY | SELL",                // 매수/매도 구분
        "quantity": "int",                    // 거래 수량
        "executionPrice": "BigDecimal",       // 체결가
        "transactionAmount": "BigDecimal"    // 거래 금액 (체결가 × 수량)
      }
    ],
    "totalCount": "int",                     // 전체 거래 내역 개수
    "hasNext": "boolean"                     // 다음 페이지 존재 여부
  }
}
```

**처리:**
1. 인증 토큰 검증
2. 인증된 사용자의 계좌 ID와 요청한 accountId 일치 여부 확인
3. 기간 설정: startAt/endAt 미지정 시 최근 1개월로 자동 설정
4. 조건에 맞는 체결 내역 조회
5. 최신순 정렬

**예외:**
- 인증 실패: HTTP 401, error.code: "AUTH_REQUIRED", message: "인증이 필요합니다"
- 권한 없음: HTTP 403, error.code: "FORBIDDEN", message: "본인의 계좌만 조회할 수 있습니다"

---

## 결제

**미결제 vs 미수금 구분:**
- **미결제**: T+2 결제일이 아직 오지 않았거나, 결제일이 왔지만 아직 정산 처리 전인 상태 (정상적인 대기 상태)
- **미수금**: T+2 결제일이 지났는데 돈이 없어서 발생한 빚 (비정상 상태, 이자 발생)

### 미결제 내역 조회

**Method:** `GET`  
**Path:** `/api/v1/accounts/{accountId}/unpaid`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Path Parameters:**
- `accountId` (string, 필수) - 계좌 ID

**Response:**
```json
{
  "success": true,
  "data": {
    "unpaidList": [                         // 미결제 내역 목록
      {
        "settlementId": "string",            // 결제 ID
        "stockCode": "string",               // 종목 코드
        "settlementDate": "LocalDate",       // 결제 마감 기한 (T+2, 날짜만)
        "amount": "BigDecimal",              // 결제 금액 (T+2일에 지불해야 할 잔금)
        "status": "PENDING | SETTLED"        // 결제 상태 (PENDING: 미결제, SETTLED: 결제 완료)
      }
    ],
    "totalCount": "int"                     // 전체 미결제 내역 개수
  }
}
```

**처리:**
1. 인증 토큰 검증
2. 인증된 사용자의 계좌 ID와 요청한 accountId 일치 여부 확인
3. 계좌의 미결제 내역 조회 (T+2 결제 마감 기한이거나 결제일이 지났지만 아직 정산 처리 전인 내역)
4. 결제 마감 기한순 정렬

**예외:**
- 인증 실패: HTTP 401, error.code: "AUTH_REQUIRED", message: "인증이 필요합니다"
- 권한 없음: HTTP 403, error.code: "FORBIDDEN", message: "본인의 계좌만 조회할 수 있습니다"

---

## 미수금

**미수금이란:**
- T+2 결제일에 계좌에 돈이 없어서 발생한 빚
- 결제일이 지난 후에도 돈을 채우지 못하면 미수금으로 전환됨
- 미수금 발생 시 이자가 누적되고, 계좌가 동결되며, 강제 청산(반대매매) 위험이 있음

### 미수금 조회

**Method:** `GET`  
**Path:** `/api/v1/accounts/{accountId}/arrears`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Path Parameters:**
- `accountId` (string, 필수) - 계좌 ID

**Response:**
```json
{
  "success": true,
  "data": {
    "arrearsId": "string",                  // 미수금 ID
    "amount": "BigDecimal",                 // 미수금 금액 (결제일에 지불하지 못한 빚)
    "occurredDate": "LocalDate",            // 발생일 (T+2 결제일, 날짜만)
    "overdueDays": "int",                   // 연체 일수 (현재일 - 발생일)
    "accumulatedInterest": "BigDecimal",    // 누적 이자 (연체 일수에 따라 누적)
    "status": "IN_ARREARS | SETTLED"        // 미수금 상태 (IN_ARREARS: 미상환, SETTLED: 상환 완료)
  }
}
```

또는 미수금이 없는 경우:
```json
{
  "success": true,
  "data": null
}
```

**처리:**
1. 인증 토큰 검증
2. 인증된 사용자의 계좌 ID와 요청한 accountId 일치 여부 확인
3. 계좌의 미수금 조회 (결제일에 돈이 없어 발생한 빚)
4. 연체 일수 계산 (현재일 - 발생일)

**예외:**
- 인증 실패: HTTP 401, error.code: "AUTH_REQUIRED", message: "인증이 필요합니다"
- 권한 없음: HTTP 403, error.code: "FORBIDDEN", message: "본인의 계좌만 조회할 수 있습니다"
- 미수금 없음: HTTP 200, data: null (정상 응답, 미수금이 없는 경우)

---

### 미수금 상환

**Method:** `POST`  
**Path:** `/api/v1/arrears/{arrearsId}/repay`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Path Parameters:**
- `arrearsId` (string, 필수) - 미수금 ID

**Request Body:**
```json
{
  "amount": "BigDecimal"  // 필수, 양수 - 상환 금액
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "remainingAmount": "BigDecimal",        // 상환 후 잔여 미수금
    "repaymentAt": "long"                   // 상환 일시 (13자리 밀리초 epoch timestamp)
  }
}
```

**처리:**
1. 인증 토큰 검증
2. 미수금 조회
3. 인증된 사용자의 계좌 ID와 미수금의 계좌 ID 일치 여부 확인
4. 현금 잔액 체크
5. 현금 차감
6. 미수금 감소
7. 미수금 0원이면:
   - 미수금 상태를 SETTLED로 변경
   - 계좌 상태를 ACTIVE로 변경
8. 상환 내역 기록

**예외:**
- 인증 실패: HTTP 401, error.code: "AUTH_REQUIRED", message: "인증이 필요합니다"
- 권한 없음: HTTP 403, error.code: "FORBIDDEN", message: "본인의 미수금만 상환할 수 있습니다"
- 현금 부족: HTTP 400, error.code: "INSUFFICIENT_BALANCE", message: "현금 잔액 부족"
- 미수금 없음: HTTP 404, error.code: "ARREARS_NOT_FOUND", message: "존재하지 않는 미수금"

---

## 시세

**종목 코드 규칙:**
- 한국거래소(KRX) 공식 종목 코드 사용 (6자리 숫자, 예: 삼성전자 "005930", 카카오 "035720")
- 종목 정보는 한국거래소에서 제공하는 공식 데이터 사용
- 초기 종목 데이터는 한국거래소에서 가져와서 내부 DB에 저장

**종목 정보:**
- 내부 DB에 저장된 종목 기본 정보 (종목 코드, 종목명, 시장 구분 등)
- 한국거래소 공식 종목 리스트를 초기에 가져와서 저장
- 모든 종목 관련 API에서 종목 존재 여부 확인 시 사용

### 현재가 조회

**Method:** `GET`  
**Path:** `/api/v1/stocks/{stockCode}/quote`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Path Parameters:**
- `stockCode` (string, 필수) - 종목 코드 (한국거래소 공식 6자리 숫자 코드)

**Response:**
```json
{
  "success": true,
  "data": {
    "stockCode": "string",          // 종목 코드
    "stockName": "string",          // 종목명
    "currentPrice": "BigDecimal",   // 현재가
    "previousClose": "BigDecimal",  // 전일 종가
    "changeRate": "BigDecimal",     // 등락률 (%)
    "open": "BigDecimal",           // 시가
    "high": "BigDecimal",           // 고가
    "low": "BigDecimal",            // 저가
    "volume": "long"                // 거래량
  }
}
```

**처리:**
1. 내부 DB의 종목 정보에서 종목 존재 여부 확인
2. 캐시(Redis)에서 시세 조회
3. 캐시에 없으면 내부 DB의 시세 테이블에서 시세 조회 (시세 업데이트 배치가 주기적으로 갱신)
4. DB에도 없거나 오래된 경우, 외부 API 호출 또는 모의 데이터 생성 후 DB 저장 및 캐시(Redis) 저장

**예외:**
- 종목 없음: HTTP 404, error.code: "STOCK_NOT_FOUND", message: "존재하지 않는 종목"

---

### 종목 검색

**Method:** `GET`  
**Path:** `/api/v1/stocks/search`

**Headers:**
- `Authorization: Bearer {token}` (필수) - JWT 토큰

**Query Parameters:**
- `keyword` (string, 필수) - 검색어

**Response:**
```json
{
  "success": true,
  "data": {
    "stocks": [                      // 종목 목록
      {
        "stockCode": "string",       // 종목 코드
        "stockName": "string",       // 종목명
        "currentPrice": "BigDecimal" // 현재가
      }
    ],
    "totalCount": "int"              // 검색된 종목 개수 (최대 20개)
  }
}
```

**처리:**
1. 내부 DB의 종목 정보에서 종목명 또는 종목 코드로 검색
2. 최대 20개까지 반환

---

## 배치 작업

### 결제일 배치
- 실행 시간: 매일 03:00
- 처리: T+2 결제 도래 건 처리

**상세 처리 로직:**
1. 오늘 날짜가 T+2 결제일인 매수 체결 건 조회
2. 각 계좌별로 결제 처리:
   - 필요한 결제 잔금 계산 (체결 금액 - 홀딩된 증거금)
   - 가용 자금 합산:
     - 기존 보유 현금
     - T+2 입금일인 매도 대금 (같은 날 결제일인 매도 체결 건)
     - 홀딩된 증거금 (매수 체결 시 홀딩한 증거금)
   - **변제 대용 처리 (회전매매 지원)**:
     - 결제일과 입금일이 다른 경우, 입금일에 들어올 매도 대금을 담보로 인정
     - "내일 매도 대금이 들어올 예정이니 오늘은 변제 대용으로 처리" (매도대금 담보 개념)
     - 입금일과 결제일이 같은 날이면 상계 처리 가능
   - 총 가용 자금과 필요한 결제 잔금 비교
3. 결제 성공 (가용 자금 >= 필요한 결제 잔금):
   - 현금에서 결제 잔금 차감
   - 홀딩된 증거금 해제 (출금 가능 현금에 반영)
   - 미결제 내역 상태를 SETTLED로 변경
   - 계좌 상태가 IN_ARREARS인 경우, 미수금이 모두 해결되었는지 확인 후 ACTIVE로 변경
4. 결제 실패 (가용 자금 < 필요한 결제 잔금):
   - 부족 금액을 미수금으로 생성
   - 계좌 상태를 IN_ARREARS로 변경
   - 미수금 발생일 기록
   - 홀딩된 증거금은 그대로 유지 (미수금 상환 시 사용)

### 강제 청산 배치
- 실행 시간: 매일 05:00
- 처리: 연체 계좌 강제 매도 (반대매매)

**법적 근거:**
- 금융투자협회 신용거래약관: 담보유지비율 하회 시 또는 상환기일 미상환 시 반대매매 실행 가능
- 반대매매 실행 조건: 담보유지비율 하회(통상 140%), 상환기일 미상환, 이자 및 수수료 미납

**상세 처리 로직:**
1. 미수금이 있고 반대매매 조건을 만족하는 계좌 조회:
   - 상환기일이 지난 미수금 (T+2 결제일 이후 미상환)
   - 담보유지비율이 기준치 이하로 하회한 경우
   - 이자 및 수수료 미납
2. 각 계좌별로 강제 청산 처리:
   - 보유 주식 조회
   - 미수금 회수에 필요한 금액 계산 (미수금 + 누적 이자)
   - 보유 주식 평가액 계산 (현재가 × 수량)
3. 강제 매도 주문 생성 (반대매매):
   - 보유 주식이 있는 경우: 시장가 매도 주문 생성 (전량 또는 일부)
   - 주문 생성 시 자동으로 FILLED 처리 (또는 즉시 체결 처리)
   - 매도 대금으로 미수금 상환 시도
4. 강제 청산 후:
   - 매도 대금으로 미수금 상환 처리
   - 미수금이 모두 해결되면 계좌 상태를 ACTIVE로 변경
   - 미수금이 남아있으면 계좌 상태는 IN_ARREARS 유지
   - 보유 주식이 없고 미수금이 남아있으면 계좌 상태를 FROZEN으로 변경

### 이자 계산 배치
- 실행 시간: 매일 00:00
- 처리: 미수금 이자 계산

**법적 근거:**
- 금융투자협회 신용거래약관: 미수금 발생 시 연체이자 부과
- 금융투자회사의 대출금리 산정 모범규준: 대출기준금리(CD수익률) + 가산금리(리스크프리미엄, 신용프리미엄 등)로 구성
- 이자율은 각 증권회사가 자율적으로 결정 (차주의 신용상황, 담보물 특성, 담보비율 등 반영)

**상세 처리 로직:**
1. 미수금이 있는 계좌 조회 (상태: IN_ARREARS)
2. 각 미수금별로:
   - 연체 일수 계산 (현재일 - 발생일)
   - 일일 이자율 적용 (회사 정책에 따라 결정, 예: 연 20% → 일 0.0548%)
   - 누적 이자 계산 (미수금 원금 × 일일 이자율 × 연체 일수)
   - 누적 이자 업데이트

### 시세 업데이트
- 실행 주기: 실시간 또는 1분
- 처리: 
  1. 외부 API 호출 또는 모의 데이터 생성으로 시세 조회
  2. 내부 DB의 시세 테이블에 저장
  3. 캐시(Redis)에도 저장 (TTL: 30초)

### 종목 정보 관리
- **종목 정보**: 내부 DB에 저장된 종목 기본 정보 (종목 코드, 종목명, 시장 구분 등)
- **초기 데이터**: 한국거래소(KRX)에서 제공하는 공식 종목 리스트를 가져와서 내부 DB에 저장
- **종목 코드 형식**: 6자리 숫자 (예: 삼성전자 "005930", 카카오 "035720")
