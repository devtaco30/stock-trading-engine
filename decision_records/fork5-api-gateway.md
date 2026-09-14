---
feature: fork5-api-gateway
date: 2026-09-14
branch: feat/fill-aeron-migration
commits: [bc3624e]
feeds: [adr, blog]
---

# fork5 — v2 api 게이트웨이 진입점

v2 주문 경로 `client → api → (aeron) → account`에서 **api(게이트웨이)** 자리를 설계했다. 지금까지 v2는 주문을 받는 쪽(계좌 엔진 `AccountOrderReceiver`)만 있고 그 앞에서 HTTP를 받아 Aeron으로 보내는 입구가 코드로 없었다 — `AccountOrderCodec.encode`(발신 인코딩)를 호출하는 메인 소스가 하나도 없는 것이 근거. fork5가 그 입구를 세우는 결정 5개를 확정했고 U1a(뼈대)를 구현했다.

## ADR 네타

### 게이트웨이는 api 모듈 안 URI 버저닝, 별개 모듈이 아니다
- **context**: v2 엔진(계좌·매칭·정산 워커)은 전부 별개 Spring Boot 앱으로 세웠다(ADR-018). 게이트웨이도 그렇게 별개 모듈로 갈지 물었다.
- **왜(문제·동기)**: "엔진들과 일관성"이라는 이유로 새 모듈을 추천했으나 근거가 약했다. 엔진이 별개 앱인 진짜 이유는 상태를 가진 샤드를 single-writer로 소유하기 때문(인스턴스마다 다른 accountId 범위) — 프로세스가 갈려야 한다. 게이트웨이는 stateless라 그 논리가 걸리지 않는다.
- **어떻게(대안·결정·트레이드오프)**: (A) 새 v2 게이트웨이 Gradle 모듈 / (B) api 모듈 안에서 `/api/v1`·`/api/v2` URI 버저닝. **(B) 채택.** v1 코드 경로(Kafka 발행)는 그대로 두고 `/api/v2/**`만 새 컨트롤러·서비스로 얹는다. 트레이드오프: api 모듈이 v1 Kafka 배선과 v2 Aeron 배선을 둘 다 짊어진다(한 Spring 컨텍스트에 두 전송) → v2 Aeron 빈을 조건부/프로파일로 갈라 v1 단독 기동 시 안 뜨게. 대신 v1/v2가 한 모듈에 나란히 있어 "같은 문제 반대 패러다임" 대조가 코드에서 바로 드러난다(학습 이점).
- **무엇을(실제 변경)**: `OrderV2Controller`(`/api/v2/orders/buy,sell`), `OrderV2ApiService` 신규(커밋 `bc3624e`). `WebMvcConfig` 인증 인터셉터 경로에 `/api/v2/**` 추가(v1 패턴·excludePathPatterns 무손상).
- **결과·수치**: U1a 단위 테스트 5케이스 GREEN(강제 재실행 확인). 성능은 미측정(C7).

### requestId는 클라이언트 필수 — 서버 생성 fallback 폐기
- **context**: v1 `OrderApiService.resolveRequestId`(:114)는 클라가 requestId(멱등키 UUID)를 보내면 그 값을 쓰고, 안 보내면 서버가 `UUID.randomUUID()`로 대신 만든다(관대한 fallback).
- **왜(문제·동기)**: v2 게이트웨이는 stateless라 인스턴스가 여럿이다. 클라가 재전송(네트워크 타임아웃 후 같은 주문)을 하면 두 요청이 서로 다른 게이트웨이 인스턴스에 도착할 수 있고, 각 인스턴스가 서버 UUID를 따로 만들면 계좌 엔진이 서로 다른 주문 둘로 인식 → 중복 예약. 재전송 멱등이 깨진다.
- **어떻게(대안·결정·트레이드오프)**: (A) v1처럼 서버 생성 유지 / (B) requestId 없는 요청을 400으로 거부(클라 필수). **(B) 채택.** 멱등키=클라 UUID 원칙(v1 `cbfdf3f`)과도 맞고, 계좌 엔진이 이미 requestId null/blank면 `INVALID_REQUEST_ID`로 거부(`AccountEventHandler:86`)하니 게이트웨이만 맞추면 정합. 대가: 클라가 반드시 requestId를 붙여야 함(API 계약 강제).
- **세 경우 구분(글로 펼칠 핵심)**: 서버는 requestId만 본다. ①재전송(같은 requestId)=서버 멱등이 막음 ②따닥/이중클릭=클라가 같은 requestId 붙이면 막힘 → 클라 UI 책임 ③연속주문(같은 조건 반복)=다른 requestId=정상 별개 주문. 서버가 자연키(계좌+종목+가격+수량)로 따닥을 막으면 안 되는 이유=③이 정상인데 그것까지 막는다. 주식은 같은 주문 반복이 정상.
- **위조와 직교**: requestId는 위조 방어 장치가 아니다. 위조는 인증(토큰→userId, `resolveAccountOwnedAndActive`)이 막는다. requestId를 클라가 만들어도 인증 못 뚫으면 게이트웨이에서 튕기고, 뚫으면 requestId가 뭐든 주문이 된다. requestId 필수/선택 결정은 보안과 직교.
- **무엇을**: `OrderV2ApiService.requireRequestId`(null/blank→`InvalidRequestException`). `resolveRequestId`(서버 생성) 미사용.

### orderId 발급 주체 = 게이트웨이가 아니라 계좌 엔진 (이미 확정, fork5에서 재확인)
- **context**: v1은 orderId를 Snowflake로 DB 저장 시점(컨슈머)에서 발급했다. 옛 v2 호스트 메모리도 "api가 orderId(Snowflake) 부여"로 적혀 있었다.
- **왜**: v2는 결정론적 replay가 필요하다(엔진 안 벽시계·랜덤 금지) → orderId=Snowflake(벽시계) 금지 → 결정론적 `(nodeId, 엔진카운터)`. 이걸 만들 수 있는 곳은 single-writer인 계좌 엔진뿐(카운터가 엔진 상태라 스냅샷·복구로 재현).
- **어떻게**: 게이트웨이는 orderId를 만들지 않는다. `AccountOrderCodec` 레이아웃에 orderId 필드가 아예 없다(주석 C5-2a). 계좌 엔진이 requestId 첫 등장 시 `AccountOrderIdGenerator`로 발급하고 `requestIdToOrderId` Map에 기록(멱등). 옛 메모리의 "api가 orderId 발급"은 그 사이 뒤집힌 낡은 서술 — 코드가 최신.
- **무엇을**: fork5에서 이 경계를 재확인만. 게이트웨이는 requestId만 실어 보내고 orderId를 모른다. 그래서 응답이 202+requestId이고 결과는 requestId로 조회(⑤).

### 게이트웨이 발신은 동기, 발신 실패는 503+클라 재전송 (U1b LLD)
- **context**: 계좌→매칭 발신(`AeronMatchingOrderSender`)은 SPSC 큐(outbox)+전용 publisher 스레드로 비동기다.
- **왜**: 그 비동기 구조는 **계좌 single-writer 로직 스레드가 Aeron `offer`(I/O 백프레셔로 블로킹 가능)에 막히면 계좌 처리 전체가 멈추기 때문**(대체 스레드 없음, LMAX 원칙). 게이트웨이가 `offer`를 부르는 건 HTTP 요청 스레드(요청당 하나)라, 막혀도 그 요청 하나만 기다린다 — 멈추면 큰일 나는 유일한 스레드가 없다. 그래서 같은 aeron `offer`인데도 큐로 뺄 이유가 게이트웨이엔 없다.
- **어떻게(결정·트레이드오프)**: 게이트웨이는 HTTP 스레드에서 **동기** encode+offer. 동기라야 `offer` 결과(성공/실패)를 그 자리에서 HTTP 응답(202/503)에 실을 수 있다(비동기면 이미 응답 보낸 뒤라 실패를 못 알림). `offer`≤0(백프레셔·미연결)이면 짧은 백오프 재시도 → 그래도 실패면 예외 → **503**. 클라가 같은 requestId로 재전송하고 계좌 멱등이 중복을 흡수. never-drop(무한 재시도로 HTTP 요청 무한 블로킹)이 아니라 실패를 빠르게 알려 클라 재전송에 위임 — 멱등이 안전망. 주문을 조용히 버리지 않는다(돈).
- **무엇을(예정, U1b)**: `AeronAccountOrderSender`(동기 encode+offer), `AccountOrderPublishConfig`(임베디드 MediaDriver+Publication, 채널·스트림=계좌 인테이크와 일치 `aeron:ipc`/`4004`), 전용 예외+503 매핑. 스코프=(가): 발신 로직만, 같은 JVM 테스트. 실 `udp` 크로스프로세스는 C5(채널 문자열만 `aeron:ipc`→`aeron:udp?endpoint=` 교체, 로직 동일).

## 블로그 네타

### "주식 주문의 '입구'를 다시 설계하다 — HTTP와 Aeron 사이에서 무엇이 갈리나"
- **훅·핵심 주장**: 주문 게이트웨이는 그냥 "받아서 넘기는 서버"가 아니다. HTTP 세상(클라)과 Aeron 세상(엔진)의 경계에서, **누가 orderId를 만드나 / requestId는 누가 책임지나 / 발신은 동기냐 비동기냐**가 전부 이 자리에서 갈린다. 같은 aeron `offer`인데 계좌→매칭은 비동기 큐를 쓰고 게이트웨이는 동기를 쓰는 이유가 그 예다.
- **context**: v1(HTTP→Kafka)과 v2(HTTP→Aeron)를 같은 api 모듈에 URI 버전으로 나란히 뒀다. 전송만 바뀐 게 아니라 책임 분담이 어떻게 달라지는지.
- **어떻게(서사·근거)**: ①게이트웨이가 stateless라는 사실 하나가 "별개 앱이냐"와 "동기 발신이냐"를 동시에 결정한다 — 엔진은 single-writer라 프로세스를 가르고 I/O를 스레드 밖으로 빼지만, 게이트웨이는 그럴 유일한 스레드가 없다. ②requestId를 클라 필수로 강제한 이유는 stateless 멀티 인스턴스에서 서버가 멱등키를 만들면 재전송을 못 알아보기 때문 — 재전송/따닥/연속주문 세 경우가 requestId 하나로 갈린다. ③orderId를 게이트웨이가 안 만드는 이유는 결정론적 replay — 벽시계 Snowflake를 버리고 계좌 single-writer가 카운터로 발급. ④발신 실패를 never-drop이 아니라 503+클라 재전송으로 위임하고 멱등을 안전망으로.
- **재료(커밋·도식·수치)**: 커밋 `bc3624e`(U1a 뼈대). 도식 `docs/_fork5_gateway_decisions.html`(요청 경로 + 결정 5개 + 이미 있는 것/새로 만드는 것). 코드 근거: `AccountOrderCodec`(orderId 없음, C5-2a), `AeronMatchingOrderSender`(비동기 큐, single-writer 보호 주석), `OrderApiService:114`(v1 서버 생성 fallback), `AccountEventHandler:86`(계좌 엔진 requestId 거부). 성능 수치 미측정(C7에서 v1 대비).
