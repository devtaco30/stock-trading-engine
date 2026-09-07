# ADR-014: 계좌 축 single-writer 인메모리 워커 (DB 비관적 락 제거)

> 소급 기록. B2(`451c6b2`)·B3a(`7c103ad`)는 이미 커밋된 뒤 이 ADR을 뒤늦게 남긴다.
> matching-disruptor는 ADR-012·013을 남겼는데 account-disruptor(B2·B3a·B3b)는 빠뜨렸기 때문이다.
> 이후 결정은 "코드 변경과 같은 커밋에 ADR" 원칙을 지킨다.

## 문제

v1은 매수 주문의 검증·예약을 `OrderWriter.writeBuyOrder`에서 Account 행 비관적 락(`PESSIMISTIC_WRITE`)
안에 처리한다. 락을 쥔 채 예약증거금 합·미결제 합을 쿼리하므로, 주문이 몰리면 락 보유 시간이 늘고
커넥션 풀이 압박받는다(ADR-001이 지목한 병목). v2는 이 hot path에서 락을 빼려 한다.

## 대안

1. DB 비관적 락 유지 (v1) — 정합성은 공짜지만 경합 시 위 병목.
2. 낙관적 락(버전) — 경합이 잦으면 재시도 폭증.
3. accountId 축으로 주문을 직렬화해 **락 자체를 없앰** — 같은 계좌를 한 처리 주체만 만지게 함.

## 트레이드오프

3번(single-writer)을 고르면:
- 얻음: 같은 계좌를 스레드 하나만 만져 락이 필요 없다. 검증·예약이 인메모리 산술이라 µs급이고,
  DB SUM 쿼리도 인메모리 러닝 합으로 대체된다.
- 대가: 상태가 인메모리라 내구성이 별도 문제다(크래시 시 소멸 → 복구는 스냅샷+리플레이, 아직 미구현).
  검증이 매칭보다 앞 단계라 orderId를 매칭 전에 부여해야 한다. 스프링 밖(순수 라이브러리)이라
  기동·설정·내구성은 이후 통합 층이 맡아야 한다.

## 결정

**별도 모듈 `account-disruptor`(프레임워크 없는 순수 라이브러리, matching-disruptor와 동일한 결)로
accountId single-writer 인메모리 계좌 워커를 만든다.** v1 `order-engine`의 검증·예약 역할을,
DB 비관적 락 대신 Disruptor 단일 소비자로 다시 세운 것이다. 증거금 산식은 `trading`의 `OrderWriter`를
재사용하고, 저장·락만 인메모리로 대체한다.

세부 결정:
- **검증·예약(B2)**: `AccountState.tryReserve`가 잔고·예약·미결제로 매수 가능 금액을 계산해 통과/거부.
- **예약 장부화(B3a)**: 예약을 총합 숫자 하나가 아니라 `orderId → 증거금` 장부로 든다. 체결 시 그
  주문의 예약만 골라 풀어야 하기 때문. 가용 계산의 예약 총합은 장부 값의 합.
- **전량 체결 반영(B3a)**: `applyBuyFill`(예약 풀기+보유+미수금, balance는 안 뺌=T+2) / `applySellFill`
  (보유 감소).
- **id 전략**: 경계=requestId(클라 발급 String UUID, 재전송 중복방지) / 내부=orderId(서버 발급 Snowflake
  Long, 매칭·체결·장부 키). 매칭이 orderId를 long으로 쓰므로 내부를 orderId로 통일.
- **tradeId 멱등(B3b)**: 체결 반영은 tradeId(체결 신원)로 계좌별 중복 방지. 매칭 미연결(격리) 동안엔
  tradeId를 호출자가 넘긴다(매칭 연결 = Phase C). 멱등 세트는 **계좌별**로 둔다 — 공유 세트면 매수자가
  처리한 tradeId를 매도자가 중복으로 잘못 스킵하는 버그가 난다.
- **이벤트 모델(B3b)**: 체결 반영을 별도 클래스가 아니라 `AccountEvent`에 `EventType{BUY, BUY_FILL,
  SELL_FILL}`을 두어 같은 링버퍼 슬롯을 재사용한다(matching-disruptor `OrderEvent`+`EventType` 패턴).

## 결과

- 매수 검증·예약·전량 체결 반영이 DB·락 없이 accountId single-writer로 동작. B2·B3a·B3b 커밋, 테스트 통과.
- v1 무손상(별도 모듈, 기존 코드 미변경).
- 알려진 한계: 전량 체결만(부분 체결과 orderId:tradeId 1:N = B3c) · tradeId 멱등 세트 무한 증가(시간창·
  스냅샷은 이후) · 매도 보유예약 없음(B2가 매수만) · Aeron·매칭 미연결(격리, Phase C) · 인메모리 복구
  (스냅샷+리플레이) 미구현.
- 이 결정은 ADR-001(매수 락 최적화)·ADR-003(락 순서)이 다룬 DB 락 경로를, v2에서 락 없는 방향으로
  대체하는 성격이다.
