---
feature: aeron-account-order-sender-buffer-reuse
date: 2026-09-16
branch: fix/aeron-account-order-sender-buffer-reuse (main에 병합됨, fbc65d6)
commits: [5967d9d, fbc65d6]
feeds: [adr, blog]
---

# AeronAccountOrderSender 인코딩 버퍼 재사용 (ThreadLocal)

## ADR 네타

### 계좌 주문 발신 버퍼를 ThreadLocal로 재사용한 이유와, 왜 단순 필드 재사용이 아니었나

- **context(무슨 상황)**: `AeronAccountOrderSender.send()`가 HTTP 요청마다 `new UnsafeBuffer(ByteBuffer.allocateDirect(256))`로 direct(off-heap) 버퍼를 새로 할당하고 있었다. 이 클래스는 Spring 싱글톤 빈이고, `send()`는 HTTP 요청 스레드에서 동기 호출된다(계좌 워커 쪽 `AeronMatchingOrderSender`와 달리 outbox+전용 스레드로 비동기화하지 않음 — 호출자가 요청 하나의 응답을 기다리는 동기 경로이기 때문).
- **왜(문제·동기)**: 계좌 워커 쪽 `AeronMatchingOrderSender`는 이미 같은 문제(발행마다 direct 버퍼 재할당)를 커밋 `9b76720`으로 고쳤다(필드로 재사용). 이 클래스만 안 고쳐진 상태로 남아 있었고, 2b가 부하 측정 준비 중 이 낭비를 지적해 배정했다.
- **어떻게(대안·결정·트레이드오프)**: `AeronMatchingOrderSender`는 발행 전용 스레드 하나만 `publish()`를 부르는 단일 writer라 필드 재사용이 안전했다. 이 클래스는 **싱글톤 빈에 여러 HTTP 요청 스레드가 동시에 `send()`를 부른다** — 그대로 필드 재사용하면 서로 다른 요청이 같은 버퍼에 동시에 encode해 주문 내용이 뒤섞인다(돈 문제). 두 방향을 검토했다:
  - **ThreadLocal<UnsafeBuffer>**(채택): 스레드마다 독립 버퍼. 스레드풀 크기 × 256바이트라 절대량 무시 가능. 기존 `offer()` 재시도 루프·503 처리 그대로 유지, 구조 변경 최소.
  - **Publication.tryClaim**(기각): `offer()`가 하는 로컬→term 버퍼 복사까지 없앨 수 있어 이론상 더 빠르지만, `AccountOrderCodec.encode`가 인코딩 **후에만** 길이를 반환(가변 필드 stockCode·requestId 때문에 길이 선계산 메서드가 없음) — tryClaim은 claim **전에** 길이를 알아야 해서 (a) 고정 256B claim(와이어 낭비, 실제 주문 40~60B의 4~6배) 아니면 (b) 스크래치에 먼저 인코딩해 길이 측정 후 정확 길이 claim(스크래치+복사 부활 = tryClaim의 zero-copy 이점 소멸) 둘 중 하나가 된다. 39 리뷰에서 "지금은 리스크는 큰데 이득이 없는 선택"으로 확인, 기각. tryClaim을 다시 고려하려면 `AccountOrderCodec`에 `encodedLength(order)`(인코딩 없이 길이 계산) 추가가 선행 조건.
- **무엇을(실제 변경·파일·커밋)**: `AeronAccountOrderSender`에 `private final ThreadLocal<UnsafeBuffer> encodeBuffer` 필드 추가, `send()`가 `new UnsafeBuffer(...)` 대신 `encodeBuffer.get()` 사용. `AeronAccountOrderSenderIntegrationTest`에 동시성 테스트 추가 — 임베디드 Aeron에 스레드 20개가 `CountDownLatch`로 동시에 `send()` 호출(서로 다른 accountId·quantity·requestId), 구독 쪽에서 20건 받아 `containsExactlyInAnyOrderElementsOf`로 검증(필드가 섞이면 이 비교가 깨짐).
- **결과·수치**: `:api:test --tests "*AeronAccountOrderSender*" --no-build-cache --rerun-tasks` 6/6 PASS(신규 동시성 테스트 포함). v1 JSON 직렬화·v2 코덱 인코딩 방식 자체는 건드리지 않음(2b가 명시적으로 그은 범위 — 두 아키텍처가 각자 고른 방식이라 측정의 비교 대상 그 자체이므로). 처리량 수치 자체는 이 fix 이후 dc가 재측정(별도 측정 트랙, `v1-v2-e2e-measurement.md` 참고).

## 블로그 네타

### "싱글톤 빈에서 direct 버퍼 재사용하기 — 필드로 빼면 왜 위험한가"

- **훅·핵심 주장**: "버퍼 재사용"이 항상 같은 해법이 아니다 — 호출자가 단일 스레드냐 여러 스레드냐에 따라 정답이 갈린다. 같은 프로젝트 안에 이미 두 사례(계좌 워커의 발행 전용 스레드 vs API 게이트웨이의 HTTP 요청 스레드 풀)가 서로 다른 답을 요구했다.
- **context**: Aeron 기반 v2 아키텍처에서 매 요청마다 off-heap 버퍼를 새로 할당하는 게 왜 문제인가(네이티브 메모리 할당·해제 오버헤드, GC 압박과 별개의 비용).
- **어떻게(서사·근거)**: ec가 먼저 계좌 워커 쪽(단일 writer)을 고쳤고, 그 패턴을 그대로 API 게이트웨이(멀티 writer)에 복붙하면 안 되는 이유를 실제로 밟아본 경위. tryClaim으로 한 단계 더 가려다 코덱의 "인코딩 후에만 길이를 안다"는 제약에 막혀 기각한 과정 — "왜 이 최적화를 안 했는가"도 서사가 된다.
- **재료(커밋·도식·수치)**: 커밋 `5967d9d`(fix) · `fbc65d6`(main 병합). 동시성 테스트 코드 자체가 좋은 도식 재료(20스레드 동시 발신 다이어그램으로 표현 가능).
