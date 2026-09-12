---
feature: transport-inversion
date: 2026-09-08
branch: feat/disruptor-matching-core
commits: [5f611e8]
feeds: [adr, blog]
---

# 전송 전략 반전 — 핫패스는 Aeron+Archive, Kafka는 off-path

## ADR 네타
### ADR-020 전송 전략 반전 (핫패스 Aeron+Archive, Kafka off-path)
- **context(무슨 상황)**: v2 핫패스는 api → account → matching → 체결 반영으로 이어진다. 이 경로를 무엇으로 전송하느냐를 정하는 자리다. 초기 설계는 돈이 오가는 구간(주문 인테이크·체결 반영)을 Kafka(order-requests·account-fills)로 두고, 매칭 입력만 Aeron으로 받았다.
- **왜(문제·동기)**: v2의 목표는 정합성과 속도 둘 다인데, 핫패스에 Kafka 홉이 하나라도 들어가면 속도가 죽는다. Jack이 물었다 — Aeron을 한 군데만 두고 나머지를 Kafka로 두면 빠르다는 보장이 되느냐, 전부 Aeron이어야 하지 않느냐. 라우팅 부담도 새로 생기는 게 아니다. 매칭축(stockCode)에서 어차피 수동으로 라우팅하니, 인테이크에서 accountId로 수동 라우팅하는 것도 같은 메커니즘이다.
- **어떻게(대안·결정·트레이드오프)**: 핫패스를 전부 Aeron + Archive 저널로 바꾼다. Kafka는 off-path에만 남긴다 — 정산 T+2 왕복(settlement-requests·account-settlements)과 조회모델 프로젝션. 내구성은 핫패스 스트림을 Archive에 녹화하고 복구는 replay로 얻는다(스냅샷 자체 구현은 별도 단계 C6). 대가는 돈이 흐르는 경로의 내구성을 브로커에 기대지 못하고 self-managed Archive로 직접 관리해야 한다는 것이다. 리스크지만 학습 목적에는 오히려 재료가 된다.
- **무엇을(실제 변경·파일·커밋)**: ADR-018·019의 "계좌=Kafka 복제 로그, Aeron=전송 전용" 부분을 이 결정이 대체한다. 이미 만들어 둔 account-fills(Kafka) 체결 반영 경로는 C5에서 Aeron으로 이전할 예정이다. 바이너리 코덱은 C5-1b(`5f611e8`)에서 시작했다(`AccountOrderCodec`). 도식은 `docs/_v2_architecture_implementation.html`에 있다.
- **결과·수치**: Kafka 홉은 대략 ms, Aeron은 대략 µs로 약 1000배 차이다. 핫패스에 Kafka 두 홉을 두면 그 사이의 Aeron 한 홉이 end-to-end에서 무의미해진다. LMAX 근거 — 단일 스레드로 락 없이 초당 600만 주문, I/O를 로직에서 분리. 부수 효과로 두 경로 정렬 난제가 완화된다: 주문도 체결도 Aeron이고 accountId 맵을 공유하면 정렬이 자동으로 성립한다(수동 Aeron과 자동 Kafka를 맞추던 문제가 사라진다). 미측정 — Archive 복구 시간, 실제 UDP 성능, 라우팅맵 리샤딩 프로토콜.

## 블로그 네타
### "핫패스에 브로커(Kafka)를 두면 안 되는 이유"
- **훅·핵심 주장**: 저지연을 노리고 한 구간만 Aeron으로 바꿔도, 앞뒤에 Kafka 홉이 남아 있으면 그 Aeron이 end-to-end에서는 티가 안 난다. 가장 느린 홉이 전체를 지배한다.
- **context**: v2 핫패스 전송 수단 선택. 처음엔 돈 구간만 Kafka, 매칭 입력만 Aeron이었다.
- **어떻게(서사·근거)**: Kafka 홉 ms vs Aeron µs = 약 1000배. LMAX가 단일 스레드로 초당 600만 주문을 내는 근거(I/O를 로직 스레드에서 분리). 전송을 전부 Aeron+Archive로 뒤집으면서 두 경로 정렬 난제까지 부수적으로 완화된 이야기.
- **재료(커밋·도식·수치)**: ms/µs 1000배, LMAX 600만/s. 도식 `docs/_v2_architecture_implementation.html`.

### "바이너리 코덱 — Kafka JSON에서 Aeron 손코덱으로"
- **훅·핵심 주장**: 전송을 Aeron으로 바꾸면 직렬화도 바뀐다. Kafka의 JSON 대신 바이트 오프셋을 직접 잡는 손코덱으로 간다.
- **context**: 전송 반전(ADR-020)으로 핫패스가 Aeron이 되면서 인테이크 메시지 인코딩이 필요해졌다.
- **어떻게(서사·근거)**: 오프셋 레이아웃, zero-alloc, mechanical sympathy. 다음 단계로 SBE(Simple Binary Encoding).
- **재료(커밋·도식·수치)**: C5-1b `5f611e8`(`AccountOrderCodec`).
