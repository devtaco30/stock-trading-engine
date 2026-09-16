# ADR-021 계좌 상태 영속 — 인메모리 authority + DB 조회모델 프로젝션

- 날짜: 2026-09-08 (구현 2026-09-15, 커밋 `139b8b2` `2de9235` `72e189d` `5c3e722` `8bad381`)
- 상태: 채택·구현
- 관련: ADR-014(계좌 single-writer 워커) · ADR-020(전송 반전)

## 문제

v2는 주문 검증에서 DB 비관적 락을 빼고 검증을 account-worker의 인메모리 single-writer로 옮겼다. 그러면 잔고·보유를 어디에 durable하게 두고 사용자 조회는 어디서 답하느냐가 남는다.

처음에는 "로그가 유일한 원천이고 DB는 없다"로 갔다. 이것은 과한 주장이었다. 무거래 신규 계좌는 접을 이벤트가 하나도 없어서 로그만으로는 그 계좌의 상태를 만들 수 없다. v2가 DB I/O를 뺀 것은 핫패스(검증)에서만이지 시스템 전체에서가 아니었다.

## 대안

1. **DB 없음, 저널이 유일한 원천**: 조회도 저널 replay로. 무거래 계좌를 표현할 수 없어 성립하지 않는다.
2. **write-behind 스레드**: account-worker가 상태 변경을 자기 스레드에서 DB에 직접 쓴다. 핫패스 옆에 DB I/O가 붙는다.
3. **별도 프로젝션 컨슈머**: account-worker는 상태가 바뀔 때마다 계좌 전체 상태(full-state)와 seq를 Kafka로 내보내고, 별도 워커가 그것을 DB 조회 테이블에 upsert한다. api는 그 테이블만 읽는다(CQRS read model).

상태 이벤트의 형식도 결정점이었다. delta(변경분)로 보낼지 full-state(전체 상태)로 보낼지다.

## 트레이드오프

| 기준 | 2. write-behind | 3. 프로젝션 컨슈머 |
|---|---|---|
| 핫패스 격리 | 워커 프로세스 안에 DB 커넥션·트랜잭션이 들어옴 | 워커는 Kafka 발행만(전용 스레드), DB는 다른 프로세스 |
| 목표 아키텍처와의 일치 | CQRS를 흉내만 냄 | 실제 모양대로(학습 목적) |
| 지연 | DB 반영이 빠름 | Kafka 홉(ms) 뒤 반영, 조회는 eventual |
| 장애 격리 | DB 장애가 워커에 번짐 | DB 장애는 프로젝션 워커에서 멈춤 |

delta vs full-state: DB가 seq 160일 때 163이 먼저 도착하고 161이 나중에 오는 경우, delta 방식은 161·162를 건너뛰어 돈이 어긋난다. 순서를 강제하면 유실 시 무한 대기다. full-state에 "들어온 seq가 저장된 seq 이하면 거부"를 붙이면 순서 뒤집힘과 유실 양쪽에 무해하고 최신 상태로 수렴한다. 대가는 이벤트 크기(보유 종목 수에 비례)다.

## 결정

대안 3, full-state + seq stale 거부.

- account-worker가 인메모리 authority다. 잔고가 실제로 바뀔 때(체결 반영·정산 반영)마다 계좌별 단조 seq를 올리고 `AccountStateEvent(accountId, balance, holdings, seq, epochMillis)`를 Kafka `account-state` 토픽에 발행한다. 발행은 SPSC 큐 + 전용 스레드로 소비자 스레드 밖에서 한다.
- 새 모듈 `account-projection-worker`가 배치로 받아 계좌별 마지막 이벤트만 남기고(coalesce) `account_projection` · `account_projection_holding`에 upsert한다. `newSeq <= storedSeq`면 무시한다.
- api는 같은 테이블을 read-only 엔티티로 따로 매핑해 `GET /api/v2/accounts/{accountId}`로 답한다. 아직 반영 안 된 계좌는 404다.
- 계좌 생성·입출금은 이 ADR 범위 밖(후속).

## 결과

- 체결 뒤 잔고가 조회 API로 보인다. fork1 3-JVM 왕복에서 로그로만 관찰하던 것이 조회로 닫혔다.
- 이 레포 첫 `@IdClass`(복합 키 보유 테이블)와 첫 `@EmbeddedKafka` 통합 테스트가 여기서 생겼다.
- seq는 스냅샷과 디스크 코덱에 영속되어 복구 뒤에도 이어진다(`139b8b2`).
- 미측정: 프로젝션 지연(요청 → read model 반영). [C7 리포트 삽입 예정]
- 남은 것: 주문 상태(접수·부분체결·전량체결·거부) 프로젝션은 별도 트랙. 지금 api의 202 응답은 requestId를 돌려주지 않는다(후속 수정 대상).
