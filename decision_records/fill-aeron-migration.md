---
feature: fill-aeron-migration
date: 2026-09-14
branch: feat/fill-aeron-migration
commits: [93e19b5, d8d6866, 3d6bf59, 86718c3, 634f7d5, 66a0d75]
feeds: [adr, blog]
---

# 체결 경로 Kafka → Aeron+Archive 마이그레이션 (U1~U4)

> ADR-032(`single-stack-aeron-archive.md`, Kafka 완전 제거 결정)의 첫 실행. 체결 경로 `account-fills`(matching-worker→account-worker)를 Kafka에서 Aeron+Archive로 옮긴 구현 기록. 정산 왕복은 별도 트랙(미착수). 협업: 설계·리뷰=한 세션 / 구현=워커 세션.

## ADR 네타

### ADR-020 첫 실행 기록 — 체결 전송을 Aeron+Archive로, IPC 유지·fan-out 제거·스냅샷 position 복구
- **context(무슨 상황)**: ADR-032에서 Kafka 완전 제거를 결정했다. 그 첫 실행으로 체결 반영 경로(매칭이 낸 체결을 계좌 엔진에 전달)를 Kafka `account-fills` 토픽에서 Aeron+Archive로 옮긴다. v2는 이미 주문 인테이크·저널이 전부 `aeron:ipc`(같은 JVM)이고, 두 워커를 실제 별도 프로세스로 UDP로 잇는 건 C5로 미뤄둔 상태였다.
- **왜(문제·동기)**: Kafka를 빼면 그것이 지던 "매칭이 체결을 냈는데 계좌가 아직 못 받은 구간의 내구성"을 다른 게 대신해야 한다. 이게 이 마이그레이션의 핵심 난제 — 속도가 아니라 내구성이다.
- **어떻게(대안·결정·트레이드오프)**:
  - **채널 = IPC 유지(UDP는 C5)**: 체결만 UDP 크로스 프로세스로 앞당기면 주문 경로(이미 IPC)와 비대칭이 되고 C5 범위를 침범한다. 아직 안 온 요구(물리 두 프로세스 연결)를 명분으로 앞당기지 않는다([[feedback_anchor_to_situational_assumption]]). 그래서 Kafka만 걷어내고 `aeron:ipc` publication+Archive로 교체, 물리 연결은 C5에서 주문·체결·정산 한꺼번에.
  - **fan-out 제거**: Kafka에서 체결 하나를 매수·매도 accountId 키로 2건 발행하던 건 **파티션 라우팅** 때문이었다(샤딩되면 매수·매도 계좌가 다른 워커로). IPC 단일 스트림엔 파티션도 키도 없어 2번 offer하면 같은 바이트가 2번 배달돼 멱등이 하나 버릴 뿐 — 라우팅 정보가 없으니 fan-out을 "남긴" 게 아니라 죽은 배관을 남긴 것. 그래서 1건만 발행하고 수신기가 매수·매도 둘 다 반영. 샤딩 fan-out은 라우팅할 샤드가 실제로 생기는 C5에서 복원. (초안에 "fan-out은 남기고 accountId 라우팅만 제거"라고 적었다가 그게 형용모순임을 지적받아 정정 — fan-out과 라우팅은 분리 불가.)
  - **내구성 모델(매칭 녹화 + 계좌 replay-from-position, 안 B)**: 매칭이 체결을 낼 때 fill publication(스트림 6001)에 offer하면서 **매칭 쪽 Archive에 녹화**(never-drop). 계좌는 라이브로 poll하되, 재기동 시 자기가 마지막에 소비한 position부터 그 Archive를 replay해 못 받은 체결을 이어받는다. 계좌 저널(4005)은 이미 소비한 체결을 담으니, fill Archive replay는 "매칭이 냈지만 계좌가 못 받은 갭"만 메운다. position 저장은 두 안 중 **안 B(스냅샷에 fillConsumedPosition 하나 + tradeId 멱등)**를 골랐다 — 안 A(계좌 저널 엔트리마다 정밀 position)는 저널 스키마·codec을 바꿔야 하는데, tradeId 멱등이 이미 견고해 스냅샷 이후 delta 재처리를 멱등이 흡수하면 그 정밀함이 값을 못 한다. 대가 = 스냅샷 주기만큼 재처리(중복은 버려짐).
  - **fill 적용 = 기존 recover 재사용**: 재기동 시 fill replay 결과(FilledTrade)를 BUY_FILL·SELL_FILL 저널 엔트리 쌍으로 변환해 기존 `engine.recover()`를 다시 부른다(엔진에 새 메서드 안 닮). dedup이 per-account tradeId라 저널 recover와 fill recover를 나눠 불러도 안전.
- **무엇을(실제 변경·파일·커밋)**:
  - U1 `93e19b5`: `FillCodec`+`FilledTrade`(core/codec). `OrderCodec` 미러, 체결 단일 타입이라 type 구분 바이트 없음, tryDecode(얇은 catch + filledQuantity<0 sanity).
  - U2 `d8d6866`: `AccountFillPublisher` Kafka→`ExclusivePublication`(6001)+never-drop offer 재작성, `MatchingFillPublishConfig`(startRecording 녹화, replay 빈 없음).
  - U3 `3d6bf59`: `AccountFillReceiver`(account-disruptor io, 폴 스레드·tryDecode·publishBuyFill+SellFill·ack 없음)+`AccountFillIntakeConfig`(6001). `AccountFillConsumer`(Kafka) 제거. `AccountKafkaErrorHandlerConfig`는 정산 컨슈머가 써서 유지.
  - U4a `634f7d5`: `AccountFillReceiver.consumedPosition()`(volatile, image.position()), `StoredAccountSnapshot.fillConsumedPosition`(워커 저장구조 — core `AccountSnapshot` 안 건드림).
  - U4b `66a0d75`: `AccountFillReplayer`(6001을 fillConsumedPosition부터 replay, 스트림 6002, 단일 recording 가정), `AccountEngineConfig` 복구 순서 restore→journal recover→fill recover.
  - v1(matching-engine `fills` 토픽) 무관.
- **결과·수치**: 각 유닛 리뷰어가 gradle 직접 실행해 통과 확인(account-disruptor·account-worker·matching-worker·core 관련). 복구 테스트 반복 6~10회 flake 없음. **성능(Aeron µs vs Kafka ms)은 미측정** — 이 마이그레이션은 내구성 재현이 목적이고 지연 벤치는 안 돌렸다.

### ADR-034 (부수) Spring Optional<T> @Bean 파라미터 해석 함정
- **context**: U4b에서 스냅샷을 읽는 새 `@Bean`을 기존 패턴(`Optional<StoredAccountSnapshot>` 파라미터)대로 짰는데 실제 스냅샷이 있어도 항상 empty를 받았다.
- **왜**: Spring이 `Optional<X>` 파라미터를 "실제로 존재하는 `Optional<X>` 리턴 타입 빈을 매칭"이 아니라 "내부 타입 X의 빈을 찾아 없으면 `Optional.empty()`로 감싸기"로 해석하는 자리가 있다. 빈 그래프 위치에 따라 갈렸다(기존 `accountJournalRecoveredEntries`는 동작, 신규 빈은 안 됨). 원인 완전 규명은 못 함(프레임워크 내부, systematic-debugging의 "환경적 원인이면 깊이 파지 말고 우회" 판단).
- **어떻게**: `Optional` 파라미터 대신 원본 `AccountSnapshotStore`를 직접 주입받아 `.read()`를 한 번 더 호출(파일 읽기라 가벼움). 기존 동작하는 두 빈은 안 건드림.
- **무엇을**: `AccountFillIntakeConfig.accountFillReplayedEntries` — 커밋 `66a0d75`.
- **결과**: 우회로 정상 동작. 재발 가능하니 기록.

## 블로그 네타

### "fan-out을 지운다 — Kafka 파티션 배관이 IPC로 오면 죽는 이유"
- **훅·핵심 주장**: 체결 하나를 매수·매도 계좌로 2번 보내던 fan-out은 "체결을 두 곳에 알린다"는 로직이 아니라 **Kafka 파티션 라우팅 배관**이었다. IPC 단일 스트림으로 오면 파티션이 없어 2번 발행이 같은 바이트 중복 배달이 될 뿐 — 남길 흐름이 아니라 지울 배관이다.
- **context**: Kafka→Aeron 체결 이관, 채널을 IPC로 유지하기로 한 뒤.
- **어떻게(서사·근거)**: 처음엔 "fan-out은 남기고 accountId 라우팅만 제거"라고 적었다가 그게 형용모순임을 지적받았다 — fan-out(2건 발행)의 존재 이유가 라우팅 키였으니 라우팅을 빼면 fan-out도 의미가 없다. "흐름은 남기고 배관은 덜어낸다"는 원칙을 적용하면, fan-out은 IPC엔 대응 흐름이 없는 배관이라 덜어내는 게 맞다. 샤딩 fan-out은 라우팅할 샤드가 실제 생기는 C5에서 복원.
- **재료**: 커밋 `d8d6866`, 시각화 `docs/_fill_migration_3segments_detail.html`.

### "복구 테스트가 가끔 틀렸다 — 근본 원인은 복구가 아니라 테스트가 성급히 읽은 것"
- **훅·핵심 주장**: 재기동 복구 테스트가 ~1/3 확률로 비결정 실패했다. 복구 버그를 의심했지만, 진단 계측(실패 시 값 출력)이 가설을 뒤집었다 — 복구(재기동 쪽)는 항상 정확했고, 틀린 건 **run1이 살아있는 계좌 상태를 다른 스레드에서 성급히 읽어** 오라클로 캡처한 부분이었다.
- **context**: Kafka 제거 마이그레이션 중 이 테스트가 흔들려서, 복구 경로 위에 U4(replay-from-position)를 얹기 전에 systematic-debugging으로 파고듦.
- **어떻게(서사·근거)**: `awaitHolding(10)`으로 "체결 끝"을 판단하고 잔고를 읽는데, `applyBuyFill`이 holdings→balance→unpaid 순으로 쓰고 필드가 non-volatile라 happens-before가 없다. holding=10을 본 순간 balance는 아직 옛 값일 수 있어 모순 스냅샷(holding=10, balance=fill 전)을 캡처했다. 실측 DIAG로 실패 시 run1 값이 (10, 1000000, 0)인데 run2 복구는 (10, 960000, 60000)로 정확함을 확인 → 테스트를 결정적 기준값 대조로 바꿔 10회 통과. **교훈: 단일 작성자(single-writer) 엔진의 라이브 상태를 다른 스레드에서 읽으면 반쪽을 본다. `accountState()`는 라이브 객체를 돌려주는 함정이라 유저 조회는 DB 조회모델로 봐야 한다.** 진단이 원인을 뒤집은 사례(증거가 인상을 이긴다).
- **재료**: 커밋 `86718c3`, DIAG 실측값(성공 960000/60000/10 vs 실패 캡처 1000000/0), 메모리 [[project_flaky_replay_recovery_test]].

### "Kafka의 내구 버퍼를 Archive로 갈아끼우기 — 무엇이 유실을 막는가"
- **훅·핵심 주장**: Kafka를 빼면 "매칭이 냈지만 계좌가 못 받은 체결"의 유실 창이 열린다. 그걸 계좌 저널(이미 소비한 것)과 매칭 fill Archive replay(못 받은 갭)로 나눠 막는다.
- **context**: ADR-032 Kafka 완전 제거의 첫 실행.
- **어떻게(서사·근거)**: 계좌 저널 4005는 계좌가 소비한 체결을 이미 durable하게 담는다 → 그 부분은 재기동 replay로 복원. 유일한 갭 = 매칭이 발행했지만 계좌가 poll하기 전 죽은 체결 → 매칭이 6001을 Archive에 녹화해두고, 계좌가 스냅샷의 fillConsumedPosition부터 replay해 이어받는다. position 정밀 저장(안 A) 대신 스냅샷+tradeId 멱등(안 B)을 고른 트레이드오프. 계좌가 매칭 소유 recording을 replay하는 cross-owner 지점은 현재 IPC/공유 드라이버라 되고, 실제 원격 replay는 C5.
- **재료**: 커밋 `634f7d5`·`66a0d75`, 시각화 `docs/_fill_migration_kafka_to_aeron.html`, LLD `docs/_fill_migration_lld.md`.
