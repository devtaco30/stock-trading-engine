---
feature: fork3-fill-fanout
date: 2026-09-15
branch: feat/fork3-fill-fanout
commits: [d0faeb6]
feeds: [adr, blog]
---

# fork3 — 크로스프로세스 체결 fan-out

계좌를 accountId로 샤딩해 계좌마다 한 프로세스가 전담(single-writer)하는 v2에서, 한 체결이 매수·매도 두 계좌를 동시에 바꾸는 문제를 다룬다. 두 계좌가 다른 샤드면 체결을 두 프로세스에 각각 보내야 한다. C5의 마지막 실질 미구현. 설계 도식: `docs/_fork3_fanout_design.html` (아티팩트 https://claude.ai/code/artifact/9cc4357a-61c9-4977-9ddc-9834aece96b8).

## ADR 네타

### 크로스샤드 체결 수신 모델 = 같은 바이트를 두 목적지로(A안)
- **context**: 매칭이 체결 하나를 만들면 매수·매도 두 계좌를 반영해야 한다. 샤딩하면 두 계좌가 다른 프로세스에 있을 수 있다.
- **왜**: 목적지별로 다른 메시지를 만드는 방식(B안: 매칭이 매수 샤드엔 buyFill만, 매도 샤드엔 sellFill만 방향 지정)은 코덱에 방향 플래그나 분리 레코드를 넣어야 하고 수신 로직도 분기가 생긴다.
- **어떻게(대안·결정·트레이드오프)**: A안 채택 — 매칭이 같은 `FilledTrade`를 매수·매도 두 샤드에 unicast로 보내고, 각 샤드가 `publishBuyFill`·`publishSellFill`을 둘 다 시도해 자기 소유 계좌만 반영한다(없는 계좌는 `ACCOUNT_NOT_FOUND`로 격리). 코덱(`FillCodec`/`FilledTrade`) 무변경. 계좌별 `processedTradeIds` 멱등이 중복 도착과 같은 샤드 이중 도착을 흡수한다. fork1의 "unicast 2번(라우팅맵으로 목적지 계산)" 결정과 일치. 목적지가 같으면 dedup으로 1번만 보낼 수 있으나, 2번 보내도 멱등이 흡수하므로 라이브 정합엔 무해. 트레이드오프: 같은 샤드일 때 중복 발행(대역폭)을 감수하는 대신 코덱을 안 건드리고 멱등을 그대로 재사용한다.
- **무엇을(변경)**: U1 라우팅 테이블 완료(`d0faeb6`). U2에서 `AccountFillPublisher`가 라우팅 테이블로 목적지 계산 후 fan-out 예정.
- **결과·수치**: 미측정(U2~U4 미구현).

### 녹화 주체 = 수신자(계좌)측 REMOTE
- **context**: 크래시 복구를 위해 fill 스트림(6001)을 Aeron Archive에 녹화한다. 현재 코드는 매칭이 자기 Archive에 LOCAL 녹화하고(`MatchingFillPublishConfig.java:45` `startRecording(fillChannel, FILL, SourceLocation.LOCAL)`), 계좌가 그 매칭 소유 recording을 cross-owner로 replay한다(`AccountFillReplayer`).
- **왜**: 현재 방식은 "단일 recording 가정"이 매칭 다중 재시작 시 깨진다 — recording이 여러 개가 되면 계좌가 어느 것을 replay할지 모른다(매칭이 낸 recordingId를 계좌 스냅샷에 실어야 함). fork1 D에서 이미 "수신자측 REMOTE 녹화"로 결정했으나 코드가 아직 안 따라간 과도기다.
- **어떻게(대안·결정·트레이드오프)**: 녹화 주체를 계좌(받는 곳)로 옮긴다. 매칭은 offer만 하고 LOCAL 녹화를 삭제하며, 각 계좌 샤드가 수신하면서 자기 Archive에 REMOTE 녹화한다. 그러면 각 계좌가 자기 fill recording을 소유하므로 "단일 recording 가정"이 자연히 성립하고(매칭 재시작과 무관), C6 복구 부담이 줄어든다. 매칭측 녹화는 단일 프로세스 시절 만든 과도기 코드라, 지금 옮기는 것은 이동이지 두 번 만드는 것이 아니다(Jack이 "두 번 만들래?"로 이 점을 짚어 확인).
- **무엇을(변경)**: U2에서 매칭 LOCAL 녹화 삭제, U3에서 계좌측 REMOTE 녹화 + `AccountFillReplayer`를 자기 recording 기반으로. 예정.
- **결과·수치**: 미측정.

### 스코프 = 라이브 fan-out만, 복구 세부는 C6로 분리
- **context**: fork3가 fan-out(발행 라우팅 + 수신 반영)과 복구(recording 다중화 대응)를 다 건드릴 수 있다.
- **왜**: 한 번에 다 하려다 터진 정산 Aeron 마이그레이션 되돌림 교훈. 라이브 경로가 서야 복구 대상이 실재한다.
- **어떻게**: fork3 = 라이브 fan-out + 녹화 주체 전환(수신자측). recording 다중화 세부와 수신자 녹화 기반 replay 재검증은 C6. 단 녹화 주체를 수신자로 옮기면 복구가 자기 recording이라 단순해지는 이점이 스코프 분리와 같은 방향이다.
- **무엇을**: U1(라우팅 테이블)~U4(로컬 크로스샤드 검증).
- **결과·수치**: 미측정.

### U1 라우팅 테이블 폴백 = shards 미설정 시 슬롯1 단일 채널
- **context**: `ShardRoutingTable`은 슬롯 커버리지(갭·중복 0)를 생성자에서 검증해 fail-fast한다. 그런데 기존 matching-worker 통합테스트 전부가 `shard-routing` 설정을 모른 채 풀 컨텍스트를 띄운다.
- **왜**: 폴백 없이 두면 `shard-routing.shards`가 비어 빈 생성 시 예외로 통합테스트 9개가 NPE로 깨진다.
- **어떻게**: `shards` 미설정 시 슬롯 1개짜리 테이블로 폴백해 기존 `transport.fill.channel` 하나로만 라우팅한다(= 샤딩 없음 = 기존 동작). fork1의 `transport.*` 미설정 시 `aeron:ipc` 기본값 패턴과 같은 결. 크로스샤드는 `shards`를 명시할 때만 opt-in. 스펙에 없던 판단이라 리뷰에서 플래그하고 수용. 대가: config 실수로 shards를 빠뜨리면 샤딩이 조용히 꺼짐(실배포 필수화 여부는 HA/배포 단계에서 재검토).
- **무엇을**: `ShardRoutingProperties`(record, null→List.of 정규화)·`ShardRoutingConfig`(빈 등록), `ShardRoutingTable`(core, floorMod+fmix64 결정론). 커밋 `d0faeb6`.
- **결과·수치**: core 10 + matching-worker 3 테스트 GREEN(강제 재실행 확인).

### 멱등 캐시 무제한 증가(OOM) 픽스 타이밍 = fork3 후
- **context**: 메모리릭 정적 리뷰에서 `AccountState.java:40-42`의 멱등 캐시 3개(`processedTradeIds`·`processedSettlementRefs`·`requestIdToOrderId`)가 상한·TTL·evict 없이 무제한 증가(최고 심각). 같은 프로젝트 `OrderBook`은 LRU(5000)+TTL(5분) 상한이 있는 비대칭.
- **왜**: fork3가 이 멱등 캐시를 재사용하고, fan-out은 중복 fill이 지연 도착할 수 있다. TTL/상한을 fan-out 특성을 모르고 지금 걸면 늦게 온 중복이 TTL을 넘겨 재처리(이중 반영)될 위험이 있다.
- **어떻게**: fork3로 fan-out 중복 도착 윈도우를 파악한 뒤 `OrderBook` 패턴(LRU+TTL)을 계좌에 미러링. 타이밍은 fork3 완료 후(Jack이 최종 확정).
- **무엇을**: 미구현.
- **결과·수치**: 미측정.

## 블로그 네타

### "체결 하나가 두 주인을 바꾼다 — 샤딩된 계좌의 fan-out과 멱등"
- **훅·핵심 주장**: 계좌를 샤딩해 락을 없앴더니 이번엔 한 체결이 두 샤드를 건드리는 문제가 생겼다. fan-out과 계좌별 멱등으로 코덱을 안 건드리고 푼다.
- **context**: v2가 accountId 샤딩으로 single-writer를 만들어 락을 제거했다(v1/v2 대비의 속도 축). 그 구조가 낳은 새 문제가 크로스샤드 체결이다.
- **어떻게(서사·근거)**: 매칭이 같은 체결을 두 샤드에 보내고 각 샤드는 자기 소유만 반영한다. 목적지별로 메시지를 다르게 만들지 않고(방향 지정 B안 대신) 소유 판정에 맡겨, 코덱을 그대로 둔다. 계좌별 멱등(`processedTradeIds`)이 중복·이중 도착을 흡수해 fan-out을 안전하게 만든다. 녹화 주체를 받는 곳으로 옮기니 복구가 자기 recording으로 단순해지는 부수 효과가 따라온다.
- **재료(커밋·도식·수치)**: 도식 `docs/_fork3_fanout_design.html`, `ShardRoutingTable`(fmix64 결정론 해시), `AccountState.processedTradeIds`, `AccountFillReplayer`의 단일 recording 가정, 커밋 `d0faeb6`.
