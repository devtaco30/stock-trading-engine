---
feature: engine-journal-durability
date: 2026-09-11
branch: feat/disruptor-matching-core
commits: [0b43ab5, df71edb, 3f81eca, 39c16ce, 83a2a13, 211a7e8, a87b3a3, 870e5ab]
feeds: [adr, blog]
---

# 엔진 저널 durability — Aeron Archive 저널 + 결정론 리플레이, ack 경계, backpressure

## ADR 네타
### ADR-025 v2 durability = 엔진별 Aeron Archive 저널 + 결정론 리플레이
- **context(무슨 상황)**: ②-b의 잔여 gap(유령 주문)을 닫는 자리다. 발신을 보장해야 하는데, LMAX 방식은 핫패스에서 발신을 보장하는 게 아니라 입력을 저널에 기록하고 리플레이로 복구를 얻는다. Jack의 지시 "2" = Archive를 공유 인프라로 두고 계좌·매칭 둘 다 쓴다.
- **왜(문제·동기)**: 계좌 상태는 주문·체결·정산 셋의 처리 순서에 의존한다(체결이 잔고를 바꿔 다음 accept를 좌우한다). 이 셋을 하나의 통합 입력 로그로 녹화해야 결정론 리플레이가 가능하다.
- **어떻게(대안·결정·트레이드오프)**: 각 엔진이 로직 + 자기 입력 링을 통째로 기록하는 durable 저널(Archive)을 갖고, 복구는 자기 저널을 리플레이한다. 저널은 엔진별로 공유하지 않고(single-writer가 주인) 인프라만 공유한다. 매칭은 인메모리 저널을 Archive로 옮기는 게 2c로 남는다. 계좌는 저널을 신규로 단다 — 전 입력을 처리 순서대로 게이팅(`handleEventsWith(journal).then(business)`)한다. 저널은 입력만 기록하므로 BUY/SELL의 orderId 발급 전(0)이고, 리플레이가 그 값을 재생성한다.
- **무엇을(실제 변경·파일·커밋)**: 2a `0b43ab5`(Archive 인프라·녹화), 2b-0(id 결정론, 별도 파일 참조), 2b-1 `df71edb`(저널 훅 인메모리), 2b-1b `3f81eca` + refactor `39c16ce`(저널 Archive durable — 녹화 대상을 인테이크에서 저널 스트림 4005로, flags 바이트 코덱, 리뷰 통과), 2b-2a `83a2a13`(recover = 링 우회 + 핸들러 재사용 + outbound no-op + generator 필드 공유), 2b-2b `211a7e8`(`AccountJournalReplayer` = 4005 다중 녹화를 시각 순으로 replay, run1 → run2 end-to-end 복구 증명).
- **결과·수치**: 계좌 크래시 복구가 end-to-end로 닫혔다(2026-09-09). 남은 건 2c(매칭 저널 Archive)·2d(스냅샷). 저널은 매 이벤트 fsync가 아니다 — 페이지캐시 append는 µs이고 flush는 배치·비동기·별도 스레드다. 프로세스 크래시는 페이지캐시로 충분(µs)하지만 노드 장애는 PV flush가 필요(ms급, flush 빈도가 노브)하다. LMAX가 모든 입력을 저널하고도 µs·수백만/s를 내는 원리와 같다. 비차단 nit(미반영): 코덱 type이 enum ordinal이라 durable 포맷인데 enum 순서를 바꾸면 옛 저널을 못 읽는다(주석/명시 코드 권장). BigDecimal 체인 한 줄(체인 금지 규칙 위반). shutdown hang은 [[shutdown hang 수정 TODO (꼭 고칠 것)]]로 남음.

### ADR-027 계좌 durable 기록 경계: Kafka ack를 저널 뒤로 (A안)
- **context(무슨 상황)**: account-worker가 Kafka로 받은 체결(account-fills)·정산(account-settlements)을 `AccountEngine` 링에 publish한 뒤 ack한다. publish는 비동기라 링에 넣기만 하고 저널·반영은 나중에 disruptor 스레드가 한다.
- **왜(문제·동기)**: publish 직후 즉시 ack하면 저널(Aeron Archive)이 그 이벤트를 durable 기록하기 전에 Kafka offset이 넘어간다. 크래시가 나면 저널에도(아직 append 전) Kafka에도(offset 커밋) 없어 유실되고 잔고가 어긋난다. 계좌 저널은 링 진입점에서 전 입력(주문 Aeron + 체결·정산 Kafka)을 한 로그로 녹화(2b-1b)하는데, 한 조각이 빠지면 replay 복구가 깨진다. fill·정산 되돌림에 공통이다.
- **어떻게(대안·결정·트레이드오프)**: `publishBuyFill/SellFill/Settlement`가 발행 시퀀스(long)를 반환한다. `AccountEngine.blockUntilJournaled(seq)`는 저널 핸들러의 disruptor 시퀀스(`getSequenceValueFor(EventHandlerIdentity)`)가 seq에 도달(= append 완료)할 때까지 스핀 대기하고 5초 타임아웃을 건다. 컨슈머는 `max(buySeq, sellSeq)`를 blockUntilJournaled한 뒤에 ack한다. `getSequenceValueFor` 게이팅은 기존 `handleEventsWith(journal).then(business)` 배리어와 같은 것이라 새 위험이 아니다.
- **무엇을(실제 변경·파일·커밋)**: 커밋 `a87b3a3`. 컨슈머가 blockUntilJournaled 뒤에 ack.
- **결과·수치**: 유실 창이 닫혔다. 실패 방향이 유실에서 중복으로 바뀐다(저널 뒤·ack 전 크래시 → Kafka 재전송 + tradeId/settlementRef 멱등 흡수 = 안전). 정상 대기는 sub-ms(off-path 컨슈머라 무해)이다. disruptor 4.0.0 API 변경 주의(`getSequenceValueFor(EventHandlerIdentity)`, EventHandler가 상속 — javap로 검증). 90개 테스트 PASS. TDD: A-2 red = ack 시점 저널 크기 expected 2/1 was 0.

### ADR-028 저널 불가 시 대응: backpressure(컨테이너 정지), 드롭/DLQ/즉시kill 아님 (A-3)
- **context(무슨 상황)**: ADR-027의 `blockUntilJournaled`가 throw(5초 타임아웃 = 저널 못 씀)했을 때 Kafka 컨슈머가 뭘 하느냐. 설정이 없으면 Boot 기본 DefaultErrorHandler를 탄다.
- **왜(문제·동기)**: 기본 DefaultErrorHandler는 재시도 후 recoverer가 로그를 남기고 offset을 커밋한다(= 스킵/드롭). 그러면 ADR-027이 막은 유실이 "ack 경쟁"에서 "재시도 소진 후 드롭"으로 자리만 옮긴다. 처음에 "즉시 fatal kill"로 기울었는데 Jack이 잡았다 — 고작 1건에 서버를 죽이느냐. 사실 여기 오는 건 예상 못 한 예외(fail-fast)뿐이고, 정상 거절은 RejectReason이라 여기 안 온다.
- **어떻게(대안·결정·트레이드오프)**: 대안은 ①기본(재시도 후 드롭) ②즉시 kill ③무한 재시도 ④DLQ ⑤pause-retry 창 후 fatal. 결정은 저널 죽음을 전용 `JournalUnavailableException`으로 던져 `CommonDelegatingErrorHandler`(causeChainTraversing)로 `CommonContainerStoppingErrorHandler`(stopAbnormally)에 위임 → 컨테이너 정지(offset을 안 넘겨 무손실). 그 외(poison·역직렬화)는 기본 스킵이고 pod은 안 죽는다. 즉 저널이 죽었을 때만 정지한다. 근거(리서치): Factor House는 downstream 일시 다운을 backpressure/head-of-line block으로 다루지 DLQ·재시도로 다루지 않는다(메시지는 멀쩡하고 인프라가 다운). Kafka Streams는 changelog 기록 실패를 fatal로 보고 재기동 시 changelog로 복원하며 KIP-572 `task.timeout.ms`로 stall 상한을 둔다. 우리 저널 = changelog, replay = 복원으로 동형이다. down-dependency(대기/정지)와 corrupt-state(fail-fast + replay)를 구분해야 하는데, 앞선 실수가 둘을 뭉갠 것이었다. pause-retry machinery는 원격 self-healing 의존성용이라 우리 in-process fail-fast 저널엔 불필요하다(짧은 정체는 5초 대기가 이미 흡수하고, throw는 이미 심각한 상태다).
- **무엇을(실제 변경·파일·커밋)**: 커밋 `870e5ab`. Boot 3.4.0이 단일 CommonErrorHandler 빈을 자동 적용(`ConcurrentKafkaListenerContainerFactoryConfigurer.setCommonErrorHandler`, javap 검증).
- **결과·수치**: 미결(중요) — ①정지 뒤 재시작 → replay 자동화는 배포층(K8s/docker restart 정책)이지 코드가 아니다([[shutdown hang 수정 TODO (꼭 고칠 것)]]와 한 묶음). ②content-poison(한 메시지가 저널 스레드 fail-fast로 엔진을 죽임) + deterministic replay 루프는 미구현(수신 검증 필요). 90개 테스트 PASS. 정상일 땐 throw가 없다(저널 healthy). stopAbnormally(Runnable) 호출로 정지(테스트에서 확인).

## 블로그 네타
### "각 엔진에 저널을 달다 — LMAX input journaling·통합 입력 로그"
- **훅·핵심 주장**: 발신을 핫패스에서 보장하는 대신, 각 엔진의 입력을 통째로 저널에 기록하고 복구는 리플레이로 얻는다. 계좌는 주문·체결·정산 셋의 처리 순서에 상태가 의존하므로 하나의 통합 입력 로그가 필요하다.
- **context**: ②-b 유령 주문 gap을 닫는 durability 설계. Archive를 계좌·매칭 공유 인프라로.
- **어떻게(서사·근거)**: 엔진 = 로직 + 자기 입력 링 저널, 복구 = 자기 저널 replay. 저널 게이팅 `handleEventsWith(journal).then(business)`. 저널은 입력만 기록 → orderId 발급 전(0), 리플레이가 재생성. 계좌 크래시 → replay → 상태 복구 end-to-end.
- **재료(커밋·도식·수치)**: 2a `0b43ab5`, 2b-1 `df71edb`, 2b-1b `3f81eca`, refactor `39c16ce`, 2b-2a `83a2a13`, 2b-2b `211a7e8`. 저널 스트림 4005, `AccountJournalReplayer`.

### "저널이 느리지 않은 이유 — 페이지캐시 + 배치 flush"
- **훅·핵심 주장**: "모든 입력을 저널하면 느리지 않냐"는 통념과 달리, 저널 append는 페이지캐시에 쓰는 µs 작업이고 디스크 flush는 배치·비동기·별도 스레드다. LMAX가 모든 입력을 저널하고도 수백만/s를 내는 원리다.
- **context**: Archive 저널이 핫패스 지연을 늘리지 않는지에 대한 답.
- **어떻게(서사·근거)**: fsync 매 이벤트 아님. 페이지캐시 append µs. 프로세스 크래시는 페이지캐시로 충분(µs), 노드 장애는 PV flush 필요(ms급, flush 빈도가 노브). 생존 경계 = 실제 flush된 지점.
- **재료(커밋·도식·수치)**: append µs, flush ms급. LMAX 수백만/s.

### "비동기 발행 뒤 즉시 ack의 함정 — 링버퍼와 저널 사이의 유실 창"
- **훅·핵심 주장**: 링에 publish만 하고 곧바로 Kafka ack를 하면, 저널이 그 이벤트를 durable 기록하기 전에 offset이 넘어간다. 크래시 시 저널에도 Kafka에도 없어 유실된다. ack를 저널 완료 뒤로 미뤄야 한다.
- **context**: account-worker가 Kafka 체결·정산을 링에 publish 후 ack하는 경로(ADR-027).
- **어떻게(서사·근거)**: before/after 타임라인 — publish는 링에 넣기만, ack가 저널 durable보다 먼저. `blockUntilJournaled(seq)`가 저널 핸들러 disruptor 시퀀스 도달까지 스핀 대기(5s 타임아웃) 후 ack. 실패 방향이 유실 → 중복(멱등이 흡수).
- **재료(커밋·도식·수치)**: `a87b3a3`. 정상 대기 sub-ms. 90 테스트 PASS. TDD red = 저널 크기 0.

### "저널이 안 되면 서버를 죽여? — throw 처리를 정하는 법" · "backpressure는 실패가 아니다"
- **훅·핵심 주장**: 저널 기록이 실패했을 때 답은 즉시 kill도 드롭도 DLQ도 아니다. downstream이 다운되면 멈추는 게(backpressure) 정상이고, offset을 안 넘기면 유실이 없다. 재시도는 다운된 시스템을 못 고친다.
- **context**: `blockUntilJournaled` throw 시 Kafka 컨슈머의 처리 정책(ADR-028). 정직한 실패 서사 — fatal kill로 기울었다가 "1건에 죽이냐"로 잡힘 → 리서치 → backpressure로 교정.
- **어떻게(서사·근거)**: 기본 DefaultErrorHandler = 재시도 후 드롭(유실이 자리만 옮김). `JournalUnavailableException` → `CommonContainerStoppingErrorHandler`로 컨테이너 정지(무손실), 그 외 poison은 기본 스킵(pod 생존). down-dependency(대기/정지) vs corrupt-state(fail-fast+replay) 구분. Factor House: head-of-line block이 decoupled 시스템의 정상 동작.
- **재료(커밋·도식·수치)**: `870e5ab`. Boot 3.4.0 단일 CommonErrorHandler. 90 테스트 PASS.

### "우리 저널 = Kafka Streams의 changelog — 같은 문제, 같은 답"
- **훅·핵심 주장**: in-process 저널 replay가 Kafka Streams의 changelog restore와 동형이다. 남의 프레임워크가 이미 정해둔 답(fatal → 재기동 시 changelog 복원, `task.timeout.ms`)을 우리 손코딩과 맞춰봤다.
- **context**: 저널 실패 처리 정책의 근거를 기존 프레임워크에서 찾은 지점(ADR-028).
- **어떻게(서사·근거)**: Kafka Streams는 changelog 기록 실패를 fatal로 보고 재기동 때 복원, KIP-572 `task.timeout.ms`가 stall 상한. 우리 저널 = changelog, replay = 복원.
- **재료(커밋·도식·수치)**: KIP-572 `task.timeout.ms`.

### "fail-fast + replay의 진짜 구멍 — 결정론적 poison은 리플레이 루프"
- **훅·핵심 주장**: fail-fast + replay는 인프라 크래시엔 통하지만, 내용/버그로 인한 결정론적 크래시엔 안 통한다. 재시작해도 같은 이벤트가 또 엔진을 죽여 무한 루프가 된다. 그래서 수신 검증이 필수다.
- **context**: ADR-028의 미결 항목 ②. content-poison + deterministic replay 루프는 미구현.
- **어떻게(서사·근거)**: 인프라 크래시는 replay로 복구, 결정론적 crash(poison 메시지)는 재시작해도 같은 이벤트가 또 죽임 → 무한 루프. fail-fast+replay가 인프라 실패에만 통한다는 한계. 수신 검증이 왜 필수인지.
- **재료(커밋·도식·수치)**: (미결, 미구현. ADR-028 ⚠️2)

### "리플레이는 크래시를 복구하고, 전송은 유실을 막는다 — 경계는 링버퍼"
- **훅·핵심 주장**: 크래시 복구(저널에 있는 걸 재생)와 전송 유실 방지(로그에 안 들어온 것)는 다른 문제다. 저널에 없으면 재생할 대상 자체가 없다. 경계는 엔진 입구 링버퍼 — 앞은 전송 책임, 뒤는 저널·리플레이 책임이다.
- **context**: Jack이 물어 도출 — A·B가 유실됐는데 C·D는 체결됐으면 리플레이로 되돌리느냐. 면접·블로그 강한 포인트.
- **어떻게(서사·근거)**: 두 개의 다른 문제 — 크래시 복구 = 리플레이(저널에 있는 것 재생), 전송 유실 = 재전송 + 멱등(로그에 안 들어온 것). 저널에 없으면 C·D를 안 되돌린다. 유실된 A·B는 재전송으로 돌아와 현재 호가창에 도착 순서(시간 우선)대로 매칭된다. "원래 A·B 먼저"는 시스템이 약속하지 않은 반사실(counterfactual)이라 정합성이 깨지는 게 아니라 보낸 순서대로가 아닐 뿐이다. 경계 = 엔진 입구 링버퍼(게이팅이라 링에 실리면 처리 전에 저널에 박힌다).
- **재료(커밋·도식·수치)**: 도식 `docs/_replay_vs_delivery_boundary.html`(경계/크래시/유실 3장). 재료 [[v1/v2 서사 (이력서용, 2026-09-08)]]·[[v2 설계 정정 이력 (Jack이 잡음)]].
