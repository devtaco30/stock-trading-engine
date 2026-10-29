---
feature: matching-sharding-impl-account-side
date: 2026-10-29
branch: feat/matching-destination-resolver
commits: [c2f2bf2, df7b63c, 7198ddc]   # c1 브랜치 위로 rebase한 해시. rebase 전 해시는 9a68203, f75d0d4, 87936e5
feeds: [adr, blog]
---

# 매칭 샤딩 구현 계좌 쪽(U8, U9, U9b)과 아직 끝나지 않은 U9e에서 설계서와 갈라진 선택

설계서(`docs/_lld_matching_sharding.md`)가 정한 계좌 쪽 단위를 구현하면서 설계서에 없거나 코드를 읽고 달리 한 선택, 그리고 구현 전에 읽어서 설계와 어긋난다고 확인한 것을 남긴다. 매칭 쪽 구현(U1~U7, 거부 회신 발신)의 선택은 `matching-sharding-impl-first-units.md`에 있고(c1), 설계 결정 자체는 `matching-slot-handoff-stray-orders.md`에 있다. 같은 주제의 기존 기록은 그 둘뿐이라 계좌 쪽을 새 파일로 쓴다. 다른 세션이 쓰는 파일을 고치면 병합 충돌이 나기 때문이다. U9e는 코드를 쓰기 전에 멈췄으므로 이 기록에서 「미완」으로 표시한 부분은 결정 재료일 뿐 구현 결과가 아니다.

## ADR 네타

### 담당 pod 표를 sender가 회차마다 한 번만 읽는다 — 표와 그 표를 읽는 view를 한 불변 객체로 묶어 교체
- **context(무슨 상황)**: 계좌 shard 안에서 스레드 둘이 같은 표를 본다. 맵 토픽 `matching-shard-map`(종목 slot 번호 → 담당 매칭 pod의 Aeron 채널 문자열)을 읽는 Kafka poll thread가 표를 고치고, sender thread(queue에서 주문을 꺼내 Aeron으로 보내는 스레드)가 주문마다 표를 읽는다. 설계서 11-1절은 계좌 방향의 `AssignmentDestinationResolver`처럼 `volatile` 배열을 통째로 교체하는 방식을 채택했다.
- **왜(문제·동기)**: 두 가지가 겹쳤다. 첫째, 한 poll에서 받은 변경 여럿이 읽는 쪽에 섞여 보이면(종목 slot A는 새 값, B는 옛 값) 이동 중 표시와 주소가 어긋난다. 둘째, 배열만 교체하고 조회마다 람다를 만들면 sender의 매 회차가 객체를 만들어 쓰레기가 쌓인다.
- **어떻게(대안·결정·트레이드오프)**: 표(`MatchingSlotTarget[]`)와 그것을 읽는 조회 함수(`MatchingTargetView`)를 한 불변 레코드로 묶어 `volatile` 참조 하나로 교체한다. 갱신할 때만 새 view를 만들고, sender는 회차 시작에 view를 한 번 받아 그 회차의 모든 판단(보낼지, 보관할지, 보관 목록을 비울지)에 쓴다. 배열 원소는 주소와 「이동 중」 표시를 함께 담은 불변 `MatchingSlotTarget`이라 한 번 읽으면 둘이 서로 맞는다. 대안은 `ConcurrentHashMap`(키 박싱, 회차 안 일관성 없음)과 칸마다 `AtomicReferenceArray`(칸 사이 일관성 없음)였고 설계서 11-1절이 이미 기각했다. 대가: 갱신마다 참조 64개를 복사하고, sender가 한 회차 늦게 표를 볼 수 있다(늦게 본 표로 틀린 pod에 보낸 주문은 거부 회신으로 돌아온다).
- **무엇을(실제 변경·파일·커밋)**: `account-worker/.../coordination/MatchingDestinationResolver`, `MatchingSlotTarget`, `MatchingTargetLookup`, `MatchingTargetView`, `MatchingDestinationMisconfiguredException`(c2f2bf2, df7b63c).
- **결과·수치**: 단위 테스트 11개(U8) 통과. 구현을 일부러 제자리 수정으로 바꾸면 「한 번에 받은 변경 둘이 섞여 읽힌다」 테스트가 실패했다(섞여 읽힌 횟수 31,621). 실제 부하에서의 효과는 미측정.

### 담당이 없거나 이동 중인 종목 slot의 주문을 sender 안의 보관 목록에 두고, 거부 회신 입구를 offer 재시도 사이마다 비운다
- **context**: 계좌 shard의 queue는 1개이고 sender thread가 모든 매칭 pod의 Publication으로 보낸다(설계서 3-3절 Y). 담당 pod가 없는 종목 slot(첫 배정 전, pod가 죽음, 이동 중)의 주문을 꺼냈을 때 그 자리에서 기다리면 queue 맨 앞이 막혀 다른 pod행 주문도 못 나간다.
- **왜**: 담당이 생길 때까지 한 종목 slot 때문에 계좌 shard 전체가 멈추면 이동하지 않는 종목 slot까지 영향을 받는다. 또 매칭 pod가 담당이 아니라며 돌려보낸 주문(거부 회신)을 받는 스레드가 sender queue에 직접 넣으면 queue가 1:1 구조(생산자 하나)라는 전제가 깨진다. 마지막으로 매칭 pod의 주문 수신 스레드가 거부 회신 queue 앞에서 기다리면 그 pod는 주문을 더 읽지 않고, 계좌 sender가 그 pod로 보내는 offer를 재시도하는 동안 거부 회신 queue를 못 비우면 서로를 기다리는 순환이 생긴다(설계서 11-2절).
- **어떻게**: 종목 slot마다 「재전송 대기 목록(앞)」과 「신규 보관 목록(뒤)」을 두고 sender thread만 만진다. 거부 회신은 전용 1:1 큐(`acceptRejectedOrder`)에 넣고 sender가 비운다. 담당이 생기면 재전송 대기 목록을 먼저, 신규를 그다음에 보내 순서를 지킨다. sender는 offer가 음수를 돌려줘 재시도하는 동안에도 `idle()` 호출 사이마다 이 입구를 비워 보관 목록으로 옮긴다. 낱개를 목록 맨 앞에 넣으면 도착 순서가 뒤집히므로 앞 목록을 따로 둔 것이다(c1 제안). 대가: 재시도 중에 도착한 거부 회신이 이미 꺼낸 신규 보관 주문보다 늦게 나가는 좁은 구간이 남는다(설계서가 허용한 순서 뒤집힘 범위). 보관 목록은 메모리에만 있어 프로세스가 죽으면 사라지며 그때 계좌에 예약이 남는지는 설계서도 미확인으로 남겼다.
- **무엇을**: `AeronMatchingOrderSender`(`acceptRejectedOrder`, `hasHoldingRoom`, 보관 목록, 재시도 중 비우기)(df7b63c).
- **결과·수치**: 라우팅 테스트 11개 통과. 재시도 중 거부 회신 비우기를 지우면 순환 완화 테스트가, 보관 목록에서 신규를 재전송보다 먼저 꺼내면 순서 테스트가 각각 실패했다. 실제 순환이 일어나는 조건(Aeron 수신 버퍼가 몇 건에서 차는지)은 설계서가 미확인으로 남긴 그대로 미측정.

### 풀 설정이 없을 때의 Publication 1개 경로를 별도 코드로 두지 않고 「항상 같은 담당」 조회로 같은 코드에 통합한다
- **context**: 설계서는 `matching-shard-routing.endpoints`가 없으면 지금의 Publication 1개 경로를 그대로 두라고 했다(f2 승인). 기존 sender 테스트와 부하 하네스가 그 경로에 의존한다.
- **왜**: 경로를 둘로 두면 라우팅 코드는 풀이 있을 때만 시험되고, 기존 경로는 새 루프를 거치지 않아 두 경로가 따로 낡는다.
- **어떻게**: 기존 생성자를 「항상 같은 담당과 Publication 1개를 돌려주는 조회」와 종목 slot 1개짜리 해시로 감싸서 새 생성자에 위임했다. 그 결과 기존 sender 테스트 10개가 새 발신 루프를 그대로 지난다. 풀 설정 여부는 `@Bean`에 붙인 `NoneNestedConditions`로 가르고, 풀이 있을 때만 주소별 Publication, resolver, 초기 읽기 대기(phase 0), 거부 회신 입구를 만든다. 조건 단계를 `PARSE_CONFIGURATION`으로 선언했다가 `@Bean` 메서드에 붙은 조건이 무시돼 두 sender 빈이 둘 다 등록되는 `BeanDefinitionOverrideException`이 났고, `REGISTER_BEAN`으로 고쳤다. `SmartLifecycle`의 기본 phase는 가장 늦은 값이라 resolver 생명주기에 phase 0을 명시했다. 대가: 풀 없음 경로에도 보관 목록 배열(1칸)과 거부 회신 큐가 생긴다.
- **무엇을**: `MatchingShardRoutingConfig`, `MatchingShardRoutingProperties`, `MatchingDestinationResolverLifecycle`, `MatchingOrderSenderConfig`의 조건(df7b63c).
- **결과·수치**: 배선 테스트 6개 통과(풀 없음 경로, 풀 있음 경로, 초기 읽기가 끝난 뒤에야 컨텍스트가 시작을 마침, 풀 9개 거부, 보관 상한 설정, 종목 slot 개수 설정). `account-worker` 전체 122개 통과(재실행 기준, rebase 전).

### 보관 목록이 가득 찼을 때의 거부 — 설계의 「api가 503」은 코드와 어긋나고, 접수 전 거부를 핸들러에 그대로 넣으면 복구 때 상태가 어긋난다 (미완, U9e)
- **context**: 설계서 3-4-2절은 새 주문을 받을 때 그 종목 slot의 보관 목록에 여유가 없으면 예약을 잡기 전에 거부하고 api가 503을 준다고 확정했다. 계좌 엔진의 `forwardPlace`는 접수(예약) 뒤에 불린다.
- **왜(문제·동기)**: 코드를 읽어 두 가지가 어긋났다. 첫째, api의 503은 주문을 계좌 인테이크에 넘기지 못했을 때(목적지 없음, api 큐 가득, offer 3회 실패)만 나온다. 계좌 엔진의 거부는 202 응답 뒤에 일어나 `OrderResultRecorder`가 `reason.name()` 문자열로 `ORDER_RESULT` 스트림에 기록하고 Kafka로 전달되는 비동기 경로뿐이고, HTTP 응답으로 돌아가는 경로가 없다(api 모듈은 `RejectReason`을 참조하지 않는다). 둘째, 저널이 핸들러보다 먼저 입력을 기록하고(`AccountEngine.java:159`) 복구가 같은 핸들러에 저널의 입력을 다시 넣되 발신기는 no-op이다(`recover`, 364줄 부근). 핸들러가 발신기의 순간 상태로 거부하면 저널에는 그 주문이 남고 복구 때는 같은 주문이 접수되어 매칭으로 간 적 없는 예약이 영구히 남는다. 기존 거부(`INSUFFICIENT` 등)는 계좌 상태만으로 정해져 복구가 같은 결과를 낸다.
- **어떻게(대안·결정·트레이드오프)**: 결정(f2, 2026-10-29): 결과는 비동기 `REJECTED`(새 사유 `MATCHING_HOLDING_FULL`)로 하고 503은 없앤다. 복구 문제는 판정을 저널에 싣는 방식으로 푼다 — 주문을 링버퍼에 올리는 수신 스레드가 `hasHoldingRoom`(`AtomicIntegerArray` 읽기)을 읽어 「보관 목록 가득」 표시를 정해 이벤트와 저널 엔트리에 싣고, 핸들러는 표시만 보고 거부한다. 복구도 같은 표시를 읽어 같은 결과를 낸다. 조건: 기존 저널 엔트리 배치를 바꾸지 않고 새 값으로 더해 옛 엔트리를 읽는 코드가 그대로 동작해야 하고, 코덱 기존 시험이 통과해야 하며, 복구 재현 시험(같은 저널을 복구하면 같은 거부가 되풀이되고 예약이 없음)을 먼저 쓴다. 기각한 안: 보관 목록 상한 없이 쌓기(Jack이 확정한 「접수 전에 거부」와 다름), 접수 후 취소 경로를 새로 만들기(3-4절에서 이미 기각). 대가: 저널 엔트리 형식이 바뀐다. 이 저장소에는 운영에 올라간 계좌 저널이 없으나 코덱이 호환되게 더할 수 있는지는 코덱을 아직 읽지 않았다 [미확인].
- **무엇을**: 코드 변경 없음(멈춤). 인계 메모에 읽은 지점과 승인된 방식을 남겼다.
- **결과·수치**: 해당 없음(미구현).

### 계측 출력은 설계서가 비워 둔 곳이라 기존 방식(카운터와 1초 로그 한 줄)을 따른다
- **context**: 설계서 3-6절은 매칭행 발신에서 세 값(queue 쌓임, 계좌 엔진 스레드가 queue에 넣으려고 기다린 시간, offer 재시도 횟수)을 세라고 했고 내보낼 곳은 `StageLatencyRegistry`에 실을 수 있는지 읽지 않아 [미확인]이었다.
- **왜**: `StageLatencyRegistry`는 지연 히스토그램(나노초)용이고 구간 분해 측정 브랜치 한정이며, 재시도 횟수는 시간이 아니라 개수라 맞지 않는다. 계좌 워커에는 이미 카운터와 1초 로그 방식의 리포터(`MetricsAccountResultListener`, `account-worker.metrics.enabled=true`일 때만)가 있다.
- **어떻게**: 같은 스위치로 `matching-send outbox=N enqueueWaits=N enqueueWaitMs=X offerRetries=N` 한 줄을 구간 증가분으로 내보낸다. 계좌 엔진 스레드와 sender thread는 `LongAdder`만 올리고 출력은 전용 데몬 스레드가 한다. 대가: 값이 로그에만 남아 하네스가 파싱해야 한다.
- **무엇을**: `MatchingSendMetrics`, `MatchingSendMetricsReporter`, 생명주기와 배선(7198ddc).
- **결과·수치**: 새 테스트 5개 통과(쌓인 개수, 기다린 시간과 횟수, 재시도 횟수, 로그 한 줄 형식, 스위치 배선). 재시도 증가 줄을 지우면 재시도 테스트가 실패했다. 실제 값은 미측정.

## 블로그 네타

### "거부했는데 복구하면 접수돼 있다 — 저널을 먼저 쓰는 엔진에서 외부 상태로 거부하면 생기는 일"
- **훅·핵심 주장**: 입력을 먼저 저널에 쓰고 나중에 판단하는 엔진은, 판단이 입력 이벤트와 엔진 상태만으로 정해질 때만 복구가 같은 결과를 낸다. 발신기의 순간 상태 같은 바깥 값으로 거부하면 복구가 그 거부를 되풀이하지 못해 예약이 유령처럼 남는다.
- **context**: 계좌 엔진은 single-writer(스레드 하나가 상태를 바꾼다)이고 링버퍼 → 저널 → 비즈니스 핸들러 순서로 처리한다. 복구는 저널의 입력을 핸들러에 다시 넣는다.
- **어떻게(서사·근거)**: 확정된 설계 문장(「예약 전에 거부, api가 503」)을 구현하려고 접수 경로를 읽다가 두 가지를 발견했다. 하나는 api의 503이 계좌 엔진의 거부와 이어져 있지 않다는 것이고, 다른 하나가 더 크다. 저널이 핸들러보다 먼저라 거부된 주문도 저널에 남고, 복구용 발신기는 no-op이라 같은 질문에 다른 답을 한다. 코드를 쓰기 전에 멈추고 보고해 판정을 저널에 싣는 방식이 승인됐다. 코드를 읽지 않고 구현했다면 테스트는 통과하고 복구 후에만 틀렸을 것이다.
- **재료**: `AccountEngine.java:159`와 `recover`(364줄 부근), 위 ADR 네타, 인계 메모의 복구 재현 시험 설계(구현 전).

### "테스트가 한 번에 다 통과했을 때 — 변이로 테스트가 일하는지 확인하기"
- **훅·핵심 주장**: 처음 돌렸더니 11개가 전부 통과했다면 구현이 맞다는 증거가 아니라 테스트가 느슨하다는 의심의 시작점이다. 구현을 일부러 망가뜨려 그 테스트가 실패하는지 본다.
- **context**: 읽는 스레드와 쓰는 스레드가 표를 공유하는 `volatile` 배열 통째 교체, 재시도 중 입구 비우기, 보관 목록 순서 같은 동시성·순서 성질은 단순 예제로는 통과하기 쉽다.
- **어떻게**: 표 갱신을 복사 없이 제자리 수정으로 바꿔 보니 일관성 테스트가 31,621번 섞인 읽기를 잡았다. 재시도 중 비우기를 지우면 순환 완화 테스트가, 목록 순서를 바꾸면 순서 테스트가 실패했다. 첫 변이 시도는 macOS `sed`가 인자를 거부해 파일이 바뀌지 않은 채 「통과」가 나왔다 — 변이가 실제로 적용됐는지 확인하지 않은 통과는 아무것도 증명하지 않는다.
- **재료**: 변이 세 가지의 실패 출력, `MatchingDestinationResolverTest`, `AeronMatchingOrderSenderRoutingTest`.

### "@Bean에 붙인 조건이 안 먹는다 — 조건을 평가하는 단계가 따로 있다"
- **훅·핵심 주장**: `NoneNestedConditions`의 단계를 `PARSE_CONFIGURATION`으로 두면 `@Bean` 메서드에 붙은 조건은 무시된다. `REGISTER_BEAN`이어야 한다. 풀 없음 경로 테스트는 통과하고 풀 있음 경로 테스트만 실패해서 찾았다.
- **어떻게**: 한쪽 경로만 통과하는 테스트로 끝냈다면 발견하지 못했다. f2가 「풀이 있을 때의 경로도 테스트로 확인하라」고 요구한 것이 이 버그를 드러냈다.
- **재료**: `BeanDefinitionOverrideException` 메시지(`matchingOrderSender` 중복 정의), `MatchingOrderSenderConfig`의 `PoolNotConfigured`, `MatchingOrderSenderConfigTest`.
