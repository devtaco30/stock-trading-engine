---
feature: matching-limit-test-harness
date: 2026-10-31
branch: feat/stock-slot-hasher
commits: [55bfad7, 3958390, 14bbe1a, 4f327f4, c493697, 91aeb74, f915aae, 548dfc2, 2730605, e778df1, 9620177, f64b7af]
feeds: [adr, blog]
---

# 주문 처리 한계 시험 하네스의 매칭 쪽 (여러 매칭 shard를 띄우고, 배정이 끝난 뒤 부하를 걸고, 실행 중에 하나 더 띄운다)

## ADR 네타

### 실행 중 증설: 호가창 이동 주소 풀은 기동 때 고정이므로 시작 shard의 풀에 증설할 shard 주소를 처음부터 넣는다
- **context(무슨 상황)**: 매칭 shard를 M개로 시작해 부하를 건 뒤 `matching-M`을 하나 더 띄워 종목 slot이 옮겨 가는 동안의 처리를 재는 시험(설계서 8절 H9)이 필요했다. 호가창 이동(X가 가진 종목 slot을 Z로 넘기는 일)은 매칭 shard끼리 Aeron으로 주고받고, 서로의 입구 주소는 설정 `matching-shard.handover.endpoints`(풀)에 적는다.
- **왜(문제·동기)**: 풀을 코드로 읽으니 기동 때 한 번만 읽힌다. `HandoverRequester`·`HandoverResponder` 생성자가 풀의 peer마다 `Publication`을 한 번 연다. 풀에 없는 shard가 나중에 합류하면 두 쪽이 다르게 실패한다. 요청하는 쪽(`HandoverRequester.request`의 `requireInPool`)은 예외를 던지고 `HandoverCoordinator`가 ERROR 로그를 남긴 뒤 간격을 두고 재시도한다. 응답하는 쪽(`HandoverResponder.handleRequest`)은 요청한 shard의 회신 주소가 자기 풀에 없으면 `requestsRejectedByPool`만 올리고 요청을 버려 응답이 가지 않으므로 이동이 끝나지 않는다. 증설에서 실제로 생기는 실패는 응답하는 쪽이다. 처음 쓴 설명은 「requireInPool이 멈춘다」였고 리뷰(f2·b4)가 부정확하다고 잡아 코드로 다시 확인해 고쳤다.
- **어떻게(대안·결정·트레이드오프)**: (가) 시작 shard의 풀에 증설할 shard의 입구 주소를 처음부터 넣는다. 아직 뜨지 않은 상대가 풀에 있는 것은 허용된다는 점을 2 shard 실제 기동으로 확인했다. 대가는 증설 여부를 시작 때 알아야 한다는 것이다(`MATCHING_EXPAND_AT_SEC` 유무로 정한다). (나) 풀을 실행 중에 바꾸게 코드를 고친다. 이 변경은 시험 하네스 단계의 범위가 아니고 풀을 동적으로 바꾸는 설계가 따로 필요해 하지 않았다. 시작 shard 수는 `MATCHING_COUNT` 검사와 같은 1·2·4만 허용한다(증설 뒤 최대 8개, 풀 상한 `HandoverEndpointPool.MAX_ENDPOINTS`).
- **무엇을(실제 변경·파일·커밋)**: `loadtest/matching-plan.sh`의 `matching_pool_count`·`matching_expansion_valid`, `loadtest/expand-matching.sh`, `run-e2e-v2.sh` 연결(f915aae, 정정 e778df1).
- **결과·수치**: 계획 인자와 검사는 `test_matching_plan.sh` 128개로 고정. 증설 뒤 호가창 이동이 실제로 일어나는지는 통합 실행에서만 보이고 실행하지 않았다(미측정).

### 배정 완료의 정의: 종목 slot 64개가 모두 올라왔고, 서로 다른 주소 수가 shard 수와 같을 때
- **context**: 부하는 모든 종목 slot이 안정 상태(평상시 주소)일 때만 걸어야 한다. 하네스가 shard N개를 띄우고 맵 토픽 `matching-shard-map`(compacted, 키=종목 slot, 값=주소 또는 `주소;moving`)에 64개가 올라오기를 기다린다.
- **왜**: 처음 구현은 「64개가 올라옴」만 봤다. 리뷰가 두 구멍을 짚었다. 한 shard가 모든 종목 slot을 먼저 받은 시점(다른 shard의 consumer 가입이 늦음)에도 통과하고, 이후 재배정으로 slot이 이동 중이 되는 동안 부하가 시작된다. 또 토픽이 compacted라 죽은 pod의 마지막 값이 남아, 직전 실행(예: 8개 shard)의 값이 새 shard가 뜨기도 전에 배정 완료로 읽힐 수 있다.
- **어떻게**: 조건에 「올라온 서로 다른 주소 수 = shard 수」를 더했다. `;moving`이 붙은 slot은 배정 끝으로 세지 않는다(f2가 부하 전에는 모든 slot이 평상시 주소여야 한다고 확정). 시작 전에 하네스 broker에서 `matching-shard-map` 토픽만 지우고 사라질 때까지 기다린다(조회·삭제 명령의 종료 코드를 확인한다). 조정 토픽과 컨슈머 그룹의 정적 멤버는 지우지 않으므로, 직전 실행보다 shard 수를 줄여 다시 띄우면 이전 멤버가 남아 배정 대기가 제한 시간까지 갈 수 있다(안전하게 실패하고 주인 없는 종목 slot을 출력한다). 제한 시간의 값(`MATCHING_ASSIGN_TIMEOUT`)은 기동 때 첫 배정에 걸리는 시간의 측정 기록이 없어 정하지 않고 필수 인자로 둔다.
- **무엇을**: `await_matching_slots_assigned`·`reset_matching_map_topic`(3958390, 4f327f4, 2730605).
- **결과·수치**: 실제 Kafka에 matching-worker 2개를 인자 그대로 띄워 종목 slot 64개가 주소 20020에 32개, 20021에 32개로 배정되는 것을 확인했다(자체 Kafka 컨테이너, 시험 잠금 안).

### 종료 스크립트는 이름 일괄 종료(pkill -f)를 쓰지 않고, 이 하네스가 PID 파일로 기록한 프로세스만 멈춘다
- **context**: `stop-v2.sh`는 같은 main 클래스 이름의 프로세스를 `pkill -f`로 모두 종료했다. 여러 세션이 같은 저장소의 worktree들에서 같은 앱을 띄우는 환경이다.
- **왜**: 다른 worktree·세션이 띄운 같은 클래스의 JVM이 같이 죽는다. 그렇다고 PID 파일의 런처 PID만 죽이면 `gradle bootRun`이 Gradle 데몬에 실행을 넘겨 실제 JVM이 런처의 자손이 아닐 수 있어 JVM이 남는다. Spring Boot 3.4.0은 `ApplicationPidFileWriter`를 자동 등록하지 않아 `spring.pid.file`로 JVM PID를 얻는 방법도 막혀 있었다(jar의 `spring.factories`로 확인).
- **어떻게(대안·결정·트레이드오프)**: 종료 대상을 둘로 줄였다. (1) PID 파일의 런처(gradlew)와 그 자손. Gradle 데몬은 다른 worktree의 빌드도 맡으므로 자손이어도 제외한다. (2) 그 라벨의 고정 포트(매칭 shard i는 UDP 20020+i, api는 TCP 8080, 계좌 워커 UDP 20040·20041)를 쥔 프로세스. 두 경로 모두 「이 worktree의 프로세스인가」(명령행에 이 저장소 경로가 있거나 작업 디렉터리가 저장소 안)와 명령행에 그 모듈의 main 클래스(런처는 gradlew)가 있는 것을 통과해야 한다. pid가 재사용되어 다른 worktree의 gradlew가 PID 파일의 pid가 되어도 건드리지 않는다. 종료 뒤에도 남는 대상, 포트 규칙이 없는 모듈(settlement-worker, account-projection-worker)의 남은 JVM은 경고만 낸다. 대가: 포트 표를 하드코딩해 포트가 바뀌면 같이 고쳐야 하고, 이 worktree 판정은 실제 `bootRun` JVM의 명령행에 저장소 경로가 있는지에 기대고 있다(대역으로만 확인).
- **무엇을**: `loadtest/stop-v2.sh`, `loadtest/test_stop_v2.sh`(91aeb74, 2730605).
- **결과·수치**: 대역 프로세스 16개 단언 통과. 실제 앱에서는 미확인.

### 거부 회신 pending과 호가창 이동 보관본을 포함한 계측 카운터의 의미를 사실대로 고친다
- **context**: 매칭 shard별 받은 주문·담당 밖 버림·낸 체결 수를 12-3절 합계 식 `라 = 마 + 바 + 사`에 쓰려는데, 카운터 주석이 실제 동작과 달랐다.
- **왜·어떻게**: 주석은 「저널을 다시 적용하며 낸 체결도 센다」고 썼지만 `MatchingEngine.recover()`는 새 핸들러 인스턴스로 저널을 재적용하므로 복구 중 체결은 라이브 카운터에 안 들어간다. 시험(복구 뒤 라이브 체결 1건만 센다)으로 고정하고 주석을 고쳤다. 「담당 밖 버림」(`notOwnedCount`)은 회신 발신 queue에 넣은 수이지 계좌가 받은 수가 아니라서, 송신 성공 카운터(`RejectReplyPublisher.sentCount`)를 더해 보고기 줄 `rejectRepliesSent`로 대조할 수 있게 했다. 받은 주문 수(`placeAcceptedCount`)는 중복 도착을 포함한다(중복은 엔진 안에서 걸러진다). 취소는 Aeron으로 보내는 코드가 main에 없어 식에 칸을 넣지 않았다.
- **한계로 남긴 것(12-3절 문안)**: 재시작이나 종목 slot 이동을 낀 시험에는 합계 식이 성립하지 않는다. 취소가 생기면 취소로 호가창을 떠난 주문이 어느 칸에도 안 세어져 유실 후보로 잘못 읽힌다.
- **무엇을(커밋)**: 14bbe1a, 링버퍼 대기 시간 `ringFullWaitMs`(c493697).

## 블로그 네타

### "시험 하네스를 짜다가 리뷰어에게 여러 번 지적받은 이야기: 부하 시험의 시작 조건이 얼마나 쉽게 거짓이 되나"
- **훅·핵심 주장**: 「64개 슬롯이 모두 배정됐다」는 한 줄 조건이 shard 수, 직전 실행의 compacted 토픽 잔재, 이동 중 표시, 읽기 timeout 네 군데에서 거짓 양성으로 샌다.
- **context**: 매칭 shard를 여러 개 띄워 주문 처리 한계를 재는 시험의 하네스. 부하를 걸기 전 「배정이 끝났다」를 어떻게 판정할지가 처음에는 한 줄이었다.
- **어떻게(서사·근거)**: (1) 64개만 보면 한 shard가 먼저 다 받은 순간 통과한다. (2) compacted 토픽이라 죽은 pod의 마지막 값이 남는다. (3) `;moving`을 배정으로 세면 이동 중에 부하가 시작된다. (4) 실제 broker에서 console consumer를 돌려 보니 `--timeout-ms 3000`이 짧아 빈 출력으로 끝났고, 빈 시각이 「성공」으로 기록되는 경로가 실제로 있었다. 형식 자체(`CreateTime:<ms>\t키\t값`)는 가정과 같았다. 대역 시험으로는 (4)가 안 보였고 실제 broker를 한 번 돌려서야 보였다.
- **재료**: 커밋 3958390, 4f327f4, 9620177. 실제 broker 확인 결과: 파서가 낸 첫 레코드 시각이 생산자가 받은 timestamp와 정확히 일치.

### "pkill -f 하나가 다른 worktree의 JVM을 죽인다: 여러 세션이 같은 저장소를 쓸 때 프로세스 종료 규칙"
- **훅·핵심 주장**: 이름으로 프로세스를 죽이면 같은 이름의 남의 프로세스가 죽는다. PID 파일로 바꾸면 이번엔 `gradle bootRun`이 JVM을 Gradle 데몬 쪽에 만들어 런처 PID로는 안 닿는다.
- **어떻게**: 런처와 자손, 고정 포트를 쥔 JVM, 둘 다 「이 worktree의 프로세스」 검사를 통과해야 한다. 테스트는 sleep과 UDP 포트를 쥔 파이썬 대역으로 「같은 이름이지만 다른 작업 디렉터리」가 살아남는지를 본다. 시험 자체가 처음에 두 번 멈췄다(백그라운드 프로세스가 `$(...)` 파이프를 쥐고 있어 300초 기다림).
- **재료**: 커밋 91aeb74, 2730605, 시험 `loadtest/test_stop_v2.sh`.

## 메모리 무한 증가 점검(2026-10-31, 읽기만)
- 결과는 `docs/_matching_memory_audit.md`(커밋 제외). 상한 없이 입력에 비례해 늘 수 있는 것: 호가창 본체(와 스냅샷·보관본 복사본), `RejectReplyPublisher.pending`, 종목코드 수에 비례하는 `books`·시세 맵. 실제 메모리 증가는 측정하지 않았다(미측정).
