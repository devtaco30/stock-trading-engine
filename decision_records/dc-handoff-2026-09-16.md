---
feature: dc-handoff
date: 2026-09-16
branch: feat/c7-load-harness
feeds: []
---

# dc 인수인계 — main 정리 완료 시점 (2b 지시로 flush)

컨텍스트 관리 차원에서 dc가 여기서 flush한다. **main 정리는 이 문서 작성 직전에 완료했다**
(아래 1절 맨 위 "완료" 참고) — 순서를 바꿔 병합 먼저, flush는 그 직후로 진행했다(2b 지시).

## 1. main 정리 — 완료

**사고**: main이 실수로 `feat/c7-load-harness`로 fast-forward됐다(원래 main 커밋 위에 병합해야
했는데, 반대로 c7 브랜치를 main에 병합하면서 fast-forward가 조용히 일어남 — 병합 커밋 없이
포인터만 이동해서 아무도 못 봤다).

**완료(2026-09-16, 이 문서 작성 직전)**:
1. `ShardRoutingConfigTest`의 예외 로그를 실패로 오판했던 것 — `--tests
   "*ShardRoutingConfigTest*"` 단독 재확인으로 **오탐 확정**(그 테스트 자체가 "갭 있는
   설정이면 기동 실패해야 한다"를 검증하는 네거티브 테스트, 3개 다 PASSED). 2b가 코드로도
   대조: slot-count=256에 0~100·150~255만 채워 101~149가 의도적으로 비어 있는 테스트
   픽스처였다. 병합 블로커 아니었음.
2. main 워크트리 `git status --short` 확인 — 추적 파일 수정 2건(`measure-e2e.sh`·
   `replay-e2e.js`)이 c7의 커밋본과 바이트 단위로 동일함을 diff로 확인 후 폐기 가능 판단.
3. `CLAUDE_GIT_OK=1 git reset --hard 2544421` 실행(파괴적 명령 훅 — 2b·Jack 승인 하에
   플래그 붙여 실행). main이 `2544421`("merge: v2 Aeron/워커 자원 관리 4건 + Gradle 캐시
   전역 비활성화 (ec)")로 정상 복귀.
4. untracked `loadtest/reset-e2e-v1-state.sh`가 병합과 충돌(c7 커밋본과 동일 확인 후 삭제,
   병합이 다시 만들어줌).
5. `git merge --no-ff feat/c7-load-harness`로 병합 완료 — 커밋 `448dd47`. 토폴로지 확인:
   ```
   *   448dd47 merge: v1/v2 전 과정(e2e) 측정 트랙 — 계측·시드·드라이버·A/B 결과 (dc·a4·ec·39)
   |\
   | * 2d7f119 docs(decision): v2 대기전략 BlockingWaitStrategy로 정정(리포트와 일치)
   | * 53c6202 docs(decision): B-2 꼬리 측정불가 최종 문구 반영 (39, 2b 최종판정)
   | * 478e168 docs(decision): v1/v2 측정 A/B 분리·실측 결과 반영 (39)
   ...
   ```
   `--no-ff`라 병합 커밋이 남았고, main 히스토리에 fast-forward 흔적이 없다.
6. `./gradlew build` — main에서 BUILD SUCCESSFUL(전부 c7과 동일 내용이라 UP-TO-DATE).
7. **push 안 함(로컬만)** — 2b 지시대로.

**아직 안 한 것(다음에 이어받을 사람이 할 일)**:
- **재발 방지 문구를 결정 기록에 아직 명시적으로 안 남겼다** — "측정·실험 브랜치는 검증이
  끝나기 전에 main에 넣지 않는다. main은 `--no-ff`로만 움직인다. 이번엔 fast-forward라
  조용히 넘어가서 아무도 못 봤다"를 어느 decision record(예: 이 파일 자체이거나 새
  `git-merge-policy.md`)에 넣을 것. 병합 커밋 메시지에는 이 취지를 한 문단 넣어뒀지만
  별도 결정 기록으로도 남기라는 게 2b 지시였다.
- `loadtest/results/*.json`(k6 원본 출력)을 커밋에 포함할지 — **아직 판단 안 함**. 지금은
  `.gitignore`(`loadtest/results/`) 대상이라 어디에도 커밋 안 돼 있다. 리포트가 이 파일들을
  "원본 데이터"로 인용하므로 같이 들어가는 게 맞다는 게 2b 의견 — 용량 확인 후 대표 파일만
  추릴지 결정할 것.
- a4·ec에게 "main 정리 끝났다" 후속 통보 아직 안 함.
- README v2 절 수치 채우기, docs 브랜치 병합은 Jack 판단 받고 진행(2b와 합의) — 아직 착수 전.

**취소된 것**: "리포트에 v1/v2 DB 쓰기 위치 비대칭 명시 + v1이 불리하게 측정된 게 아니라는
근거 두 줄 추가" 지시는 2b가 냈다가 Jack이 스탑 걸어서 취소됐다(v1이 DB를 거치고 v2가 안
거치는 게 두 아키텍처의 차이 자체이자 v2가 빠른 이유이므로, 그걸 "비대칭 주의사항"처럼
다는 건 방향이 틀렸다는 게 최종 판단) — 시작도 안 했으니 되돌릴 것 없음, 리포트는 지금
상태(9절까지) 그대로 둔다.

## 2. 측정 실행 방법 (재현·재실행용)

### 스크립트
- `loadtest/run-e2e-v1.sh` / `run-e2e-v2.sh` — v1/v2 앱 기동. **v2용 api를 수동으로 띄울 땐
  반드시 `--spring.profiles.active=udp,e2e`로(`e2e`만 주면 Aeron 채널 설정이 안 실려 전체
  주문이 100% 실패한다 — 오늘 실제로 겪은 버그, `ORDER_PUBLISH_FAILED` offer=-1로 나타남).**
- `loadtest/stop-v1.sh` / `stop-v2.sh` — 정지.
- `loadtest/reset-e2e-v1-state.sh <partitions>` — v1 측정 전 필수. Postgres(계좌·주문·holdings·
  unpaids, 2001~2100 범위)와 `order-requests` 토픽을 **삭제·재생성**한다. **Postgres만 지우면
  안 된다** — 토픽에 남은 이전 실행 백로그를 새 컨슈머 그룹이 그대로 이어받아 소비해 몇 시간
  전 메시지의 지연이 새 측정에 섞인다(오늘 두 번 재현, `order-requests` lag 4620·48만 건
  잔재 등). `kafka-consumer-groups.sh --list`에 그룹이 안 보여도 `__consumer_offsets`에
  오프셋이 남아있을 수 있어 안심할 수 없다 — 토픽 자체를 지우는 게 확실하다.
- `loadtest/replay-e2e.js` — k6 리플레이 드라이버. `INPUT_FILE`(절대경로 권장)·`VERSION`
  (v1/v2)·`PASS`(requestId 접미사, 같은 파일 여러 패스 재생 시 필수)·`N`(건수 제한, A용)·
  `RATE`(초당 건수, B용 — 있으면 constant-arrival-rate로 전환, 없으면 shared-iterations)
  환경변수로 제어.
- `loadtest/generate-e2e-input.py` — ec가 만든 결정론 페어 입력 생성기. 재실행하면
  `loadtest/results/e2e-input.jsonl`·`api/src/main/resources/e2e-seed.sql`·`account-worker/
  src/main/resources/application-e2e.yml`을 전부 재생성한다(완전히 결정론적, diff 없음
  확인됨).

### 히스토그램 트리거(a4가 만든 것)
- `measure.latency.enabled=true` + `measure.latency.snapshot-trigger-path=<절대경로>` +
  `measure.latency.output-path=<절대경로>`를 v1은 order-engine에, v2는 account-worker에
  준다(`run-e2e-v1.sh`/`run-e2e-v2.sh`가 이미 이렇게 넘긴다).
- **경로는 반드시 절대경로.** `gradlew bootRun`의 JVM 작업 디렉터리(`user.dir`)가 리포
  루트가 아니라 **모듈 서브디렉터리**(예: `account-worker/`)라서, 상대경로를 주면 트리거
  폴링 스레드가 엉뚱한 곳(`account-worker/loadtest/results/...`)을 본다 — 오늘 이걸로
  한 번 데이터가 "유실된 줄" 알았다가 실제로는 그 서브디렉터리 밑에 멀쩡히 있는 걸 찾아
  옮긴 적 있다.
- 트리거 파일에 원하는 출력 경로를 한 줄로 쓰면(예:
  `echo "$(pwd)/loadtest/results/latency-v1-pass1.json" > loadtest/results/latency-trigger-v1`),
  0.2초 폴링 주기로 감지해 그 경로에 스냅샷을 쓰고(원자적 rename) 트리거 파일을 지운다.
  트리거 파일이 사라짐 = 처리 완료.

### 검증(반드시 할 것)
매 패스 끝나면 "보낸 건수 == 반영된 건수"를 확인한다 — v1은
`SELECT count(*) FROM orders WHERE account_id BETWEEN 2001 AND 2100`(패스 전후 delta),
v2는 히스토그램 스냅샷의 `count` 필드. 오늘 이 검증을 안 하다가 v2 api 프로파일 버그를
count=0을 보고서야 뒤늦게 발견했다(2b 지시로 이후 검증을 넣음, 다만 `measure-e2e.sh`
스크립트 자체에는 아직 자동화 안 돼 있고 수동 확인만 함 — 스크립트에 넣는 건 남은 일).

## 3. 최종 확정 측정 결과 (이미 리포트·결정 기록에 반영됨, 참고용 요약)

- **A(용량)**: v1 동시성3=227.3건/초, 동시성9=384.6건/초(3배 안 오름=DB가 일부 벽으로
  추정, 락 경합인지 insert 단가인지 미확인), v2≥4000건/초(k6 송신 속도 자체가 한계, 실제
  천장 미측정).
- **B(지연, 헤드라인=B-1)**: v1(동시성9) @192건/초 p50 12~13ms·p99 50~56ms, v2 @192건/초
  p50 1.9~2.0ms·p99 3.5~4.2ms. B-2(v2 @2000건/초)는 p50만 인용(0.36~0.92ms), p99·max는
  "이 장비에서 측정 불가"(k6 자체가 CPU 54~69% 먹어 서버와 경쟁 — CPU 샘플링으로 확인).
- 산출물: `docs/_c7_v1_v2_throughput_report.html` 9절, `decision_records/
  v1-v2-e2e-measurement.md`(39가 개정).

## 4. 아직 커밋 안 된 것 / 확인 필요한 것

- c7 워크트리(`feat/c7-load-harness`)는 이 flush 시점 기준 전부 커밋됨(마지막 커밋
  `2d7f119` "v2 대기전략 BlockingWaitStrategy로 정정"). `git status --short`로 재확인할 것.
- main 워크트리(`../stock-trading-engine-main`)는 `docs/`(커밋 제외 대상)만 미커밋 —
  정상이다. 그 외 미커밋이 있으면 위 1절의 reset 전에 먼저 처리할 것.
- `loadtest/results/*.json`(k6·히스토그램 원본 출력)은 main worktree의 gitignore(`results/`)
  대상이라 지금 어디에도 커밋 안 돼 있다 — 위 1-6절 판단 필요.
- v1 프로세스는 마지막으로 B 측정(9절) 이후 정지시켰다. v2는 B-2 재실행 이후 정지시켰다.
  둘 다 지금 안 떠 있어야 정상 — `ps aux | grep -E "OrderEngineApplication|
  AccountWorkerApplication"`로 확인할 것.
- a4·ec에게 "잠깐 기다려달라"(main 정리용 단독 테스트 때문에)는 메시지를 보냈지만
  "이제 끝났다"는 후속을 아직 못 보냄 — main 정리가 재개되면 다시 확인 후 알릴 것.

## 5. 진행 중이던 대화 맥락

- 2b(조정), 39(설계·리뷰), a4(계측 훅 구현), ec(입력·시드 생성) 네 세션과 오늘 하루
  종일 이 e2e 측정 트랙을 병렬로 진행했다.
- 남은 일(README v2 절 수치 채우기, docs 브랜치 병합)은 Jack 판단 받고 진행하기로
  2b와 합의된 상태 — dc가 먼저 손대지 않는다.
