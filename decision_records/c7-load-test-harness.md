---
feature: c7-load-test-harness
date: 2026-09-15
branch: feat/c7-load-harness
commits: []
feeds: [adr, blog]
---

# C7 부하 테스트 하네스 — v1/v2 처리량 대조 자료 만들기

이력서 제출용 수치(v1 vs v2 × 단일계좌 vs 50계좌 분산)를 만들려고 기동 스크립트·메트릭 카운터·
k6 스크립트를 짰다. 애플리케이션 코드(AccountState·리스너)는 하루 만에 붙었는데, 실제로 막힌
지점은 전부 **여러 Claude 세션이 같은 머신을 동시에 쓰는 협업 환경 + 로컬 OS/도구의 관용구
차이**였다 — 코드 문제가 아니라 인프라 문제였다는 게 이 작업의 핵심 교훈.

## ADR 네타

### 공유 워킹 디렉터리에서 브랜치를 바꾸면 다른 세션이 밟고 있는 땅이 흔들린다 — worktree로 격리
- **context**: 이 세션(dc)과 설계 세션(39), 조율 세션(2b), 그 외(ec)까지 같은 저장소 경로를
  동시에 씀. 새 작업(C7)을 받으면서 `git checkout -b`로 공유 디렉터리에 새 브랜치를 만들었다.
- **왜(문제)**: `git checkout -b`는 그 순간 공유 워킹 디렉터리의 HEAD·워킹트리 파일을 통째로
  바꾼다. 이 디렉터리를 동시에 참조하는 다른 세션(ec가 `fix/v2-resource-management`를 체크아웃
  중이었음)이 갑자기 다른 브랜치의 파일을 보게 된다 — 사고.
- **어떻게(정정)**: 즉시 원래 브랜치로 되돌리고, `git worktree add ../stock-trading-engine-c7
  -b feat/c7-load-harness 5a65cd3`로 별도 디렉터리에 격리된 워킹트리를 새로 만들어 그 안에서
  작업. 다른 세션(2b)도 같은 패턴으로 `../stock-trading-engine-docs` 워크트리 사용.
- **무엇을**: 이후 전 작업을 `/Users/jack/Study/develop/F-lab/stock-trading-engine-c7`에서 진행.
- **결과·수치**: 미측정(사고 자체는 checkout 즉시 되돌려 실제 피해 없음, ec에게 영향 줬는지는
  확인 못 함).

### `docker-compose.loadtest.yml`은 커밋 대상이 아니라 워크트리마다 따로 있어야 한다
- **context**: `docker-compose.loadtest.yml`(postgres 포트 9702 오버라이드)은 "머신 로컬 전용"
  이라 `.gitignore` 대상. worktree는 git 추적 파일만 새 디렉터리에 나타나고 untracked 파일은
  공유되지 않는다.
- **왜**: `run-v2.sh`를 처음 돌렸을 때 `open .../docker-compose.loadtest.yml: no such file or
  directory`로 즉시 실패 — 메인 체크아웃에는 있는데 새 워크트리엔 없었다.
- **어떻게**: 메인 디렉터리에서 그대로 복사해 넣었다. worktree를 새로 만들 때마다 이 파일을
  로컬 전용으로 다시 복사해야 한다는 뜻 — git이 자동으로 못 해준다.
- **무엇을**: `stock-trading-engine-c7/docker-compose.loadtest.yml`(로컬 전용, 커밋 안 됨).
- **결과·수치**: 복사 후 정상 동작.

### docker compose 프로젝트명이 디렉터리명에 묶여, 워크트리에서 돌리면 컨테이너 이름이 충돌한다
- **context**: `run-v2.sh`가 이미 7일째 떠 있는 공유 인프라 컨테이너(`stock-trading-postgres`
  등, `container_name`으로 고정된 이름)를 재사용해야 한다.
- **왜**: Docker Compose는 `-p`(프로젝트명)를 안 주면 **현재 디렉터리명**을 기본 프로젝트명으로
  쓴다. 메인 체크아웃은 `stock-trading-engine`, 워크트리는 `stock-trading-engine-c7`이라 서로
  다른 프로젝트로 인식 — 같은 `container_name`(`stock-trading-redis` 등)을 가진 컨테이너를 새로
  만들려다 "Conflict: 이미 그 이름의 컨테이너가 있다"로 실패. (redis·postgres는 조용히 같이
  실패, 부산물로 빈 네트워크·볼륨만 생성됨 — 수동 `docker network rm`/`docker volume rm`으로 정리.)
- **어떻게**: `docker compose -p stock-trading-engine -f docker-compose.yml -f
  docker-compose.loadtest.yml up -d`로 프로젝트명을 메인 체크아웃과 동일하게 고정 — 기존
  컨테이너를 "같은 프로젝트"로 인식해 재생성 없이 재사용.
- **무엇을**: `run-v2.sh`·`run-v1.sh` 둘 다 `-p` 플래그 추가.
- **결과·수치**: 실측 — 고정 후 2회 연속 `run-v2.sh` 성공, 컨테이너 재생성(따라서 recording 등
  상태 초기화) 없이 기존 인프라 그대로 사용됨.

### macOS 기본 `/bin/bash`(3.2)는 연관 배열을 못 쓴다
- **context**: `stop-v2.sh`가 모듈명→main 클래스명 매핑에 `declare -A`(연관 배열) 사용.
- **왜**: macOS는 GPLv3 라이선스 회피로 시스템 `/bin/bash`를 3.2.57(2007년 릴리스)에 계속
  고정해 배포한다. `declare -A`는 bash 4.0(2009)부터 있는 기능이라, `#!/usr/bin/env bash`가
  이 시스템 bash로 풀리면 스크립트가 `line 13: api: unbound variable`로 즉시 죽는다 — 연관
  배열 선언 자체가 조용히 무시되고 이후 인덱싱이 깨지는 방식.
- **어떻게**: 연관 배열 대신 `case`문 안의 함수(`main_class_for`)로 모듈명→클래스명 매핑을
  bash 3.2에서도 동작하게 다시 썼다.
- **무엇을**: `stop-v2.sh` 수정.
- **결과·수치**: 실측 — 수정 후 `bash -n` 문법 검사 통과, 실행 시 3개 프로세스 전부 `pkill -f`로
  정상 종료 확인(`lsof`·`ps`로 잔존 없음 재확인).

### Gradle Daemon이 `bootRun`을 포크하면, 런처 PID를 죽여도 실제 앱은 안 죽을 수 있다
- **context**: `run-v2.sh`가 `nohup ./gradlew :module:bootRun ... &`로 백그라운드 기동 후 그
  PID를 파일에 저장, `stop-v2.sh`가 그 PID로 종료하는 게 첫 설계였다.
- **왜**: Gradle은 기본적으로 영속 데몬(Gradle Daemon)에 실행을 맡긴다. 데몬이 이미 떠 있으면
  `./gradlew` 클라이언트 프로세스는 빌드를 데몬에 위임하고, `bootRun`이 포크하는 실제 Spring
  Boot JVM은 **데몬의 자식**이 된다 — `nohup`으로 띄운 `./gradlew` 클라이언트 프로세스 자체를
  죽여도 데몬 밑에서 도는 실제 앱 프로세스는 안 죽을 수 있다.
- **어떻게**: PID 파일은 로그·참고용으로 남기되, 실제 정지는 앱마다 고유한 `@SpringBootApplication`
  main 클래스 전체 이름(예: `com.flab.stocktradingengine.account.worker.AccountWorkerApplication`)
  으로 `pkill -f`해 실제 JVM 프로세스를 직접 잡는다.
- **무엇을**: `stop-v2.sh`.
- **결과·수치**: 실측 — 종료 후 20040/20020/20060 UDP, 8080 TCP 전부 `lsof` 무응답, main 클래스명
  `ps aux | grep` 무응답으로 완전 정지 확인.

### v2 api는 v1의 기존 loadtest 시드를 그대로 재사용할 수 있었다 — 새 파일 불필요
- **context**: v2 부하 테스트도 계좌 1001~1050(잔고 1e11, margin 1.00)·종목 A900110(가격
  1003)이 필요했다.
- **왜**: `api/src/main/resources/application-loadtest.yml`(v1, 2026-09-04 구축)이 이미 정확히
  이 조합을 로드하고 있었다 — `spring.sql.init.data-locations: classpath:stocks.sql,
  classpath:quotes.sql, classpath:loadtest-seed.sql`. `market` 모듈의 기본 `stocks.sql`/
  `quotes.sql`에 A900110/현재가 1003이 **이미 들어있음**을 코드로 확인(당시엔 몰랐음 —
  udp-demo-seed.sql이 별도로 이 데이터를 중복 정의해둔 걸 보고서야 기존 파일에도 있는지
  확인했다). `--spring.profiles.active=udp,loadtest`로 두 프로파일을 함께 켜면, 스칼라
  프로퍼티(`spring.sql.init.data-locations`)는 **나중에 나열된 프로파일이 앞 프로파일 값을
  완전히 덮어쓴다** — `udp` 프로파일의 `udp-demo-seed.sql` 지정이 `loadtest` 프로파일의
  값으로 통째로 교체된다.
- **어떻게**: 새 api 설정 파일을 만들지 않고 기존 `application-loadtest.yml`을 그대로 재사용.
- **무엇을**: api 모듈 변경 없음(0 파일). account-worker만 신규
  `application-loadtest.yml`(계좌 50개 시드 + 메트릭 리스너 활성화 프로퍼티)을 추가.
- **결과·수치**: 실측 — `curl -X POST /api/v2/orders/buy`로 계좌 1001·1050(시드 범위 양 끝)
  둘 다 202 확인, `Authorization` 헤더 없이 시도 시 v2도 v1과 같은 인증 인터셉터가 걸리는지는
  기존 배선(`WebMvcConfig`, `/api/v2/**` 등록) 확인으로 대체(직접 401 재현은 안 함).

### 메트릭 리스너는 기본 꺼둔다 — 하네스 전용 로그가 데모·기본 실행에 새지 않게
- **context**: `MetricsAccountResultListener`(콜백 카운터, 1초마다 로그)는 `@Component`라
  프로파일 없이 항상 `CompositeAccountResultListener`에 잡힌다 — 켜두면 fork1 데모(udp)나
  로컬 개발 실행에서도 1초마다 로그가 새어나간다.
- **왜**: 리뷰(39)가 "부하 하네스 전용이면 프로퍼티/프로파일 게이트를 고려하라"고 지적 — 카운터
  증가 자체(LongAdder increment)는 공짜라 항상 켜도 되지만, **로그 출력**은 그렇지 않다.
- **어떻게**: 카운팅은 그대로 항상 켜두고(리스너는 `@Component` 유지), 1초 리포터 스레드를
  띄우는 `SmartLifecycle` 빈만 `@ConditionalOnProperty(prefix = "account-worker.metrics",
  name = "enabled", havingValue = "true")`로 게이트. `account-worker.metrics.enabled`가
  `application-loadtest.yml`에서만 `true`.
- **무엇을**: `MetricsReporterConfig`.
- **결과·수치**: 실측 — loadtest 프로파일로 기동 시 `acceptedTotal=` 로그 1초 주기 확인, 다른
  프로파일에서는 리포터 빈 자체가 안 생겨 로그 없음(코드로 확인, 별도 프로파일 재기동 검증은
  안 함).

## 블로그 네타

### "부하 테스트 하네스가 코드보다 인프라 문제였던 이유"
- **훅·핵심 주장**: v1/v2 처리량을 대조하려고 스크립트 세 개를 짰다. 정작 시간을 잡아먹은 건
  Aeron이나 계좌 엔진 코드가 아니라 — 워크트리 경로, docker compose 프로젝트명, macOS 시스템
  bash 버전, Gradle Daemon의 프로세스 트리 같은, "짠 대로 당연히 될 줄 알았던" 로컬 인프라
  네 가지였다.
- **context**: 여러 Claude 세션이 같은 macOS 머신·같은 git 저장소를 동시에 쓰는 협업 환경에서,
  마감(다음날 오전) 안에 v1/v2 부하 테스트 하네스(기동 스크립트·메트릭 카운터·k6 스크립트)를
  만들어야 했다.
- **어떻게(서사·근거)**: ①공유 디렉터리에서 브랜치를 바꿨다가 다른 세션의 땅을 흔들 뻔해서
  git worktree로 격리 → ②격리된 디렉터리에서 `docker compose up`을 돌리니 디렉터리명이 곧
  프로젝트명이 돼 이미 떠 있는 공유 컨테이너와 이름이 충돌 → ③`stop-v2.sh`의 연관 배열이
  macOS 기본 bash(라이선스 정책 때문에 2007년 버전에 멈춰 있는)에서 조용히 안 먹혀 즉사 →
  ④PID로 프로세스를 죽이려니 Gradle Daemon이 실제 앱을 자기 자식으로 숨겨놔서 안 죽는다.
  넷 다 "이 정도는 당연히 되겠지"라고 넘겼다가 실행해보고서야 걸린 것들 — 코드 리뷰로는 하나도
  안 잡히고 **실제로 두 번 기동해봐서** 잡았다.
- **재료(커밋·도식·수치)**: `loadtest/run-v2.sh`·`stop-v2.sh`·`run-v1.sh`,
  `MetricsAccountResultListener`, 실기동 로그(202 응답, `dup=1` 카운트, `acceptedTotal=` 누적),
  worktree 경로 `../stock-trading-engine-c7`. 커밋은 39 리뷰 승인 대기 중(이 flush 시점 기준).
