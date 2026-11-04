# s0b 하네스에 매칭 shard N개와 종목 균등 선택을 넣는 변경안

작성: c1, 2026-11-04. ec의 `measure/s0-supplement`(444e248)를 건드리지 않고, 합칠 때 쓸 파일만 모았다. 실제 기동은 하지 않았고(오늘 밤 ec 시험 때문에) 가짜 docker·java로만 확인했다.

## 구성
| 파일 | 내용 |
|---|---|
| `stock_select.py`, `test_stock_select.py` | 배정 표(slot→shard 주소)를 읽어 shard마다 같은 수로 `%06d` 코드를 고른다. 단위 시험 23개 |
| `patches/0001-s0b-lib.patch` | 새 파일 `loadtest/s0b/lib.sh`(공통 블록 + 매칭 N개 기동 + 배정 완료 대기 + 종목 선택)와 `test_lib.sh`(51개) |
| `patches/0002-s0b-run-ladder-use-lib.patch` | `run.sh`·`ladder.sh`가 공통 블록 대신 lib.sh를 부른다 |
| `patches/0003-s0b-mix-use-lib.patch` | `mix.sh`가 같은 일을 한다. ec가 쓰는 파일이라 따로 뒀다 |

## 적용 전제와 순서
1. ec 브랜치에 `loadtest/matching-plan.sh`와 `loadtest/s0b_multi/stock_select.py`가 있어야 한다(둘 다 c1 브랜치 `chore/s0b-multi-matching-select`, `chore/limit-test-harness-merge`에 있다).
2. 저장소 루트에서 `patch -p1 < loadtest/s0b_multi/patches/0001-...patch` 순서대로. 444e248 기준으로 만들었고 `git apply --check`와 임시 복사본 적용으로 충돌이 없음을 확인했다. ec가 s0b 스크립트를 고치면 패치를 다시 만들어야 한다.

## 쓰는 법 (적용 뒤)
- `MATCHING_COUNT=1|2|4|8`(기본 1). 2 이상이면 `MATCHING_ASSIGN_TIMEOUT`(초)이 필수다. 값은 정해지지 않았다.
- 기동 순서: 자체 Kafka → 매칭 N개 → 종목 slot 64개가 N개 주소로 배정될 때까지 대기 → 종목 선택 → 시드 생성 → 계좌 워커.
- 종목은 N>1이면 shard마다 같은 수(30개, N=4면 8·8·7·7)로 고르고 결과 폴더에 `slots.txt`(코드=slot 주소)와 `shard-stocks.txt`를 남긴다. N=1은 기존 `codes-many`이고, `BALANCED_STOCKS=1`이면 N=1에서도 같은 선택기를 쓴다.
- N=1이고 `BALANCED_STOCKS`가 없으면 매칭 JVM 명령 인자가 기존과 같다(시험으로 비교함).
- 계좌 워커에는 N>1일 때만 `--matching-shard-routing.endpoints[i]`(N개)와 Kafka 주소가 붙는다.

## ec와 맞출 점
1. **종목 수 변수**: `run.sh`·`ladder.sh`는 `STOCK_COUNT=3` 고정, `mix.sh`만 환경변수(기본 3). 종목 수는 shard 수 이상이어야 하고(선택기가 거부), 30으로 고정할지 정해야 한다.
2. **30이 4·8로 안 나뉜다**: N=4면 shard당 8·8·7·7, N=8이면 4가 여섯 shard, 3이 두 shard다. 종목마다 쌓는 양이 같으므로 shard 사이 호가 수 차이가 최대 8/7(약 14%), 4/3(약 33%)다. 완전히 같게 하려면 종목 수를 8의 배수(예 32)로 바꿔야 하고, 그러면 아래 3번 규칙도 바뀐다.
3. **C 검사 규칙**: `mix.sh`는 C가 종목 수의 배수, RATE×초가 종목 수×2의 배수를 요구한다. 30이면 C는 30의 배수, RATE×초는 60의 배수. 32로 바꾸면 32와 64. `GROUP_BY_SHARD=1`은 종목 수가 2의 배수이고 C가 종목 수/2의 배수여야 한다.
4. **A안과 종목 집합이 다르다**: A안(N=1, 30개)은 `codes-many 64 0 30`이라 000001~000030이고 slot이 겹친다(slot 27이 3개, 21·42·58이 각 2개; ec의 slots.txt 기준). 균등 선택기는 slot을 겹치지 않게 고르므로 코드가 다르다. N=1을 A안과 직접 비교하려면 기존 방식 N=1 한 번과 `BALANCED_STOCKS=1` N=1 한 번이 모두 필요하다.
5. **포트**: 매칭 입구 UDP 20020+i, Archive 제어 8100+i. 단 `mix.sh ACCOUNT_WORKERS=2`는 계좌 Archive 제어가 8100·8101이라 겹쳐서 이때 매칭 제어는 8200+i로 간다(lib.sh가 처리). N=1은 yml 값(8011) 그대로다. u10 yml의 matching-1(8012)은 쓰지 않는다.
6. **계좌 쪽 주소 풀**: N=1은 `u10-account.yml`의 풀 2개(20020, 20021)를 그대로 둔다(20021에는 아무도 없다, 지금과 같다). N>1은 N개로 덮어쓴다.
7. **cpu 역할 이름**: N>1이면 `matching-0=pid …`로 남긴다. `cpu_summary.py`·`balance_check.py`가 역할 하나(`matching`)만 가정하는지는 확인하지 못했다.
8. **종료**: `s0b_kill_pids`는 종료 신호, 대기, 강제 종료 순이다. 기존 `run.sh`·`ladder.sh`에는 강제 종료 단계가 없었고 `mix.sh`에만 있었다.
9. **자원**: 아무 부하 없이 매칭 8개만 기동했을 때 JVM당 rss 약 290MB, idle CPU 합 약 131%(코어 1.3개)였다. 발신기와 계좌 워커가 같이 돌 때의 CPU 경합은 재지 않았다.
10. **실제 기동 미확인**: 새 블록은 가짜 `docker`·`java`·`lsof`로만 돌렸다. 시험 잠금 안에서 N=1(기존과 같은 결과)과 N=2로 한 번씩 돌려 봐야 한다. 특히 s0b-kafka 컨테이너 안의 `kafka-console-consumer`로 맵을 읽는 부분(`matching_assigned_map`)은 이 하네스 컨테이너에서 실행한 적이 없다.
