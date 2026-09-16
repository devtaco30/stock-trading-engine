# v1/v2 전 과정(e2e) 측정 확정 근거

39 설계(`decision_records/v1-v2-e2e-measurement.md`)와 `docs/_c7_v1_v2_throughput_report.html`이
인용하는 실측 원본 데이터. 값은 다시 계산하지 않고 측정 당시 출력을 그대로 옮겼다.

`loadtest/results/`(gitignore 대상, 재측정 때마다 새로 쌓이는 작업 공간)와 달리 이 디렉터리는
확정 근거만 담아 추적한다. 재측정으로 새 파일이 생겨도 이 디렉터리엔 자동으로 안 들어온다 —
새 확정 근거를 남기려면 이 README도 같이 갱신하고 명시적으로 옮겨야 한다.

## 파일 구성

- `{v1,v2}-hot{0,1}-run{1,2,3}.json` — 4절, 접수 처리량(req/s) 측정, **버퍼 수정 전**.
- `post-buf-{v1,v2}-hot{0,1}-run{1,2,3}.json` — 8절, 같은 측정 **버퍼 수정 후**.
- `rev-{v1,v2}-hot{0,1}-run{1,2,3}.json` — 6절, HOT=1/HOT=0 실행 순서를 반전해 재측정한 것
  (순서 자체가 결과에 영향을 주는지 확인).
- `latency-v1-{warmup,pass1,pass2,pass3}.json` — 9절 B, v1 접수 지연(동시성9, 192건/초).
- `latency-v2-b1-{warmup,pass1,pass2,pass3}.json` — 9절 B-1, v2를 v1과 동일 절대 도착률
  (192건/초)로 잰 지연 — 리포트 헤드라인 비교 대상.
- `latency-v2-b2-{warmup,pass1,pass2,pass3}.json` — 9절 B-2, v2를 v1과 동일 상대 이용률
  (~50%, 2000건/초)로 잰 지연, 첫 실행.
- `latency-v2-b2r-{warmup,pass1,pass2,pass3}.json` — 9절 B-2 재실행(꼬리 지연 재확인).
- `cpu-v2-b2-{warmup,pass1,pass2,pass3}.log` — B-2 측정 중 CPU 샘플링(부하 생성기와 측정
  대상이 같은 머신을 써서 경쟁했는지 확인하는 근거).
- `latency-v2-capA.json` — A(용량) 절 "v2 ≥4000건/초" 주장의 근거(count=30000 확인용 단독 트리거).

## 입력 재현

여기 없는 `e2e-input.jsonl`(주문 10만 건, 11.7MB)은 계속 gitignore 대상이다.
`loadtest/generate-e2e-input.py`를 인자 없이 실행하면(고정 시드) 바이트 단위로 동일하게
재현된다. 이 디렉터리의 결과들은 그 입력으로 `loadtest/measure-e2e.sh`를 돌려 나온 것이다.

## 제외한 것

같은 측정 세션에서 나왔지만 리포트·결정 기록 어디에도 인용되지 않는 파일(39 설계가 A/B로
갈라지기 전의 최초 전체 실행 잔재, 종료 시 안전망 덤프)은 여기 포함하지 않았다 — 원본은
로컬 `loadtest/results/`에 그대로 남아 있다.
