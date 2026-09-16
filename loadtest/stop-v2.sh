#!/usr/bin/env bash
set -uo pipefail

# run-v2.sh가 기동한 v2 프로세스(api·account-worker·matching-worker)를 멈춘다.
# gradle bootRun은 이미 떠 있는 Gradle 데몬에 실행을 넘기는 경우가 많아, run-v2.sh가 적어둔
# 런처 PID를 그대로 kill해도 실제 Spring Boot JVM(데몬의 자식)은 안 죽을 수 있다 — 그래서
# 앱마다 고유한 main 클래스명으로 pkill -f 해 실제 JVM을 확실히 잡는다. PID 파일은 정리만 한다.
# 인프라(docker compose)는 그대로 둔다 — 같은 밤 반복 측정(U4) 때 재기동 비용 없이 그대로 쓴다.
#
# ⚠️ pkill -f는 워크트리를 구분하지 않는다 — 다른 세션/워크트리에서 같은 main 클래스로 띄운
# JVM도 같이 죽는다(예: AccountWorkerApplication은 어느 워크트리에서 떠 있든 이름이 같다).
# 측정을 도는 동안에는 다른 세션이 같은 v2 앱들을 기동하지 않아야 한다.
cd "$(dirname "$0")/.."

PID_DIR="loadtest/pids"

# macOS 기본 /bin/bash(3.2, GPLv2 라이선스 동결)는 연관 배열(declare -A, bash 4+)을 못 쓴다 —
# case문으로 대체.
main_class_for() {
    case "$1" in
        api) echo "com.flab.stocktradingengine.api.StockTradingEngineApplication" ;;
        matching-worker) echo "com.flab.stocktradingengine.matching.worker.MatchingWorkerApplication" ;;
        account-worker) echo "com.flab.stocktradingengine.account.worker.AccountWorkerApplication" ;;
    esac
}

for module in api matching-worker account-worker; do
    main_class=$(main_class_for "$module")
    # main_class_for가 아는 모듈만 case에 있다 — 빈 문자열이면 pkill -f ""가 전체 프로세스를
    # 매칭해버리니(파멸적) 건너뛴다. 지금 루프는 고정 3개 모듈이라 실제로는 안 비지만 방어로 둔다.
    # I8 U3 — account-worker를 WORKER_COUNT=2로 띄우면 같은 main 클래스의 JVM이 둘이라, 이
    # pkill 한 번으로 A·B 둘 다 잡힌다(라벨은 pid/로그 파일명에만 쓰고, main 클래스는 공유).
    if [ -n "$main_class" ] && pkill -f "$main_class" 2>/dev/null; then
        echo "${module} 정지(${main_class})"
    fi
    rm -f "${PID_DIR}/${module}.pid" "${PID_DIR}/${module}-a.pid" "${PID_DIR}/${module}-b.pid"
done

echo "v2 프로세스 정지 완료 (인프라는 유지 — 인프라까지 내리려면 docker compose -f docker-compose.yml -f docker-compose.loadtest.yml down)"
