#!/usr/bin/env bash
set -uo pipefail

# run-v1.sh/run-e2e-v1.sh가 기동한 v1 프로세스(order-engine·matching-engine·settlement-engine·api)를
# 멈춘다. stop-v2.sh와 같은 이유로 PID kill 대신 main 클래스명으로 pkill -f 한다.
cd "$(dirname "$0")/.."

PID_DIR="loadtest/pids"

main_class_for() {
    case "$1" in
        api) echo "com.flab.stocktradingengine.api.StockTradingEngineApplication" ;;
        order-engine) echo "com.flab.stocktradingengine.OrderEngineApplication" ;;
        matching-engine) echo "com.flab.stocktradingengine.matching.MatchingApplication" ;;
        settlement-engine) echo "com.flab.stocktradingengine.settlement.engine.SettlementEngineApplication" ;;
    esac
}

for module in order-engine matching-engine settlement-engine api; do
    main_class=$(main_class_for "$module")
    if [ -n "$main_class" ] && pkill -f "$main_class" 2>/dev/null; then
        echo "${module} 정지(${main_class})"
    fi
    rm -f "${PID_DIR}/${module}.pid"
done

echo "v1 프로세스 정지 완료 (인프라는 유지)"
