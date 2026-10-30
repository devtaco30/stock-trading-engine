#!/usr/bin/env bash
set -uo pipefail

# stop-v2.sh 가 이름 일괄 종료(pkill -f) 없이 이 하네스가 PID 파일로 기록한 프로세스만 멈추는지 확인한다.
# 실제 앱 없이 대역 프로세스(sleep, UDP 포트를 쥔 파이썬)로 돈다.
# 실행: bash loadtest/test_stop_v2.sh   (저장소 루트에서)

cd "$(dirname "$0")/.."
STOP_V2_SOURCE_ONLY=1 source loadtest/stop-v2.sh
set +e

PASSES=0
FAILS=0
MAIN_CLASS="com.flab.stocktradingengine.matching.worker.MatchingWorkerApplication"
TMP="$(mktemp -d)"
PIDS_TO_CLEAN=""

assert_alive() {
    if kill -0 "$2" 2> /dev/null; then PASSES=$((PASSES + 1)); else FAILS=$((FAILS + 1)); echo "FAIL ${1}: 살아 있어야 한다(pid ${2})"; fi
}
assert_dead() {
    if kill -0 "$2" 2> /dev/null; then FAILS=$((FAILS + 1)); echo "FAIL ${1}: 종료되어야 한다(pid ${2})"; else PASSES=$((PASSES + 1)); fi
}
assert_eq() {
    if [ "$2" = "$3" ]; then PASSES=$((PASSES + 1)); else FAILS=$((FAILS + 1)); echo "FAIL ${1}: 기대=[${2}] 실제=[${3}]"; fi
}

# 런처 대역: 이름에 gradle 이 들어가고 자손(sleep)을 하나 둔다.
cat > "${TMP}/gradlew-fake" << 'EOF'
#!/bin/bash
sleep 300 &
wait
EOF

# UDP 포트를 쥐고 명령행에 클래스 이름이 들어간 프로세스 대역. $1=포트, $2=명령행에 넣을 이름(없으면 빈 문자열 대신 plain)
start_port_holder() {
    python3 -I -c 'import socket,time
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.bind(("127.0.0.1",int(__import__("sys").argv[1]))); time.sleep(300)' "$1" "$2" > /dev/null 2>&1 &
    echo $!
}

PID_DIR="${TMP}/pids"
mkdir -p "$PID_DIR"

# 1) 런처 + 자손 (matching-worker-0)
bash "${TMP}/gradlew-fake" > /dev/null 2>&1 &
LAUNCHER=$!
sleep 0.5
CHILD="$(pgrep -P "$LAUNCHER" | head -n 1)"
echo "$LAUNCHER" > "${PID_DIR}/matching-worker-0.pid"
assert_alive "시작 전 런처" "$LAUNCHER"
assert_alive "시작 전 자손" "$CHILD"

# 2) 입구 포트(20027 = matching-worker-7)를 쥔 같은 이름 프로세스: PID 파일의 pid 는 이미 없는 값
HOLDER7="$(start_port_holder 20027 "$MAIN_CLASS")"
echo 999999 > "${PID_DIR}/matching-worker-7.pid"

# 3) 입구 포트(20026 = matching-worker-6)를 쥐었지만 이름이 다른 프로세스
HOLDER6="$(start_port_holder 20026 unrelated-process)"
echo 999999 > "${PID_DIR}/matching-worker-6.pid"

# 4) 이 하네스가 기록하지 않은 같은 이름의 무관한 프로세스(다른 세션·워크트리의 JVM 대역)
bash -c 'sleep 300; : "$0"' "$MAIN_CLASS" > /dev/null 2>&1 &
DECOY=$!
sleep 0.5

PIDS_TO_CLEAN="$LAUNCHER $CHILD $HOLDER7 $HOLDER6 $DECOY"
assert_alive "시작 전 포트 쥔 같은 이름" "$HOLDER7"

stop_module matching-worker > /dev/null 2>&1

assert_dead "PID 파일의 런처" "$LAUNCHER"
assert_dead "런처의 자손" "$CHILD"
assert_dead "입구 포트를 쥔 같은 이름 프로세스" "$HOLDER7"
assert_alive "입구 포트를 쥐었지만 이름이 다른 프로세스는 건드리지 않는다" "$HOLDER6"
assert_alive "PID 파일에도 포트에도 없는 같은 이름 프로세스는 건드리지 않는다" "$DECOY"
assert_eq "정리한 모듈의 PID 파일은 지운다" "0" "$(ls "$PID_DIR" | grep -c '^matching-worker')"

# 5) 이름 일괄 종료가 코드에 남아 있지 않다
assert_eq "pkill 이 실행 코드에 없다" "0" "$(grep -v '^\s*#' loadtest/stop-v2.sh | grep -c 'pkill')"

for pid in $PIDS_TO_CLEAN; do kill "$pid" 2> /dev/null; done
rm -rf "$TMP"

echo "통과 ${PASSES}, 실패 ${FAILS}"
[ "$FAILS" -eq 0 ]
