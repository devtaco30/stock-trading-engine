#!/usr/bin/env bash
set -uo pipefail

# run-v2.sh·run-e2e-v2.sh가 기동한 v2 프로세스(api·account-worker·matching-worker 등)를 멈춘다.
# 종료 대상은 이 하네스가 PID 파일로 기록한 것만이다. 이름 일괄 종료(pkill -f)는 쓰지 않는다. 다른 세션/워크트리에서 같은 main 클래스로
# 띄운 JVM이 같이 죽기 때문이다. 대상은 둘이다.
#   1) PID 파일의 런처(gradlew)와 그 자손. Gradle 데몬은 다른 worktree의 빌드도 맡으므로 자손이어도 종료하지 않는다.
#   2) gradle bootRun은 Gradle 데몬에 실행을 넘겨 JVM이 런처의 자손이 아닐 수 있으므로, 그 라벨의 고정 포트
#      (매칭 shard i = 입구 UDP 20020+i, api = TCP 8080, 계좌 워커 = 입구 UDP 20040·20041)를 쥔 프로세스.
#      포트를 쥐었어도 명령행에 그 모듈의 main 클래스 이름이 없거나 작업 디렉터리·명령행이 이 worktree 안이 아니면 건드리지 않는다.
# 런처와 포트 모두 「이 worktree의 프로세스인가」(process_in_worktree)를 통과해야 한다. pid가 재사용되어 다른 worktree의
# gradlew가 PID 파일의 pid가 되어도 종료하지 않는다. 종료한 뒤에도 같은 모듈의 JVM이 이 worktree에 남아 있으면 경고만 낸다.
# 인프라(docker compose)는 그대로 둔다 — 같은 밤 반복 측정(U4) 때 재기동 비용 없이 그대로 쓴다.
cd "$(dirname "$0")/.."

PID_DIR="loadtest/pids"
# 이 저장소(worktree)의 절대경로. 종료 대상이 이 worktree에서 띄운 프로세스인지 판별한다.
REPO_ROOT="$(pwd -P)"

# macOS 기본 /bin/bash(3.2, GPLv2 라이선스 동결)는 연관 배열(declare -A, bash 4+)을 못 쓴다 —
# case문으로 대체.
main_class_for() {
    case "$1" in
        api) echo "com.flab.stocktradingengine.api.StockTradingEngineApplication" ;;
        matching-worker) echo "com.flab.stocktradingengine.matching.worker.MatchingWorkerApplication" ;;
        account-worker) echo "com.flab.stocktradingengine.account.worker.AccountWorkerApplication" ;;
        settlement-worker) echo "com.flab.stocktradingengine.settlement.worker.SettlementWorkerApplication" ;;
        account-projection-worker) echo "com.flab.stocktradingengine.account.projection.worker.AccountProjectionWorkerApplication" ;;
    esac
}

# 모듈 하나의 pid 파일 경로(있는 것만, 한 줄에 하나). 계좌 워커 둘(-a, -b)과 매칭 shard 여럿(-0~-7, MATCHING_COUNT>1일 때
# matching-plan.sh 의 라벨)을 포함한다. pid 파일명은 모듈 이름 뒤에 한 글자 접미사만 허용해 다른 모듈 파일을 건드리지 않는다.
module_pid_files() {
    local module="$1" file
    for file in "${PID_DIR}/${module}.pid" "${PID_DIR}/${module}-a.pid" "${PID_DIR}/${module}-b.pid" "${PID_DIR}/${module}"-[0-9].pid; do
        [ -e "$file" ] && echo "$file"
    done
    return 0
}

# 프로세스가 이 worktree에서 띄운 것인지: 명령행에 이 저장소 경로가 있거나 작업 디렉터리가 이 저장소 안이다.
process_in_worktree() {
    local pid="$1" cwd
    case "$(process_command "$pid")" in
        *"$REPO_ROOT"/*) return 0 ;;
    esac
    cwd="$(lsof -a -p "$pid" -d cwd -Fn 2> /dev/null | sed -n 's/^n//p' | head -n 1)"
    case "$cwd" in
        "$REPO_ROOT"|"$REPO_ROOT"/*) return 0 ;;
    esac
    return 1
}

# 인자로 받은 pid 중 하나라도 살아 있으면 0.
any_alive() {
    local pid
    for pid in "$@"; do
        kill -0 "$pid" 2> /dev/null && return 0
    done
    return 1
}

process_command() { ps -p "$1" -o command= 2> /dev/null; }

# 프로세스 $1의 자손 pid를 한 줄에 하나씩 낸다.
descendants_of() {
    local child
    for child in $(pgrep -P "$1" 2> /dev/null); do
        # Gradle 데몬은 다른 worktree의 빌드도 맡으므로 종료 대상에서 뺀다(그 아래 자손도 보지 않는다).
        case "$(process_command "$child")" in
            *GradleDaemon*) continue ;;
        esac
        echo "$child"
        descendants_of "$child"
    done
}

# 라벨(pid 파일 이름에서 .pid를 뺀 것)이 고정으로 쥐는 포트("udp:포트" 또는 "tcp:포트"). 없으면 빈 문자열.
label_port_spec() {
    case "$1" in
        api) echo "tcp:8080" ;;
        account-worker|account-worker-a) echo "udp:20040" ;;
        account-worker-b) echo "udp:20041" ;;
        matching-worker) echo "udp:20020" ;;
        matching-worker-[0-7]) echo "udp:$((20020 + ${1##*-}))" ;;
    esac
}

port_holder_pids() {
    case "${1%%:*}" in
        udp) lsof -nP -t -iUDP:"${1##*:}" 2> /dev/null ;;
        tcp) lsof -nP -t -iTCP:"${1##*:}" -sTCP:LISTEN 2> /dev/null ;;
    esac
}

# pid 파일 하나에서 종료 대상 pid를 한 줄에 하나씩 낸다. $1=pid 파일, $2=모듈의 main 클래스.
stop_targets_for_pid_file() {
    local pid_file="$1" main_class="$2" label launcher command holder spec
    label="$(basename "$pid_file" .pid)"
    launcher="$(cat "$pid_file" 2> /dev/null)"
    # 런처 pid가 재사용되어 다른 프로세스가 되었을 수 있으므로 gradlew 런처이거나 그 모듈의 JVM이면서 이 worktree의 프로세스일 때만 대상으로 한다.
    if [ -n "$launcher" ] && kill -0 "$launcher" 2> /dev/null && process_in_worktree "$launcher"; then
        command="$(process_command "$launcher")"
        case "$command" in
            *gradlew*|*"$main_class"*) echo "$launcher"; descendants_of "$launcher" ;;
        esac
    fi
    spec="$(label_port_spec "$label")"
    if [ -n "$spec" ]; then
        for holder in $(port_holder_pids "$spec"); do
            case "$(process_command "$holder")" in
                *"$main_class"*) process_in_worktree "$holder" && echo "$holder" ;;
            esac
        done
    fi
}

# PID 파일과 포트로 찾지 못했지만 이 worktree에 같은 모듈의 JVM이 남아 있으면 경고만 낸다(종료하지 않는다).
# bootRun의 JVM이 런처의 자손이 아니고 포트 규칙도 없는 모듈(settlement-worker, account-projection-worker)이 안 멈추는 경우를 알린다.
warn_leftover_processes() {
    local module="$1" main_class="$2" pid leftover=""
    for pid in $(pgrep -f "$main_class" 2> /dev/null); do
        [ "$pid" = "$$" ] && continue
        process_in_worktree "$pid" && leftover="${leftover:+${leftover} }${pid}"
    done
    if [ -n "$leftover" ]; then
        echo "경고: ${module}의 JVM이 이 worktree에 남았다(pid ${leftover}). PID 파일과 포트로 찾지 못해 종료하지 않았다. 직접 확인한다" >&2
    fi
}

# 모듈 하나를 멈춘다: 대상에 종료 신호를 보내고 5초 안에 안 끝나면 강제 종료한 뒤 PID 파일을 지운다.
stop_module() {
    local module="$1" main_class pid_file targets pid waited
    main_class="$(main_class_for "$module")"
    # 빈 main 클래스로는 어떤 명령행에도 맞아 버린다. 모르는 모듈은 건드리지 않는다.
    [ -n "$main_class" ] || return 0
    while read -r pid_file; do
        targets="$(stop_targets_for_pid_file "$pid_file" "$main_class" | sort -u)"
        for pid in $targets; do kill "$pid" 2> /dev/null; done
        waited=0
        while [ "$waited" -lt 5 ] && any_alive $targets; do
            sleep 1
            waited=$((waited + 1))
        done
        for pid in $targets; do kill -9 "$pid" 2> /dev/null; done
        [ -n "$targets" ] && echo "${module} 정지(${pid_file##*/}: $(echo $targets | tr '\n' ' '))"
        if any_alive $targets; then
            echo "경고: ${module}의 종료 대상이 강제 종료 뒤에도 남아 있다(${pid_file##*/}: $(echo $targets | tr '\n' ' '))" >&2
        fi
        rm -f "$pid_file"
    done < <(module_pid_files "$module")
    warn_leftover_processes "$module" "$main_class"
}

# 시험이 위 함수만 불러 확인하도록 source 할 때는 여기서 멈춘다(아래는 실제 프로세스를 종료한다).
if [ "${STOP_V2_SOURCE_ONLY:-0}" = "1" ]; then
    return 0 2> /dev/null || exit 0
fi

for module in api matching-worker account-worker settlement-worker account-projection-worker; do
    stop_module "$module"
done

echo "v2 프로세스 정지 완료 (인프라는 유지 — 인프라까지 내리려면 docker compose -f docker-compose.yml -f docker-compose.loadtest.yml down)"
