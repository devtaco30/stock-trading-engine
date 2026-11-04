#!/usr/bin/env bash
# trend_sample.sh 시험(서버·gradle 없이 가짜 df/jcmd와 가짜 프로세스로). 실행: bash loadtest/s0b/test_trend_sample.sh (저장소 루트에서)
cd "$(dirname "$0")/../.."
PASS=0; FAIL=0
check() { if printf '%s' "$3" | grep -qF -- "$2"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); echo "FAIL $1: [$2] 가 없다"; fi; }
check_not() { if printf '%s' "$3" | grep -qF -- "$2"; then FAIL=$((FAIL+1)); echo "FAIL $1: [$2] 가 있으면 안 된다"; else PASS=$((PASS+1)); fi; }

T=$(mktemp -d); trap 'pkill -f "fake-account-$$" 2>/dev/null; pkill -f "u10.U10Tool fake-$$" 2>/dev/null; rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/w/account" "$T/w/matching-0"
head -c 100 /dev/zero > "$T/w/account/account-snapshot.dat"; head -c 50 /dev/zero > "$T/w/matching-0/matching-snapshot.dat"
cat > "$T/bin/df" <<'S'
#!/bin/sh
echo "Filesystem 1024-blocks Used Available Capacity iused ifree %iused Mounted"
echo "/dev/x 1000 1 $(cat "$FAKE_DF_FILE") 1% 1 1 1% /"
S
cat > "$T/bin/jcmd" <<'S'
#!/bin/sh
echo "THREAD_DUMP pid=$1 $2"
S
chmod +x "$T/bin/df" "$T/bin/jcmd"
GB=1048576

# 1. 남은 공간이 충분하면 시작 줄과 표본 줄을 남기고 가드는 안 건다
echo $((100 * GB)) > "$T/df1"
PATH="$T/bin:$PATH" FAKE_DF_FILE="$T/df1" W="$T/w" TREND_INTERVAL=1 bash loadtest/s0b/trend_sample.sh "$T/trend1.log" "$T/stop1" "$T/guard1" &
SP=$!; sleep 3.5; touch "$T/stop1"; wait $SP
L1=$(cat "$T/trend1.log")
check "시작 줄에 남은 공간" "START free_gb=100" "$L1"
check "표본 줄에 남은 공간" "free_gb=100" "$L1"
check "계좌 스냅샷 크기" "a0_snap_bytes=100" "$L1"
check "매칭 스냅샷 크기" "m_snap_bytes=50" "$L1"
check "스냅샷 mtime" "a0_snap_mtime=" "$L1"
check "계좌 디렉터리 용량" "acct_dir_mb=" "$L1"
[ "$(grep -c '^ts=' "$T/trend1.log")" -ge 2 ] && PASS=$((PASS+1)) || { FAIL=$((FAIL+1)); echo "FAIL 표본이 2줄 이상이어야 한다"; }
check_not "가드 안 걸림" "DISK_GUARD_STOP" "$L1"
[ ! -e "$T/guard1" ] && PASS=$((PASS+1)) || { FAIL=$((FAIL+1)); echo "FAIL 가드 파일이 생기면 안 된다"; }

# 2. 남은 공간이 한도 아래면 발신기를 끄고 DISK_GUARD_STOP을 남긴 뒤 스스로 끝난다
echo $((100 * GB)) > "$T/df2"
PATH="$T/bin:$PATH" FAKE_DF_FILE="$T/df2" W="$T/w" TREND_INTERVAL=1 DISK_MIN_GB=20 bash loadtest/s0b/trend_sample.sh "$T/trend2.log" "$T/stop2" "$T/guard2" &
SP=$!; sleep 0.3
bash -c "while :; do sleep 1; done # u10.U10Tool fake-$$" & SENDER=$!    # 표본기가 시작된 뒤에 뜬 발신기 흉내
sleep 1.5; echo $((10 * GB)) > "$T/df2"   # 시험 도중 남은 공간이 한도 아래로 떨어진다
sleep 3; kill -0 $SENDER 2>/dev/null && SENDER_ALIVE=yes || SENDER_ALIVE=no
kill -0 $SP 2>/dev/null && SAMPLER_ALIVE=yes || SAMPLER_ALIVE=no
check "가드 줄이 로그에" "DISK_GUARD_STOP free_gb=10" "$(cat "$T/trend2.log")"
check "가드 파일에도" "DISK_GUARD_STOP" "$(cat "$T/guard2" 2>/dev/null)"
check "발신기가 꺼졌다" "SENDER_ALIVE=no" "SENDER_ALIVE=$SENDER_ALIVE"
check "표본기는 스스로 끝났다" "SAMPLER_ALIVE=no" "SAMPLER_ALIVE=$SAMPLER_ALIVE"
kill $SENDER $SP 2>/dev/null; wait 2>/dev/null

# 3. 지정한 시각에 계좌 JVM의 스레드 덤프를 뜬다
echo $((100 * GB)) > "$T/df3"
bash -c "while :; do sleep 1; done # fake-account-$$" & ACC=$!
PATH="$T/bin:$PATH" FAKE_DF_FILE="$T/df3" W="$T/w" TREND_INTERVAL=1 JCMD_AT="1,2" JCMD_OUT="$T/dumps" ACCOUNT_PID_PATTERN="fake-account-$$" bash loadtest/s0b/trend_sample.sh "$T/trend3.log" "$T/stop3" "$T/guard3" &
SP=$!; sleep 4; touch "$T/stop3"; wait $SP; kill $ACC 2>/dev/null
check "첫 덤프" "THREAD_DUMP pid=$ACC" "$(cat "$T/dumps/threads-1.txt" 2>/dev/null)"
check "둘째 덤프" "THREAD_DUMP pid=$ACC" "$(cat "$T/dumps/threads-2.txt" 2>/dev/null)"

echo "통과 $PASS, 실패 $FAIL"
[ "$FAIL" = 0 ]
