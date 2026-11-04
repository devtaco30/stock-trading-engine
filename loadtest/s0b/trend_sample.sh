#!/usr/bin/env bash
# 장시간 유지 시험의 추세 표본기. 1분(TREND_INTERVAL초)마다 남은 디스크, /tmp/s0b 안 계좌·매칭 디렉터리 용량, 스냅샷 파일의 mtime·크기를 한 줄로 남긴다.
# 남은 공간이 DISK_MIN_GB(기본 20GB) 아래로 내려가면 발신기(U10Tool JVM)를 끄고 DISK_GUARD_STOP을 로그와 가드 파일에 남긴 뒤 스스로 끝난다.
# 서버는 건드리지 않는다: 발신이 멈추면 mix.sh가 SEND_NOT_CONFIRMED로 정상 종료 절차(프로세스 정리·로그 복사)를 탄다.
# JCMD_AT="300,3600,6900"처럼 시작 뒤 초 단위로 주면 그 시각에 계좌 JVM의 jcmd Thread.print를 JCMD_OUT/threads-<번호>.txt에 남긴다(스냅샷 소요 시간을 직접 못 재서 대신 보는 표본).
# 사용: trend_sample.sh <로그 파일> <중지 파일> <가드 파일>   환경: W(기본 /tmp/s0b) TREND_INTERVAL DISK_MIN_GB JCMD_AT JCMD_OUT ACCOUNT_PID_PATTERN(기본 account-.*\.jar)
LOGF=$1; STOP=$2; GUARD=$3
W=${W:-/tmp/s0b}; INTERVAL=${TREND_INTERVAL:-60}; MIN_GB=${DISK_MIN_GB:-20}
ACC_PAT=${ACCOUNT_PID_PATTERN:-account-.*\\.jar}
STARTED=$(date +%s)
free_kb() { df -k "${W%/*}" 2>/dev/null | awk 'NR==2{print $4}'; }
mtime() { stat -f %m "$1" 2>/dev/null || echo 0; }
size() { stat -f %z "$1" 2>/dev/null || echo 0; }
dir_mb() { [ -d "$1" ] && du -sk "$1" 2>/dev/null | awk '{printf "%d", $1/1024}' || echo 0; }
elapsed_seconds() { ps -o etime= -p "$1" 2>/dev/null | awk '{n=split($1,a,/[-:]/); if(n==2)s=a[1]*60+a[2]; else if(n==3)s=a[1]*3600+a[2]*60+a[3]; else if(n==4)s=a[1]*86400+a[2]*3600+a[3]*60+a[4]; else s=-1; print s}'; }
stop_senders() { local now sp age; now=$(date +%s)
  for sp in $(pgrep -u "$(id -u)" -f 'u10.U10Tool' 2>/dev/null); do
    age=$(elapsed_seconds "$sp"); [ -n "$age" ] && [ "$age" -ge 0 ] && [ "$age" -le $((now - STARTED + 2)) ] || continue
    kill "$sp" 2>/dev/null
  done; }
KB=$(free_kb); echo "START free_gb=$((KB / 1048576)) ts=$STARTED min_gb=$MIN_GB" >> "$LOGF"
JCMD_DONE=0; IFS=',' read -r -a JCMD_TIMES <<< "${JCMD_AT:-}"
while [ ! -f "$STOP" ]; do
  now=$(date +%s); KB=$(free_kb)
  line="ts=$now free_gb=$((KB / 1048576)) acct_dir_mb=$(( $(dir_mb "$W/account") + $(dir_mb "$W/account-0") + $(dir_mb "$W/account-1") )) match_dir_mb=$(dir_mb "$W/matching-0")"
  i=0; for d in "$W/account" "$W/account-0" "$W/account-1"; do
    f=$d/account-snapshot.dat; [ -f "$f" ] && { line="$line a${i}_snap_mtime=$(mtime "$f") a${i}_snap_bytes=$(size "$f")"; i=$((i + 1)); }
  done
  f=$W/matching-0/matching-snapshot.dat; [ -f "$f" ] && line="$line m_snap_mtime=$(mtime "$f") m_snap_bytes=$(size "$f")"
  echo "$line" >> "$LOGF"
  if [ "$KB" -lt $((MIN_GB * 1048576)) ]; then
    msg="DISK_GUARD_STOP free_gb=$((KB / 1048576)) ts=$now min_gb=$MIN_GB"
    echo "$msg" >> "$LOGF"; echo "$msg" >> "$GUARD"; stop_senders; exit 0
  fi
  if [ -n "${JCMD_OUT:-}" ] && [ "$JCMD_DONE" -lt "${#JCMD_TIMES[@]}" ] && [ $((now - STARTED)) -ge "${JCMD_TIMES[$JCMD_DONE]}" ]; then
    JCMD_DONE=$((JCMD_DONE + 1)); mkdir -p "$JCMD_OUT"
    pid=$(pgrep -u "$(id -u)" -f "$ACC_PAT" 2>/dev/null | head -1)
    [ -n "$pid" ] && jcmd "$pid" Thread.print > "$JCMD_OUT/threads-$JCMD_DONE.txt" 2>&1
  fi
  sleep "$INTERVAL"
done
