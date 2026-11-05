#!/usr/bin/env bash
# 수명 만료 삭제(작업 4.1). k8s/run.sh가 네임스페이스를 만든 직후 백그라운드로 띄운다. 시험이 끝나지 않았어도 수명이 지나면 지운다.
# 사용: reaper.sh <kubectl 컨텍스트> <네임스페이스> <수명 분> <결과 폴더>
# 지우면 <결과 폴더>/namespace-deleted.txt에 「DELETED_BY=lifetime ns=<이름> deleted_at=<epoch 초> deleted_at_iso=<시각>」을 남긴다.
# 로컬(kind)용이다. 맥이 잠들면 이 프로세스도 멈추므로 EKS에서는 맥 밖에서 도는 장치가 따로 필요하다(다음 변경).
CTX=${1:?}; NS=${2:?}; MIN=${3:?}; OUT=${4:?}
sleep $((MIN * 60))
if kubectl --context "$CTX" get ns "$NS" >/dev/null 2>&1; then
  kubectl --context "$CTX" delete ns "$NS" --wait=true --timeout=300s >/dev/null 2>&1
  RC=$?
  printf 'DELETED_BY=lifetime ns=%s lifetime_min=%s deleted_at=%s deleted_at_iso=%s delete_rc=%s\n' \
    "$NS" "$MIN" "$(date +%s)" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RC" >> "$OUT/namespace-deleted.txt"
fi
