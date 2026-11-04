#!/usr/bin/env python3
"""2시간 유지 시험 한 번의 통과 판정을 한 파일(<base>.verdict.json)로 모은다. base는 sustained-run.sh가 정한 결과 이름 접두사(<결과 폴더>/sustained-R<R>-<태그>).
읽는 파일: <base>.check.json(balance_check, status·fills_complete), <base>.windows.json(window_report, all_windows_ok·trend), <base>.stdout(exit= 줄), <base>.guard(있으면 DISK_GUARD_STOP).
verdict: PASS = 종료 코드 0이고 check.status ok이고 체결 반영이 끝났고 모든 창이 통과, FAIL = 시험은 유효하지만 창이 실패, INVALID = 시험이 끝까지 못 갔거나 판정 파일을 못 읽음(디스크 가드 포함).
누적 의심(GC 직후 힙·RSS)은 통과 기준이 아니고 값만 그대로 싣는다.
사용: sustained_verdict.py <base>   (verdict JSON을 stdout으로, 사용 편의로 <base>.verdict.json에도 쓴다)
"""
import json
import os
import re
import sys


def _load(path, reasons):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError) as e:
        reasons.append("%s를 읽을 수 없다: %s" % (os.path.basename(path), e))
        return None


def verdict(base):
    reasons = []
    check = _load(base + ".check.json", reasons)
    windows = _load(base + ".windows.json", reasons)
    exit_code = None
    try:
        with open(base + ".stdout", errors="replace") as f:
            exits = re.findall(r"^exit=(\d+)$", f.read(), re.M)
        exit_code = int(exits[-1]) if exits else None
    except OSError:
        reasons.append("stdout를 읽을 수 없다")
    guard = os.path.exists(base + ".guard") and "DISK_GUARD_STOP" in open(base + ".guard").read()
    invalid = bool(reasons)
    if guard:
        invalid = True
        reasons.append("남은 디스크가 한도 아래로 내려가 발신을 멈췄다(DISK_GUARD_STOP)")
    if exit_code is None or exit_code != 0:
        invalid = True
        reasons.append("시험 종료 코드가 0이 아니다: %s" % exit_code)
    if check is not None:
        if check.get("status") != "ok":
            invalid = True
            reasons.append("balance_check status=%s" % check.get("status"))
        if not check.get("fills_complete", False):
            invalid = True
            reasons.append("체결 반영이 끝나지 않았다")
    failed = []
    if windows is not None and not invalid:
        for w in windows.get("windows", []):
            if not w.get("ok"):
                failed.append(w.get("index"))
        if not windows.get("all_windows_ok"):
            reasons.append("통과하지 못한 창: %s" % failed if failed else "all_windows_ok=false")
    trend = (windows or {}).get("trend", {})
    rss_roles = [r for r, v in (trend.get("rss") or {}).items() if v.get("suspect")]
    result = {
        "verdict": "INVALID" if invalid else ("PASS" if windows and windows.get("all_windows_ok") else "FAIL"),
        "reasons": reasons, "exit_code": exit_code, "disk_guard_stopped": guard,
        "check_status": (check or {}).get("status"), "fills_complete": (check or {}).get("fills_complete"),
        "all_windows_ok": (windows or {}).get("all_windows_ok"), "failed_windows": failed,
        "gc_after_suspect": trend.get("gc_after_suspect"), "gc_after_ratio": trend.get("gc_after_ratio"),
        "rss_suspect_roles": rss_roles,
    }
    return result


def main(argv):
    if len(argv) != 2:
        sys.exit(__doc__)
    result = verdict(argv[1])
    text = json.dumps(result, ensure_ascii=False, indent=1)
    with open(argv[1] + ".verdict.json", "w") as f:
        f.write(text + "\n")
    print(text)


if __name__ == "__main__":
    main(sys.argv)
