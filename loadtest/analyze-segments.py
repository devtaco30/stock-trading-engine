#!/usr/bin/env python3
"""구간 분해 측정(묶음 C) 분석기. 선언 문서는 measure-protocol-c.md, 단계 대응은 설계서 2-3a절.

입력: <실행 디렉터리>/<NN>-<단계>-<조건>-st<계측>/<cond>/ 아래의 파일 (measure-segments.sh 가 만든다)
  rounds-<cond>.csv                 회차 기록. valid 열(1이 유효)을 쓴다. r01 은 워밍업이라 집계에서 뺀다
  saturation-<cond>.csv             회차별 포화 판정(reasons 가 none 이 아니면 포화 회차)
  stages-<cond>-<프로세스>-rNN.json  프로세스별 단계 스냅샷 {단계: {count, p50Micros, p95Micros, p99Micros, maxMicros, meanMicros}}
  latency-<cond>-rNN.json           끝점① 스냅샷 (계측 켠/끈 영향 판정에 쓴다)
  series-<cond>-<프로세스>.csv       초 단위 시계열 (이 분석기는 읽지 않는다. 포화 판정은 하네스가 이미 계산했다)

사용:
  analyze-segments.py report <실행 디렉터리> [--out analysis-segments.md]
  (analyze-baseline.py compare-segments <실행 디렉터리> [--out ...] 가 같은 일을 한다)

보고서에 쓰지 않는 것: 신뢰구간, p값, 원인 서술. 비교 문장은 중앙값 비율과 범위 겹침 여부로만 쓴다(protocol-c 3절).
"""
import argparse
import csv
import json
import re
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path

RESOLUTION_LIMIT_US = 5.0      # 이 값 미만인 단계는 해상도 미만(마이크로초 해상도 1µs)으로 표시하고 p99 를 싣지 않는다
RESIDUAL_LIMIT = 0.01          # 합 정합성: 잔차 비율이 이 값을 넘으면 불성립 (임의값, protocol-c 2절)
WIDE_RATIO = 0.5               # 계측 영향: 끈 세트 범위 폭의 이 비율을 넘는 차이는 애매함 (protocol-c 5절)
WARMUP_ROUND = "r01"
SET_RE = re.compile(r"^(\d+)-(S1X|S1R|S1|S3|S5|I\d+)-(v1idx|v2)-st([01])$")
STAGE_FILE_RE = re.compile(r"^stages-(v1|v2)-(.+)-(r\d+)\.json$")
EXPECTED_PROCESSES = {
    "v1": ["api", "order-engine", "matching-engine", "settlement-engine"],
    "v2": ["api", "account-worker", "matching-worker"],
}
STEP_RATES = {"S1": 192, "S1X": 192, "S1R": 192, "S3": 1000, "S5": 4000}
IMPACT_STEPS = ("S1X", "S1R")
LABEL_TEXT = {"v1idx": "v1(인덱스 있음)", "v2": "v2"}

# 단계 표시 순서. 목록에 없는 단계는 이름 순으로 뒤에 붙인다
STAGE_ORDER = {
    "v1": ["G", "A", "b_idem", "b_lock", "b_sum", "b_decide", "b_insert", "B", "b_commit", "b_send_call", "C", "D",
           "A@aggr", "B@aggr", "C@aggr", "fill_transit", "E", "F"],
    "v2": ["G", "api_send_cost", "A", "ring_publish", "hop_to_journal", "journal_append", "hop_to_business", "B_biz", "B",
           "C", "m_ring_publish", "m_hop_to_business", "D", "A@aggr", "B@aggr", "C@aggr", "fill_queue", "fill_offer",
           "fill_transit", "fill_hop_to_journal", "fill_journal_append", "fill_hop_to_business", "E", "F"],
}

# 설계서 2-3a절의 같은 정의 / 다른 정의
DEFINITION = {
    "G": "같은 정의(api 입구, 두 구현 공통)",
    "A": "같은 정의", "B": "같은 정의", "C": "같은 정의(시작 시각이 다름)", "D": "다른 정의", "E": "다른 정의",
    "F": "같은 정의(반영의 뜻이 다름)", "fill_transit": "같은 정의",
    "A@aggr": "A와 같음(체결 기준)", "B@aggr": "B와 같음(체결 기준)", "C@aggr": "C와 같음(체결 기준)",
    "b_decide": "다른 정의(판정 도달)", "B_biz": "다른 정의(판정 도달)",
    "b_commit": "다른 정의(영속화 비용)", "journal_append": "다른 정의(영속화 비용)",
}
ONE_SIDED = "한쪽 구현에만 있음"

CORRESPONDENCE = [
    ("A", "같은 정의", "발행 시각부터 처리 프로세스의 첫 읽기까지. v1은 Kafka 프로듀서·브로커·컨슈머 fetch·역직렬화, v2는 UDP와 폴링 스레드"),
    ("B", "같은 정의", "처리 프로세스의 첫 읽기부터 영속화를 포함한 접수 완료까지. v1은 DB 커밋, v2는 저널 기록 뒤 승인"),
    ("판정 도달", "다른 정의", "v1 b_decide(락 획득부터 한도 검사 통과까지, 합계 쿼리 포함), v2 B_biz(업무 핸들러가 이벤트를 받은 뒤 승인까지)"),
    ("영속화 비용", "다른 정의", "v1 b_commit(저장 뒤 트랜잭션 커밋 완료까지), v2 journal_append(Archive 스트림 offer, fsync 보장 없음)"),
    ("C", "같은 정의, 시작 시각이 다름", "접수 완료부터 매칭 프로세스의 첫 읽기까지. v1은 커밋 직후라 send 호출 시간이 안에 있고, v2는 onAccepted 콜백 뒤라 콜백 시간이 빠진다"),
    ("D", "다른 정의", "체결을 일으킨 주문의 첫 체결 기준. v1은 호가창 매칭에 최근 체결가 Redis 저장과 로그가 들어 있고 v2는 호가창 매칭만"),
    ("E", "다른 정의", "v1은 Kafka 전달과 정산 DB 트랜잭션 커밋까지, v2는 Aeron 전달과 저널 기록, 메모리 잔고 반영까지"),
    ("F", "같은 정의, 반영의 뜻이 다름", "체결을 일으킨 주문의 발행 시각부터 계좌 반영 완료까지. v1은 정산 트랜잭션 커밋, v2는 메모리 계좌 상태 갱신"),
    ("fill_transit", "같은 정의", "체결 방출부터 반영 프로세스의 첫 읽기까지. 전달 수단이 다름"),
]


# ---------------------------------------------------------------- 읽기

@dataclass
class SetName:
    name: str
    index: int
    step: str
    label: str
    cond: str
    stages: int


def parse_set_name(name):
    match = SET_RE.match(name)
    if not match:
        return None
    label = match.group(3)
    return SetName(name=name, index=int(match.group(1)), step=match.group(2), label=label,
                   cond="v1" if label.startswith("v1") else "v2", stages=int(match.group(4)))


@dataclass
class SetData:
    meta: SetName
    rounds: dict = field(default_factory=dict)        # round -> {"valid": bool, "flags": str}
    saturation: dict = field(default_factory=dict)    # round -> reasons 문자열
    stage_rounds: dict = field(default_factory=dict)  # round -> {단계: 스냅샷}
    processes_seen: dict = field(default_factory=dict)  # round -> {프로세스}
    latency: dict = field(default_factory=dict)       # round -> {"p50": µs, "p99": µs}
    has_saturation_file: bool = False

    def measured_rounds(self):
        ids = set(self.rounds) | set(self.stage_rounds) | set(self.latency)
        return sorted(r for r in ids if r != WARMUP_ROUND)

    def is_valid(self, round_id):
        if self.rounds:
            return self.rounds.get(round_id, {}).get("valid", False)
        return True

    def is_saturated(self, round_id):
        return self.saturation.get(round_id, "none") not in ("none", "")

    def included_rounds(self):
        return [r for r in self.measured_rounds() if self.is_valid(r) and not self.is_saturated(r)]


def read_csv_rows(path):
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_set(set_dir, meta):
    data = SetData(meta=meta)
    cond_dir = Path(set_dir) / meta.cond
    for row in read_csv_rows(cond_dir / f"rounds-{meta.cond}.csv"):
        data.rounds[row["round"]] = {"valid": row.get("valid") == "1", "flags": row.get("flags", "")}
    sat_path = cond_dir / f"saturation-{meta.cond}.csv"
    data.has_saturation_file = sat_path.exists()
    for row in read_csv_rows(sat_path):
        data.saturation[row["round"]] = row.get("reasons", "none")
    if cond_dir.exists():
        for path in sorted(cond_dir.iterdir()):
            match = STAGE_FILE_RE.match(path.name)
            if match and match.group(1) == meta.cond:
                _, process, round_id = match.groups()
                try:
                    snapshots = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
                merged = data.stage_rounds.setdefault(round_id, {})
                for stage_name, snapshot in snapshots.items():
                    previous = merged.get(stage_name)
                    if previous is None or snapshot.get("count", 0) > previous.get("count", 0):
                        merged[stage_name] = snapshot
                data.processes_seen.setdefault(round_id, set()).add(process)
        for path in sorted(cond_dir.glob(f"latency-{meta.cond}-r*.json")):
            round_id = path.stem.rsplit("-", 1)[1]
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                data.latency[round_id] = {"p50": raw["p50Nanos"] / 1000.0, "p99": raw["p99Nanos"] / 1000.0}
            except (OSError, ValueError, KeyError):
                continue
    return data


def load_run(run_dir):
    sets = []
    run_path = Path(run_dir)
    if run_path.is_dir():
        for child in sorted(run_path.iterdir()):
            meta = parse_set_name(child.name)
            if meta and child.is_dir():
                sets.append(load_set(child, meta))
    return sorted(sets, key=lambda d: d.meta.index)


# ---------------------------------------------------------------- 집계

def median(values):
    return statistics.median(values) if values else None


def stage_values(data, stage_name, key):
    """집계에 쓰는 회차(워밍업·무효·포화 회차 제외)에서 단계의 값 목록."""
    values = []
    for round_id in data.included_rounds():
        snapshot = data.stage_rounds.get(round_id, {}).get(stage_name)
        if snapshot and snapshot.get("count", 0) > 0 and key in snapshot:
            values.append(snapshot[key])
    return values


def stage_summary(data, stage_name):
    columns = {"count": "count", "p50": "p50Micros", "p95": "p95Micros", "p99": "p99Micros", "max": "maxMicros",
               "mean": "meanMicros"}
    summary = {"rounds": len(stage_values(data, stage_name, "count"))}
    for label, key in columns.items():
        values = stage_values(data, stage_name, key)
        summary[label] = median(values)
        summary[f"{label}_min"] = min(values) if values else None
        summary[f"{label}_max"] = max(values) if values else None
    return summary


def below_resolution(p50_micros):
    return p50_micros < RESOLUTION_LIMIT_US


def residual_ratio(f_mean, part_means):
    return abs(f_mean - sum(part_means)) / f_mean


def residual_fails(ratio):
    return ratio > RESIDUAL_LIMIT


def step_saturated(saturated_count, measured_count):
    return saturated_count * 2 > measured_count


def duplicate_warnings(counts):
    """E·F 표본 수가 D 표본 수보다 크면 경고 문구 목록. counts: {단계: {"count": 중앙값}}."""
    d_count = counts.get("D", {}).get("count")
    warnings = []
    if d_count is None:
        return warnings
    for name in ("E", "F"):
        count = counts.get(name, {}).get("count")
        if count is not None and count > d_count:
            warnings.append(f"{name} {count:.0f} > D {d_count:.0f}")
    return warnings


def g_promotion_signal(g_p99, f_p50):
    """protocol-c 4-3절 승급 신호: G p99 가 같은 회차 F p50 이상."""
    return g_p99 is not None and f_p50 is not None and g_p99 >= f_p50


def impact_verdict(off_p50, off_p99, on_p50, on_p99):
    """protocol-c 5절. 끈 세트 회차별 값 목록과 켠 세트 중앙값으로 판정한다."""
    results = []
    for off_values, on_value in ((off_p50, on_p50), (off_p99, on_p99)):
        low, high, mid = min(off_values), max(off_values), statistics.median(off_values)
        if not (low <= on_value <= high):
            results.append("outside")
        elif abs(on_value - mid) > WIDE_RATIO * (high - low):
            results.append("wide")
        else:
            results.append("inside")
    if results == ["inside", "inside"]:
        return "영향 안 보임"
    if results == ["outside", "outside"]:
        return "영향 가능성 있음"
    return "애매함"


IMPACT_TEXT = {
    "영향 안 보임": "영향 안 보임 (계측 영향이 회차 간 변동 안에서는 보이지 않는다)",
    "영향 가능성 있음": "영향 가능성 있음",
    "애매함": "애매함 (끈 → 켠 → 끈 세 세트로 올린다)",
}


# ---------------------------------------------------------------- 출력

def fmt_count(value):
    return f"{value:,.0f}"


def fmt_micros(value):
    if value is None:
        return "—"
    if abs(value) >= 100:
        return f"{value:,.0f}"
    return f"{value:,.1f}"


def with_range(summary, key):
    if summary[key] is None:
        return "—"
    low, high = summary[f"{key}_min"], summary[f"{key}_max"]
    if low == high:
        return fmt_micros(summary[key])
    return f"{fmt_micros(summary[key])} ({fmt_micros(low)}~{fmt_micros(high)})"


def table(header, rows):
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
    return "\n".join(lines)


def ordered_stages(data):
    present = set()
    for snapshots in data.stage_rounds.values():
        present.update(snapshots)
    preferred = STAGE_ORDER[data.meta.cond]
    first = [name for name in preferred if name in present]
    rest = sorted(present - set(first))
    return first + rest


def set_title(data):
    meta = data.meta
    stage_text = "단계 계측 켬" if meta.stages else "단계 계측 끔"
    return f"{meta.name} — {meta.step} {LABEL_TEXT[meta.label]}, {stage_text}"


def stage_table(data):
    rows = []
    footnotes = False
    for name in ordered_stages(data):
        summary = stage_summary(data, name)
        if summary["rounds"] == 0:
            continue
        definition = DEFINITION.get(name, ONE_SIDED)
        if summary["p50"] is not None and below_resolution(summary["p50"]):
            p99_cell = "— 해상도 미만(1µs)"
            footnotes = True
        else:
            p99_cell = with_range(summary, "p99")
        rows.append([name, definition, summary["rounds"], fmt_count(summary["count"]), with_range(summary, "p50"),
                     fmt_micros(summary["p95"]), p99_cell, fmt_micros(summary["max"]), fmt_micros(summary["mean"])])
    if not rows:
        return None, footnotes
    header = ["단계", "정의(설계서 2-3a)", "집계 회차", "count(회차 중앙값)", "p50 µs (최소~최대)", "p95 µs", "p99 µs (최소~최대)",
              "max µs", "mean µs"]
    return table(header, rows), footnotes


READING_LINES = [
    "읽는 방법 1: 같은 정의인 단계끼리만 직접 비교한다. 정의가 다른 단계는 표의 「정의」 칸으로 가려 읽는다.",
    "읽는 방법 2: 다른 정의인 단계의 값 차이는 두 구현의 구조 차이이지 속도 차이가 아니다.",
]


def set_section(data):
    lines = [f"### {set_title(data)}", ""]
    measured = data.measured_rounds()
    included = data.included_rounds()
    saturated = [r for r in measured if data.is_saturated(r)]
    invalid = [r for r in measured if not data.is_valid(r)]
    lines.append(f"측정 회차 {len(measured)}개(워밍업 {WARMUP_ROUND} 제외) 중 집계에 쓴 회차 {len(included)}개, "
                 f"무효 {len(invalid)}개({', '.join(invalid) or '없음'}), 포화 {len(saturated)}개({', '.join(saturated) or '없음'}).")
    lines.append("")
    if not data.stage_rounds:
        lines.append("단계 덤프 없음" + (" (단계 계측이 꺼진 세트)." if data.meta.stages == 0 else " — 입력 파일이 없다."))
        return "\n".join(lines)
    rendered, footnotes = stage_table(data)
    if rendered is None:
        lines.append("집계에 쓸 수 있는 회차가 없어 단계 표를 만들지 못했다.")
    else:
        lines.append(rendered)
        lines.append("")
        lines.extend(f"- {text}" for text in READING_LINES)
        lines.append(f"- 집계: 워밍업·무효·포화 회차를 뺀 회차별 값의 중앙값. p50·p99 칸의 괄호는 회차별 값의 최소~최대.")
        if footnotes:
            lines.append(f"- 해상도 미만(1µs): 시각 해상도가 1µs라 p50이 {RESOLUTION_LIMIT_US:g}µs 미만인 단계는 값이 해상도에 가깝다. p99를 싣지 않는다.")
    missing = missing_inputs(data)
    if missing:
        lines.append("")
        lines.append("입력 누락: " + ", ".join(missing))
    return "\n".join(lines)


def missing_inputs(data):
    if data.meta.stages == 0 or not data.stage_rounds:
        return []
    missing = []
    for round_id in data.measured_rounds():
        seen = data.processes_seen.get(round_id, set())
        for process in EXPECTED_PROCESSES[data.meta.cond]:
            if process not in seen:
                missing.append(f"{process} {round_id}")
    return missing


def residual_section(sets):
    lines = ["## 합 정합성 검사", "",
             "같은 체결 한 건에서 F = A@aggr + B@aggr + C@aggr + D + E가 정의상 성립한다. 평균으로 대조해 "
             "|mean(F) − 다섯 평균의 합| / mean(F)를 잔차 비율로 싣는다. 잔차 비율이 1%를 넘으면 그 세트를 불성립으로 표시한다. "
             "p99는 구간별로 더할 수 없어 이 검사에 쓰지 않는다. 전체 지연의 분포는 F를 직접 기록한 값을 본다.", ""]
    rows = []
    notes = []
    for data in sets:
        if data.meta.stages == 0:
            continue
        needed = ["F", "A@aggr", "B@aggr", "C@aggr", "D", "E"]
        if not data.stage_rounds:
            continue
        ratios = []
        counts = {}
        for round_id in data.included_rounds():
            snapshots = data.stage_rounds.get(round_id, {})
            if not all(name in snapshots and snapshots[name].get("count", 0) > 0 for name in needed):
                continue
            ratios.append(residual_ratio(snapshots["F"]["meanMicros"], [snapshots[n]["meanMicros"] for n in needed[1:]]))
            for name in needed:
                counts.setdefault(name, []).append(snapshots[name]["count"])
        if not ratios:
            rows.append([data.meta.name, "—", "—", "해당 없음(여섯 단계가 모두 있는 회차가 없다)"])
            continue
        ratio = median(ratios)
        count_mismatch = len({median(values) for values in counts.values()}) > 1
        verdict = "합 정합성 불성립" if residual_fails(ratio) else "성립"
        if count_mismatch:
            verdict += ", 표본 수 불일치"
            notes.append(f"{data.meta.name}: 여섯 단계의 표본 수 중앙값이 서로 다르다(" +
                         ", ".join(f"{n} {fmt_count(median(counts[n]))}" for n in needed) + ")")
        rows.append([data.meta.name, f"{ratio * 100:.3f}%", f"{len(ratios)}", verdict])
    if rows:
        lines.append(table(["세트", "잔차 비율(회차 중앙값)", "검사한 회차", "판정"], rows))
    else:
        lines.append("검사할 수 있는 세트가 없다(단계 계측이 켜진 세트가 없다).")
    if notes:
        lines.append("")
        lines.extend(f"- {note}" for note in notes)
    return "\n".join(lines)


def pooled(sets, key_stage, key):
    values = []
    for data in sets:
        values.extend(stage_values(data, key_stage, key))
    return values


def ladder_section(sets):
    lines = ["## 속도 단계별 비교", "",
             "대표 구간은 F와 B(protocol-c 3절)이다. 같은 단계·조건의 세트가 여럿이면(S1 교차) 회차를 합쳐 중앙값을 낸다. "
             "포화 회차는 집계에서 뺀다. 단계에서 측정 회차의 과반이 포화이면 그 단계는 포화한 단계이고, "
             "포화한 단계의 값은 대표값 인용 대상에서 제외한다(표에는 「—」로 둔다).", ""]
    groups = {}
    for data in sets:
        if data.meta.stages != 1 or data.meta.step in IMPACT_STEPS:
            continue
        groups.setdefault((data.meta.step, data.meta.label), []).append(data)
    if not groups:
        lines.append("속도 단계 세트가 없다.")
        return "\n".join(lines)

    def rate_of(step):
        return STEP_RATES.get(step) or int(step[1:])

    rows = []
    summaries = {}
    promotion_notes = []
    for (step, label), members in sorted(groups.items(), key=lambda kv: (rate_of(kv[0][0]), kv[0][1])):
        measured = sum(len(m.measured_rounds()) for m in members)
        saturated_count = sum(1 for m in members for r in m.measured_rounds() if m.is_saturated(r))
        reasons = sorted({reason for m in members for r in m.measured_rounds() if m.is_saturated(r)
                          for reason in m.saturation[r].split(",")})
        is_saturated = measured > 0 and step_saturated(saturated_count, measured)
        status = f"포화한 단계 ({saturated_count}/{measured}회)" if is_saturated else (
            f"일부 포화 회차 {saturated_count}개" if saturated_count else "정상")
        cell = {}
        for stage_name in ("F", "B"):
            for key in ("p50Micros", "p99Micros"):
                values = [] if is_saturated else pooled(members, stage_name, key)
                cell[(stage_name, key)] = (median(values), min(values) if values else None, max(values) if values else None)
        summaries[(step, label)] = cell
        g_values = {key: pooled(members, "G", key) for key in ("p50Micros", "p99Micros")}
        g_p50 = median(g_values["p50Micros"])
        g_p99 = median(g_values["p99Micros"])
        f_p50_all = median(pooled(members, "F", "p50Micros"))
        if g_promotion_signal(g_p99, f_p50_all):
            promotion_notes.append(f"- {step} {LABEL_TEXT[label]}: 승급 신호: G p99 ≥ F p50 (G p99 {fmt_micros(g_p99)} µs, F p50 {fmt_micros(f_p50_all)} µs). "
                                   "protocol-c 4-3절에 따라 이 단계와 더 높은 단계는 HTTP 경로 결과를 인용하지 않는다.")
        def show(stage_name, key):
            value, low, high = cell[(stage_name, key)]
            if value is None:
                return "—"
            return fmt_micros(value) if low == high else f"{fmt_micros(value)} ({fmt_micros(low)}~{fmt_micros(high)})"
        rows.append([f"{step} ({rate_of(step)}/초)", LABEL_TEXT[label], measured, status, ", ".join(reasons) or "—",
                     show("F", "p50Micros"), show("F", "p99Micros"), show("B", "p50Micros"), show("B", "p99Micros"),
                     fmt_micros(g_p50), fmt_micros(g_p99)])
    lines.append(table(["단계", "조건", "측정 회차", "포화 표시", "포화 이유", "F p50 µs", "F p99 µs", "B p50 µs", "B p99 µs",
                        "G p50 µs", "G p99 µs"], rows))
    lines.append("")
    if promotion_notes:
        lines.extend(promotion_notes)
        lines.append("")

    compare_rows = []
    steps = sorted({step for step, _ in groups}, key=rate_of)
    for step in steps:
        v1 = summaries.get((step, "v1idx"))
        v2 = summaries.get((step, "v2"))
        if not v1 or not v2:
            continue
        for stage_name in ("F", "B"):
            a, a_low, a_high = v1[(stage_name, "p50Micros")]
            b, b_low, b_high = v2[(stage_name, "p50Micros")]
            if a is None or b is None:
                compare_rows.append([step, stage_name, "—", "—"])
                continue
            overlap = "예" if not (a_high < b_low or b_high < a_low) else "아니오"
            compare_rows.append([step, stage_name, f"{a / b:.2f}", overlap])
    if compare_rows:
        lines.append("v1(인덱스 있음)과 v2의 p50 중앙값 비율(v1 ÷ v2)과 회차별 값 범위가 겹치는지. 범위가 겹치면 차이를 주장하지 않는다.")
        lines.append("")
        lines.append(table(["단계", "구간", "p50 비율(v1 ÷ v2)", "범위 겹침"], compare_rows))
    else:
        lines.append("v1(인덱스 있음)과 v2를 함께 비교할 수 있는 단계가 없다(포화한 단계이거나 한쪽 세트가 없다).")
    return "\n".join(lines)


def impact_section(sets):
    lines = ["## 계측 켠/끈 영향 판정", "",
             "S1X(리허설은 S1R) 세트의 끝점① 지연(latency-<조건>-rNN.json)으로 판정한다. 끈 세트에도 이 값은 있다. 켠 세트의 유효 회차 "
             "p50·p99 중앙값이 끈 세트 유효 회차의 최소~최대 범위 안이면 영향 안 보임, 둘 다 밖이면 영향 가능성 있음이다. "
             "한쪽만 밖이거나 안이어도 끈 중앙값과의 차이가 끈 범위 폭의 50%를 넘으면 애매함이다(protocol-c 5절).", ""]
    if any(d.meta.step == "S1R" for d in sets):
        lines += ["리허설(S1R)은 회차가 적어 대략 비교로만 본다. 본 판정은 S1X 세트(각 11회)로 한다.", ""]
    rows = []
    by_label = {}
    for data in sets:
        if data.meta.step in IMPACT_STEPS:
            by_label.setdefault(data.meta.label, {0: [], 1: []})[data.meta.stages].append(data)
    for label, groups in sorted(by_label.items()):
        off_p50, off_p99, on_p50, on_p99 = [], [], [], []
        for target, group in ((off_p50, groups[0]), (on_p50, groups[1])):
            for data in group:
                target.extend(data.latency[r]["p50"] for r in data.measured_rounds() if data.is_valid(r) and r in data.latency)
        for target, group in ((off_p99, groups[0]), (on_p99, groups[1])):
            for data in group:
                target.extend(data.latency[r]["p99"] for r in data.measured_rounds() if data.is_valid(r) and r in data.latency)
        if not off_p50 or not on_p50:
            rows.append([LABEL_TEXT[label], "—", "—", "—", "입력 부족(끈 세트 또는 켠 세트가 없다)"])
            continue
        verdict = impact_verdict(off_p50, off_p99, median(on_p50), median(on_p99))
        rows.append([LABEL_TEXT[label],
                     f"{fmt_micros(median(off_p50))} ({fmt_micros(min(off_p50))}~{fmt_micros(max(off_p50))}) / {fmt_micros(median(on_p50))}",
                     f"{fmt_micros(median(off_p99))} ({fmt_micros(min(off_p99))}~{fmt_micros(max(off_p99))}) / {fmt_micros(median(on_p99))}",
                     f"{len(off_p50)} / {len(on_p50)}", IMPACT_TEXT[verdict]])
    if rows:
        lines.append(table(["조건", "p50 µs 끈 중앙값(범위) / 켠 중앙값", "p99 µs 끈 중앙값(범위) / 켠 중앙값", "유효 회차 끈 / 켠", "판정"], rows))
    else:
        lines.append("S1X·S1R 세트가 없어 판정하지 못했다.")
    return "\n".join(lines)


def correspondence_section():
    lines = ["## 두 구현의 단계 대응", "", "설계서 2-3a절을 옮긴 것이다. 위 표와 다르면 설계서가 기준이다.", "",
             table(["단계", "같은 정의 / 다른 정의", "내용"], CORRESPONDENCE), "",
             "- v1에만 있는 단계: b_idem, b_lock, b_sum, b_insert, b_send_call.",
             "- v2에만 있는 단계: ring_publish, hop_to_journal, hop_to_business, m_ring_publish, m_hop_to_business, fill_hop 계열.",
             "- 한계: 계좌 → 매칭 방향 발신 큐는 따로 측정하지 않고 C 단계에 포함된다.",
             "- 체결 한 건의 기록 규칙은 두 구현이 같다(v2 매수 체결 이벤트, v1 정산 컨슈머가 체결 이벤트 한 건을 처리한 뒤)."]
    return "\n".join(lines)


def warning_section(sets):
    lines = ["## 경고", ""]
    found = False
    for data in sets:
        if data.meta.stages == 0 or not data.stage_rounds:
            continue
        counts = {name: {"count": stage_summary(data, name)["count"]} for name in ("D", "E", "F")}
        warnings = duplicate_warnings({k: v for k, v in counts.items() if v["count"] is not None})
        if warnings:
            found = True
            lines.append(f"- {data.meta.name}: 중복 재전달 가능성: {', '.join(warnings)} "
                         "(정산 컨슈머가 이미 반영한 체결의 재전달을 정상 반영과 구분하지 못하는 구조다)")
        missing = missing_inputs(data)
        if missing:
            found = True
            lines.append(f"- {data.meta.name}: 입력 누락: {', '.join(missing)}")
        if not data.has_saturation_file and data.measured_rounds():
            found = True
            lines.append(f"- {data.meta.name}: 포화 판정 파일(saturation-{data.meta.cond}.csv)이 없어 포화 회차를 가리지 못했다")
    if not found:
        lines.append("경고 없음.")
    return "\n".join(lines)


def build_segments_report(run_dir):
    sets = load_run(run_dir)
    lines = [f"# 구간 분해 측정 분석 ({Path(run_dir).name})", ""]
    if not sets:
        lines.append("세트 폴더가 없다. <NN>-<단계>-<조건>-st<계측> 형식의 폴더를 찾지 못했다.")
        return "\n".join(lines)
    lines.append(table(["세트", "단계", "조건", "단계 계측", "측정 회차", "집계 회차"],
                       [[d.meta.name, d.meta.step, LABEL_TEXT[d.meta.label], "켬" if d.meta.stages else "끔",
                         len(d.measured_rounds()), len(d.included_rounds())] for d in sets]))
    lines += ["", "## 세트별 단계 표", ""]
    for data in sets:
        lines += [set_section(data), ""]
    for section in (residual_section(sets), ladder_section(sets), impact_section(sets), correspondence_section(),
                    warning_section(sets)):
        lines += [section, ""]
    return "\n".join(lines).rstrip() + "\n"


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    report = sub.add_parser("report")
    report.add_argument("run_dir")
    report.add_argument("--out")
    args = parser.parse_args(argv)
    text = build_segments_report(args.run_dir)
    print(text)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
