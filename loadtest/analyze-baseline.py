#!/usr/bin/env python3
"""기준선 재측정(묶음 B 단계 1) 분석기. 설계서 2-10절.

입력: <결과 디렉터리>/<cond>/ 아래의 파일 (cond = v1, v2)
  rounds-<cond>.csv           회차 기록. 이 분석기는 round, k6_start, k6_end, k6_exit, db_delta, flags만 읽는다
  latency-<cond>-rNN.json     히스토그램 스냅샷 (count, p50Nanos, p95Nanos, p99Nanos, maxNanos)
  k6-<cond>-rNN.json          k6 --summary-export 결과
  cpu-<cond>-rNN.csv          sample-env.sh 출력

제외 규칙(measure-protocol-b0.md, 측정 전에 고정한 문서)은 이 파일의 상수와 judge_reasons()에만 있다.
값의 크기는 제외 사유로 쓰지 않는다.

사용:
  analyze-baseline.py report <결과 디렉터리> [--out analysis.md]
  analyze-baseline.py judge --dir <cond 디렉터리> --cond v1 --round r05 --k6-start N --k6-end N --k6-exit N
                            [--db-delta N] [--e6] [--e7]
  analyze-baseline.py compare [--out cmp.md] 라벨=결과디렉터리[:조건] ...   (조건 기본 v1) 여러 세트를 한 표에 나란히
  analyze-baseline.py seed-check --input <e2e-input.jsonl> --seed-yml <application-e2e.yml> [--count 10000] [--rounds 11]
                                 점검 8: 입력 앞 N건의 계좌별 필요량 x 회차 수가 시드 잔고·보유 안에 들어오는지
"""
import argparse
import csv
import json
import os
import re
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path

# 임시값 — Jack 확정 전 (measure-protocol-b0.md 맨 위와 같은 값)
EXPECTED_COUNT = 10000
WARMUP_ROUND = "r01"
IDLE_LIMIT = 30.0          # E1: 전체 idle%가 이 값 미만
K6_CPU_LIMIT = 40.0        # E2: k6 CPU가 이 값 이상 (1코어 대비 %)
EXTERNAL_CPU_LIMIT = 25.0  # E4: 허용 목록 밖 프로세스 CPU가 이 값 이상
CONSECUTIVE = 2            # E1, E2, E4: 연속 샘플 수
MIN_VALID_ROUNDS = 7       # 유효 측정 회차가 이 값 미만이면 조건 불성립
EXPECTED_ROUNDS = 11

# E4 허용 목록. 첫 리허설에서 실제 top 출력을 보고 확정한다(설계서 2-3절).
DEFAULT_E4_ALLOW = r"^(k6|java|kernel_task)$|[Dd]ocker|Virtua|vmnet|qemu"

REASON_ORDER = ["WARMUP", "E1", "E2", "E3", "E4", "E5", "E6", "COUNT"]
ROUNDS_HEADER = ["round", "k6_start", "k6_end", "k6_exit", "checks_fail", "http_fail", "dropped", "hist_count",
                 "db_delta", "flags", "valid"]
METRICS = [("p50Nanos", "p50"), ("p95Nanos", "p95"), ("p99Nanos", "p99"), ("maxNanos", "max")]


@dataclass
class K6Facts:
    present: bool = False
    checks_fail: int = 0
    http_fail: int = 0
    dropped: int = 0
    http_reqs: int = 0


@dataclass
class Round:
    round_id: str
    k6_start: int = 0
    k6_end: int = 0
    k6_exit: int = 0
    db_delta: int | None = None
    flags: list = field(default_factory=list)
    latency: dict | None = None
    k6: K6Facts = field(default_factory=K6Facts)
    cpu: list = field(default_factory=list)
    reasons: list = field(default_factory=list)

    @property
    def valid(self):
        return not self.reasons

    @property
    def hist_count(self):
        return None if self.latency is None else self.latency.get("count")


@dataclass
class Summary:
    n: int
    median_ms: float | None
    min_ms: float | None
    max_ms: float | None


@dataclass
class Stat:
    count: int
    maximum: float
    average: float
    minimum: float


def format_ms(nanos):
    return f"{nanos / 1_000_000:.2f}"


def to_int(text):
    text = str(text if text is not None else "").strip()
    return int(float(text)) if text else None


def to_float(text):
    text = (text or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


# ---------- 입력 읽기 ----------

def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def metric_values(summary, name):
    metrics = summary.get("metrics", {})
    metric = metrics.get(name)
    if not metric:
        return {}
    return metric.get("values", metric)


def read_k6_facts(path):
    summary = read_json(path)
    if summary is None:
        return K6Facts(present=False)
    # 필드 의미는 첫 리허설에서 요약 JSON을 열어 확인한다. 원본 기록(numbers.md 8)에서는
    # http_req_failed.passes가 실패 수, fails가 성공 수였다.
    return K6Facts(
        present=True,
        checks_fail=int(metric_values(summary, "checks").get("fails", 0)),
        http_fail=int(metric_values(summary, "http_req_failed").get("passes", 0)),
        dropped=int(metric_values(summary, "dropped_iterations").get("count", 0)),
        http_reqs=int(metric_values(summary, "http_reqs").get("count", 0)),
    )


def read_cpu_samples(path):
    try:
        with open(path, newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except OSError:
        return []


def window_samples(samples, start, end):
    selected = []
    for sample in samples:
        ts = to_int(sample.get("ts"))
        if ts is not None and start <= ts <= end:
            selected.append(sample)
    return selected


# ---------- 판정 ----------

def has_consecutive(flags, count):
    run = 0
    for flag in flags:
        run = run + 1 if flag else 0
        if run >= count:
            return True
    return False


def parse_top5(text):
    processes = []
    for entry in (text or "").split("|"):
        entry = entry.strip()
        if ":" not in entry:
            continue
        name, cpu = entry.rsplit(":", 1)
        value = to_float(cpu)
        if value is not None:
            processes.append((name.strip(), value))
    return processes


def judge_reasons(round_, cond):
    """이 회차의 제외 사유를 REASON_ORDER 순서로 돌려준다. 빈 목록이면 유효 회차다."""
    reasons = set()
    if round_.round_id == WARMUP_ROUND:
        reasons.add("WARMUP")

    samples = window_samples(round_.cpu, round_.k6_start, round_.k6_end)
    if has_consecutive([(to_float(s.get("idle")) is not None and to_float(s.get("idle")) < IDLE_LIMIT)
                        for s in samples], CONSECUTIVE):
        reasons.add("E1")
    if has_consecutive([(to_float(s.get("k6_cpu")) is not None and to_float(s.get("k6_cpu")) >= K6_CPU_LIMIT)
                        for s in samples], CONSECUTIVE):
        reasons.add("E2")
    if any(is_power_or_thermal_problem(s) for s in samples):
        reasons.add("E3")
    allow = re.compile(os.environ.get("E4_ALLOW") or DEFAULT_E4_ALLOW)
    if has_consecutive([any(cpu >= EXTERNAL_CPU_LIMIT and not allow.search(name)
                            for name, cpu in parse_top5(s.get("top5")))
                        for s in samples], CONSECUTIVE):
        reasons.add("E4")

    facts = round_.k6
    if (not facts.present or round_.k6_exit != 0 or facts.checks_fail >= 1 or facts.http_fail >= 1
            or facts.dropped >= 1):
        reasons.add("E5")
    if "E6" in round_.flags:
        reasons.add("E6")

    count_mismatch = round_.hist_count != EXPECTED_COUNT
    if cond == "v1" and round_.db_delta != EXPECTED_COUNT:
        count_mismatch = True
    if count_mismatch:
        reasons.add("COUNT")

    return [code for code in REASON_ORDER if code in reasons]


def is_power_or_thermal_problem(sample):
    batt = (sample.get("batt") or "").strip()
    if batt and batt != "AC":
        return True
    limit = to_float(sample.get("therm_limit"))
    return limit is not None and limit < 100.0


def build_round(cond_dir, cond, round_id, row):
    cond_dir = Path(cond_dir)
    flags = (row.get("flags") or "").split()
    round_ = Round(
        round_id=round_id,
        k6_start=to_int(row.get("k6_start")) or 0,
        k6_end=to_int(row.get("k6_end")) or 0,
        k6_exit=to_int(row.get("k6_exit")) or 0,
        db_delta=to_int(row.get("db_delta")),
        flags=flags,
        latency=read_json(cond_dir / f"latency-{cond}-{round_id}.json"),
        k6=read_k6_facts(cond_dir / f"k6-{cond}-{round_id}.json"),
        cpu=read_cpu_samples(cond_dir / f"cpu-{cond}-{round_id}.csv"),
    )
    round_.reasons = judge_reasons(round_, cond)
    return round_


def load_condition(cond_dir, cond):
    rounds_path = Path(cond_dir) / f"rounds-{cond}.csv"
    with open(rounds_path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    rounds = [build_round(cond_dir, cond, row["round"], row) for row in rows]
    return sorted(rounds, key=lambda r: r.round_id)


def aborted(rounds):
    return any("E7" in r.flags for r in rounds)


def valid_measurement_count(rounds):
    return sum(1 for r in rounds if r.round_id != WARMUP_ROUND and r.valid)


def condition_holds(rounds):
    return not aborted(rounds) and valid_measurement_count(rounds) >= MIN_VALID_ROUNDS


# ---------- 요약 ----------

def summarize(rounds, key, only_valid):
    values = []
    for r in rounds:
        if r.latency is None or key not in r.latency:
            continue
        if only_valid and not r.valid:
            continue
        values.append(r.latency[key])
    if not values:
        return Summary(0, None, None, None)
    return Summary(len(values), statistics.median(values) / 1e6, min(values) / 1e6, max(values) / 1e6)


def cpu_window_stats(samples, start, end):
    selected = window_samples(samples, start, end)
    columns = {"idle": "idle", "k6": "k6_cpu", "pg": "pg_cpu", "kafka": "kafka_cpu"}
    if selected:
        for name in selected[0].keys():
            if name.startswith("jvm_"):
                columns[name] = name
    stats = {}
    for label, column in columns.items():
        values = [v for v in (to_float(s.get(column)) for s in selected) if v is not None]
        if values:
            stats[label] = Stat(len(values), max(values), sum(values) / len(values), min(values))
    return stats


# ---------- 회차 기록 한 줄 ----------

def judge_row(cond_dir, cond, round_id, k6_start, k6_end, k6_exit, db_delta, e6=False, e7=False):
    flags = ["E6"] if e6 else []
    row = {"flags": " ".join(flags), "k6_start": k6_start, "k6_end": k6_end, "k6_exit": k6_exit,
           "db_delta": "" if db_delta is None else db_delta}
    round_ = build_round(cond_dir, cond, round_id, row)
    marks = list(round_.reasons) + (["E7"] if e7 else [])
    return {
        "round": round_id,
        "k6_start": k6_start,
        "k6_end": k6_end,
        "k6_exit": k6_exit,
        "checks_fail": round_.k6.checks_fail,
        "http_fail": round_.k6.http_fail,
        "dropped": round_.k6.dropped,
        "hist_count": "" if round_.hist_count is None else str(round_.hist_count),
        "db_delta": "" if db_delta is None else db_delta,
        "flags": " ".join(marks),
        "valid": "1" if round_.valid and not e7 else "0",
    }


def append_row(cond_dir, cond, row):
    path = Path(cond_dir) / f"rounds-{cond}.csv"
    is_new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=ROUNDS_HEADER)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


# ---------- 보고서 ----------

def table(header, rows):
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
    return "\n".join(lines)


def status_text(rounds):
    if aborted(rounds):
        return "조건 중단(E7: 앱 JVM 종료)"
    if not condition_holds(rounds):
        return f"측정 불성립(유효 측정 회차 {valid_measurement_count(rounds)}개, 기준 {MIN_VALID_ROUNDS}개 이상)"
    return f"성립(유효 측정 회차 {valid_measurement_count(rounds)}개)"


def per_round_table(rounds):
    rows = []
    for r in rounds:
        latency = r.latency or {}
        cells = [format_ms(latency[key]) if key in latency else "-" for key, _ in METRICS]
        rows.append([r.round_id] + cells + [
            r.hist_count if r.hist_count is not None else "-",
            r.db_delta if r.db_delta is not None else "-",
            r.k6.http_reqs if r.k6.present else "-",
            "유효" if r.valid else "제외",
            " ".join(r.reasons) or "-",
        ])
    header = ["회차", "p50(ms)", "p95(ms)", "p99(ms)", "max(ms)", "히스토그램 건수", "DB 증가분", "k6 요청 수",
              "판정", "제외 사유"]
    return table(header, rows)


def summary_table(rounds_by_cond, only_valid):
    rows = []
    for cond, rounds in rounds_by_cond.items():
        for key, label in METRICS:
            s = summarize(rounds, key, only_valid)
            if s.n == 0:
                rows.append([cond, label, "-", "-", 0])
            else:
                rows.append([cond, label, f"{s.median_ms:.2f}", f"{s.min_ms:.2f} ~ {s.max_ms:.2f}", s.n])
    return table(["조건", "지표", "중앙값(ms)", "범위(ms)", "n"], rows)


def trend_lines(rounds):
    lines = []
    for key, label in (("p50Nanos", "p50"), ("p99Nanos", "p99")):
        values = []
        for r in rounds:
            if r.round_id == WARMUP_ROUND or r.latency is None:
                continue
            mark = "" if r.valid else "*"
            values.append(f"{r.round_id}={format_ms(r.latency[key])}{mark}")
        lines.append(f"- {label}(ms): " + ", ".join(values))
    return "\n".join(lines)


def comparison_lines(rounds_by_cond):
    v1, v2 = rounds_by_cond.get("v1"), rounds_by_cond.get("v2")
    if v1 is None or v2 is None:
        return "한 조건의 결과가 없어 비교하지 않는다."
    broken = [cond for cond, rounds in rounds_by_cond.items() if not condition_holds(rounds)]
    if broken:
        return "측정 불성립 또는 중단된 조건이 있어(" + ", ".join(broken) + ") 비교하지 않는다."
    rows = []
    for key, label in METRICS:
        a, b = summarize(v1, key, True), summarize(v2, key, True)
        ratio = f"{a.median_ms / b.median_ms:.2f}" if b.median_ms else "-"
        overlap = "겹쳤다" if (a.min_ms <= b.max_ms and b.min_ms <= a.max_ms) else "겹치지 않았다"
        rows.append([label, f"{a.median_ms:.2f}", f"{b.median_ms:.2f}", ratio, overlap])
    return table(["지표", "v1 중앙값(ms)", "v2 중앙값(ms)", "중앙값 비율(v1/v2)", "두 범위"], rows)


def cpu_table(rounds):
    jvm_columns = []
    for r in rounds:
        stats = cpu_window_stats(r.cpu, r.k6_start, r.k6_end)
        for name in stats:
            if name.startswith("jvm_") and name not in jvm_columns:
                jvm_columns.append(name)
    header = ["회차", "샘플 수", "idle 최소/평균/최대", "k6 최대/평균"] + [f"{c} 최대/평균" for c in jvm_columns] + [
        "postgres 최대/평균", "kafka 최대/평균", "걸린 규칙"]
    rows = []
    for r in rounds:
        stats = cpu_window_stats(r.cpu, r.k6_start, r.k6_end)

        def pair(name):
            s = stats.get(name)
            return f"{s.maximum:.1f}/{s.average:.1f}" if s else "-"

        idle = stats.get("idle")
        idle_cell = f"{idle.minimum:.1f}/{idle.average:.1f}/{idle.maximum:.1f}" if idle else "-"
        env_codes = [code for code in r.reasons if code in ("E1", "E2", "E3", "E4", "E5", "E6")]
        rows.append([r.round_id, idle.count if idle else 0, idle_cell, pair("k6")] + [pair(c) for c in jvm_columns] + [
            pair("pg"), pair("kafka"), " ".join(env_codes) or "-"])
    return table(header, rows)


def build_report(result_dir):
    result_dir = Path(result_dir)
    rounds_by_cond = {}
    for cond in ("v1", "v2"):
        if (result_dir / cond / f"rounds-{cond}.csv").exists():
            rounds_by_cond[cond] = load_condition(result_dir / cond, cond)

    out = ["# 기준선 재측정 분석", "",
           "단위는 밀리초(소수 둘째 자리)다. 제외 사유 코드는 measure-protocol-b0.md의 규칙을 따른다. "
           "WARMUP은 첫 회차, COUNT는 회차 건수가 1만이 아닌 경우다. 제외된 회차의 값도 표에 그대로 싣는다.", ""]
    for cond, rounds in rounds_by_cond.items():
        out.append(f"- {cond}: {status_text(rounds)}")
    out.append("")

    out += ["## 1. 회차별 표", ""]
    for cond, rounds in rounds_by_cond.items():
        out += [f"### {cond}", "", per_round_table(rounds), ""]

    out += ["## 2. 규칙을 적용한 요약 (유효 측정 회차만)", "", summary_table(rounds_by_cond, True), ""]
    out += ["## 3. 11회 전체 요약 (제외된 회차 포함)", "", summary_table(rounds_by_cond, False), ""]

    out += ["## 4. 회차 순서별 추이 (r02~r11, 별표는 제외된 회차)", ""]
    for cond, rounds in rounds_by_cond.items():
        out += [f"### {cond}", "", trend_lines(rounds), ""]

    out += ["## 5. v1 대 v2 (유효 측정 회차의 중앙값 비율과 범위 겹침)", "", comparison_lines(rounds_by_cond), ""]

    out += ["## 6. CPU (k6 시작~종료 구간 안의 샘플, 단위 %, JVM과 k6는 1코어 대비)", ""]
    for cond, rounds in rounds_by_cond.items():
        out += [f"### {cond}", "", cpu_table(rounds), ""]

    out += ["## 7. 한계", "",
            "- 11회가 한 JVM에서 연속으로 돌아 회차끼리 독립이 아니다. 유효 측정 회차 10개는 "
            "한 프로세스 안에서 반복한 10개 관측이다.",
            "- 스냅샷은 백분위 네 값과 건수만 저장하므로 회차들의 분포를 합쳐 전체 분포를 복원할 수 없다.",
            "- \"k6 요청 수\" 열은 확인용이다. 1만이 아니어도 제외 사유로 쓰지 않았다(규칙 4는 히스토그램 건수와 "
            "v1 DB 증가분만 본다).", ""]
    return "\n".join(out)




# ---------- 여러 세트 나란히 (compare) ----------

DBSTATS_HEADER = ["round", "phase", "orders_total", "orders_e2e", "pending_e2e", "filled_e2e", "unpaids_e2e",
                  "dead_orders", "orders_bytes", "q1_ms", "q2_ms", "qh_ms", "q1_plan", "q2_plan", "qh_plan",
                  "clear_ms", "deleted_orders"]


def load_dbstats(cond_dir, cond):
    """dbstats-<cond>.csv를 {"start": {회차: 행}, "cleared": {회차: 행}}로 읽는다. 파일이 없으면 빈 딕셔너리 둘."""
    path = Path(cond_dir) / f"dbstats-{cond}.csv"
    stats = {"start": {}, "cleared": {}}
    try:
        with open(path, newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                phase = row.get("phase", "")
                if phase in stats:
                    stats[phase][row["round"]] = row
    except OSError:
        pass
    return stats


def round_cell(round_, key):
    if round_ is None or round_.latency is None or key not in round_.latency:
        return "-"
    return format_ms(round_.latency[key]) + ("" if round_.valid else "*")


def window_idle_min(round_):
    stats = cpu_window_stats(round_.cpu, round_.k6_start, round_.k6_end)
    idle = stats.get("idle")
    return f"{idle.minimum:.1f}" if idle else "-"


def window_pg_average(round_):
    stats = cpu_window_stats(round_.cpu, round_.k6_start, round_.k6_end)
    pg = stats.get("pg")
    return f"{pg.average:.1f}" if pg else "-"


def compare_table(labels, loaded, cell):
    all_rounds = sorted({r.round_id for rounds in loaded.values() for r in rounds})
    rows = []
    for round_id in all_rounds:
        row = [round_id]
        for label in labels:
            match = next((r for r in loaded[label] if r.round_id == round_id), None)
            row.append(cell(match))
        rows.append(row)
    return table(["회차"] + labels, rows)


def dbstats_tables(label, stats):
    if not stats["start"]:
        return [f"### {label}", "", "- 기록 없음", ""]
    rows = []
    for round_id in sorted(stats["start"]):
        row = stats["start"][round_id]

        def cell(key, fmt=None):
            value = (row.get(key) or "").strip()
            if not value:
                return "-"
            return fmt(value) if fmt else value

        rows.append([round_id, cell("orders_total"), cell("orders_e2e"), cell("pending_e2e"), cell("dead_orders"),
                     cell("orders_bytes", lambda v: str(int(float(v)) // 1024)),
                     cell("q1_ms", lambda v: f"{float(v):.2f}"), cell("q2_ms", lambda v: f"{float(v):.2f}"),
                     cell("qh_ms", lambda v: f"{float(v):.2f}"), cell("q1_plan"), cell("q2_plan"), cell("qh_plan")])
    header = ["회차", "orders 전체 행", "계좌 2001~2100 행", "PENDING 행", "dead tuple", "orders 파일(KB)",
              "예약 증거금 합 ms", "매도 잔량 합 ms", "보유 락 ms", "예약 증거금 합 계획", "매도 잔량 합 계획", "보유 락 계획"]
    out = [f"### {label}", "", table(header, rows), ""]
    if stats["cleared"]:
        cleared_rows = []
        for round_id in sorted(stats["cleared"]):
            row = stats["cleared"][round_id]
            cleared_rows.append([round_id, row.get("deleted_orders") or "-", row.get("clear_ms") or "-",
                                 row.get("orders_e2e") or "-", row.get("pending_e2e") or "-"])
        out += ["비움 직후", "", table(["회차", "지운 orders 행", "걸린 ms", "남은 계좌 2001~2100 행", "남은 PENDING 행"],
                                       cleared_rows), ""]
    return out


def build_compare(entries):
    """entries: [(라벨, 결과 디렉터리, 조건)]. 세트마다 같은 표에 열을 하나씩 둔다."""
    labels = []
    loaded = {}
    stats_by_label = {}
    for label, result_dir, cond in entries:
        cond_dir = Path(result_dir) / cond
        labels.append(label)
        loaded[label] = load_condition(cond_dir, cond)
        stats_by_label[label] = load_dbstats(cond_dir, cond)

    out = ["# 세트 비교", "",
           "단위는 밀리초(소수 둘째 자리)다. 별표는 제외된 회차(사유 코드는 각 세트의 analysis.md)이고, r01은 워밍업이다.", ""]
    for label in labels:
        out.append(f"- {label}: {status_text(loaded[label])}")
    out.append("")

    out += ["## p50 추이", "", compare_table(labels, loaded, lambda r: round_cell(r, "p50Nanos")), ""]
    out += ["## p99 추이", "", compare_table(labels, loaded, lambda r: round_cell(r, "p99Nanos")), ""]

    summary_rows = []
    for label in labels:
        for key, name in (("p50Nanos", "p50"), ("p99Nanos", "p99")):
            valid = summarize(loaded[label], key, True)
            everything = summarize(loaded[label], key, False)
            summary_rows.append([label, name,
                                 f"{valid.median_ms:.2f} ({valid.min_ms:.2f} ~ {valid.max_ms:.2f}), n={valid.n}"
                                 if valid.n else "-",
                                 f"{everything.median_ms:.2f} ({everything.min_ms:.2f} ~ {everything.max_ms:.2f}), "
                                 f"n={everything.n}" if everything.n else "-"])
    out += ["## 요약 (중앙값, 최소~최대)", "",
            table(["세트", "지표", "유효 측정 회차만", "11회 전체"], summary_rows), ""]

    out += ["## 구간 idle 최솟값 (%, k6 시작~종료 구간 안 샘플)", "",
            compare_table(labels, loaded, lambda r: "-" if r is None else window_idle_min(r)), ""]
    out += ["## postgres 평균 CPU (1코어 대비 %, k6 시작~종료 구간 안 샘플)", "",
            compare_table(labels, loaded, lambda r: "-" if r is None else window_pg_average(r)), ""]

    out += ["## 회차 시작 시점의 DB 상태와 쿼리 시간", ""]
    for label in labels:
        out += dbstats_tables(label, stats_by_label[label])
    return "\n".join(out)


# ---------- 점검 8: 시드 충분성 ----------

def check_seed(input_path, seed_yml_path, count=EXPECTED_COUNT, rounds=EXPECTED_ROUNDS):
    """입력 앞 count건의 계좌별 필요량 x rounds가 시드 안에 들어오는지 본다. 문제 목록을 돌려주고, 비면 통과다.

    매수 필요량은 price x quantity의 합이고 증거금률 1.00을 전제로 한다(시드가 1.00이다). 증거금률이 1.00이
    아닌 계좌는 필요량에 증거금률을 곱해 비교한다. 매도 필요량은 (계좌, 종목)별 수량의 합이다.
    """
    input_path = Path(input_path)
    if not input_path.exists():
        return [f"입력 파일이 없다: {input_path} (python3 loadtest/generate-e2e-input.py 를 먼저 실행한다)"]
    try:
        import yaml
    except ImportError:
        return ["PyYAML 이 필요하다 (python3 -m pip install pyyaml)"]

    buy_need = {}
    sell_need = {}
    with open(input_path, encoding="utf-8") as handle:
        for index, line in enumerate(handle):
            if index >= count:
                break
            if not line.strip():
                continue
            order = json.loads(line)
            account = order["accountId"]
            if order["side"] == "BUY":
                buy_need[account] = buy_need.get(account, 0) + order["price"] * order["quantity"]
            else:
                key = (account, order["stockCode"])
                sell_need[key] = sell_need.get(key, 0) + order["quantity"]

    with open(seed_yml_path, encoding="utf-8") as handle:
        seed = yaml.safe_load(handle)
    seeded = {entry["account-id"]: entry for entry in seed["account-worker"]["seed-accounts"]}

    problems = []
    for account, need in sorted(buy_need.items()):
        entry = seeded.get(account)
        if entry is None:
            problems.append(f"계좌 {account}: 시드에 없다 (매수 필요량 {need} x {rounds})")
            continue
        margin_rate = float(entry.get("margin-rate", 1.0))
        required = need * margin_rate * rounds
        if entry["balance"] < required:
            problems.append(f"계좌 {account}: 잔고 {entry['balance']} < 필요 {required:g} (매수 {need} x 증거금률 "
                            f"{margin_rate:g} x {rounds}회차)")
    for (account, stock), need in sorted(sell_need.items()):
        entry = seeded.get(account)
        if entry is None:
            problems.append(f"계좌 {account}: 시드에 없다 (매도 {stock} 필요량 {need} x {rounds})")
            continue
        held = (entry.get("holdings") or {}).get(stock, 0)
        if held < need * rounds:
            problems.append(f"계좌 {account} 종목 {stock}: 보유 {held} < 필요 {need * rounds} ({need} x {rounds}회차)")
    return problems


# ---------- CLI ----------

def main(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    report = sub.add_parser("report")
    report.add_argument("result_dir")
    report.add_argument("--out")

    judge = sub.add_parser("judge")
    judge.add_argument("--dir", required=True)
    judge.add_argument("--cond", required=True, choices=["v1", "v2"])
    judge.add_argument("--round", required=True)
    judge.add_argument("--k6-start", type=int, required=True)
    judge.add_argument("--k6-end", type=int, required=True)
    judge.add_argument("--k6-exit", type=int, required=True)
    judge.add_argument("--db-delta", type=int)
    judge.add_argument("--e6", action="store_true")
    judge.add_argument("--e7", action="store_true")

    compare = sub.add_parser("compare")
    compare.add_argument("--out")
    compare.add_argument("specs", nargs="+", help="라벨=결과디렉터리[:조건]")

    seed = sub.add_parser("seed-check")
    seed.add_argument("--input", required=True)
    seed.add_argument("--seed-yml", required=True)
    seed.add_argument("--count", type=int, default=EXPECTED_COUNT)
    seed.add_argument("--rounds", type=int, default=EXPECTED_ROUNDS)

    args = parser.parse_args(argv)
    if args.command == "compare":
        entries = []
        for spec in args.specs:
            label, _, target = spec.partition("=")
            path, _, cond = target.partition(":")
            entries.append((label, path, cond or "v1"))
        text = build_compare(entries)
        print(text)
        if args.out:
            Path(args.out).write_text(text + "\n", encoding="utf-8")
        return 0

    if args.command == "seed-check":
        problems = check_seed(args.input, args.seed_yml, args.count, args.rounds)
        for problem in problems:
            print(problem)
        if not problems:
            print(f"시드 충분: 앞 {args.count}건 x {args.rounds}회차가 시드 잔고·보유 안에 들어온다")
        return 1 if problems else 0

    if args.command == "report":
        text = build_report(args.result_dir)
        print(text)
        if args.out:
            Path(args.out).write_text(text + "\n", encoding="utf-8")
        return 0

    row = judge_row(args.dir, args.cond, args.round, args.k6_start, args.k6_end, args.k6_exit, args.db_delta,
                    e6=args.e6, e7=args.e7)
    append_row(args.dir, args.cond, row)
    print(f"{args.round} valid={row['valid']} flags={row['flags'] or '-'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
