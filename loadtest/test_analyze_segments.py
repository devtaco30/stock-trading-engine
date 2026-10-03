#!/usr/bin/env python3
"""analyze-segments.py 단위 테스트. 합성 데이터(stages-*.json, series-*.csv, saturation-*.csv, rounds-*.csv, latency-*.json)만 쓴다.

실행: python3 -m unittest loadtest/test_analyze_segments.py -v   (저장소 루트에서)
"""
import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("analyze-segments.py")
spec = importlib.util.spec_from_file_location("analyze_segments", MODULE_PATH)
asg = importlib.util.module_from_spec(spec)
spec.loader.exec_module(asg)

BASELINE_PATH = Path(__file__).with_name("analyze-baseline.py")
spec_b = importlib.util.spec_from_file_location("analyze_baseline_for_segments", BASELINE_PATH)
ab = importlib.util.module_from_spec(spec_b)
spec_b.loader.exec_module(ab)

ROUNDS_HEADER = ["round", "k6_start", "k6_end", "k6_exit", "checks_fail", "http_fail", "dropped", "hist_count",
                 "db_delta", "flags", "valid"]
SAT_HEADER = ["round", "reasons", "dropped", "http_fail", "hist_count", "n", "lag", "first_p50_us", "last_p50_us",
              "drain_sec", "arrival_sec"]


def stage(mean, count=10000, p99_factor=3.0):
    """평균이 mean(µs)인 단계 스냅샷 하나."""
    return {"count": count, "p50Micros": round(mean), "p95Micros": round(mean * 2), "p99Micros": round(mean * p99_factor),
            "maxMicros": round(mean * 10), "meanMicros": float(mean)}


V2_ACCOUNT = {"A": 120, "B": 300, "B_biz": 20, "ring_publish": 2, "hop_to_journal": 90, "journal_append": 30,
              "hop_to_business": 80}


def v2_stage_files(f_error=0.0, counts=None, small=False, g_mean=20):
    """round 하나의 v2 프로세스별 단계 dict. F 에 오차 비율을 섞는다."""
    counts = counts or {}
    a, b, c, d, e = 1.0, 74.0, 0.8, 46.0, 110.0
    if small:
        a, c = 1.0, 1.0
    f_mean = (a + b + c + d + e) * (1 + f_error)
    account = {name: stage(value) for name, value in V2_ACCOUNT.items()}
    account["E"] = stage(e, counts.get("E", 10000))
    account["F"] = stage(f_mean, counts.get("F", 10000))
    matching = {"C": stage(150), "D": stage(d, counts.get("D", 10000)), "A@aggr": stage(a, 10000),
                "B@aggr": stage(b, 10000), "C@aggr": stage(c, 10000)}
    return {"account-worker": account, "matching-worker": matching, "api": {"api_send_cost": stage(30), "G": stage(g_mean)}}


class SetFixture:
    """세트 폴더 하나(<NN>-<단계>-<조건>-st<계측>/<cond>/)를 합성한다."""

    def __init__(self, run_dir: Path, name: str, cond: str):
        self.name = name
        self.cond = cond
        self.dir = run_dir / name / cond
        self.dir.mkdir(parents=True)
        self.rounds = []
        self.saturation = []
        self.rates = []

    def add_round(self, round_id, valid=1, flags="", reasons="none", stages=None, skip_processes=(),
                  latency=None, hist_count=10000, cpu_rows=None):
        self.rounds.append([round_id, 1000, 1052, 0, 0, 0, 0, hist_count, "", flags, valid])
        self.saturation.append([round_id, reasons, 0, 0, hist_count, 10000, 0, "NA", "NA", "NA", 52])
        for process, snapshots in (stages or {}).items():
            if process in skip_processes:
                continue
            (self.dir / f"stages-{self.cond}-{process}-{round_id}.json").write_text(json.dumps(snapshots))
        if cpu_rows is not None:
            header = list(cpu_rows[0].keys())
            with open(self.dir / f"cpu-{self.cond}-{round_id}.csv", "w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=header)
                writer.writeheader()
                writer.writerows(cpu_rows)
        if latency is not None:
            p50, p99 = latency
            (self.dir / f"latency-{self.cond}-{round_id}.json").write_text(json.dumps(
                {"count": hist_count, "p50Nanos": p50 * 1000, "p95Nanos": p99 * 500, "p99Nanos": p99 * 1000,
                 "maxNanos": p99 * 2000}))

    def write(self):
        if self.rates:
            with open(self.dir / f"rates-{self.cond}.csv", "w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["round", "rate", "n"])
                writer.writerows(self.rates)
        with open(self.dir / f"rounds-{self.cond}.csv", "w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(ROUNDS_HEADER)
            writer.writerows(self.rounds)
        with open(self.dir / f"saturation-{self.cond}.csv", "w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(SAT_HEADER)
            writer.writerows(self.saturation)


def make_run(tmp, builders):
    """builders: [(세트 이름, 조건, 함수(SetFixture))]. 보고서 텍스트를 돌려준다."""
    run_dir = Path(tmp) / "run"
    for name, cond, build in builders:
        fixture = SetFixture(run_dir, name, cond)
        build(fixture)
        fixture.write()
    return run_dir


def v2_set(rounds=5, f_error=0.0, **kwargs):  # kwargs: counts, small, g_mean
    def build(fixture):
        fixture.add_round("r01", valid=0, flags="WARMUP", stages=v2_stage_files(0.5), latency=(2000, 8000))
        for number in range(2, rounds + 2):
            fixture.add_round(f"r{number:02d}", stages=v2_stage_files(f_error, **kwargs), latency=(2000, 8000))
    return build


def resource_rows(with_memory=True):
    """창(1000~1052) 안 샘플 두 개와 창 밖 샘플 하나. 첫 샘플 빈 값은 제외돼야 한다."""
    def row(ts, api, worker, pg, kafka, rss_api, rss_worker, pg_mem, kafka_mem):
        base = {"ts": ts, "jvm_api": api, "jvm_account-worker": worker, "pg_cpu": pg, "kafka_cpu": kafka}
        if with_memory:
            base.update({"rss_api": rss_api, "rss_account-worker": rss_worker, "pg_mem": pg_mem, "kafka_mem": kafka_mem})
        return base
    return [row(999, 900, 900, 90, 90, 9000, 9000, 900, 900),
            row(1000, "", "", 10, 20, "", "", 30, 400),
            row(1010, 50, 100, 10, 20, 100, 200, 30, 400),
            row(1020, 70, 120, 10, 40, 120, 220, 30, 400)]


class ResourceSectionTest(unittest.TestCase):
    def run_text(self, with_memory=True, with_cpu=True):
        def build(fixture):
            fixture.add_round("r01", valid=0, flags="WARMUP", stages=v2_stage_files(0.0), latency=(2000, 8000))
            for number in range(2, 5):
                fixture.add_round(f"r{number:02d}", stages=v2_stage_files(0.0), latency=(2000, 8000),
                                  cpu_rows=resource_rows(with_memory) if with_cpu else None)
        with tempfile.TemporaryDirectory() as tmp:
            return asg.build_segments_report(make_run(tmp, [("01-S1-v2-st1", "v2", build)]))

    def test_cpu_in_cores_over_window_samples_only(self):
        text = self.run_text()
        self.assertIn("## 자원 사용", text)
        # 창 밖 샘플(ts 999)과 빈 값 샘플은 빠지고 50/70 → 평균 0.60코어, 최대 0.70코어
        self.assertIn("| api | 0.60 | 0.60~0.60 | 0.70 | 110.0 | 120.0 |", text)
        self.assertIn("| account-worker | 1.10 | 1.10~1.10 | 1.20 | 210.0 | 220.0 |", text)

    def test_container_rows_and_sums(self):
        text = self.run_text()
        # kafka CPU는 창 안 세 샘플(20, 20, 40)의 평균 26.7% = 0.27코어, 최대 0.40코어
        self.assertIn("| postgres(컨테이너) | 0.10 | 0.10~0.10 | 0.10 | 30.0 | 30.0 |", text)
        self.assertIn("| kafka(컨테이너) | 0.27 | 0.27~0.27 | 0.40 | 400.0 | 400.0 |", text)
        # JVM 합계: (150+190)/2=1.70 평균, 최대 1.90. RSS 합계: (300+340)/2=320.0 평균, 최대 340.0
        self.assertIn("| 앱 JVM 합계 | 1.70 | 1.70~1.70 | 1.90 | 320.0 | 340.0 |", text)
        # 전체 합계: 첫 샘플 1.80, 둘째 2.40 → 평균 2.10, 최대 2.40. 메모리: 300+30+400=730, 340+30+400=770 → 평균 750.0, 최대 770.0
        self.assertIn("| 앱 JVM + Postgres + Kafka 합계 | 2.10 | 2.10~2.10 | 2.40 | 750.0 | 770.0 |", text)

    def test_old_csv_without_memory_columns_shows_dash(self):
        text = self.run_text(with_memory=False)
        self.assertIn("| api | 0.60 | 0.60~0.60 | 0.70 | — | — |", text)

    def test_set_without_cpu_files_is_noted(self):
        text = self.run_text(with_cpu=False)
        self.assertIn("## 자원 사용", text)
        self.assertIn("CPU 기록 파일이 없다", text)


def ladder_set(plan):
    """plan: [(회차, 속도, N, 포화 이유, G p99 µs, F p50 µs)]. v1 세트 모양(api 의 G, settlement-engine 의 F)."""
    def build(fixture):
        for round_id, rate, n, reasons, g_p99, f_p50 in plan:
            stages = {"api": {"G": {"count": n, "p50Micros": 10, "p95Micros": 20, "p99Micros": g_p99, "maxMicros": g_p99 * 2, "meanMicros": 12.0}},
                      "settlement-engine": {"F": {"count": n // 2, "p50Micros": f_p50, "p95Micros": f_p50 * 2, "p99Micros": f_p50 * 3,
                                                   "maxMicros": f_p50 * 5, "meanMicros": float(f_p50)}}}
            fixture.add_round(round_id, valid=0 if round_id == "r01" else 1, flags="WARMUP" if round_id == "r01" else "",
                              reasons=reasons, stages=stages, latency=(3000, 7000), hist_count=n)
            fixture.rates.append([round_id, rate, n])
    return build


class LadderSearchTest(unittest.TestCase):
    def report(self, plan):
        with tempfile.TemporaryDirectory() as tmp:
            return asg.build_segments_report(make_run(tmp, [("01-L-v1idx-st1", "v1", ladder_set(plan))]))

    PLAN = [("r01", 192, 3840, "k6_dropped", 500, 3000),     # 워밍업의 포화 신호는 판정에서 뺀다
            ("r02", 300, 6000, "none", 400, 3000),
            ("r03", 700, 14000, "none", 900, 3500),
            ("r04", 1000, 20000, "k6_dropped", 1200, 4000),
            ("r05", 1500, 30000, "none", 5000, 4500)]

    def test_set_name_with_ladder_step_is_accepted(self):
        parsed = asg.parse_set_name("01-L-v1idx-st1")
        self.assertEqual(("L", "v1idx", 1), (parsed.step, parsed.label, parsed.stages))

    def test_s5x_set_name_is_accepted_at_4000_per_second(self):
        parsed = asg.parse_set_name("02-S5X-v2-st1")
        self.assertEqual(("S5X", "v2"), (parsed.step, parsed.label))
        self.assertEqual(4000, asg.STEP_RATES["S5X"])

    def test_table_has_one_row_per_round_with_rate(self):
        text = self.report(self.PLAN)
        self.assertIn("## 포화점 탐색", text)
        self.assertIn("| r02 | 300 | 6,000 | none | 400 | 3,000 | 아니오 | 정상 |", text)
        self.assertIn("| r04 | 1,000 | 20,000 | k6_dropped | 1,200 | 4,000 | 아니오 | 포화 |", text)

    def test_g_p99_at_or_above_f_p50_is_a_saturation_signal(self):
        text = self.report(self.PLAN)
        self.assertIn("| r05 | 1,500 | 30,000 | none | 5,000 | 4,500 | 예 | 포화 |", text)

    def test_first_saturated_rate_is_reported_and_warmup_is_ignored(self):
        text = self.report(self.PLAN)
        self.assertIn("처음 포화 신호가 나온 속도: 1,000건/초 (k6_dropped)", text)
        self.assertIn("마지막 정상 속도: 700건/초", text)
        self.assertIn("| r01 | 192 | 3,840 | k6_dropped | 500 | 3,000 | 아니오 | 워밍업 |", text)

    def test_no_saturation_says_so_with_highest_rate(self):
        plan = [("r01", 192, 3840, "none", 100, 3000), ("r02", 300, 6000, "none", 100, 3000), ("r03", 500, 10000, "none", 100, 3000)]
        text = self.report(plan)
        self.assertIn("포화 신호가 나온 속도가 없다 (탐색한 가장 높은 속도 500건/초)", text)

    def test_ladder_set_is_kept_out_of_stage_table_and_step_comparison(self):
        text = self.report(self.PLAN)
        self.assertIn("계단 세트는 회차마다 속도가 달라 단계 표를 집계하지 않는다", text)
        self.assertIn("속도 단계 세트가 없다", text)  # 계단 세트는 속도 단계 비교 표에 들어가지 않는다


class SetNameTest(unittest.TestCase):
    def test_parses_set_folder_name(self):
        parsed = asg.parse_set_name("03-S3-v1idx-st1")
        self.assertEqual((parsed.index, parsed.step, parsed.label, parsed.cond, parsed.stages), (3, "S3", "v1idx", "v1", 1))
        parsed = asg.parse_set_name("07-I450-v2-st0")
        self.assertEqual((parsed.step, parsed.cond, parsed.stages), ("I450", "v2", 0))

    def test_rejects_other_names(self):
        self.assertIsNone(asg.parse_set_name("v1"))
        self.assertIsNone(asg.parse_set_name("01-S9-v1-st1"))
        self.assertIsNone(asg.parse_set_name("01-S1-v1-st1"))  # 인덱스 없는 v1은 측정 조건이 아니다


class ResidualTest(unittest.TestCase):
    def test_small_residual_holds(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set(f_error=0.005))])
            text = asg.build_segments_report(run)
        self.assertIn("합 정합성", text)
        self.assertNotIn("합 정합성 불성립", text)
        self.assertIn("p99는 구간별로 더할 수 없어", text)

    def test_residual_over_one_percent_marks_set_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set(f_error=0.02))])
            text = asg.build_segments_report(run)
        self.assertIn("합 정합성 불성립", text)

    def test_exactly_one_percent_holds(self):
        # 경계: 잔차 비율이 정확히 1%면 불성립이 아니다(1% 초과만 불성립)
        ratio = asg.residual_ratio(100.0, [50.0, 49.0])
        self.assertAlmostEqual(ratio, 0.01)
        self.assertFalse(asg.residual_fails(ratio))
        self.assertTrue(asg.residual_fails(0.0101))

    def test_sample_count_mismatch_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set(counts={"F": 9000}))])
            text = asg.build_segments_report(run)
        self.assertIn("표본 수 불일치", text)

    def test_uninstrumented_set_has_no_residual_row(self):
        def build(fixture):
            for number in range(1, 4):
                fixture.add_round(f"r{number:02d}", latency=(2000, 8000))
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1X-v2-st0", "v2", build)])
            text = asg.build_segments_report(run)
        self.assertIn("단계 계측이 꺼진 세트", text)


class ResolutionTest(unittest.TestCase):
    def test_stage_under_5us_gets_footnote_and_no_p99(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set())])
            text = asg.build_segments_report(run)
        ring_row = next(line for line in text.splitlines() if line.startswith("| ring_publish"))
        self.assertIn("해상도 미만(1µs)", ring_row)
        self.assertIn("—", ring_row)
        self.assertNotIn("6", ring_row.split("|")[7])  # p99 칸(3 x 2µs = 6)에 값이 없다
        hop_row = next(line for line in text.splitlines() if line.startswith("| hop_to_journal"))
        self.assertNotIn("해상도 미만", hop_row)
        self.assertIn("270", hop_row)

    def test_threshold_is_5us(self):
        self.assertTrue(asg.below_resolution(4.9))
        self.assertFalse(asg.below_resolution(5.0))


class SaturationTest(unittest.TestCase):
    def build_mixed(self):
        def build(fixture):
            fixture.add_round("r01", valid=0, flags="WARMUP", stages=v2_stage_files())
            fixture.add_round("r02", stages=v2_stage_files())
            fixture.add_round("r03", reasons="k6_dropped,lag", stages=v2_stage_files(f_error=0.5))
            fixture.add_round("r04", reasons="drain", stages=v2_stage_files(f_error=0.5))
            fixture.add_round("r05", reasons="latency_growth", stages=v2_stage_files(f_error=0.5))
        return build

    def test_saturated_rounds_are_excluded_from_stage_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", self.build_mixed())])
            text = asg.build_segments_report(run)
        f_row = next(line for line in text.splitlines() if line.startswith("| F "))
        self.assertIn("| 1 |", f_row)  # 집계에 쓴 회차 1개
        self.assertNotIn("합 정합성 불성립", text)  # 포화 회차(오차 큰 값)는 합 정합성에도 쓰지 않는다

    def test_ladder_marks_saturated_step_and_reasons(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", self.build_mixed())])
            text = asg.build_segments_report(run)
        self.assertIn("포화한 단계", text)
        self.assertIn("k6_dropped", text)
        self.assertIn("latency_growth", text)
        self.assertIn("대표값 인용 대상에서 제외", text)

    def test_unsaturated_step_is_not_marked(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set())])
            text = asg.build_segments_report(run)
        self.assertNotIn("포화한 단계 (", text)

    def test_step_saturated_rule_is_majority_of_measured_rounds(self):
        self.assertTrue(asg.step_saturated(3, 5))
        self.assertFalse(asg.step_saturated(2, 4))
        self.assertTrue(asg.step_saturated(3, 4))


class DuplicateWarningTest(unittest.TestCase):
    def test_warns_when_e_or_f_count_exceeds_d(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set(counts={"E": 10005, "F": 10005}))])
            text = asg.build_segments_report(run)
        self.assertIn("중복 재전달", text)
        self.assertIn("E 10005 > D 10000", text)

    def test_no_warning_when_counts_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set())])
            text = asg.build_segments_report(run)
        self.assertNotIn("중복 재전달 가능성", text)

    def test_smaller_counts_are_not_a_duplicate_warning(self):
        warnings = asg.duplicate_warnings({"D": {"count": 100}, "E": {"count": 90}, "F": {"count": 100}})
        self.assertEqual(warnings, [])


class ImpactTest(unittest.TestCase):
    OFF50 = [2000, 2100, 1900, 2050, 1950]   # 최소 1900, 최대 2100, 중앙값 2000
    OFF99 = [8000, 8400, 7600, 8200, 7800]   # 최소 7600, 최대 8400, 중앙값 8000

    def verdict(self, on50, on99):
        return asg.impact_verdict(self.OFF50, self.OFF99, on50, on99)

    def test_inside_and_close_means_no_visible_effect(self):
        self.assertEqual(self.verdict(2010, 8050), "영향 안 보임")

    def test_both_outside_means_possible_effect(self):
        self.assertEqual(self.verdict(2500, 9500), "영향 가능성 있음")

    def test_p50_inside_p99_outside_is_ambiguous(self):
        self.assertEqual(self.verdict(2010, 9500), "애매함")

    def test_inside_but_far_from_off_median_is_ambiguous(self):
        # 범위 폭 200의 50%는 100. 2100은 안이지만 끈 중앙값(2000)과 차이가 100이라 경계(초과 아님) → 영향 안 보임
        self.assertEqual(self.verdict(2100, 8000), "영향 안 보임")
        # 2101 은 범위 밖
        self.assertEqual(self.verdict(2101, 9000), "영향 가능성 있음")
        # 안이지만 차이 > 50%: 끈 범위 1900~2100, 중앙값 2000 → 차이 > 100 은 안에 있을 수 없으므로 비대칭 분포로 만든다
        off = [1000, 1000, 1000, 1000, 2000]   # 중앙값 1000, 범위 폭 1000
        self.assertEqual(asg.impact_verdict(off, off, 1600, 1000), "애매함")

    def test_boundary_values_are_inside(self):
        self.assertEqual(self.verdict(2100, 8400), "영향 안 보임" if abs(2100 - 2000) <= 100 and abs(8400 - 8000) <= 400 else "애매함")

    def test_report_has_impact_section_from_s1x_sets(self):
        def build_off(fixture):
            for number, p50 in enumerate(self.OFF50, start=1):
                fixture.add_round(f"r{number:02d}", valid=0 if number == 1 else 1, flags="WARMUP" if number == 1 else "",
                                  latency=(p50, self.OFF99[number - 1]))

        def build_on(fixture):
            for number in range(1, 6):
                fixture.add_round(f"r{number:02d}", valid=0 if number == 1 else 1, flags="WARMUP" if number == 1 else "",
                                  stages=v2_stage_files(), latency=(2500, 9500))
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1X-v2-st0", "v2", build_off), ("02-S1X-v2-st1", "v2", build_on)])
            text = asg.build_segments_report(run)
        self.assertIn("계측 켠/끈 영향", text)
        self.assertIn("영향 가능성 있음", text)


class GapStageTest(unittest.TestCase):
    def test_g_row_is_in_stage_table_with_definition(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set())])
            text = asg.build_segments_report(run)
        g_row = next(line for line in text.splitlines() if line.startswith("| G |"))
        self.assertIn("api 입구", g_row)

    def test_ladder_shows_g_p99_and_no_promotion_note_when_small(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set())])
            text = asg.build_segments_report(run)
        self.assertIn("G p99 µs", text)
        self.assertNotIn("승급 신호: G p99 ≥ F p50", text)

    def test_ladder_notes_promotion_signal_when_g_p99_reaches_f_p50(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set(g_mean=400))])  # G p99 = 1200 >= F p50 = 232
            text = asg.build_segments_report(run)
        self.assertIn("승급 신호: G p99 ≥ F p50", text)
        self.assertIn("01-S1-v2-st1".split("-")[1], text)

    def test_g_signal_function_boundary(self):
        self.assertTrue(asg.g_promotion_signal(232.0, 232.0))
        self.assertFalse(asg.g_promotion_signal(231.9, 232.0))
        self.assertFalse(asg.g_promotion_signal(None, 232.0))
        self.assertFalse(asg.g_promotion_signal(100.0, None))

    def test_v1_expects_api_dump_too(self):
        def build(fixture):
            fixture.add_round("r01", valid=0, flags="WARMUP", stages={"api": {"G": stage(10)}, "order-engine": {"B": stage(1)},
                                                                        "matching-engine": {"C": stage(1)},
                                                                        "settlement-engine": {"F": stage(5)}})
            fixture.add_round("r02", stages={"order-engine": {"B": stage(1)}, "matching-engine": {"C": stage(1)},
                                             "settlement-engine": {"F": stage(5)}})
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v1idx-st1", "v1", build)])
            text = asg.build_segments_report(run)
        self.assertIn("api r02", text)


class RehearsalImpactTest(unittest.TestCase):
    def test_s1r_sets_also_produce_the_impact_section(self):
        def build_off(fixture):
            for number in range(1, 4):
                fixture.add_round(f"r{number:02d}", valid=0 if number == 1 else 1, flags="WARMUP" if number == 1 else "",
                                  latency=(2000, 8000))

        def build_on(fixture):
            for number in range(1, 4):
                fixture.add_round(f"r{number:02d}", valid=0 if number == 1 else 1, flags="WARMUP" if number == 1 else "",
                                  stages=v2_stage_files(), latency=(2000, 8000))
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1R-v2-st0", "v2", build_off), ("02-S1R-v2-st1", "v2", build_on)])
            text = asg.build_segments_report(run)
        self.assertIn("영향 안 보임", text)
        self.assertIn("리허설(S1R)은 회차가 적어 대략 비교로만 본다", text)


class LadderTest(unittest.TestCase):
    def test_ladder_compares_v1idx_and_v2_side_by_side(self):
        def v1_set(fixture):
            fixture.add_round("r01", valid=0, flags="WARMUP", stages=None, latency=(9000, 30000))
            for number in range(2, 5):
                fixture.add_round(f"r{number:02d}", stages={"order-engine": {"B": stage(4000), "A": stage(900)},
                                                            "settlement-engine": {"F": stage(20000), "E": stage(8000)},
                                                            "matching-engine": {}},
                                  latency=(4000, 12000))
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v1idx-st1", "v1", v1_set), ("02-S1-v2-st1", "v2", v2_set())])
            text = asg.build_segments_report(run)
        self.assertIn("속도 단계별 비교", text)
        self.assertIn("v1(인덱스 있음)", text)
        self.assertIn("v2", text)
        self.assertIn("범위 겹침", text)

    def test_never_writes_forbidden_wording(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set())])
            text = asg.build_segments_report(run)
        for word in ("신뢰구간", "p값", "안정적", "병목"):
            self.assertNotIn(word, text)


class TableShapeTest(unittest.TestCase):
    def test_stage_table_header_and_reading_lines(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set())])
            text = asg.build_segments_report(run)
        header = next(line for line in text.splitlines() if line.startswith("| 단계 |"))
        for column in ("정의", "집계 회차", "count", "p50", "p95", "p99", "max", "mean"):
            self.assertIn(column, header)
        self.assertIn("같은 정의인 단계끼리만 직접 비교한다", text)
        self.assertIn("구조 차이이지 속도 차이가 아니다", text)

    def test_definition_column_uses_section_2_3a(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set())])
            text = asg.build_segments_report(run)
        b_row = next(line for line in text.splitlines() if line.startswith("| B |"))
        self.assertIn("같은 정의", b_row)
        d_row = next(line for line in text.splitlines() if line.startswith("| D |"))
        self.assertIn("다른 정의", d_row)

    def test_correspondence_table_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set())])
            text = asg.build_segments_report(run)
        self.assertIn("두 구현의 단계 대응", text)
        self.assertIn("v1에만 있는 단계", text)

    def test_correspondence_has_no_unimplemented_fwd_stage(self):
        # fwd_queue·fwd_offer(계좌 → 매칭 발신 큐)는 구현하지 않기로 했다. 대응표와 선언 문서에 남아 있으면 안 된다.
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set())])
            text = asg.build_segments_report(run)
        self.assertNotIn("fwd", text)
        self.assertIn("계좌 → 매칭 방향 발신 큐는 따로 측정하지 않고 C 단계에 포함된다", text)
        protocol = Path(__file__).with_name("measure-protocol-c.md").read_text(encoding="utf-8")
        self.assertNotIn("fwd", protocol)
        self.assertIn("계좌 → 매칭 방향 발신 큐는 따로 측정하지 않고 C 단계에 포함된다", protocol)

    def test_report_has_no_index_less_v1_condition(self):
        def v1_set(fixture):
            for number in range(1, 4):
                fixture.add_round(f"r{number:02d}", valid=0 if number == 1 else 1, flags="WARMUP" if number == 1 else "",
                                  stages={"order-engine": {"B": stage(4000), "A": stage(900)}}, latency=(4000, 12000))
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v1idx-st1", "v1", v1_set), ("02-S1-v2-st1", "v2", v2_set())])
            text = asg.build_segments_report(run)
        self.assertNotIn("인덱스 없", text)
        self.assertNotIn("v1(인덱스 없음)", text)

    def test_warmup_round_is_not_in_stage_values(self):
        def build(fixture):
            fixture.add_round("r01", valid=0, flags="WARMUP", stages={"account-worker": {"F": stage(99999)}})
            for number in range(2, 5):
                fixture.add_round(f"r{number:02d}", stages={"account-worker": {"F": stage(500)}})
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", build)])
            text = asg.build_segments_report(run)
        self.assertNotIn("99999", text)


class MissingInputTest(unittest.TestCase):
    def test_missing_process_dump_is_listed_and_does_not_crash(self):
        def build(fixture):
            fixture.add_round("r01", valid=0, flags="WARMUP", stages=v2_stage_files())
            fixture.add_round("r02", stages=v2_stage_files())
            fixture.add_round("r03", stages=v2_stage_files(), skip_processes=("matching-worker",))
            fixture.add_round("r04", stages=v2_stage_files())
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", build)])
            text = asg.build_segments_report(run)
        self.assertIn("입력 누락", text)
        self.assertIn("matching-worker r03", text)

    def test_set_without_any_dump_says_so(self):
        def build(fixture):
            for number in range(1, 4):
                fixture.add_round(f"r{number:02d}")
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", build)])
            text = asg.build_segments_report(run)
        self.assertIn("단계 덤프 없음", text)

    def test_empty_run_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            text = asg.build_segments_report(Path(tmp))
        self.assertIn("세트 폴더가 없다", text)


class CommandTest(unittest.TestCase):
    def test_compare_segments_command_writes_file_and_keeps_old_commands(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(tmp, [("01-S1-v2-st1", "v2", v2_set())])
            out = Path(tmp) / "analysis-segments.md"
            status = ab.main(["compare-segments", str(run), "--out", str(out)])
            self.assertEqual(status, 0)
            self.assertIn("구간 분해 측정 분석", out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
