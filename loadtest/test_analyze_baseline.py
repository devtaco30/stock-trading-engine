#!/usr/bin/env python3
"""analyze-baseline.py 단위 테스트. 합성 데이터(latency-*.json, k6-*.json, cpu-*.csv, rounds-*.csv)만 쓴다.

실행: python3 -m unittest loadtest/test_analyze_baseline.py -v   (저장소 루트에서)
"""
import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("analyze-baseline.py")
spec = importlib.util.spec_from_file_location("analyze_baseline", MODULE_PATH)
ab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ab)

K6_START = 1000
K6_END = 1058
CPU_HEADER = ["ts", "k6_cpu", "jvm_order-engine", "pg_cpu", "kafka_cpu", "idle", "user", "sys", "top5", "batt",
              "therm_limit"]


def cpu_row(ts, idle=60.0, k6=10.0, top5="java:50|k6:10", batt="AC", therm="100"):
    return [ts, k6, 30.0, 5.0, 5.0, idle, 20.0, 20.0, top5, batt, therm]


def healthy_cpu(count=12):
    return [cpu_row(K6_START + 5 * index) for index in range(count)]


class Fixture:
    """조건 하나(v1 또는 v2)의 결과 디렉터리를 합성한다."""

    def __init__(self, root: Path, cond: str):
        self.cond = cond
        self.dir = root / cond
        self.dir.mkdir(parents=True)
        self.rounds = []

    def add_round(self, round_id, p50=3_000_000, p95=5_000_000, p99=8_000_000, max_nanos=20_000_000,
                  hist_count=10000, k6_exit=0, checks_fails=0, http_failed=0, dropped=0, db_delta=None,
                  flags="", cpu=None, k6_summary=True):
        cond = self.cond
        if hist_count is not None:
            (self.dir / f"latency-{cond}-{round_id}.json").write_text(json.dumps(
                {"count": hist_count, "p50Nanos": p50, "p95Nanos": p95, "p99Nanos": p99, "maxNanos": max_nanos}))
        if k6_summary:
            (self.dir / f"k6-{cond}-{round_id}.json").write_text(json.dumps({"metrics": {
                "checks": {"passes": 10000 - checks_fails, "fails": checks_fails, "value": 1.0},
                "http_req_failed": {"passes": http_failed, "fails": 10000 - http_failed, "value": 0.0},
                "dropped_iterations": {"count": dropped, "rate": 0.0},
                "http_reqs": {"count": 10000, "rate": 192.0},
            }}))
        rows = cpu if cpu is not None else healthy_cpu()
        with open(self.dir / f"cpu-{cond}-{round_id}.csv", "w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(CPU_HEADER)
            writer.writerows(rows)
        self.rounds.append([round_id, K6_START, K6_END, k6_exit, "" if db_delta is None else db_delta, flags])

    def write_rounds(self):
        with open(self.dir / f"rounds-{self.cond}.csv", "w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["round", "k6_start", "k6_end", "k6_exit", "db_delta", "flags"])
            writer.writerows(self.rounds)


def full_condition(root, cond, db_delta=None, **overrides):
    fixture = Fixture(root, cond)
    for number in range(1, 12):
        fixture.add_round(f"r{number:02d}", db_delta=db_delta, **overrides)
    fixture.write_rounds()
    return fixture


def judged(root, cond, round_id):
    rounds = ab.load_condition(Path(root) / cond, cond)
    return next(r for r in rounds if r.round_id == round_id)


class ExclusionRuleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def condition_with(self, round_id, cond="v1", db_delta=None, **kwargs):
        fixture = Fixture(self.root, cond)
        for number in range(1, 12):
            current = f"r{number:02d}"
            if current == round_id:
                fixture.add_round(current, db_delta=db_delta if db_delta is not None else (10000 if cond == "v1" else None), **kwargs)
            else:
                fixture.add_round(current, db_delta=10000 if cond == "v1" else None)
        fixture.write_rounds()
        return judged(self.root, cond, round_id)

    def test_정상_회차는_제외되지_않는다(self):
        result = self.condition_with("r05")
        self.assertEqual([], result.reasons)
        self.assertTrue(result.valid)

    def test_첫_회차는_값과_무관하게_워밍업으로_제외된다(self):
        result = self.condition_with("r01")
        self.assertEqual(["WARMUP"], result.reasons)
        self.assertFalse(result.valid)

    def test_값이_커도_제외하지_않는다(self):
        result = self.condition_with("r06", p50=900_000_000, p99=5_000_000_000, max_nanos=9_000_000_000)
        self.assertEqual([], result.reasons)
        self.assertTrue(result.valid)

    def test_히스토그램_건수가_1만이_아니면_건수_불일치로_제외한다(self):
        result = self.condition_with("r04", hist_count=9999)
        self.assertEqual(["COUNT"], result.reasons)

    def test_스냅샷_파일이_없으면_건수_불일치로_제외한다(self):
        result = self.condition_with("r04", hist_count=None)
        self.assertIn("COUNT", result.reasons)

    def test_v1_DB_증가분이_1만이_아니면_건수_불일치로_제외한다(self):
        result = self.condition_with("r04", db_delta=10001)
        self.assertEqual(["COUNT"], result.reasons)

    def test_v2는_DB_증가분이_비어_있어도_제외하지_않는다(self):
        result = self.condition_with("r04", cond="v2")
        self.assertEqual([], result.reasons)

    def test_E1_idle이_30_미만인_샘플이_연속_2개면_제외한다(self):
        cpu = healthy_cpu()
        cpu[3][5] = 25.0
        cpu[4][5] = 29.9
        result = self.condition_with("r05", cpu=cpu)
        self.assertEqual(["E1"], result.reasons)

    def test_E1_idle이_낮은_샘플이_하나뿐이거나_떨어져_있으면_제외하지_않는다(self):
        cpu = healthy_cpu()
        cpu[3][5] = 10.0
        cpu[5][5] = 10.0
        result = self.condition_with("r05", cpu=cpu)
        self.assertEqual([], result.reasons)

    def test_E1_idle이_정확히_30이면_포화가_아니다(self):
        cpu = healthy_cpu()
        cpu[3][5] = 30.0
        cpu[4][5] = 30.0
        result = self.condition_with("r05", cpu=cpu)
        self.assertEqual([], result.reasons)

    def test_E1_k6_구간_밖_샘플은_판정하지_않는다(self):
        cpu = healthy_cpu() + [cpu_row(K6_END + 5, idle=5.0), cpu_row(K6_END + 10, idle=5.0),
                               cpu_row(K6_START - 10, idle=5.0), cpu_row(K6_START - 5, idle=5.0)]
        result = self.condition_with("r05", cpu=cpu)
        self.assertEqual([], result.reasons)

    def test_E2_k6_CPU가_40_이상인_샘플이_연속_2개면_제외한다(self):
        cpu = healthy_cpu()
        cpu[2][1] = 40.0
        cpu[3][1] = 55.0
        result = self.condition_with("r05", cpu=cpu)
        self.assertEqual(["E2"], result.reasons)

    def test_E2_39_9는_통과한다(self):
        cpu = healthy_cpu()
        cpu[2][1] = 39.9
        cpu[3][1] = 39.9
        result = self.condition_with("r05", cpu=cpu)
        self.assertEqual([], result.reasons)

    def test_E3_배터리_샘플이_하나라도_있으면_제외한다(self):
        cpu = healthy_cpu()
        cpu[6][9] = "Battery"
        result = self.condition_with("r05", cpu=cpu)
        self.assertEqual(["E3"], result.reasons)

    def test_E3_CPU_속도_제한이_100_미만이면_제외한다(self):
        cpu = healthy_cpu()
        cpu[6][10] = "90"
        result = self.condition_with("r05", cpu=cpu)
        self.assertEqual(["E3"], result.reasons)

    def test_E3_속도_제한_값이_없음이면_제외하지_않는다(self):
        cpu = healthy_cpu()
        cpu[6][10] = "NA"
        result = self.condition_with("r05", cpu=cpu)
        self.assertEqual([], result.reasons)

    def test_E4_허용_목록에_없는_프로세스가_25_이상으로_연속_2개면_제외한다(self):
        cpu = healthy_cpu()
        cpu[2][8] = "Chrome:30.0|java:50|k6:10"
        cpu[3][8] = "Chrome:26.0|java:50|k6:10"
        result = self.condition_with("r05", cpu=cpu)
        self.assertEqual(["E4"], result.reasons)

    def test_E4_허용_목록_프로세스는_높아도_제외하지_않는다(self):
        cpu = healthy_cpu()
        cpu[2][8] = "java:300|k6:90|kernel_task:80|com.docker.backend:70"
        cpu[3][8] = "java:300|k6:90|kernel_task:80|com.docker.backend:70"
        result = self.condition_with("r05", cpu=cpu)
        self.assertEqual([], result.reasons)

    def test_E4_top이_잘라낸_Docker_VM_이름도_허용한다(self):
        cpu = healthy_cpu()
        cpu[2][8] = "com.apple.Virtua:105.0|com.docker.backe:30.0"
        cpu[3][8] = "com.apple.Virtua:107.0|com.docker.backe:31.0"
        result = self.condition_with("r05", cpu=cpu)
        self.assertEqual([], result.reasons)

    def test_E4_허용_목록에_없는_프로세스가_연속이_아니면_제외하지_않는다(self):
        cpu = healthy_cpu()
        cpu[2][8] = "Chrome:30.0|java:50"
        cpu[4][8] = "Chrome:30.0|java:50"
        result = self.condition_with("r05", cpu=cpu)
        self.assertEqual([], result.reasons)

    def test_E5_k6_종료_코드가_0이_아니면_제외한다(self):
        result = self.condition_with("r05", k6_exit=99)
        self.assertEqual(["E5"], result.reasons)

    def test_E5_checks_실패가_있으면_제외한다(self):
        result = self.condition_with("r05", checks_fails=1)
        self.assertEqual(["E5"], result.reasons)

    def test_E5_http_req_failed_발생이_있으면_제외한다(self):
        result = self.condition_with("r05", http_failed=1)
        self.assertEqual(["E5"], result.reasons)

    def test_E5_dropped_iterations가_있으면_제외한다(self):
        result = self.condition_with("r05", dropped=1)
        self.assertEqual(["E5"], result.reasons)

    def test_E5_k6_요약이_없으면_제외한다(self):
        result = self.condition_with("r05", k6_summary=False)
        self.assertEqual(["E5"], result.reasons)

    def test_E6_회차_기록의_재배정_표시는_제외_사유다(self):
        result = self.condition_with("r05", flags="E6")
        self.assertEqual(["E6"], result.reasons)

    def test_여러_사유가_겹치면_모두_기록한다(self):
        cpu = healthy_cpu()
        cpu[6][9] = "Battery"
        result = self.condition_with("r01", cpu=cpu, hist_count=9000)
        self.assertEqual(["WARMUP", "E3", "COUNT"], result.reasons)


class ConditionValidityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def condition_with_invalid(self, invalid_count):
        fixture = Fixture(self.root, "v2")
        for number in range(1, 12):
            round_id = f"r{number:02d}"
            broken = 2 <= number < 2 + invalid_count
            fixture.add_round(round_id, hist_count=9000 if broken else 10000)
        fixture.write_rounds()
        return ab.load_condition(self.root / "v2", "v2")

    def test_유효_측정_회차가_7개면_조건이_성립한다(self):
        rounds = self.condition_with_invalid(3)
        self.assertEqual(7, ab.valid_measurement_count(rounds))
        self.assertTrue(ab.condition_holds(rounds))

    def test_유효_측정_회차가_6개면_조건이_불성립이다(self):
        rounds = self.condition_with_invalid(4)
        self.assertEqual(6, ab.valid_measurement_count(rounds))
        self.assertFalse(ab.condition_holds(rounds))

    def test_워밍업은_유효_회차_수에_들어가지_않는다(self):
        rounds = self.condition_with_invalid(0)
        self.assertEqual(10, ab.valid_measurement_count(rounds))


class SummaryAndReportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def build(self, v1_p50, v2_p50, v1_db=10000, extra_v1=None):
        for cond, base in (("v1", v1_p50), ("v2", v2_p50)):
            fixture = Fixture(self.root, cond)
            for number in range(1, 12):
                round_id = f"r{number:02d}"
                kwargs = {"p50": base + number * 1_000_000, "p95": base * 2, "p99": base * 3, "max_nanos": base * 5}
                if cond == "v1":
                    kwargs["db_delta"] = v1_db
                if cond == "v1" and extra_v1 and round_id in extra_v1:
                    kwargs.update(extra_v1[round_id])
                fixture.add_round(round_id, **kwargs)
            fixture.write_rounds()

    def test_중앙값과_범위는_유효_측정_회차만으로_계산한다(self):
        self.build(10_000_000, 2_000_000, extra_v1={"r03": {"hist_count": 9000, "p50": 999_000_000}})
        rounds = ab.load_condition(self.root / "v1", "v1")
        summary = ab.summarize(rounds, "p50Nanos", only_valid=True)
        self.assertEqual(9, summary.n)
        self.assertEqual(round(summary.median_ms, 2), 17.0)   # r02,r04..r11 -> 12,14..21 의 중앙값(9개 중 5번째)
        self.assertEqual((12.0, 21.0), (round(summary.min_ms, 2), round(summary.max_ms, 2)))

    def test_전체_11회_요약은_제외된_회차도_포함한다(self):
        self.build(10_000_000, 2_000_000, extra_v1={"r03": {"hist_count": 9000, "p50": 999_000_000}})
        rounds = ab.load_condition(self.root / "v1", "v1")
        summary = ab.summarize(rounds, "p50Nanos", only_valid=False)
        self.assertEqual(11, summary.n)
        self.assertEqual(round(summary.max_ms, 2), 999.0)

    def test_단위는_밀리초이고_소수_둘째_자리까지_쓴다(self):
        self.assertEqual("11.50", ab.format_ms(11_501_567))
        self.assertEqual("466.62", ab.format_ms(466_616_319))

    def test_범위가_겹치지_않으면_겹치지_않았다고만_쓰고_중앙값_비율을_따로_쓴다(self):
        self.build(30_000_000, 2_000_000)
        report = ab.build_report(self.root)
        self.assertIn("겹치지 않았다", report)
        self.assertIn("중앙값 비율", report)

    def test_범위가_겹치면_겹쳤다고_쓴다(self):
        self.build(2_000_000, 2_000_000)
        report = ab.build_report(self.root)
        self.assertIn("겹쳤다", report)

    def test_보고서에_쓰지_않는_것은_출력하지_않는다(self):
        self.build(30_000_000, 2_000_000)
        report = ab.build_report(self.root)
        for forbidden in ("신뢰구간", "p값", "p 값", "안정적", "때문에 느렸"):
            self.assertNotIn(forbidden, report)

    def test_보고서에_회차별_표와_두_요약표와_추이와_CPU_표가_들어간다(self):
        self.build(30_000_000, 2_000_000)
        report = ab.build_report(self.root)
        for heading in ("회차별 표", "규칙을 적용한 요약", "11회 전체 요약", "회차 순서별 추이", "v1 대 v2",
                        "CPU"):
            self.assertIn(heading, report)

    def test_제외된_회차의_값도_사유_코드와_함께_표에_싣는다(self):
        self.build(10_000_000, 2_000_000, extra_v1={"r05": {"hist_count": 9000}})
        report = ab.build_report(self.root)
        self.assertIn("COUNT", report)
        self.assertIn("WARMUP", report)

    def test_불성립_조건이_있으면_비교하지_않고_불성립으로_표시한다(self):
        extra = {f"r{n:02d}": {"hist_count": 9000} for n in range(2, 7)}
        self.build(30_000_000, 2_000_000, extra_v1=extra)
        report = ab.build_report(self.root)
        self.assertIn("측정 불성립", report)
        self.assertNotIn("겹치지 않았다", report)

    def test_CPU_표는_k6_구간_안의_최댓값과_평균값을_쓴다(self):
        self.build(10_000_000, 2_000_000)
        cpu = healthy_cpu()
        cpu[1][5] = 50.0
        cpu[2][5] = 70.0
        cpu.append(cpu_row(K6_END + 100, idle=1.0))
        stats = ab.cpu_window_stats(
            [dict(zip(CPU_HEADER, map(str, row))) for row in cpu], K6_START, K6_END)
        self.assertEqual(70.0, stats["idle"].maximum)
        self.assertEqual(50.0, stats["idle"].minimum)
        self.assertAlmostEqual(60.0, stats["idle"].average)
        self.assertEqual(12, stats["idle"].count)


class JudgeCliTest(unittest.TestCase):
    def test_judge는_회차_기록_한_줄을_모든_열로_덧붙인다(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture = Fixture(root, "v1")
            fixture.add_round("r05", db_delta=10000)
            row = ab.judge_row(root / "v1", "v1", "r05", K6_START, K6_END, 0, 10000, e6=False)
            self.assertEqual(ab.ROUNDS_HEADER, list(row.keys()))
            self.assertEqual("r05", row["round"])
            self.assertEqual("", row["flags"])
            self.assertEqual("1", row["valid"])
            self.assertEqual("10000", row["hist_count"])

    def test_judge는_제외_사유를_flags에_쓰고_valid를_0으로_둔다(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture = Fixture(root, "v1")
            fixture.add_round("r05", db_delta=10000, hist_count=9000)
            row = ab.judge_row(root / "v1", "v1", "r05", K6_START, K6_END, 0, 10000, e6=True)
            self.assertEqual("E6 COUNT", row["flags"])
            self.assertEqual("0", row["valid"])


class SeedCheckTest(unittest.TestCase):
    """점검 8: 입력 앞 N건의 계좌별 필요량 x 회차 수가 시드 잔고·보유 안에 들어오는지."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def write_input(self, orders):
        path = self.root / "input.jsonl"
        path.write_text("\n".join(json.dumps(o) for o in orders) + "\n", encoding="utf-8")
        return path

    def write_seed(self, accounts):
        lines = ["account-worker:", "  seed-accounts:"]
        for account in accounts:
            lines.append(f"    - account-id: {account['id']}")
            lines.append(f"      balance: {account['balance']}")
            lines.append("      margin-rate: 1.00")
            if account.get("holdings"):
                lines.append("      holdings:")
                for stock, quantity in account["holdings"].items():
                    lines.append(f"        {stock}: {quantity}")
        path = self.root / "seed.yml"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def orders(self):
        return [
            {"accountId": 2001, "side": "BUY", "stockCode": "A1", "price": 100, "quantity": 2},
            {"accountId": 2001, "side": "BUY", "stockCode": "A1", "price": 50, "quantity": 2},
            {"accountId": 2051, "side": "SELL", "stockCode": "A1", "price": 100, "quantity": 3},
            {"accountId": 2051, "side": "SELL", "stockCode": "A1", "price": 100, "quantity": 1},
        ]

    def test_필요량의_회차_수배가_시드_이하면_문제가_없다(self):
        # 2001 필요량 = 100*2 + 50*2 = 300 -> x11 = 3300 / 2051 보유 필요량 = 4 -> x11 = 44
        problems = ab.check_seed(self.write_input(self.orders()),
                                 self.write_seed([{"id": 2001, "balance": 3300},
                                                  {"id": 2051, "balance": 0, "holdings": {"A1": 44}}]),
                                 count=4, rounds=11)
        self.assertEqual([], problems)

    def test_잔고가_1이라도_모자라면_그_계좌를_알린다(self):
        problems = ab.check_seed(self.write_input(self.orders()),
                                 self.write_seed([{"id": 2001, "balance": 3299},
                                                  {"id": 2051, "balance": 0, "holdings": {"A1": 44}}]),
                                 count=4, rounds=11)
        self.assertEqual(1, len(problems))
        self.assertIn("2001", problems[0])

    def test_보유_수량이_모자라면_계좌와_종목을_알린다(self):
        problems = ab.check_seed(self.write_input(self.orders()),
                                 self.write_seed([{"id": 2001, "balance": 3300},
                                                  {"id": 2051, "balance": 0, "holdings": {"A1": 43}}]),
                                 count=4, rounds=11)
        self.assertEqual(1, len(problems))
        self.assertIn("2051", problems[0])
        self.assertIn("A1", problems[0])

    def test_시드에_없는_계좌는_문제다(self):
        problems = ab.check_seed(self.write_input(self.orders()),
                                 self.write_seed([{"id": 2001, "balance": 3300}]), count=4, rounds=11)
        self.assertTrue(any("2051" in p for p in problems))

    def test_앞_N건만_본다(self):
        # count=2 면 매수 두 건만 본다. 매도 계좌는 시드에 없어도 된다
        problems = ab.check_seed(self.write_input(self.orders()),
                                 self.write_seed([{"id": 2001, "balance": 3300}]), count=2, rounds=11)
        self.assertEqual([], problems)

    def test_입력_파일이_없으면_문제로_알린다(self):
        problems = ab.check_seed(self.root / "none.jsonl", self.write_seed([]), count=4, rounds=11)
        self.assertEqual(1, len(problems))
        self.assertIn("generate-e2e-input.py", problems[0])


DBSTATS_HEADER = ["round", "phase", "orders_total", "orders_e2e", "pending_e2e", "filled_e2e", "unpaids_e2e",
                  "dead_orders", "orders_bytes", "q1_ms", "q2_ms", "qh_ms", "q1_plan", "q2_plan", "qh_plan",
                  "clear_ms", "deleted_orders"]


def write_dbstats(cond_dir, cond, rows):
    path = Path(cond_dir) / f"dbstats-{cond}.csv"
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=DBSTATS_HEADER)
        writer.writeheader()
        for row in rows:
            full = {key: "" for key in DBSTATS_HEADER}
            full.update(row)
            writer.writerow(full)


class CompareTest(unittest.TestCase):
    """여러 세트의 결과 디렉터리를 한 표에 나란히 놓는 compare 명령."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def make_set(self, name, base, invalid_rounds=(), with_dbstats=False):
        set_dir = self.root / name
        fixture = Fixture(set_dir, "v1")
        for number in range(1, 12):
            round_id = f"r{number:02d}"
            kwargs = {"p50": base + number * 1_000_000, "p99": base * 3 + number * 1_000_000, "db_delta": 10000}
            if round_id in invalid_rounds:
                kwargs["hist_count"] = 9000
            cpu = healthy_cpu()
            for row in cpu:
                row[3] = 100.0 + number          # pg_cpu
            fixture.add_round(round_id, cpu=cpu, **kwargs)
        fixture.write_rounds()
        if with_dbstats:
            rows = []
            for number in range(1, 12):
                rows.append({"round": f"r{number:02d}", "phase": "start", "orders_total": 54000 + number * 10000,
                             "orders_e2e": number * 10000, "pending_e2e": 5, "filled_e2e": 10, "dead_orders": 7,
                             "orders_bytes": 1048576 * number, "q1_ms": 0.25 * number, "q2_ms": 0.5, "qh_ms": 0.05,
                             "q1_plan": "Index Scan", "q2_plan": "Bitmap Heap Scan", "qh_plan": "Index Scan"})
            write_dbstats(set_dir / "v1", "v1", rows)
        return set_dir

    def entries(self, *specs):
        return [(label, str(path), "v1") for label, path in specs]

    def test_p50_추이는_회차별로_세트를_나란히_놓고_제외된_회차에_별표를_붙인다(self):
        a = self.make_set("a", 10_000_000, invalid_rounds=("r04",))
        b = self.make_set("b", 3_000_000)
        report = ab.build_compare(self.entries(("A", a), ("B", b)))
        self.assertIn("p50 추이", report)
        line = next(l for l in report.splitlines() if l.startswith("| r04 ") and "14.00*" in l)
        self.assertIn("7.00", line)

    def test_세트마다_성립_여부와_유효_회차_수를_쓴다(self):
        a = self.make_set("a", 10_000_000, invalid_rounds=("r02", "r03", "r04", "r05", "r06"))
        b = self.make_set("b", 3_000_000)
        report = ab.build_compare(self.entries(("A", a), ("B", b)))
        self.assertIn("A: 측정 불성립", report)
        self.assertIn("B: 성립", report)

    def test_구간_idle_최솟값과_postgres_평균_CPU_표가_있다(self):
        a = self.make_set("a", 10_000_000)
        report = ab.build_compare(self.entries(("A", a)))
        self.assertIn("구간 idle 최솟값", report)
        self.assertIn("postgres 평균 CPU", report)
        self.assertIn("111.0", report)   # r11의 pg_cpu 평균

    def test_dbstats가_있으면_테이블_크기와_쿼리_시간_표를_싣는다(self):
        a = self.make_set("a", 10_000_000, with_dbstats=True)
        report = ab.build_compare(self.entries(("A", a)))
        self.assertIn("회차 시작 시점의 DB 상태", report)
        self.assertIn("Index Scan", report)
        self.assertIn("2.75", report)    # r11 q1_ms = 0.25 * 11

    def test_dbstats가_없는_세트는_없다고_쓴다(self):
        a = self.make_set("a", 10_000_000)
        report = ab.build_compare(self.entries(("A", a)))
        self.assertIn("기록 없음", report)

    def test_보고서에_쓰지_않는_것은_출력하지_않는다(self):
        a = self.make_set("a", 10_000_000, with_dbstats=True)
        report = ab.build_compare(self.entries(("A", a)))
        for forbidden in ("신뢰구간", "p값", "안정적", "때문에"):
            self.assertNotIn(forbidden, report)

    def test_dbstats_읽기는_시작_행만_회차로_모은다(self):
        a = self.make_set("a", 10_000_000)
        write_dbstats(a / "v1", "v1", [
            {"round": "r01", "phase": "start", "orders_e2e": 0, "q1_ms": 0.1},
            {"round": "r01", "phase": "cleared", "orders_e2e": 12, "clear_ms": 340, "deleted_orders": 9500},
            {"round": "r02", "phase": "start", "orders_e2e": 12, "q1_ms": 0.2},
        ])
        stats = ab.load_dbstats(a / "v1", "v1")
        self.assertEqual(["r01", "r02"], sorted(stats["start"].keys()))
        self.assertEqual("9500", stats["cleared"]["r01"]["deleted_orders"])


if __name__ == "__main__":
    unittest.main()
