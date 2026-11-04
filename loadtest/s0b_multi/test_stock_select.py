import os
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stock_select as ss

HERE = os.path.dirname(os.path.abspath(__file__))

# ec의 s0b 실행 결과(slots.txt, U10Tool slot-of)에서 가져온 Java 계산값. 종목 slot 개수 64.
JAVA_SLOTS = {
    "000001": 42, "000002": 56, "000003": 48, "000004": 58, "000005": 32, "000006": 23,
    "000007": 27, "000008": 21, "000009": 59, "000010": 62, "000011": 37, "000012": 12,
    "000013": 58, "000014": 45, "000015": 51, "000016": 46, "000017": 28, "000018": 26,
    "000019": 42, "000020": 34, "000021": 38, "000022": 36, "000023": 21, "000024": 19,
    "000025": 5, "000026": 53, "000027": 27, "000028": 18, "000029": 27, "000030": 55,
}


def addresses(n):
    return ["localhost:%d" % (20020 + i) for i in range(n)]


def round_robin_map(n, slot_count=64):
    addrs = addresses(n)
    return {slot: addrs[slot % n] for slot in range(slot_count)}


def map_text(slot_to_address):
    return "".join("%d\t%s\n" % (s, a) for s, a in sorted(slot_to_address.items()))


class SlotOfTest(unittest.TestCase):
    def test_matches_java_for_thirty_codes(self):
        for code, slot in JAVA_SLOTS.items():
            self.assertEqual(slot, ss.slot_of(code, 64), code)


class ParseMapTest(unittest.TestCase):
    def test_two_column_and_last_value_wins(self):
        parsed = ss.parse_map("0\ta:1\n1\tb:2\n0\tc:3\n")
        self.assertEqual({0: "c:3", 1: "b:2"}, parsed)

    def test_null_empty_and_moving_are_not_assigned(self):
        parsed = ss.parse_map("0\ta:1\n1\tnull\n2\t\n3\tb:2;moving\n4\tb:2\n")
        self.assertEqual({0: "a:1", 4: "b:2"}, parsed)

    def test_history_format_uses_third_column_and_last_record(self):
        parsed = ss.parse_map("0\t1000\ta:1\n0\t2000\tb:2\n1\t1500\tnull\n")
        self.assertEqual({0: "b:2"}, parsed)

    def test_later_null_removes_earlier_address(self):
        parsed = ss.parse_map("0\ta:1\n0\tnull\n")
        self.assertEqual({}, parsed)


class QuotaTest(unittest.TestCase):
    def test_quota_by_count(self):
        cases = {1: [30], 2: [15, 15], 4: [8, 8, 7, 7], 8: [4, 4, 4, 4, 4, 4, 3, 3], 7: [5, 5, 4, 4, 4, 4, 4]}
        for n, expected in cases.items():
            self.assertEqual(expected, ss.quotas(30, n), n)

    def test_quota_sum_is_count(self):
        for n in (1, 2, 3, 4, 5, 8):
            self.assertEqual(30, sum(ss.quotas(30, n)))


class SelectTest(unittest.TestCase):
    def test_each_shard_gets_its_quota(self):
        for n in (1, 2, 4, 8):
            result = ss.select(round_robin_map(n), 30)
            per_shard = {}
            for pick in result:
                per_shard[pick.address] = per_shard.get(pick.address, 0) + 1
            addrs = addresses(n)
            self.assertEqual(ss.quotas(30, n), [per_shard[a] for a in addrs], n)

    def test_codes_are_six_digits_distinct_slots_and_owner_matches(self):
        slot_map = round_robin_map(4)
        result = ss.select(slot_map, 30)
        self.assertEqual(30, len(result))
        self.assertEqual(30, len({p.slot for p in result}))
        for p in result:
            self.assertRegex(p.code, r"^\d{6}$")
            self.assertEqual(ss.slot_of(p.code, 64), p.slot)
            self.assertEqual(slot_map[p.slot], p.address)

    def test_codes_ascend_within_a_shard_and_start_from_one(self):
        result = ss.select(round_robin_map(2), 30)
        for addr in addresses(2):
            codes = [int(p.code) for p in result if p.address == addr]
            self.assertEqual(sorted(codes), codes)
        self.assertNotIn("000000", [p.code for p in result])

    def test_output_order_interleaves_shards(self):
        result = ss.select(round_robin_map(4), 30)
        first_four = [p.address for p in result[:4]]
        self.assertEqual(addresses(4), first_four)

    def test_deterministic(self):
        a = [p.code for p in ss.select(round_robin_map(8), 30)]
        b = [p.code for p in ss.select(round_robin_map(8), 30)]
        self.assertEqual(a, b)

    def test_address_order_is_sorted_ascending(self):
        # 입력 줄 순서를 뒤집어도 같은 결과
        slot_map = round_robin_map(4)
        reversed_map = dict(reversed(list(slot_map.items())))
        self.assertEqual([p.code for p in ss.select(slot_map, 30)], [p.code for p in ss.select(reversed_map, 30)])

    def test_remainder_goes_to_first_addresses(self):
        result = ss.select(round_robin_map(4), 30)
        counts = [sum(1 for p in result if p.address == a) for a in addresses(4)]
        self.assertEqual([8, 8, 7, 7], counts)

    def test_incomplete_map_is_error(self):
        slot_map = round_robin_map(2)
        del slot_map[10]
        with self.assertRaises(ss.SelectError):
            ss.select(slot_map, 30)

    def test_empty_map_is_error(self):
        with self.assertRaises(ss.SelectError):
            ss.select({}, 30)

    def test_shard_with_too_few_slots_is_error(self):
        with self.assertRaises(ss.SelectError):
            ss.select(round_robin_map(8, slot_count=8), 30, slot_count=8)

    def test_slot_beyond_slot_count_is_error(self):
        slot_map = round_robin_map(2)
        slot_map[64] = "localhost:20020"
        with self.assertRaises(ss.SelectError):
            ss.select(slot_map, 30)

    def test_count_must_be_positive_and_at_least_shard_count(self):
        with self.assertRaises(ss.SelectError):
            ss.select(round_robin_map(4), 0)
        with self.assertRaises(ss.SelectError):
            ss.select(round_robin_map(8), 5)


class CliTest(unittest.TestCase):
    def run_cli(self, stdin_text, *args):
        return subprocess.run([sys.executable, "-I", os.path.join(HERE, "stock_select.py"), *args],
                              input=stdin_text, capture_output=True, text=True)

    def test_prints_stocks_slots_and_shard_counts(self):
        out = self.run_cli(map_text(round_robin_map(4)), "--count", "30")
        self.assertEqual(0, out.returncode, out.stderr)
        lines = out.stdout.splitlines()
        stocks_line = [l for l in lines if l.startswith("STOCKS=")]
        self.assertEqual(1, len(stocks_line))
        codes = stocks_line[0][len("STOCKS="):].split()
        self.assertEqual(30, len(codes))
        slot_lines = [l for l in lines if l.startswith("SLOT ")]
        self.assertEqual(30, len(slot_lines))
        self.assertRegex(slot_lines[0], r"^SLOT \d{6}=\d+ localhost:\d+$")
        counts = [l for l in lines if l.startswith("SHARD_STOCKS ")]
        self.assertEqual(["SHARD_STOCKS localhost:20020=8", "SHARD_STOCKS localhost:20021=8",
                          "SHARD_STOCKS localhost:20022=7", "SHARD_STOCKS localhost:20023=7"], counts)

    def test_reads_map_file(self):
        path = os.path.join(HERE, "_tmp_map.txt")
        with open(path, "w") as f:
            f.write(map_text(round_robin_map(2)))
        try:
            out = self.run_cli("", "--count", "30", "--map", path)
        finally:
            os.remove(path)
        self.assertEqual(0, out.returncode, out.stderr)
        self.assertIn("STOCKS=", out.stdout)

    def test_error_exit_code_and_message(self):
        out = self.run_cli("", "--count", "30")
        self.assertEqual(1, out.returncode)
        self.assertEqual("", out.stdout)
        self.assertIn("배정", out.stderr)

    def test_slot_count_option(self):
        out = self.run_cli(map_text(round_robin_map(2, slot_count=16)), "--count", "6", "--slot-count", "16")
        self.assertEqual(0, out.returncode, out.stderr)


if __name__ == "__main__":
    unittest.main()
