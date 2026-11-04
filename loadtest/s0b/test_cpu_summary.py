import unittest

import cpu_summary as cs


class CpuSummaryTest(unittest.TestCase):
    def test_parse_cputime(self):
        self.assertAlmostEqual(cs.parse_cputime("1:02:03.50"), 3723.5)
        self.assertAlmostEqual(cs.parse_cputime("2:03.25"), 123.25)

    def test_summarize_uses_cpu_delta_over_wall_delta_inside_window(self):
        rows = [
            (100, "account", "9", 10.0, 1024),
            (110, "account", "9", 20.0, 2048),   # 구간 밖(시작 전) 표본은 제외
            (120, "account", "9", 40.0, 4096),
            (130, "account", "9", 70.0, 3072),
            (110, "sender", "7", 1.0, 100),
            (130, "sender", "7", 2.0, 200),
        ]
        result = cs.summarize(rows, 110, 130)
        self.assertEqual(result["account"]["avg_cpu_pct"], 250.0)   # (70-20) / 20초
        self.assertEqual(result["account"]["max_rss_mb"], 4)
        self.assertEqual(result["sender"]["avg_cpu_pct"], 5.0)

    def test_summarize_ignores_single_sample(self):
        self.assertEqual(cs.summarize([(100, "a", "1", 1.0, 1)], 0, 200), {})

    def test_parse_threads_and_usage(self):
        dump1 = '"aeron-driver" #20 [1] prio=5 os_prio=31 cpu=1000.00ms elapsed=10.00s tid=0x1 nid=6659 runnable\n"idle" #21 [2] prio=5 os_prio=31 cpu=5.00ms elapsed=10.00s tid=0x2 nid=6915 waiting\n'
        dump2 = '"aeron-driver" #20 [1] prio=5 os_prio=31 cpu=1900.00ms elapsed=20.00s tid=0x1 nid=6659 runnable\n"idle" #21 [2] prio=5 os_prio=31 cpu=6.00ms elapsed=20.00s tid=0x2 nid=6915 waiting\n'
        usage = cs.thread_usage(cs.parse_threads(dump1), cs.parse_threads(dump2))
        self.assertEqual(usage[0], {"thread": "aeron-driver", "cpu_pct": 9.0})
        self.assertEqual(usage[1]["thread"], "idle")


if __name__ == "__main__":
    unittest.main()
