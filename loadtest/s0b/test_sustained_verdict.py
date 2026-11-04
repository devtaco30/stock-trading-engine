import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import sustained_verdict as sv


def write(d, name, obj):
    with open(os.path.join(d, name), "w") as f:
        json.dump(obj, f)


def base(d, check_status="ok", fills=True, windows_ok=True, gc_suspect=False, rss_suspect=False, exit_code=0, guard=False):
    write(d, "x.check.json", {"status": check_status, "fills_complete": fills, "balanced": check_status == "ok"})
    write(d, "x.windows.json", {"all_windows_ok": windows_ok, "invalid_reasons": [],
                                "windows": [{"index": 0, "ok": windows_ok, "checks": {"accept_ratio": windows_ok}}],
                                "trend": {"gc_after_suspect": gc_suspect, "gc_after_ratio": 1.2 if gc_suspect else 1.0,
                                          "rss": {"account": {"suspect": rss_suspect, "ratio": 1.3 if rss_suspect else 1.0}}}})
    with open(os.path.join(d, "x.stdout"), "w") as f:
        f.write("VALID accepted=1\nexit=%d\n" % exit_code)
    if guard:
        with open(os.path.join(d, "x.guard"), "w") as f:
            f.write("DISK_GUARD_STOP free_gb=10 ts=1 min_gb=20\n")


class VerdictTest(unittest.TestCase):
    def verdict(self, **kw):
        with tempfile.TemporaryDirectory() as d:
            base(d, **kw)
            return sv.verdict(os.path.join(d, "x"))

    def test_모두_맞으면_PASS(self):
        v = self.verdict()
        self.assertEqual("PASS", v["verdict"])
        self.assertEqual([], v["reasons"])

    def test_창_하나라도_실패하면_FAIL(self):
        v = self.verdict(windows_ok=False)
        self.assertEqual("FAIL", v["verdict"])
        self.assertTrue(any("창" in r for r in v["reasons"]))

    def test_누적_의심은_통과를_막지_않고_표시만_한다(self):
        v = self.verdict(gc_suspect=True, rss_suspect=True)
        self.assertEqual("PASS", v["verdict"])
        self.assertTrue(v["gc_after_suspect"])
        self.assertEqual(["account"], v["rss_suspect_roles"])

    def test_디스크_가드로_멈췄으면_INVALID(self):
        v = self.verdict(guard=True, exit_code=3)
        self.assertEqual("INVALID", v["verdict"])
        self.assertTrue(v["disk_guard_stopped"])

    def test_판정기가_invalid_이거나_체결_반영이_안_끝났으면_INVALID(self):
        self.assertEqual("INVALID", self.verdict(check_status="invalid")["verdict"])
        self.assertEqual("INVALID", self.verdict(fills=False)["verdict"])

    def test_시험_종료_코드가_0이_아니면_INVALID(self):
        self.assertEqual("INVALID", self.verdict(exit_code=3)["verdict"])

    def test_결과_파일이_없으면_INVALID이고_이유에_파일_이름을_적는다(self):
        with tempfile.TemporaryDirectory() as d:
            v = sv.verdict(os.path.join(d, "none"))
        self.assertEqual("INVALID", v["verdict"])
        self.assertTrue(any("check.json" in r for r in v["reasons"]))


if __name__ == "__main__":
    unittest.main()
