import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "make-seed.py")


def run_seed(accounts, holding, stocks, env_extra=None):
    env = dict(os.environ)
    env.pop("BUYER_ACCOUNTS", None)
    env.pop("BUYER_BALANCE", None)
    env.update(env_extra or {})
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "seed.yml")
        subprocess.run([sys.executable, "-I", SCRIPT, path, str(accounts), str(holding)] + stocks, check=True, env=env)
        return open(path, encoding="utf-8").read()


class MakeSeedTest(unittest.TestCase):
    def test_매수_계좌_환경변수가_없으면_기존_출력과_같다(self):
        text = run_seed(2, 5, ["000001", "000002"])
        self.assertEqual(text.count("account-id:"), 3)  # 90001 + 매도 계좌 2
        self.assertIn("account-id: 90002", text)
        self.assertIn("account-id: 90003", text)
        self.assertNotIn("account-id: 90004", text)

    def test_매수_전용_계좌는_매도_계좌_다음_번호부터_보유_없이_지정_잔고로_시딩한다(self):
        text = run_seed(2, 5, ["000001"], {"BUYER_ACCOUNTS": "3", "BUYER_BALANCE": "10000000000"})
        self.assertEqual(text.count("account-id:"), 1 + 2 + 3)
        blocks = text.split("    - account-id: ")[1:]
        by_id = {int(b.split("\n")[0]): b for b in blocks}
        self.assertEqual(sorted(by_id), [90001, 90002, 90003, 90004, 90005, 90006])
        for buyer in (90004, 90005, 90006):
            self.assertIn("balance: 10000000000", by_id[buyer])
            self.assertNotIn("holdings", by_id[buyer])
        self.assertIn("holdings", by_id[90002])
        self.assertIn("balance: 100000000\n", by_id[90002])

    def test_매수_계좌_수만_주고_잔고가_없으면_거부한다(self):
        with self.assertRaises(subprocess.CalledProcessError):
            run_seed(2, 5, ["000001"], {"BUYER_ACCOUNTS": "3"})


if __name__ == "__main__":
    unittest.main()
