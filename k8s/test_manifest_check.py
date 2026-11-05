"""manifest_check.py 검사. 실행: python3 -I k8s/test_manifest_check.py (워크트리 루트에서)

클러스터 없이 매니페스트 파일 내용만 검사한다.
"""
import io
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

HERE = pathlib.Path(__file__).parent
SCRIPT = HERE / "manifest_check.py"
sys.path.insert(0, str(HERE))

FULL = {"requests": {"cpu": "100m", "memory": "64Mi"}, "limits": {"cpu": "200m", "memory": "128Mi"}}

STATEFULSET = """
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: demo
spec:
  template:
    spec:
      nodeSelector:
        ste-role: server
      initContainers:
        - name: init-a
          image: busybox:1
          resources: %(init)s
      containers:
        - name: app-a
          image: demo/app:local
          resources: %(app)s
"""

JOB = """
apiVersion: batch/v1
kind: Job
metadata:
  name: sender
spec:
  template:
    spec:
      containers:
        - name: sender
          image: demo/sender:local
          resources: %(sender)s
---
apiVersion: v1
kind: Service
metadata:
  name: only-service
spec:
  clusterIP: None
"""


def flow(resources):
    """resources 사전을 한 줄 YAML(flow 스타일)로 바꾼다."""
    import json
    return json.dumps(resources)


def write(directory, name, text):
    path = os.path.join(directory, name)
    with open(path, "w", encoding="utf-8") as out:
        out.write(text)
    return path


def run(*args):
    return subprocess.run([sys.executable, "-I", str(SCRIPT)] + list(args), capture_output=True, text=True)


class LimitsTest(unittest.TestCase):
    def test_모든_컨테이너에_한도가_있으면_종료_코드_0(self):
        with tempfile.TemporaryDirectory() as d:
            a = write(d, "a.yaml", STATEFULSET % {"init": flow(FULL), "app": flow(FULL)})
            b = write(d, "b.yaml", JOB % {"sender": flow(FULL)})
            result = run("limits", a, b)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_limits_memory가_빠지면_멈추고_컨테이너_이름을_출력한다(self):
        missing = {"requests": FULL["requests"], "limits": {"cpu": "200m"}}
        with tempfile.TemporaryDirectory() as d:
            a = write(d, "a.yaml", STATEFULSET % {"init": flow(FULL), "app": flow(missing)})
            result = run("limits", a)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("app-a", result.stdout)
        self.assertIn("limits.memory", result.stdout)

    def test_init_컨테이너의_requests_누락도_잡는다(self):
        missing = {"limits": FULL["limits"]}
        with tempfile.TemporaryDirectory() as d:
            a = write(d, "a.yaml", STATEFULSET % {"init": flow(missing), "app": flow(FULL)})
            result = run("limits", a)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("init-a", result.stdout)
        self.assertIn("requests.cpu", result.stdout)
        self.assertNotIn("app-a", result.stdout)

    def test_Job_컨테이너에_resources가_아예_없어도_잡는다(self):
        with tempfile.TemporaryDirectory() as d:
            b = write(d, "b.yaml", JOB % {"sender": "{}"})
            result = run("limits", b)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("sender", result.stdout)

    def test_저장소_매니페스트는_모두_한도를_가진다(self):
        manifests = [str(HERE / name) for name in
                     ("kafka.yaml", "account-statefulset.yaml", "matching-statefulset.yaml", "sender-job.yaml")]
        result = run("limits", *manifests)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)


class ImagesTest(unittest.TestCase):
    def test_이미지와_노드_역할을_한_줄씩_낸다(self):
        with tempfile.TemporaryDirectory() as d:
            a = write(d, "a.yaml", STATEFULSET % {"init": flow(FULL), "app": flow(FULL)})
            b = write(d, "b.yaml", JOB % {"sender": flow(FULL)})
            result = run("images", a, b)
        self.assertEqual(0, result.returncode, result.stderr)
        lines = sorted(result.stdout.strip().splitlines())
        self.assertEqual(["busybox:1\tserver", "demo/app:local\tserver", "demo/sender:local\t-"], lines)

    def test_저장소_매니페스트의_발신기는_서버와_다른_노드_역할이다(self):
        manifests = [str(HERE / name) for name in
                     ("account-statefulset.yaml", "matching-statefulset.yaml", "sender-job.yaml")]
        result = run("images", *manifests)
        roles = dict(line.split("\t") for line in result.stdout.strip().splitlines())
        self.assertEqual("server", roles["stock-trading-engine/account-worker:local"])
        self.assertEqual("server", roles["stock-trading-engine/matching-worker:local"])
        self.assertEqual("load", roles["stock-trading-engine/u10-sender:local"])


if __name__ == "__main__":
    unittest.main()
