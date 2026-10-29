"""matching-statefulset.yaml 검사. 실행: python3 -I k8s/test_matching_statefulset.py (워크트리 루트에서)

클러스터에 적용해 본 것이 아니라 파일 내용만 검사한다.
"""
import pathlib
import unittest

import yaml

MANIFEST = pathlib.Path(__file__).with_name("matching-statefulset.yaml")


def load_documents():
    return [doc for doc in yaml.safe_load_all(MANIFEST.read_text()) if doc]


def find(kind):
    return next(doc for doc in load_documents() if doc["kind"] == kind)


def container_env():
    container = find("StatefulSet")["spec"]["template"]["spec"]["containers"][0]
    return container["env"]


class MatchingStatefulSetTest(unittest.TestCase):
    def test_pod_이름이_matching_순번이_되도록_StatefulSet_이름이_matching이다(self):
        self.assertEqual("matching", find("StatefulSet")["metadata"]["name"])

    def test_replicas는_정수_하나이고_serviceName은_headless_Service와_같다(self):
        statefulset = find("StatefulSet")
        service = find("Service")
        self.assertIsInstance(statefulset["spec"]["replicas"], int)
        self.assertEqual(service["metadata"]["name"], statefulset["spec"]["serviceName"])
        self.assertEqual("None", service["spec"]["clusterIP"])

    def test_신원은_pod_이름에서_온다(self):
        env = {item["name"]: item for item in container_env()}
        field_path = env["MATCHING_WORKER_INSTANCE_ID"]["valueFrom"]["fieldRef"]["fieldPath"]
        self.assertEqual("metadata.name", field_path)

    def test_서버_번호는_사람이_적지_않는다(self):
        names = [item["name"] for item in container_env()]
        self.assertNotIn("SNOWFLAKE_NODE_ID", names)

    def test_조정_모드가_켜져_있다(self):
        env = {item["name"]: item.get("value") for item in container_env()}
        self.assertEqual("true", env["MATCHING_SHARD_COORDINATION_ENABLED"])

    def test_주문_입구_주소는_신원_변수_뒤에서_pod_DNS_이름으로_만든다(self):
        names = [item["name"] for item in container_env()]
        env = {item["name"]: item.get("value") for item in container_env()}
        channel = env["TRANSPORT_MATCHING_INTAKE_CHANNEL"]
        self.assertIn("$(MATCHING_WORKER_INSTANCE_ID).matching", channel)
        self.assertLess(names.index("MATCHING_WORKER_INSTANCE_ID"),
                        names.index("TRANSPORT_MATCHING_INTAKE_CHANNEL"))


if __name__ == "__main__":
    unittest.main()
