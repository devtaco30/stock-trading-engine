#!/usr/bin/env python3
"""시험 매니페스트를 띄우기 전에 검사한다(작업 3.6, 4.1). 클러스터에 묻지 않고 파일 내용만 본다.

사용:
  manifest_check.py limits <매니페스트...>
      파드를 만드는 문서(Pod, StatefulSet, Deployment, Job 등)의 모든 컨테이너(initContainers 포함)에
      requests.cpu, requests.memory, limits.cpu, limits.memory가 있는지 본다.
      빠진 컨테이너마다 「MISSING_LIMITS <파일> <종류>/<이름> <컨테이너> <빠진 항목,...>」 한 줄을 내고 종료 코드 2.
  manifest_check.py images <매니페스트...>
      컨테이너 이미지마다 「<이미지>\\t<노드 역할>」 한 줄을 낸다. 노드 역할은 nodeSelector의 ste-role 값이고 없으면 -.
      실행 스크립트가 그 역할의 노드에 이미지가 있는지 확인하는 데 쓴다.
"""
import sys

import yaml

REQUIRED = ("requests.cpu", "requests.memory", "limits.cpu", "limits.memory")


def pod_spec(doc):
    """문서에서 파드 spec을 꺼낸다. 파드를 만들지 않는 문서(Service, ConfigMap 등)는 None."""
    kind = doc.get("kind")
    spec = doc.get("spec") or {}
    if kind == "Pod":
        return spec
    if kind == "CronJob":
        return ((spec.get("jobTemplate") or {}).get("spec") or {}).get("template", {}).get("spec")
    template = spec.get("template")
    if isinstance(template, dict):
        return template.get("spec")
    return None


def containers(spec):
    return list(spec.get("initContainers") or []) + list(spec.get("containers") or [])


def missing_items(container):
    resources = container.get("resources") or {}
    missing = []
    for item in REQUIRED:
        group, key = item.split(".")
        if not (resources.get(group) or {}).get(key):
            missing.append(item)
    return missing


def documents(paths):
    for path in paths:
        with open(path, encoding="utf-8") as source:
            for doc in yaml.safe_load_all(source):
                if doc:
                    yield path, doc


def check_limits(paths):
    problems = []
    for path, doc in documents(paths):
        spec = pod_spec(doc)
        if not spec:
            continue
        name = "%s/%s" % (doc.get("kind"), (doc.get("metadata") or {}).get("name"))
        for container in containers(spec):
            missing = missing_items(container)
            if missing:
                problems.append("MISSING_LIMITS %s %s %s %s" % (path, name, container.get("name"), ",".join(missing)))
    return problems


def images(paths):
    lines = []
    for _path, doc in documents(paths):
        spec = pod_spec(doc)
        if not spec:
            continue
        role = (spec.get("nodeSelector") or {}).get("ste-role", "-")
        for container in containers(spec):
            line = "%s\t%s" % (container.get("image"), role)
            if line not in lines:
                lines.append(line)
    return lines


def main(argv):
    if len(argv) < 3 or argv[1] not in ("limits", "images"):
        sys.exit(__doc__)
    paths = argv[2:]
    if argv[1] == "limits":
        problems = check_limits(paths)
        for line in problems:
            print(line)
        return 2 if problems else 0
    for line in images(paths):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
