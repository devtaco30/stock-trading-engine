#!/usr/bin/env python3
"""회수 때 파드 목록(kubectl get pods -o json)에서 파드별 자원 한도·노드와 재시작 기록을 표로 뽑는다(작업 4.3, 4.4).
사용: pod_report.py <pods.json> <resources.tsv> <restarts.tsv>
  resources.tsv: pod node container kind(init|main) requests.cpu requests.memory limits.cpu limits.memory
  restarts.tsv : pod node container restartCount state last_reason last_exit_code last_finished_at
"""
import json
import sys


def main(argv):
    pods = json.load(open(argv[1], encoding="utf-8"))["items"]
    with open(argv[2], "w", encoding="utf-8") as res, open(argv[3], "w", encoding="utf-8") as rst:
        res.write("pod\tnode\tcontainer\tkind\trequests.cpu\trequests.memory\tlimits.cpu\tlimits.memory\n")
        rst.write("pod\tnode\tcontainer\trestartCount\tstate\tlast_reason\tlast_exit_code\tlast_finished_at\n")
        for pod in pods:
            name = pod["metadata"]["name"]
            node = pod["spec"].get("nodeName", "-")
            for kind, key in (("init", "initContainers"), ("main", "containers")):
                for c in pod["spec"].get(key) or []:
                    r = c.get("resources") or {}
                    req, lim = r.get("requests") or {}, r.get("limits") or {}
                    res.write("\t".join([name, node, c["name"], kind, req.get("cpu", "-"), req.get("memory", "-"),
                                         lim.get("cpu", "-"), lim.get("memory", "-")]) + "\n")
            statuses = (pod.get("status") or {}).get("initContainerStatuses", []) + (pod.get("status") or {}).get("containerStatuses", [])
            for s in statuses:
                state = next(iter(s.get("state") or {"-": None}))
                last = (s.get("lastState") or {}).get("terminated") or {}
                rst.write("\t".join([name, node, s["name"], str(s.get("restartCount", 0)), state, str(last.get("reason", "-")),
                                     str(last.get("exitCode", "-")), str(last.get("finishedAt", "-"))]) + "\n")


if __name__ == "__main__":
    main(sys.argv)
