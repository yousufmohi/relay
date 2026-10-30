"""Kubernetes target for the experiment scenarios (RELAY_TARGET=k8s).

Implements the same interface as harness.LocalCluster on top of the Helm
release created by `make cluster-up`: failures are injected with Chaos Mesh
(deploy/chaos/*) or kubectl, the router is reached through its NodePort /
port-forward, and the load generator runs locally against it.

Environment:
  RELAY_NAMESPACE   (default "relay")
  RELAY_RELEASE     (default "relay")
  RELAY_ROUTER_URL  (default http://localhost:30080 — k3d maps the NodePort)
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

from harness import BIN, EXE, ROOT, _procs, http_get, log, rate_scale_args
from plot import rdir

NS = os.environ.get("RELAY_NAMESPACE", "relay")
REL = os.environ.get("RELAY_RELEASE", "relay")
URL = os.environ.get("RELAY_ROUTER_URL", "http://localhost:30080")


def kubectl(*args: str, input: str | None = None, check: bool = True) -> str:
    r = subprocess.run(["kubectl", "-n", NS, *args], input=input, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout


def pods(component: str) -> list[dict]:
    out = json.loads(kubectl("get", "pods", "-l", f"app.kubernetes.io/name=relay-{component}", "-o", "json"))
    items = [p for p in out["items"] if p["metadata"].get("deletionTimestamp") is None]
    return sorted(items, key=lambda p: p["metadata"]["name"])


class K8sCluster:
    def __init__(self, name: str, workers: int = 4, routers: int = 2, policy: str = "relay",
                 router_env: dict | None = None, **_ignored):
        self.name, self.workers, self.routers, self.policy = name, workers, routers, policy
        self.router_env = router_env or {}
        self.dir = rdir(name)
        self.logs = self.dir / "logs"
        self.logs.mkdir(parents=True, exist_ok=True)
        self._alias: dict[str, str] = {}

    # Same context-manager shape as LocalCluster.
    def __enter__(self) -> "K8sCluster":
        # Fixed-size experiments: the autoscaler must not resize the pool.
        sets = [f"router.policy={self.policy}", f"worker.replicas={self.workers}", f"router.replicas={self.routers}",
                "autoscaler.enabled=false"]
        sets += [f"router.env.{k}={v}" for k, v in self.router_env.items()]
        os.environ["MSYS_NO_PATHCONV"] = "1"
        args = ["helm", "upgrade", "--install", REL, str(ROOT / "deploy" / "helm" / "relay"), "-n", NS, "--reuse-values"]
        for s in sets:
            args += ["--set-string" if "env." in s else "--set", s]
        subprocess.run(args, check=True, capture_output=True)
        # Fresh caches for every run, like the local harness.
        kubectl("rollout", "restart", f"deployment/{REL}-worker", f"deployment/{REL}-router")
        kubectl("rollout", "status", f"deployment/{REL}-worker", "--timeout=600s")
        kubectl("rollout", "status", f"deployment/{REL}-router", "--timeout=300s")
        self.wait_ready()
        return self

    def __exit__(self, *exc) -> None:
        kubectl("delete", "podchaos,networkchaos", "--all", "--ignore-not-found", check=False)
        for c in ("e4-worker-kill", "e5-partition", "e6-router-kill"):
            kubectl("delete", "-f", str(ROOT / "deploy" / "chaos" / f"{c}.yaml"), "--ignore-not-found", check=False)

    @property
    def router_urls(self) -> list[str]:
        return [URL]

    def alias(self, worker_id: str) -> str:
        """Routers name workers by pod IP; map to w0..wN by pod name order."""
        if worker_id not in self._alias:
            for i, p in enumerate(pods("worker")):
                self._alias[p["status"].get("podIP", "")] = f"w{i}"
        return self._alias.get(worker_id, worker_id)

    def wait_ready(self, timeout: float = 300) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                d = json.loads(http_get(URL + "/debug/workers"))
                if sum(1 for w in d["workers"] if w["Healthy"] and not w["Suspect"]) >= self.workers:
                    return
            except Exception:
                pass
            time.sleep(1)
        raise TimeoutError("k8s cluster not ready")

    # --------------------------------------------------------- failure hooks
    def _worker_pod(self, idx: int) -> str:
        return pods("worker")[idx % self.workers]["metadata"]["name"]

    def kill_worker(self, idx: int) -> None:
        pod = self._worker_pod(idx)
        log(f"k8s: pod-kill {pod}")
        self._chaos("PodChaos", f"kill-{pod}", {"action": "pod-kill", "mode": "all", "gracePeriod": 0,
                                                "selector": {"pods": {NS: [pod]}}})

    def restart_worker(self, idx: int) -> None:
        pass  # the Deployment recreates killed pods on its own

    def partition_worker(self, idx: int, on: bool) -> None:
        name = f"partition-w{idx}"
        if not on:
            kubectl("delete", "networkchaos", name, "--ignore-not-found", check=False)
            return
        pod = self._worker_pod(idx)
        self._chaos("NetworkChaos", name, {
            "action": "partition", "mode": "all", "direction": "both",
            "selector": {"pods": {NS: [pod]}},
            "target": {"mode": "all", "selector": {"namespaces": [NS],
                                                   "labelSelectors": {"app.kubernetes.io/name": "relay-router"}}},
        })

    def kill_router(self, i: int) -> None:
        pod = pods("router")[i % self.routers]["metadata"]["name"]
        self._chaos("PodChaos", f"kill-{pod}", {"action": "pod-kill", "mode": "all", "gracePeriod": 0,
                                                "selector": {"pods": {NS: [pod]}}})

    def start_router(self, i: int) -> None:
        pass  # recreated by its Deployment

    def drain_and_remove_worker(self, idx: int) -> None:
        kubectl("scale", f"deployment/{REL}-worker", f"--replicas={max(0, self.workers - 1)}")
        self.workers -= 1

    def add_worker(self):
        self.workers += 1
        kubectl("scale", f"deployment/{REL}-worker", f"--replicas={self.workers}")

    def live_workers(self) -> list:
        return pods("worker")

    def _chaos(self, kind: str, name: str, spec: dict) -> None:
        doc = {"apiVersion": "chaos-mesh.org/v1alpha1", "kind": kind,
               "metadata": {"name": name, "namespace": NS}, "spec": spec}
        kubectl("apply", "-f", "-", input=json.dumps(doc))

    # --------------------------------------------------------------- loadgen
    def loadgen(self, workload: str, out: str, rate: float | None = None, duration: str | None = None,
                targets: list[str] | None = None, background: bool = False, extra: list[str] | None = None):
        args = [str(BIN / f"loadgen{EXE}"), "-workload", str(ROOT / "experiments" / "workloads" / f"{workload}.yaml"),
                "-targets", ",".join(targets or self.router_urls), "-out", str(self.dir / out)]
        if rate:
            args += ["-rate", str(rate)]
        if duration:
            args += ["-duration", duration]
        args += (extra or []) + rate_scale_args()
        p = subprocess.Popen(args, stdout=open(self.logs / (Path(out).stem + ".loadgen.log"), "ab"),
                             stderr=subprocess.STDOUT, cwd=ROOT)
        _procs.append(p)
        if background:
            return p
        p.wait()
        return p
