"""Local cluster harness for experiments.

Starts simulated workers (simworker), one sidecar per worker and one or more
router replicas as local processes, and offers the failure-injection hooks
the experiments need (kill / partition / drain a worker, kill a router).

The same scenarios can target a Kubernetes cluster instead: see
experiments/k8s.py, selected with RELAY_TARGET=k8s.
"""
from __future__ import annotations

import atexit
import json
import os
import platform
import shutil
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "bin"
EXE = ".exe" if platform.system() == "Windows" else ""
RESULTS = ROOT / "results"

_procs: list[subprocess.Popen] = []


def _cleanup() -> None:
    for p in _procs:
        if p.poll() is None:
            p.kill()


atexit.register(_cleanup)


def build() -> None:
    """Compile the Go binaries once per run (RELAY_NO_BUILD=1 skips it, e.g.
    when another experiment is already running from bin/)."""
    if os.environ.get("RELAY_NO_BUILD") == "1":
        return
    BIN.mkdir(exist_ok=True)
    for name in ("router", "sidecar", "simworker", "loadgen", "autoscaler"):
        out = BIN / f"{name}{EXE}"
        subprocess.run(["go", "build", "-o", str(out), f"./{name}/cmd/{name}"], cwd=ROOT, check=True)


def rate_scale_args() -> list[str]:
    """RELAY_RATE_SCALE multiplies every workload/offered rate (the default
    rates are calibrated for the simulated workers; GPU workers are faster)."""
    s = os.environ.get("RELAY_RATE_SCALE")
    return ["-rate-scale", s] if s else []


def http_get(url: str, timeout: float = 2.0) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode()


def http_post(url: str, timeout: float = 5.0) -> str:
    req = urllib.request.Request(url, data=b"", method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode()


def wait_http(url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            http_get(url, 1.0)
            return
        except Exception:
            time.sleep(0.2)
    raise TimeoutError(f"{url} not ready after {timeout}s")


@dataclass
class Worker:
    idx: int
    port: int
    grpc_port: int
    http_port: int
    sim: subprocess.Popen | None = None
    sidecar: subprocess.Popen | None = None

    @property
    def id(self) -> str:
        return f"w{self.idx}"

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def entry(self) -> str:
        return f"{self.id}=http://127.0.0.1:{self.port}|127.0.0.1:{self.grpc_port}"


@dataclass
class Router:
    idx: int
    port: int
    proc: subprocess.Popen | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


def Cluster(name: str, **kw):
    """Factory used by every scenario: a local process cluster by default, or
    the Kubernetes deployment when RELAY_TARGET=k8s (see k8s.py)."""
    if os.environ.get("RELAY_TARGET") == "k8s":
        from k8s import K8sCluster

        return K8sCluster(name, **kw)
    return LocalCluster(name, **kw)


@dataclass
class LocalCluster:
    """A local Relay deployment. Use as a context manager."""

    name: str
    workers: int = 4
    routers: int = 1
    policy: str = "relay"
    router_env: dict = field(default_factory=dict)
    sim_args: list = field(default_factory=lambda: ["-slots", "4", "-cache-tokens", "24000"])
    base_port: int = 0  # 0 = derived from the experiment name, so experiments can run in parallel

    def __post_init__(self) -> None:
        if not self.base_port:
            n = int(self.name[1]) if len(self.name) > 1 and self.name[0] == "e" and self.name[1].isdigit() else 0
            self.base_port = 20000 + 1000 * n
        self.dir = RESULTS / self.name
        self.logs = self.dir / "logs"
        self.logs.mkdir(parents=True, exist_ok=True)
        # Per port range: parallel clusters of one experiment must not share it.
        self.workers_file = self.dir / f"workers-{self.base_port}.txt"
        self._workers: list[Worker] = []
        self._routers: list[Router] = []
        self._next_worker = 0

    # ------------------------------------------------------------ lifecycle
    def __enter__(self) -> "LocalCluster":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    def _spawn(self, args: list[str], log: str, env: dict | None = None) -> subprocess.Popen:
        f = open(self.logs / log, "ab")
        e = dict(os.environ)
        e.update(env or {})
        p = subprocess.Popen(args, stdout=f, stderr=subprocess.STDOUT, env=e, cwd=ROOT)
        _procs.append(p)
        return p

    def start(self) -> None:
        for _ in range(self.workers):
            self.add_worker(write=False)
        self._write_members()
        for i in range(self.routers):
            self.start_router(i)
        self.wait_ready()

    def stop(self) -> None:
        for r in self._routers:
            if r.proc and r.proc.poll() is None:
                r.proc.kill()
        for w in self._workers:
            for p in (w.sim, w.sidecar):
                if p and p.poll() is None:
                    p.kill()
        time.sleep(0.5)

    # --------------------------------------------------------------- workers
    def add_worker(self, write: bool = True) -> Worker:
        i = self._next_worker
        self._next_worker += 1
        w = Worker(i, self.base_port + 100 + i, self.base_port + 300 + i, self.base_port + 500 + i)
        w.sim = self._spawn([str(BIN / f"simworker{EXE}"), "-addr", f"127.0.0.1:{w.port}", *self.sim_args], f"{w.id}.log")
        w.sidecar = self._spawn([str(BIN / f"sidecar{EXE}")], f"{w.id}-sidecar.log", {
            "MODEL_URL": w.url, "GRPC_ADDR": f"127.0.0.1:{w.grpc_port}", "HTTP_ADDR": f"127.0.0.1:{w.http_port}",
            "WORKER_ID": w.id, "SIDECAR_SCRAPE_MS": "250", "DRAIN_GRACE_S": "60",
        })
        wait_http(w.url + "/health")
        self._workers.append(w)
        if write:
            self._write_members()
        return w

    def live_workers(self) -> list[Worker]:
        return [w for w in self._workers if w.sim and w.sim.poll() is None]

    def _write_members(self) -> None:
        tmp = self.workers_file.with_suffix(".tmp")
        tmp.write_text("\n".join(w.entry() for w in self.live_workers()) + "\n")
        os.replace(tmp, self.workers_file)

    def kill_worker(self, idx: int) -> None:
        """Crash: the process dies instantly (like a pod/node failure)."""
        w = self._worker(idx)
        w.sim.kill()
        w.sidecar.kill()

    def restart_worker(self, idx: int) -> None:
        w = self._worker(idx)
        w.sim = self._spawn([str(BIN / f"simworker{EXE}"), "-addr", f"127.0.0.1:{w.port}", *self.sim_args], f"{w.id}.log")
        w.sidecar = self._spawn([str(BIN / f"sidecar{EXE}")], f"{w.id}-sidecar.log", {
            "MODEL_URL": w.url, "GRPC_ADDR": f"127.0.0.1:{w.grpc_port}", "HTTP_ADDR": f"127.0.0.1:{w.http_port}",
            "WORKER_ID": w.id})
        wait_http(w.url + "/health")

    def partition_worker(self, idx: int, on: bool) -> None:
        """Network partition as seen by the routers: the model server stops
        answering and the sidecar stream goes silent."""
        w = self._worker(idx)
        http_post(f"{w.url}/admin/fault?mode={'hang' if on else 'none'}")
        http_post(f"http://127.0.0.1:{w.http_port}/admin/pause?on={'1' if on else '0'}")

    def drain_and_remove_worker(self, idx: int) -> None:
        """Graceful scale-down: drain via the sidecar, then stop the worker."""
        w = self._worker(idx)
        try:
            http_post(f"http://127.0.0.1:{w.http_port}/drain", timeout=90)
        finally:
            w.sim.kill()
            w.sidecar.kill()
            self._workers.remove(w)
            self._write_members()

    def _worker(self, idx: int) -> Worker:
        return next(w for w in self._workers if w.idx == idx)

    # --------------------------------------------------------------- routers
    def start_router(self, i: int) -> Router:
        if i < len(self._routers):
            r = self._routers[i]
        else:
            r = Router(i, self.base_port + i)
            self._routers.append(r)
        env = {
            "LISTEN_ADDR": f"127.0.0.1:{r.port}", "ROUTER_ID": f"router-{i}", "POLICY": self.policy,
            "WORKERS_FILE": str(self.workers_file), "WORKER_MODEL": "sim-qwen2.5-7b",
            "STALL_TIMEOUT_MS": "1500", "HEALTH_INTERVAL_MS": "500",
        }
        env.update({k: str(v) for k, v in self.router_env.items()})
        r.proc = self._spawn([str(BIN / f"router{EXE}")], f"router-{i}.log", env)
        wait_http(r.url + "/healthz")
        return r

    def kill_router(self, i: int) -> None:
        self._routers[i].proc.kill()

    @property
    def router_urls(self) -> list[str]:
        return [r.url for r in self._routers]

    def wait_ready(self, timeout: float = 20.0) -> None:
        """Every router sees every live worker eligible with fresh stats."""
        want = len(self.live_workers())
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                ok = 0
                for r in self._routers:
                    if r.proc.poll() is not None:
                        continue
                    d = json.loads(http_get(r.url + "/debug/workers"))
                    fresh = [w for w in d["workers"] if w["Healthy"] and not w["Suspect"]]
                    ok += len(fresh) >= want
                if ok == sum(1 for r in self._routers if r.proc.poll() is None):
                    return
            except Exception:
                pass
            time.sleep(0.3)
        raise TimeoutError("cluster not ready")

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
        log = open(self.logs / (Path(out).stem + ".loadgen.log"), "ab")
        p = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT)
        _procs.append(p)
        if background:
            return p
        p.wait()
        return p

    def metrics(self, router: int = 0) -> str:
        return http_get(self._routers[router].url + "/metrics")


class Monitor:
    """Polls every router's /debug/workers in a thread and records a time
    series (eligible workers, queue depth, per-worker load)."""

    def __init__(self, cluster, interval: float = 0.25):
        import threading

        self.c, self.interval, self.rows = cluster, interval, []
        self.t0 = time.time()
        self._stop = threading.Event()
        self._th = threading.Thread(target=self._loop, daemon=True)

    def __enter__(self) -> "Monitor":
        self.t0 = time.time()
        self._th.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._th.join()

    def _loop(self) -> None:
        while not self._stop.is_set():
            t = time.time() - self.t0
            for i, url in enumerate(self.c.router_urls):
                try:
                    d = json.loads(http_get(url + "/debug/workers", 1.0))
                except Exception:
                    self.rows.append({"t": t, "router": i, "up": 0})
                    continue
                ws = d["workers"]
                alias = getattr(self.c, "alias", lambda i: i)
                for w in ws:
                    w["ID"] = alias(w["ID"])
                self.rows.append({
                    "t": t, "router": i, "up": 1, "queue": d["queue_depth"], "inflight": d["inflight"],
                    "workers": len(ws),
                    "eligible": sum(1 for w in ws if w["Healthy"] and not w["Suspect"] and not w["Draining"] and w["Breaker"] != "open"),
                    "worker_queue": sum(w["Waiting"] for w in ws),
                    **{f"load_{w['ID']}": w["Load"] for w in ws},
                    **{f"ok_{w['ID']}": int(w["Healthy"] and not w["Suspect"] and w["Breaker"] != "open") for w in ws},
                })
            self._stop.wait(self.interval)

    def save(self, path: Path) -> None:
        import csv

        keys = sorted({k for r in self.rows for k in r}, key=lambda k: (k not in ("t", "router"), k))
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(self.rows)


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)
    sys.stdout.flush()


def which(cmd: str) -> bool:
    return shutil.which(cmd) is not None
