#!/usr/bin/env python3
"""Backend adapter contract.

A backend knows how to (1) accept line-protocol writes, (2) run the two
canonical query shapes in its own query language, and (3) report its
storage footprint. Everything else — workload generation, timing, resource
monitoring, results — is backend-agnostic and lives in the framework.

To add a solution: subclass Backend, implement the abstract methods,
register the class in backends/__init__.py, add a compose file under
compose/ so anyone can reproduce the target, and validate the shape
queries return the same row counts as a reference backend at smoke scale.
"""

import subprocess
import threading
import time

import requests


class WriteError(RuntimeError):
    pass


class Backend:
    name = "base"                # registry key / --backend value
    display = "?"                # human name for reports
    default_url = None
    default_db = "poc"
    default_container = None     # docker container for stats + storage
    data_dir = None              # data path inside the container
    untested = False             # True => warn: validate queries before trusting

    def __init__(self, url=None, db=None, token=""):
        self.url = (url or self.default_url).rstrip("/")
        self.db = db or self.default_db
        self.token = (token or "").strip()
        self._local = threading.local()

    # ── transport ────────────────────────────────────────────────────────────

    def session(self):
        s = getattr(self._local, "s", None)
        if s is None:
            s = requests.Session()
            if self.token:
                s.headers["Authorization"] = f"Bearer {self.token}"
            self._local.s = s
        return s

    # ── lifecycle ────────────────────────────────────────────────────────────

    def prepare(self):
        """Idempotently create the database/table if the engine needs it."""

    def healthy(self):
        raise NotImplementedError

    def wait_healthy(self, timeout=60):
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self.healthy():
                return True
            time.sleep(2)
        return False

    def version(self):
        return None

    def post_ingest(self):
        """Hook after a write scenario (e.g. force a flush so queries and
        storage measurements see everything just written)."""

    # ── writes ───────────────────────────────────────────────────────────────

    def write_lines(self, lines, timeout=180, retries=6):
        """POST one line-protocol batch with retry/backoff.
        Returns (latency_s_of_successful_attempt, bytes_sent); raises
        WriteError after exhausting retries."""
        body = ("\n".join(lines)).encode()
        last = None
        for attempt in range(retries):
            t0 = time.perf_counter()
            try:
                r = self._write_once(body, timeout)
                if r.status_code // 100 == 2:
                    return time.perf_counter() - t0, len(body)
                last = f"HTTP {r.status_code}: {r.text[:200]}"
            except Exception as e:
                last = repr(e)
            time.sleep(min(1.0 * (attempt + 1), 5))
        raise WriteError(f"write failed after {retries} attempts: {last}")

    def _write_once(self, body, timeout):
        """One write attempt; returns a requests.Response."""
        raise NotImplementedError

    # ── queries ──────────────────────────────────────────────────────────────

    def query(self, q, timeout=300):
        """Execute one query (whatever object the shape_* methods produce).
        Returns (elapsed_s, rowcount_or_None, error_or_None). Never raises —
        an OOM/timeout/error is a *result* this framework exists to record."""
        raise NotImplementedError

    def shape_a_query(self, product_id):
        """Single-product journey over the last 2 days, time-ordered."""
        raise NotImplementedError

    def shape_b_queries(self):
        """dict name->query. Canonical five (same semantics per backend):
        B1_funnel_24h, B2_funnel_48h, B3_inflight_24h,
        B4_hourly_throughput_48h, B5_route_rollup_24h."""
        raise NotImplementedError

    def context_queries(self):
        """Optional dict name->query for dataset-size sanity counts."""
        return {}

    def scalar(self, q):
        """Best-effort numeric value of a single-value aggregate query
        (dataset sanity checks across backends); None if unsupported."""
        return None

    def sample_product_ids(self, n=20):
        """n real product_ids present in the data (for Shape A)."""
        raise NotImplementedError

    # ── storage ──────────────────────────────────────────────────────────────

    def storage_bytes(self, container=None):
        """On-disk size of the engine's data dir, via docker exec du."""
        container = container or self.default_container
        if not (container and self.data_dir):
            return None
        for args, mult in ((["du", "-sb", self.data_dir], 1),
                           (["du", "-sk", self.data_dir], 1024)):
            try:
                r = subprocess.run(["docker", "exec", container] + args,
                                   capture_output=True, text=True, timeout=300)
                if r.returncode == 0 and r.stdout.strip():
                    return int(r.stdout.split()[0]) * mult
            except Exception:
                pass
        return None
