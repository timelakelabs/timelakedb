#!/usr/bin/env python3
"""InfluxDB 2.x adapter (best-effort — validate before trusting comparisons).

The generation between the 1.x incumbent and InfluxDB 3. Same TSM storage
family as 1.x with a TSI series index, so this workload's cardinality
(~2M product_ids/day x step x event => tens of millions of series at full
scale) is exactly its known weak spot. A collapse here is a *finding*.

Writes: /api/v2/write?org=&bucket=&precision=ns with Token auth.
Queries: Flux via POST /api/v2/query (CSV response, no annotations).
Notes:
- B3/B4 return per-step[/hour] event counts; the trivial start-minus-stop
  arithmetic is client-side (same pragmatism as the influxdb1 adapter).
- B5 uses two yields (distinct-product count + mean duration).
- The `db` parameter maps to the bucket; org is fixed "poc" (compose file).

Launch the target with: docker compose -f compose/influxdb2.yml up -d
"""

import time

import requests

from .base import Backend


class InfluxDB2(Backend):
    name = "influxdb2"
    display = "InfluxDB 2.x"
    default_url = "http://localhost:8086"
    default_db = "poc"           # bucket
    default_container = "influxdb2"
    data_dir = "/var/lib/influxdb2"
    untested = True

    ORG = "poc"

    def __init__(self, url=None, db=None, token=""):
        super().__init__(url, db, token or "bench-token")

    def session(self):
        s = getattr(self._local, "s", None)
        if s is None:
            s = requests.Session()
            s.headers["Authorization"] = f"Token {self.token}"
            self._local.s = s
        return s

    def healthy(self):
        try:
            r = self.session().get(f"{self.url}/health", timeout=5)
            return r.status_code // 100 == 2
        except Exception:
            return False

    def version(self):
        try:
            r = self.session().get(f"{self.url}/health", timeout=5)
            return r.json().get("version")
        except Exception:
            return None

    def _write_once(self, body, timeout):
        return self.session().post(
            f"{self.url}/api/v2/write",
            params={"org": self.ORG, "bucket": self.db, "precision": "ns"},
            data=body,
            headers={"Content-Type": "text/plain; charset=utf-8"},
            timeout=timeout,
        )

    # ── Flux ────────────────────────────────────────────────────────────────

    def _flux(self, q, timeout=300):
        t0 = time.perf_counter()
        try:
            r = self.session().post(
                f"{self.url}/api/v2/query",
                params={"org": self.ORG},
                json={"query": q,
                      "dialect": {"annotations": [], "header": True}},
                timeout=timeout,
            )
            dt = time.perf_counter() - t0
            if r.status_code // 100 != 2:
                return dt, None, f"HTTP {r.status_code}: {r.text[:300]}"
            return dt, r.text, None
        except Exception as e:
            return time.perf_counter() - t0, None, repr(e)

    @staticmethod
    def _csv_rows(text):
        """Data-row count of an annotation-free Flux CSV response."""
        n = 0
        for line in text.splitlines():
            if not line.strip() or line.startswith("#"):
                continue
            if line.startswith("result,table") or line.startswith(",result,table"):
                continue  # header row of a table block
            n += 1
        return n

    def query(self, q, timeout=300):
        dt, text, err = self._flux(q, timeout)
        return dt, (self._csv_rows(text) if text is not None else None), err

    # stop: 30d (far future) matches the SQL adapters' unbounded upper time
    # bound — Flux's default stop:now() silently drops the slightly-future
    # event timestamps the generator legitimately produces.
    def _from(self, range_s):
        return (f'from(bucket: "{self.db}") '
                f'|> range(start: {range_s}, stop: 30d) '
                f'|> filter(fn: (r) => r._measurement == "pipeline_events")')

    def shape_a_query(self, product_id):
        return (
            f'from(bucket: "{self.db}") |> range(start: -2d, stop: 30d) '
            f'|> filter(fn: (r) => r._measurement == "pipeline_events" '
            f'and r.product_id == "{product_id}") '
            f'|> keep(columns: ["_time","step","event","route","worker_ip",'
            f'"_field","_value"]) '
            f'|> sort(columns: ["_time"])'
        )

    def shape_b_queries(self):
        distinct = (
            '{src} '
            '|> filter(fn: (r) => r.event == "stop" and r._field == "duration_s") '
            '|> keep(columns: [{keep}, "product_id"]) '
            '|> group(columns: [{grp}]) '
            '|> distinct(column: "product_id") '
            '|> count() '
            '|> group() |> sort(columns: [{grp}])'
        )
        return {
            "B1_funnel_24h": distinct.format(
                src=self._from("-24h"), keep='"step"', grp='"step"'),
            "B2_funnel_48h": distinct.format(
                src=self._from("-48h"), keep='"step"', grp='"step"'),
            "B3_inflight_24h": (
                f'{self._from("-24h")} '
                f'|> group(columns: ["step","event"]) |> count() '
                f'|> group() |> sort(columns: ["step","event"])'),
            "B4_hourly_throughput_48h": (
                # toFloat() unifies the int `value` / float `duration_s`
                # schemas; counting mixed types in one group is an error
                f'{self._from("-48h")} '
                f'|> toFloat() '
                f'|> group(columns: ["step"]) '
                f'|> aggregateWindow(every: 1h, fn: count, createEmpty: false) '
                f'|> group() |> sort(columns: ["_time","step"])'),
            "B5_route_rollup_24h": (
                f'base = {self._from("-24h")} '
                f'|> filter(fn: (r) => r.event == "stop" '
                f'and r._field == "duration_s")\n'
                f'base |> keep(columns: ["route","step","product_id"]) '
                f'|> group(columns: ["route","step"]) '
                f'|> distinct(column: "product_id") |> count() '
                f'|> group() |> yield(name: "products")\n'
                f'base |> group(columns: ["route","step"]) |> mean() '
                f'|> group() |> yield(name: "avg_step_s")'),
        }

    def context_queries(self):
        return {
            "rows_48h": f'{self._from("-48h")} |> group() |> count()',
            "host_rows_6h": (
                f'from(bucket: "{self.db}") |> range(start: -6h, stop: 30d) '
                f'|> filter(fn: (r) => r._measurement == "host_metrics" '
                f'and r._field == "cpu_pct") |> group() |> count()'),
            "disk_rows_6h": (
                f'from(bucket: "{self.db}") |> range(start: -6h, stop: 30d) '
                f'|> filter(fn: (r) => r._measurement == "disk_metrics" '
                f'and r._field == "used_pct") |> group() |> count()'),
        }

    def scalar(self, q):
        _, text, err = self._flux(q)
        if err or not text:
            return None
        header, data = None, None
        for line in text.splitlines():
            if not line.strip() or line.startswith("#"):
                continue
            if line.startswith("result,table") or line.startswith(",result,table"):
                header = line.split(",")
                continue
            if header is not None:
                data = line.split(",")
                break
        if header and data and "_value" in header:
            try:
                return float(data[header.index("_value")])
            except (ValueError, IndexError):
                return None
        return None

    def sample_product_ids(self, n=20):
        q = (
            'import "influxdata/influxdb/schema"\n'
            f'schema.tagValues(bucket: "{self.db}", tag: "product_id", '
            f'start: -2d) |> limit(n: {n})'
        )
        _, text, err = self._flux(q, timeout=120)
        if err or not text:
            return []
        pids, header_seen = [], False
        for line in text.splitlines():
            if not line.strip() or line.startswith("#"):
                continue
            if line.startswith("result,table") or line.startswith(",result,table"):
                header_seen = True
                continue
            if header_seen:
                pids.append(line.split(",")[-1].strip())
        return [p for p in pids if p][:n]
