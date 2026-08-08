#!/usr/bin/env python3
"""InfluxDB 1.x adapter (best-effort — validate before trusting comparisons).

This is the incumbent system, so numbers from it anchor the comparison.
Writes: classic /write?db=...&precision=ns. Queries: InfluxQL via /query.

InfluxQL cannot COUNT(DISTINCT <tag>) directly, so the funnel queries use
the standard subquery idiom (inner GROUP BY product_id, outer COUNT) —
semantically equivalent, and it exercises exactly the series-index pain
that motivated the migration. B3/B4 return grouped counts; the trivial
start-minus-stop arithmetic happens client-side, which does not affect the
measured engine latency. Expect these to be slow or to fail at full
cardinality — that is a finding, not a bug in the harness.

Launch a test target with: docker compose -f compose/influxdb1.yml up -d
"""

import time

from .base import Backend


class InfluxDB1(Backend):
    name = "influxdb1"
    display = "InfluxDB 1.x"
    default_url = "http://localhost:8086"
    default_db = "poc"
    default_container = "influxdb1"
    data_dir = "/var/lib/influxdb"
    untested = True

    def prepare(self):
        try:
            self.session().post(f"{self.url}/query",
                                params={"q": f'CREATE DATABASE "{self.db}"'},
                                timeout=15)
        except Exception:
            pass

    def healthy(self):
        try:
            r = self.session().get(f"{self.url}/ping", timeout=5)
            return r.status_code in (200, 204)
        except Exception:
            return False

    def version(self):
        try:
            r = self.session().get(f"{self.url}/ping", timeout=5)
            return r.headers.get("X-Influxdb-Version")
        except Exception:
            return None

    def _write_once(self, body, timeout):
        return self.session().post(f"{self.url}/write",
                                   params={"db": self.db, "precision": "ns"},
                                   data=body, timeout=timeout)

    def query(self, q, timeout=300):
        t0 = time.perf_counter()
        try:
            r = self.session().get(f"{self.url}/query",
                                   params={"db": self.db, "q": q, "epoch": "ms"},
                                   timeout=timeout)
            dt = time.perf_counter() - t0
            if r.status_code // 100 != 2:
                return dt, None, f"HTTP {r.status_code}: {r.text[:300]}"
            js = r.json()
            rows = 0
            for res in js.get("results", []):
                if "error" in res:
                    return dt, None, str(res["error"])[:300]
                for series in res.get("series", []):
                    rows += len(series.get("values", []))
            return dt, rows, None
        except Exception as e:
            return time.perf_counter() - t0, None, repr(e)

    def shape_a_query(self, product_id):
        return (f"SELECT duration_s, value, step::tag, event::tag, "
                f"route::tag, worker_ip::tag "
                f"FROM pipeline_events "
                f"WHERE product_id = '{product_id}' AND time >= now() - 2d")

    def shape_b_queries(self):
        funnel = ("SELECT COUNT(fv) AS products FROM "
                  "(SELECT FIRST(duration_s) AS fv FROM pipeline_events "
                  "WHERE event = 'stop' AND time >= now() - {w} "
                  "GROUP BY step, product_id) GROUP BY step")
        return {
            "B1_funnel_24h": funnel.format(w="1d"),
            "B2_funnel_48h": funnel.format(w="2d"),
            "B3_inflight_24h":
                # start/stop counts per step; subtraction is client-side
                "SELECT COUNT(value) AS starts, COUNT(duration_s) AS stops "
                "FROM pipeline_events WHERE time >= now() - 1d GROUP BY step",
            "B4_hourly_throughput_48h":
                "SELECT COUNT(value) AS starts, COUNT(duration_s) AS stops "
                "FROM pipeline_events WHERE time >= now() - 2d "
                "GROUP BY time(1h), step",
            "B5_route_rollup_24h":
                "SELECT COUNT(fv) AS products, MEAN(fv) AS avg_step_s FROM "
                "(SELECT FIRST(duration_s) AS fv FROM pipeline_events "
                "WHERE event = 'stop' AND time >= now() - 1d "
                "GROUP BY route, step, product_id) GROUP BY route, step",
        }

    def context_queries(self):
        # starts + stops counted separately; scalar() surfaces the first
        # column, so "rows_48h" here is start-event count only — compare
        # against other backends accordingly.
        return {
            "rows_48h": "SELECT COUNT(value) AS starts, "
                        "COUNT(duration_s) AS stops "
                        "FROM pipeline_events WHERE time >= now() - 2d",
            "host_rows_6h": "SELECT COUNT(cpu_pct) AS n FROM host_metrics "
                            "WHERE time >= now() - 6h",
            "disk_rows_6h": "SELECT COUNT(used_pct) AS n FROM disk_metrics "
                            "WHERE time >= now() - 6h",
        }

    def scalar(self, q):
        try:
            r = self.session().get(f"{self.url}/query",
                                   params={"db": self.db, "q": q, "epoch": "ms"},
                                   timeout=300)
            for res in r.json().get("results", []):
                for series in res.get("series", []):
                    vals = series.get("values", [])
                    if vals and len(vals[0]) > 1:
                        return vals[0][1]
        except Exception:
            pass
        return None

    def sample_product_ids(self, n=20):
        try:
            r = self.session().get(
                f"{self.url}/query",
                params={"db": self.db,
                        "q": f'SHOW TAG VALUES FROM "pipeline_events" '
                             f'WITH KEY = "product_id" LIMIT {n}'},
                timeout=120)
            js = r.json()
            for res in js.get("results", []):
                for series in res.get("series", []):
                    return [v[1] for v in series.get("values", [])][:n]
        except Exception:
            pass
        return []
