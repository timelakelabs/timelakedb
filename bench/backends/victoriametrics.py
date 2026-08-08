#!/usr/bin/env python3
"""VictoriaMetrics adapter (best-effort — validate before trusting comparisons).

Writes: InfluxDB line protocol on POST /write (port 8428). VM maps each
field to its own metric: `pipeline_events,product_id=X,... value=1i` becomes
`pipeline_events_value{product_id="X",...}` and `duration_s=` becomes
`pipeline_events_duration_s{...}`. Queries are MetricsQL.

Caveats baked into this adapter:
- post_ingest() calls /internal/force_flush so just-written data is
  searchable before the query scenarios run.
- VM's -search.latencyOffset (default 30s) ignores the newest ~30s at
  query time; irrelevant for 24/48h windows, visible in tiny smoke runs.
- Defaults cap unique series per query; compose/victoriametrics.yml raises
  -search.max* limits so the high-cardinality funnel is genuinely attempted
  (hitting a limit would otherwise masquerade as a fast failure).

Launch the target with: docker compose -f compose/victoriametrics.yml up -d
"""

import time

from .base import Backend


class VictoriaMetrics(Backend):
    name = "victoriametrics"
    display = "VictoriaMetrics"
    default_url = "http://localhost:8428"
    default_db = "vm"            # no db concept; unused
    default_container = "victoriametrics"
    data_dir = "/victoria-metrics-data"
    untested = True

    def healthy(self):
        try:
            r = self.session().get(f"{self.url}/health", timeout=5)
            return r.status_code // 100 == 2
        except Exception:
            return False

    def version(self):
        try:
            r = self.session().get(f"{self.url}/api/v1/status/buildinfo", timeout=5)
            return r.json().get("data", {}).get("version")
        except Exception:
            return None

    def _write_once(self, body, timeout):
        return self.session().post(f"{self.url}/write", data=body, timeout=timeout)

    def post_ingest(self):
        try:
            self.session().get(f"{self.url}/internal/force_flush", timeout=60)
        except Exception:
            pass

    def query(self, q, timeout=300):
        """q is {'path': '/api/v1/query'|'/api/v1/query_range', 'params': {...}}."""
        if isinstance(q, str):
            q = {"path": "/api/v1/query", "params": {"query": q}}
        t0 = time.perf_counter()
        try:
            r = self.session().get(f"{self.url}{q['path']}",
                                   params=q["params"], timeout=timeout)
            dt = time.perf_counter() - t0
            if r.status_code // 100 != 2:
                return dt, None, f"HTTP {r.status_code}: {r.text[:300]}"
            js = r.json()
            if js.get("status") != "success":
                return dt, None, str(js.get("error", "unknown error"))[:300]
            result = js.get("data", {}).get("result", [])
            # matrix results: count samples; vector: count series
            rows = sum(len(s.get("values", [None])) for s in result) if result else 0
            return dt, rows, None
        except Exception as e:
            return time.perf_counter() - t0, None, repr(e)

    def shape_a_query(self, product_id):
        return {"path": "/api/v1/query",
                "params": {"query":
                           '{__name__=~"pipeline_events_.*",'
                           f'product_id="{product_id}"}}[48h]'}}

    def shape_b_queries(self):
        now = int(time.time())
        distinct = ('count by (step) (count by (step, product_id) '
                    '(last_over_time(pipeline_events_duration_s'
                    '{event="stop"}[%s])))')
        return {
            "B1_funnel_24h":
                {"path": "/api/v1/query", "params": {"query": distinct % "24h"}},
            "B2_funnel_48h":
                {"path": "/api/v1/query", "params": {"query": distinct % "48h"}},
            "B3_inflight_24h":
                {"path": "/api/v1/query", "params": {"query":
                    'sum by (step) (count_over_time('
                    'pipeline_events_value{event="start"}[24h])) '
                    '- sum by (step) (count_over_time('
                    'pipeline_events_duration_s{event="stop"}[24h]))'}},
            "B4_hourly_throughput_48h":
                {"path": "/api/v1/query_range", "params": {
                    "query": 'sum by (step) (count_over_time('
                             '{__name__=~"pipeline_events_.*"}[1h]))',
                    "start": now - 48 * 3600, "end": now, "step": "1h"}},
            "B5_route_rollup_24h":
                {"path": "/api/v1/query", "params": {"query":
                    'count by (route, step) (count by (route, step, product_id) '
                    '(last_over_time(pipeline_events_duration_s'
                    '{event="stop"}[24h])))'}},
        }

    def context_queries(self):
        return {
            "rows_48h": {"path": "/api/v1/query", "params": {"query":
                'sum(count_over_time({__name__=~"pipeline_events_.*"}[48h]))'}},
            "host_rows_6h": {"path": "/api/v1/query", "params": {"query":
                'sum(count_over_time(host_metrics_cpu_pct[6h]))'}},
            "disk_rows_6h": {"path": "/api/v1/query", "params": {"query":
                'sum(count_over_time(disk_metrics_used_pct[6h]))'}},
        }

    def scalar(self, q):
        if isinstance(q, str):
            q = {"path": "/api/v1/query", "params": {"query": q}}
        try:
            r = self.session().get(f"{self.url}{q['path']}",
                                   params=q["params"], timeout=300)
            result = r.json().get("data", {}).get("result", [])
            if result:
                v = result[0].get("value") or (result[0].get("values") or [None])[-1]
                return float(v[1]) if v else None
        except Exception:
            pass
        return None

    def sample_product_ids(self, n=20):
        try:
            r = self.session().get(f"{self.url}/api/v1/label/product_id/values",
                                   params={"limit": n}, timeout=60)
            vals = r.json().get("data", [])
            return vals[:n]
        except Exception:
            return []
