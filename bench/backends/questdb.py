#!/usr/bin/env python3
"""QuestDB adapter (best-effort — validate before trusting comparisons).

Writes: InfluxDB line protocol over HTTP (POST /write on port 9000,
QuestDB 7.4+). Tags become SYMBOL columns; the designated timestamp column
is named `timestamp`. Queries: REST /exec with QuestDB SQL.

Launch the target with: docker compose -f compose/questdb.yml up -d
"""

import time

from .base import Backend


class QuestDB(Backend):
    name = "questdb"
    display = "QuestDB"
    default_url = "http://localhost:9000"
    default_db = "qdb"           # QuestDB has no db concept over ILP; unused
    default_container = "questdb"
    data_dir = "/var/lib/questdb"
    untested = True

    def healthy(self):
        try:
            r = self.session().get(f"{self.url}/exec",
                                   params={"query": "select 1"}, timeout=5)
            return r.status_code // 100 == 2
        except Exception:
            return False

    def version(self):
        try:
            r = self.session().get(f"{self.url}/exec",
                                   params={"query": "select build"}, timeout=5)
            ds = r.json().get("dataset")
            return str(ds[0][0])[:60] if ds else None
        except Exception:
            return None

    def _write_once(self, body, timeout):
        return self.session().post(f"{self.url}/write",
                                   params={"precision": "n"},
                                   data=body, timeout=timeout)

    def _exec(self, sql, timeout=300):
        t0 = time.perf_counter()
        try:
            r = self.session().get(f"{self.url}/exec",
                                   params={"query": sql, "timings": "false"},
                                   timeout=timeout)
            dt = time.perf_counter() - t0
            if r.status_code // 100 != 2:
                return dt, None, f"HTTP {r.status_code}: {r.text[:300]}"
            js = r.json()
            if "error" in js:
                return dt, None, str(js["error"])[:300]
            return dt, js, None
        except Exception as e:
            return time.perf_counter() - t0, None, repr(e)

    def query(self, q, timeout=300):
        dt, js, err = self._exec(q, timeout)
        rows = len(js.get("dataset", [])) if js is not None else None
        return dt, rows, err

    def shape_a_query(self, product_id):
        return (f"SELECT timestamp, step, event, route, worker_ip, duration_s "
                f"FROM pipeline_events "
                f"WHERE product_id = '{product_id}' "
                f"AND timestamp >= dateadd('d', -2, now()) "
                f"ORDER BY timestamp")

    def shape_b_queries(self):
        return {
            "B1_funnel_24h":
                "SELECT step, count_distinct(product_id) AS products "
                "FROM pipeline_events "
                "WHERE event = 'stop' AND timestamp >= dateadd('h', -24, now()) "
                "GROUP BY step ORDER BY step",
            "B2_funnel_48h":
                "SELECT step, count_distinct(product_id) AS products "
                "FROM pipeline_events "
                "WHERE event = 'stop' AND timestamp >= dateadd('h', -48, now()) "
                "GROUP BY step ORDER BY step",
            "B3_inflight_24h":
                "SELECT step, "
                "sum(CASE WHEN event = 'start' THEN 1 ELSE 0 END) "
                " - sum(CASE WHEN event = 'stop' THEN 1 ELSE 0 END) AS in_flight "
                "FROM pipeline_events "
                "WHERE timestamp >= dateadd('h', -24, now()) "
                "GROUP BY step ORDER BY step",
            "B4_hourly_throughput_48h":
                # SAMPLE BY groups by the interval + non-aggregate columns
                "SELECT timestamp AS hour, step, count() AS events "
                "FROM pipeline_events "
                "WHERE timestamp >= dateadd('h', -48, now()) "
                "SAMPLE BY 1h",
            "B5_route_rollup_24h":
                "SELECT route, step, count_distinct(product_id) AS products, "
                "avg(duration_s) AS avg_step_s "
                "FROM pipeline_events "
                "WHERE event = 'stop' AND timestamp >= dateadd('h', -24, now()) "
                "GROUP BY route, step ORDER BY route, step",
        }

    def context_queries(self):
        return {
            "rows_48h": "SELECT count() AS n FROM pipeline_events "
                        "WHERE timestamp >= dateadd('h', -48, now())",
            "host_rows_6h": "SELECT count() AS n FROM host_metrics "
                            "WHERE timestamp >= dateadd('h', -6, now())",
            "disk_rows_6h": "SELECT count() AS n FROM disk_metrics "
                            "WHERE timestamp >= dateadd('h', -6, now())",
        }

    def scalar(self, q):
        _, js, _ = self._exec(q)
        ds = js.get("dataset") if js else None
        return ds[0][0] if ds and ds[0] else None

    def sample_product_ids(self, n=20):
        _, js, _ = self._exec(
            f"SELECT DISTINCT product_id FROM pipeline_events LIMIT {n}")
        if js and js.get("dataset"):
            return [row[0] for row in js["dataset"]]
        return []
