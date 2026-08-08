#!/usr/bin/env python3
"""InfluxDB 3 (Core/Enterprise) adapter — the reference backend.

Endpoints and SQL are identical to influxdb3-poc (write_lp + query_sql);
the shape queries are copied verbatim from queries/run_query_tests.py so
framework numbers stay comparable with the compose-stack harness.
"""

import time

from .base import Backend


class InfluxDB3(Backend):
    name = "influxdb3"
    display = "InfluxDB 3 Core"
    default_url = "http://localhost:8181"
    default_db = "poc"
    default_container = "influxdb3"
    data_dir = "/var/lib/influxdb3/data"

    def prepare(self):
        try:
            self.session().post(f"{self.url}/api/v3/configure/database",
                                json={"db": self.db}, timeout=15)
        except Exception:
            pass  # server auto-creates on write anyway

    def healthy(self):
        try:
            r = self.session().get(f"{self.url}/health", timeout=5)
            return r.status_code // 100 == 2
        except Exception:
            return False

    def version(self):
        try:
            r = self.session().get(f"{self.url}/ping", timeout=5)
            try:
                return r.json().get("version")
            except Exception:
                return r.headers.get("x-influxdb-version")
        except Exception:
            return None

    def _write_once(self, body, timeout):
        return self.session().post(
            f"{self.url}/api/v3/write_lp",
            params={"db": self.db},
            data=body,
            headers={"Content-Type": "text/plain; charset=utf-8"},
            timeout=timeout,
        )

    def _query_rows(self, sql, timeout=300):
        t0 = time.perf_counter()
        try:
            r = self.session().post(f"{self.url}/api/v3/query_sql",
                                    json={"db": self.db, "q": sql, "format": "json"},
                                    timeout=timeout)
            dt = time.perf_counter() - t0
            if r.status_code // 100 != 2:
                return dt, None, f"HTTP {r.status_code}: {r.text[:300]}"
            return dt, r.json(), None
        except Exception as e:
            return time.perf_counter() - t0, None, repr(e)

    def query(self, q, timeout=300):
        dt, rows, err = self._query_rows(q, timeout)
        return dt, (len(rows) if rows is not None else None), err

    def shape_a_query(self, product_id):
        return (f"SELECT time, step, event, route, worker_ip, duration_s "
                f"FROM pipeline_events "
                f"WHERE product_id = '{product_id}' "
                f"AND time >= now() - INTERVAL '2 days' "
                f"ORDER BY time")

    def shape_b_queries(self):
        return {
            "B1_funnel_24h":
                "SELECT step, COUNT(DISTINCT product_id) AS products "
                "FROM pipeline_events "
                "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' "
                "GROUP BY step ORDER BY step",
            "B2_funnel_48h":
                "SELECT step, COUNT(DISTINCT product_id) AS products "
                "FROM pipeline_events "
                "WHERE event = 'stop' AND time >= now() - INTERVAL '48 hours' "
                "GROUP BY step ORDER BY step",
            "B3_inflight_24h":
                "SELECT step, "
                "SUM(CASE WHEN event = 'start' THEN 1 ELSE 0 END) "
                " - SUM(CASE WHEN event = 'stop' THEN 1 ELSE 0 END) AS in_flight "
                "FROM pipeline_events "
                "WHERE time >= now() - INTERVAL '24 hours' "
                "GROUP BY step ORDER BY step",
            "B4_hourly_throughput_48h":
                "SELECT date_bin(INTERVAL '1 hour', time) AS hour, step, "
                "COUNT(*) AS events "
                "FROM pipeline_events "
                "WHERE time >= now() - INTERVAL '48 hours' "
                "GROUP BY 1, step ORDER BY 1, step",
            "B5_route_rollup_24h":
                "SELECT route, step, COUNT(DISTINCT product_id) AS products, "
                "AVG(duration_s) AS avg_step_s "
                "FROM pipeline_events "
                "WHERE event = 'stop' AND time >= now() - INTERVAL '24 hours' "
                "GROUP BY route, step ORDER BY route, step",
        }

    def context_queries(self):
        return {
            "rows_48h": "SELECT COUNT(*) AS n FROM pipeline_events "
                        "WHERE time >= now() - INTERVAL '48 hours'",
            "host_rows_6h": "SELECT COUNT(*) AS n FROM host_metrics "
                            "WHERE time >= now() - INTERVAL '6 hours'",
            "disk_rows_6h": "SELECT COUNT(*) AS n FROM disk_metrics "
                            "WHERE time >= now() - INTERVAL '6 hours'",
        }

    def scalar(self, q):
        _, rows, err = self._query_rows(q)
        if rows and isinstance(rows[0], dict):
            vals = list(rows[0].values())
            return vals[0] if vals else None
        return None

    def sample_product_ids(self, n=20):
        for where in ("WHERE time >= now() - INTERVAL '6 hours'",
                      "WHERE time >= now() - INTERVAL '2 days'", ""):
            _, rows, err = self._query_rows(
                f"SELECT DISTINCT product_id FROM pipeline_events {where} LIMIT {n}")
            if rows:
                return [r["product_id"] for r in rows]
        return []
