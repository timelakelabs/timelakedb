#!/usr/bin/env python3
"""Normalized results: one run.json per benchmark run + cross-run comparison.

run.json is the contract that makes solutions comparable — every backend
run, at any scale, produces the same metric keys. `bench.py compare`
renders any set of run.json files side by side.
"""

import csv
import glob
import json
import os
import time

FRAMEWORK = "tsdb-bench/1.0"


class Reporter:
    def __init__(self, run_dir, run_id):
        self.run_dir = run_dir
        self.run_id = run_id
        self.started_at = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.metrics = {}
        self.query_rows = [["test", "run", "ms", "rows", "status", "error"]]
        self.notes = []
        os.makedirs(run_dir, exist_ok=True)

    def add_metric(self, section, data):
        self.metrics[section] = data

    def add_query_row(self, test, run, ms, rows, status, error=""):
        self.query_rows.append([test, run, f"{ms:.0f}",
                                rows if rows is not None else "", status,
                                (error or "")[:500]])

    def note(self, msg):
        self.notes.append(msg)
        print(f"  note: {msg}")

    def finalize(self, backend_info, config, environment, monitor=None):
        if monitor is not None:
            res = monitor.summary()
            if res:
                self.add_metric("resources", res)
            monitor.write_csv(os.path.join(self.run_dir, "resources.csv"))
        with open(os.path.join(self.run_dir, "query_details.csv"),
                  "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(self.query_rows)
        doc = {
            "framework": FRAMEWORK,
            "run_id": self.run_id,
            **backend_info,
            "started_at": self.started_at,
            "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "config": config,
            "environment": environment,
            "metrics": self.metrics,
            "notes": self.notes,
        }
        path = os.path.join(self.run_dir, "run.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2)
        return path


# ── comparison ───────────────────────────────────────────────────────────────

def load_runs(paths):
    """Accepts run.json paths, run dirs, or globs; returns list of dicts."""
    runs = []
    for p in paths:
        expanded = glob.glob(p) or [p]
        for e in expanded:
            if os.path.isdir(e):
                e = os.path.join(e, "run.json")
            if os.path.isfile(e):
                with open(e, encoding="utf-8") as f:
                    runs.append(json.load(f))
            else:
                print(f"  warning: no run.json at {p}")
    return runs


def _g(run, *keys, default=None):
    cur = run
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _num(x, nd=0, suffix=""):
    if x is None:
        return "-"
    try:
        return f"{x:,.{nd}f}{suffix}"
    except (TypeError, ValueError):
        return str(x)


def _cold_warm(qm):
    if not qm:
        return "-"
    c, w = qm.get("cold_ms"), qm.get("warm_ms")
    err = qm.get("errors", 0)
    s = f"{_num(c)} / {_num(w)}"
    return s + (f"  [{err} ERR]" if err else "")


def compare_rows(runs):
    """[(metric_label, [value_per_run])] over the union of shape-B queries."""
    b_names = []
    for r in runs:
        for name in (_g(r, "metrics", "query_shape_b", "queries") or {}):
            if name not in b_names:
                b_names.append(name)

    rows = [
        ("Backend", lambda r: _g(r, "backend_display") or _g(r, "backend", default="?")),
        ("Version", lambda r: _g(r, "backend_version") or "-"),
        ("Run", lambda r: _g(r, "run_id", default="-")),
        ("Date", lambda r: (_g(r, "started_at") or "-")[:16]),
        ("Scale (products/day x days)",
         lambda r: f"{_num(_g(r, 'config', 'products_per_day'))} x "
                   f"{_num(_g(r, 'config', 'backfill_days'))}"),
        ("Hosts x history h",
         lambda r: f"{_num(_g(r, 'config', 'hosts'))} x "
                   f"{_num(_g(r, 'config', 'host_hours'))}"),
        ("Ingest lines/s", lambda r: _num(_g(r, "metrics", "ingest", "lines_per_s"))),
        ("Ingest wall s", lambda r: _num(_g(r, "metrics", "ingest", "wall_s"), 1)),
        ("Ingest errors", lambda r: _num(_g(r, "metrics", "ingest", "errors"))),
        ("Ingest batch p95 ms", lambda r: _num(_g(r, "metrics", "ingest", "batch_p95_ms"))),
        ("Host ingest lines/s", lambda r: _num(_g(r, "metrics", "host_ingest", "lines_per_s"))),
        ("Burst events", lambda r: _num(_g(r, "metrics", "burst", "events"))),
        ("Burst wall s", lambda r: _num(_g(r, "metrics", "burst", "wall_s"), 2)),
        ("Burst events/s", lambda r: _num(_g(r, "metrics", "burst", "events_per_s"))),
        ("Burst errors", lambda r: _num(_g(r, "metrics", "burst", "errors"))),
        ("Query during burst ms",
         lambda r: _num(_g(r, "metrics", "burst", "concurrent_query_ms"))
         + ("" if _g(r, "metrics", "burst", "concurrent_query_ok") in (True, None)
            else "  [ERR]")),
        ("Shape A n", lambda r: _num(_g(r, "metrics", "query_shape_a", "n"))),
        ("Shape A median ms", lambda r: _num(_g(r, "metrics", "query_shape_a", "median_ms"))),
        ("Shape A p95 ms", lambda r: _num(_g(r, "metrics", "query_shape_a", "p95_ms"))),
        ("Shape A errors", lambda r: _num(_g(r, "metrics", "query_shape_a", "errors"))),
    ]
    for name in b_names:
        rows.append((f"{name} cold/warm ms",
                     lambda r, n=name: _cold_warm(
                         _g(r, "metrics", "query_shape_b", "queries", n))))
    rows += [
        ("Shape B all completed",
         lambda r: {True: "YES", False: "NO"}.get(
             _g(r, "metrics", "query_shape_b", "all_completed"), "-")),
        ("Baseline mem MB", lambda r: _num(_g(r, "metrics", "resources", "baseline_mem_mb"))),
        ("Peak mem ingest MB",
         lambda r: _num(_g(r, "metrics", "resources", "phases", "ingest", "peak_mem_mb"))),
        ("Peak mem shape B MB",
         lambda r: _num(_g(r, "metrics", "resources", "phases", "query_b", "peak_mem_mb"))),
        ("Peak mem burst MB",
         lambda r: _num(_g(r, "metrics", "resources", "phases", "burst", "peak_mem_mb"))),
        ("Mem returned to baseline",
         lambda r: {True: "YES", False: "NO"}.get(
             _g(r, "metrics", "resources", "returned_to_baseline"), "-")),
        ("Storage GB total", lambda r: _num(_g(r, "metrics", "storage", "gb_total"), 2)),
        ("Storage GB/day", lambda r: _num(_g(r, "metrics", "storage", "gb_per_day"), 2)),
        ("Projected 90d GB", lambda r: _num(_g(r, "metrics", "storage", "projected_gb_90d"), 1)),
        ("Projected 365d GB", lambda r: _num(_g(r, "metrics", "storage", "projected_gb_365d"), 1)),
    ]
    return [(label, [fn(r) for r in runs]) for label, fn in rows]


def compare_markdown(runs):
    headers = ["Metric"] + [(_g(r, "config", "label") or _g(r, "run_id", default="run"))
                            for r in runs]
    rows = compare_rows(runs)
    widths = [max(len(headers[0]), *(len(lbl) for lbl, _ in rows))]
    for i in range(len(runs)):
        widths.append(max(len(headers[i + 1]), *(len(v[i]) for _, v in rows)))
    out = ["| " + " | ".join(h.ljust(w) for h, w in zip(headers, widths)) + " |",
           "|-" + "-|-".join("-" * w for w in widths) + "-|"]
    for lbl, vals in rows:
        cells = [lbl.ljust(widths[0])] + [v.ljust(widths[i + 1])
                                          for i, v in enumerate(vals)]
        out.append("| " + " | ".join(cells) + " |")
    return "\n".join(out)


def compare_csv(runs, path):
    headers = ["metric"] + [(_g(r, "config", "label") or _g(r, "run_id", default="run"))
                            for r in runs]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for lbl, vals in compare_rows(runs):
            w.writerow([lbl] + vals)
