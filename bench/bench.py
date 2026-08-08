#!/usr/bin/env python3
"""tsdb-bench: run the product-pipeline + host-metrics workload against a
time-series database and record normalized performance metrics, so
solutions can be compared apples-to-apples.

  python bench.py run --backend influxdb3 --scale smoke --label idb3-smoke
  python bench.py run --backend influxdb3 --scale full  --label idb3-full
  python bench.py compare results/*/run.json
  python bench.py backends

Each run writes results/<run-id>/run.json (normalized metrics), plus
query_details.csv (every timed query) and resources.csv (server cpu/mem
per phase, from docker stats).
"""

import argparse
import os
import platform
import re
import subprocess
import sys
import time

import scenarios as sc
from backends import BACKENDS, get_backend
from monitor import ResourceMonitor, container_exists
from report import Reporter, load_runs, compare_markdown, compare_csv

HERE = os.path.dirname(os.path.abspath(__file__))

SCALES = {
    # harness shakeout on any machine, ~1 minute
    "smoke": dict(products_per_day=2_000, backfill_days=2, hosts=50,
                  host_hours=1.0, burst_size=5_000, batch_size=5_000, workers=4),
    # laptop dry run (matches the plan's guidance)
    "laptop": dict(products_per_day=100_000, backfill_days=2, hosts=300,
                   host_hours=6.0, burst_size=100_000, batch_size=10_000,
                   workers=4),
    # full-fidelity numbers on the evaluation box
    "full": dict(products_per_day=1_000_000, backfill_days=2, hosts=2_500,
                 host_hours=6.0, burst_size=100_000, batch_size=10_000,
                 workers=8),
}


def _docker(args_list, timeout=20):
    try:
        r = subprocess.run(["docker"] + args_list, capture_output=True,
                           text=True, timeout=timeout)
        return r.stdout.strip() if r.returncode == 0 else None
    except Exception:
        return None


def build_parser():
    p = argparse.ArgumentParser(prog="bench.py", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="run scenarios against one backend")
    r.add_argument("--backend", required=True, choices=sorted(BACKENDS))
    r.add_argument("--url", help="override backend URL")
    r.add_argument("--db", help="override database name")
    r.add_argument("--token", default=os.environ.get("BENCH_TOKEN", ""))
    r.add_argument("--scale", choices=sorted(SCALES), default="laptop",
                   help="preset workload size (default: laptop)")
    r.add_argument("--label", default="", help="short name shown in compare")
    r.add_argument("--scenarios", default="all",
                   help=f"comma list of {','.join(sc.DEFAULT_ORDER)} (or all)")
    r.add_argument("--container",
                   help="docker container to monitor/measure "
                        "(default: backend's; 'none' disables)")
    r.add_argument("--results-dir", default=os.path.join(HERE, "results"))
    # explicit overrides beat the scale preset
    r.add_argument("--products-per-day", type=int)
    r.add_argument("--backfill-days", type=int)
    r.add_argument("--steps", type=int, default=10)
    r.add_argument("--dropout-pct", type=float, default=2.0)
    r.add_argument("--hosts", type=int)
    r.add_argument("--host-hours", type=float)
    r.add_argument("--disk-devices", type=int, default=7,
                   help="disk devices per host in host history (default 7)")
    r.add_argument("--burst-size", type=int)
    r.add_argument("--batch-size", type=int)
    r.add_argument("--workers", type=int)
    r.add_argument("--shape-a-samples", type=int, default=20)
    r.add_argument("--shape-b-repeats", type=int, default=3)

    c = sub.add_parser("compare", help="side-by-side table from run.json files")
    c.add_argument("runs", nargs="+", help="run.json paths, run dirs, or globs")
    c.add_argument("--out", help="also write markdown to this file")
    c.add_argument("--csv", help="also write CSV to this file")

    sub.add_parser("backends", help="list available backends")
    return p


def resolve_config(args):
    preset = dict(SCALES[args.scale])
    for k in preset:
        v = getattr(args, k, None)
        if v is not None:
            preset[k] = v
    for k, v in preset.items():
        setattr(args, k, v)
    if args.scenarios == "all":
        args.scenario_list = list(sc.DEFAULT_ORDER)
    else:
        args.scenario_list = [s.strip() for s in args.scenarios.split(",")
                              if s.strip()]
        unknown = [s for s in args.scenario_list if s not in sc.SCENARIOS]
        if unknown:
            sys.exit(f"unknown scenario(s): {', '.join(unknown)}")
    return args


def cmd_run(args):
    cfg = resolve_config(args)
    cls = get_backend(cfg.backend)
    be = cls(url=cfg.url, db=cfg.db, token=cfg.token)

    label = re.sub(r"[^A-Za-z0-9._-]+", "-", cfg.label) if cfg.label else ""
    run_id = "-".join(x for x in
                      [cfg.backend, label, time.strftime("%Y%m%d-%H%M%S")] if x)
    run_dir = os.path.join(cfg.results_dir, run_id)

    print(f"tsdb-bench run: {run_id}")
    print(f"  backend   {be.display} @ {be.url} (db={be.db})")
    print(f"  scale     {cfg.scale}: {cfg.products_per_day:,} products/day x "
          f"{cfg.backfill_days}d, {cfg.hosts} hosts x {cfg.host_hours}h, "
          f"burst {cfg.burst_size:,}")
    print(f"  scenarios {', '.join(cfg.scenario_list)}")
    if be.untested:
        print(f"  WARNING: the {be.name} adapter's queries are best-effort "
              f"translations - validate row counts against a reference "
              f"backend at smoke scale before trusting comparisons.")

    if not be.healthy():
        sys.exit(f"backend not reachable at {be.url} - is the container up? "
                 f"(see compose files / influxdb3-poc)")
    be.prepare()

    container = cfg.container or be.default_container
    if container == "none":
        container = None
    if container and not container_exists(container):
        print(f"  note: container '{container}' not found - resource "
              f"monitoring and storage measurement disabled "
              f"(use --container to point at the right name)")
        container = None
    cfg.container = container

    mon = None
    if container:
        mon = ResourceMonitor(container)
        mon.start()
        needs_baseline = any(s in cfg.scenario_list
                             for s in ("ingest", "hosts", "burst"))
        if needs_baseline:
            print("  sampling baseline memory (6s)...")
            time.sleep(6)

    rep = Reporter(run_dir, run_id)
    t0 = time.perf_counter()
    for name in cfg.scenario_list:
        sc.SCENARIOS[name](be, cfg, rep, mon)
    if mon:
        mon.set_phase("settle")
        time.sleep(8)
        mon.stop()
        mon.join(timeout=10)

    backend_info = {
        "backend": be.name,
        "backend_display": be.display,
        "backend_version": be.version(),
        "target": {"url": be.url, "db": be.db, "container": container,
                   "image": _docker(["inspect", "--format", "{{.Config.Image}}",
                                     container]) if container else None},
    }
    config = {
        "scale": cfg.scale, "label": cfg.label or None,
        "products_per_day": cfg.products_per_day,
        "backfill_days": cfg.backfill_days, "steps": cfg.steps,
        "dropout_pct": cfg.dropout_pct, "hosts": cfg.hosts,
        "host_hours": cfg.host_hours, "disk_devices": cfg.disk_devices,
        "burst_size": cfg.burst_size,
        "batch_size": cfg.batch_size, "workers": cfg.workers,
        "shape_a_samples": cfg.shape_a_samples,
        "shape_b_repeats": cfg.shape_b_repeats,
        "scenarios": cfg.scenario_list,
    }
    mem_total = _docker(["info", "--format", "{{.MemTotal}}"])
    environment = {
        "platform": platform.platform(),
        "host_cpu_count": os.cpu_count(),
        "docker_mem_total_bytes": int(mem_total) if (mem_total or "").isdigit()
                                  else mem_total,
    }
    path = rep.finalize(backend_info, config, environment, mon)
    print(f"\nRun complete in {time.perf_counter() - t0:,.0f}s")
    print(f"  {path}")
    print(f"  compare runs with: python bench.py compare "
          f"{os.path.join(cfg.results_dir, '*', 'run.json')}")


def cmd_compare(args):
    runs = load_runs(args.runs)
    if not runs:
        sys.exit("no run.json files found")
    runs.sort(key=lambda r: r.get("started_at", ""))
    table = compare_markdown(runs)
    print(table)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(f"# tsdb-bench comparison ({len(runs)} runs)\n\n"
                    + table + "\n")
        print(f"\nwrote {args.out}")
    if args.csv:
        compare_csv(runs, args.csv)
        print(f"wrote {args.csv}")


def cmd_backends(_args):
    for name in sorted(BACKENDS):
        cls = BACKENDS[name]
        flag = "  (best-effort: validate queries)" if cls.untested else ""
        print(f"  {name:<16} {cls.display:<18} {cls.default_url}{flag}")


def main():
    args = build_parser().parse_args()
    {"run": cmd_run, "compare": cmd_compare, "backends": cmd_backends}[args.cmd](args)


if __name__ == "__main__":
    main()
