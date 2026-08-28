# Cluster-monitoring Grafana dashboard (timelakedb#117)

The cross-node, time-series view of a role-split cluster — the deep "how is
each tier doing" to the U3 console cluster view's at-a-glance "who's up"
(#111). Modelled on VictoriaMetrics' cluster dashboard: ingesters map to
vminsert, queriers to vmselect, the compactor + object store to vmstorage,
plus the router and a convergence row.

This is **separate from, not merged into**, the two other Grafana trees:

- `deploy/grafana/` — the single-node operator console, sourced from **that
  node's own `_system`** database over Flight SQL (U2).
- `fixtures/grafana/` — the FR-8 / AT-6 compatibility fixtures.

It lives in its own directory precisely so the console rig, which mounts the
whole `deploy/grafana/dashboards` dir, never scoops up a Prometheus dashboard
it has no datasource for.

## Run it

```sh
docker compose -f deploy/compose/timelakedb-cluster-s3.yml \
               -f deploy/compose/timelakedb-cluster-s3.monitoring.yml up -d --build
# Grafana  → http://localhost:3006  (admin/admin)
# Prometheus → http://localhost:9490
docker compose -f deploy/compose/timelakedb-cluster-s3.yml \
               -f deploy/compose/timelakedb-cluster-s3.monitoring.yml down -v
```

Build **both** files — a stale per-service image serves an older `/metrics`
and a node shows up mislabelled (the trap the U3 drill hit).

## Why Prometheus, not `_system`

The console dashboard reads each node's own `_system` over Flight SQL. That
path is down exactly when the cluster view is needed: a node whose query
engine is unhealthy still answers `/metrics` from atomics (CONSOLE.md §7.5),
and a CL-3 querier stores nothing in `_system` at all. So the cluster
dashboard scrapes `/metrics` — the robust, VM-native source. The exposition
carries no node/role label; Prometheus attaches them, one static target per
node (`prometheus.yml`), so every panel groups by `node` and filters by
`role`.

## What the metrics actually expose

The exposition is per-role, and the panels tolerate a role missing a series
rather than drawing a zero:

- **`config_revision`** — all five engine nodes; the router holds no engine
  and has none, so the convergence spread is over five, not six.
- **`catalog_head`** — every engine node's applied manifest head; the router
  holds no catalog and has none. The ingesters — the write front — expose it
  as of #123, so the convergence panels compare the read tier against the
  writers, not just followers against each other.
- **`compactions_total`** — ticks on the ingesters, which compact their own
  files; the compactor can read 0. The by-node panels attribute it honestly
  instead of assuming a tier.

## Verify

`deploy/compose/cluster-drill/cluster_dashboard_drill.sh` drives traffic
through the router and asserts, against Prometheus, that every tier's panel
query returns per-node series, that the convergence spread flips when a node
is advanced, and that CL-2 degraded lights up when an ingester dies. Evidence:
`docs/evidence/cluster-dashboard-drill.log`.

Images track the repo's `grafana:latest` convention; pin both Grafana and
Prometheus for a long-lived deployment.
