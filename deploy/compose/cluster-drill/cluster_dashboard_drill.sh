#!/bin/sh
# Cluster-dashboard drill (timelakedb#117 gate).
#
# Proves the Grafana cluster dashboard has real data to draw: Prometheus
# scrapes every node's /metrics relabeled by node+role, and the PromQL each
# tier's panels use returns per-node series. It asserts against PROMETHEUS,
# not Grafana pixels — if the query the panel is built on returns the series,
# the panel draws it. That is also the headless-honest check: a panel can be
# green because a query is wrong and returns nothing "successfully".
#
#   Tiers populate   — overview up=6; ingester lines/s on BOTH; router
#                       forwarded/s; querier query rate; CL-2 replication;
#                       object-store puts (a flush actually happened).
#   Provisioning     — Grafana loaded the dashboard and the Prometheus
#                       datasource by uid.
#   Convergence      — spread is 0 with every node level, then flips to 1 the
#                       moment one node's applied config is advanced (#109
#                       config_revision) — the panel that flags a node behind.
#   CL-2 degraded    — kill an ingester and its peer's timelake_cl2_degraded
#                       lights up (PR-7), and up{role="ingester"} drops to 1.
#
# Bring the cluster up WITH the monitoring overlay first (build both files, or
# a stale per-service image serves an old /metrics — see the U3 drill):
#
#   docker compose -f deploy/compose/timelakedb-cluster-s3.yml \
#                  -f deploy/compose/timelakedb-cluster-s3.monitoring.yml \
#                  up -d --build
#   # wait for the six nodes healthy, then:
#   sh deploy/compose/cluster-drill/cluster_dashboard_drill.sh \
#       | tee docs/evidence/cluster-dashboard-drill.log
#   docker compose -f deploy/compose/timelakedb-cluster-s3.yml \
#                  -f deploy/compose/timelakedb-cluster-s3.monitoring.yml down -v
#
# Needs docker + curl on the host; JSON parsing is pure sh. Re-run against a
# FRESH rig (down -v) — the convergence leg rotates ingester-a's admin
# password, which persists in the shared store.
set -eu

PROM=http://localhost:9490
GRAFANA=http://localhost:3006
ROUTER=cl3-router
ING_A=cl3-ingester-a
ING_B=cl3-ingester-b
QRY_A=cl3-querier-a
RUN=$(date +%s)

pass=0; fail=0
ck() { if [ "$1" = "$2" ]; then echo "  PASS  $3"; pass=$((pass+1));
       else echo "  FAIL  $3 (got '$1' want '$2')"; fail=$((fail+1)); fi; }

# Prometheus scalar value for an expression — wrapped in scalar() so the
# result is [ts,"value"] and extracts with one sed, no json tool.
pv() { curl -s "$PROM/api/v1/query" --data-urlencode "query=scalar($1)" \
       | sed -n 's/.*"result":\[[0-9.]*,"\([^"]*\)"\].*/\1/p'; }

# Poll a scalar expression until it equals want, up to $2 seconds.
pv_wait() {  # <expr> <secs> <want>
  s=$(date +%s); got=""
  while [ "$(( $(date +%s) - s ))" -lt "$2" ]; do
    got=$(pv "$1"); [ "$got" = "$3" ] && break; sleep 2
  done
  printf '%s' "$got"
}

# Write LP through the router across N measurements so the shard hash spreads
# them over both ingesters.
drive_writes() {  # <n_measurements> <rows_each>
  m=0
  while [ "$m" -lt "$1" ]; do
    seq 1 "$2" | awk -v M="$m" '{print "sensor"M",host=h"($1%50)" v="$1"i"}' \
      | docker exec -i "$ROUTER" curl -s -o /dev/null \
          -X POST "http://localhost:1963/api/v3/write_lp?db=poc&precision=ns" \
          --data-binary @-
    m=$((m+1))
  done
}

# Admin session on ingester-a (loopback listener), for the config bump.
adm() { docker exec "$ING_A" curl -s "$@"; }
CSRF=""
login() {
  body=$(adm -c /tmp/cj -X POST http://localhost:1966/admin/session \
              -H 'content-type: application/json' \
              -d "{\"username\":\"admin\",\"password\":\"$1\"}")
  CSRF=$(printf '%s' "$body" | sed -n 's/.*"csrf":"\([^"]*\)".*/\1/p')
}

echo "== cluster-dashboard drill =="

echo "-- phase 1: drive traffic through the router + a querier --"
drive_writes 6 3000
for m in 0 1 2 3 4 5; do
  docker exec "$QRY_A" curl -s -X POST http://localhost:1963/api/sql \
    -H 'content-type: application/json' \
    -d "{\"db\":\"poc\",\"sql\":\"SELECT COUNT(*) AS n FROM sensor$m\"}" >/dev/null
done
echo "   driven; waiting for a flush (flush_age 15s) + scrapes"
sleep 20

echo "-- phase 2: Grafana provisioned the dashboard + datasource --"
ck "$(curl -s -u admin:admin "$GRAFANA/api/dashboards/uid/timelakedb-cluster" \
       | grep -c '"uid": *"timelakedb-cluster"')" 1 "cluster dashboard provisioned"
ck "$(curl -s -u admin:admin "$GRAFANA/api/datasources/uid/timelakedb-prometheus" \
       | grep -c '"uid":"timelakedb-prometheus"')" 1 "Prometheus datasource provisioned"

echo "-- phase 3: every tier has per-node data --"
ck "$(pv 'sum(up)')" 6 "overview: 6 targets up"
ck "$(pv 'sum(rate(timelake_lines_written_total[1m])) > bool 0')" 1 "overview: cluster ingest > 0"
ck "$(pv 'count(rate(timelake_lines_written_total{role="ingester"}[1m]) > 0)')" 2 "ingesters: both writing"
ck "$(pv 'sum(rate(timelake_cl2_replicated_total[1m])) > bool 0')" 1 "ingesters: CL-2 replication flowing"
ck "$(pv 'sum(rate(timelake_router_forwarded_total[1m])) > bool 0')" 1 "router: forwarding writes"
ck "$(pv 'sum(rate(timelake_queries_total{role="querier"}[1m])) > bool 0')" 1 "queriers: serving reads"
ck "$(pv 'sum(rate(timelake_s3_put_total[1m])) > bool 0')" 1 "object store: puts (a flush landed)"

echo "-- phase 4: convergence panel flags a node held behind --"
ck "$(pv 'count(timelake_config_revision)')" 5 "config_revision on all 5 engine nodes (router has none)"
ck "$(pv 'count(timelake_catalog_head)')" 5 "catalog_head on all 5 engine nodes (#123: was followers-only)"
ck "$(pv 'max(timelake_config_revision) - min(timelake_config_revision)')" 0 "baseline: converged (spread 0)"
login admin
adm -b /tmp/cj -X POST http://localhost:1966/admin/password \
    -H 'content-type: application/json' -H "x-timelake-csrf: $CSRF" \
    -d '{"current_password":"admin","new_password":"drill console password"}' >/dev/null
login "drill console password"
adm -b /tmp/cj -X PUT http://localhost:1966/admin/config/gc_grace_secs \
    -H 'content-type: application/json' -H "x-timelake-csrf: $CSRF" \
    -d '{"value":1500}' >/dev/null
ck "$(pv_wait 'max(timelake_config_revision) - min(timelake_config_revision)' 15 1)" 1 \
   "after advancing ingester-a: spread flips to 1 (flagged)"
ck "$(pv 'timelake_config_revision{node="ingester-a"}')" 1 "ingester-a is the node ahead (rev 1)"

echo "-- phase 5: CL-2 degraded lights up when an ingester dies --"
docker stop "$ING_B" >/dev/null
# a write on the survivor makes its replicator notice the peer is gone
seq 1 200 | awk '{print "degr,host=h"($1%20)" v="$1"i"}' \
  | docker exec -i "$ING_A" curl -s -o /dev/null \
      -X POST "http://localhost:1963/api/v3/write_lp?db=poc&precision=ns" --data-binary @-
ck "$(pv_wait 'timelake_cl2_degraded{node="ingester-a"}' 20 1)" 1 \
   "ingester-a raises CL-2 degraded with its peer down"
ck "$(pv_wait 'sum(up{role="ingester"})' 20 1)" 1 "up{role=ingester} drops to 1"

echo "== cluster-dashboard drill: $pass passed, $fail failed =="
test "$fail" -eq 0
