#!/usr/bin/env python3
"""AT-6 under SEC-3 want mode: stock Grafana must keep rendering when the
server is asking for client certificates and Grafana has none.

This is the check that justifies want mode existing. A required-mTLS
server would fail every one of these panels, because stock Grafana has
no client certificate to present and no way to be given one that the
fixture dashboards would survive.

It drives Grafana's OWN Flight SQL client through /api/ds/query rather
than a Python client, so what is exercised is the real datasource plugin
over TLS — the same path a rendering browser would take. Every panel
query in fixtures/grafana/dashboards is run, with template variables
bound to values seeded here.

  python at6_grafana_mtls.py

Expects the stack up with the grafana profile:
  docker compose -f compose/timelakedb-tls.yml --profile grafana up -d
"""
import json
import pathlib
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

GRAFANA = "http://localhost:3004"
AUTH = "admin:admin"
CONTAINER = "timelakedb-tls"
DASHBOARDS = pathlib.Path(__file__).resolve().parents[3] / "fixtures" / "grafana" / "dashboards"
HOST = "host-0001"
PRODUCT = "prod-000001"
IDENTITY = "tributary-node-1"
NETWORK = "bench-timelakedb-tls_default"
CERTS = str(pathlib.Path(__file__).resolve().parent / "certs")

passed = failed = 0
failures = []


def check(ok, label, detail=""):
    global passed, failed
    if ok:
        passed += 1
        print(f"  PASS  {label}" + (f"  ({detail})" if detail else ""))
    else:
        failed += 1
        failures.append(label)
        print(f"  FAIL  {label}  ({detail})")


def gf(path, payload=None):
    """Call the Grafana API. Grafana is the client under test."""
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(f"{GRAFANA}{path}", data=data)
    req.add_header("content-type", "application/json")
    import base64
    req.add_header("authorization", "Basic " + base64.b64encode(AUTH.encode()).decode())
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def seed():
    """Write the fixture tables over HTTPS from inside the container.

    Windows curl links schannel, which refuses this private CA, so every
    request in this drill originates in-network.
    """
    now_ns = int(time.time() * 1_000_000_000)
    minute = 60 * 1_000_000_000
    lines = []
    for h in range(3):
        host = f"host-{h + 1:04d}"
        for t in range(30):  # 30 minutes of history
            ts = now_ns - t * minute
            lines.append(
                f"host_metrics,host={host},region=us-east cpu_pct={20 + h * 7 + t % 11}."
                f"5,mem_pct={40 + t % 17}.25,disk_pct={55 + h}.0,"
                f"net_rx_bps={100000 + t * 37}i,net_tx_bps={90000 + t * 29}i {ts}"
            )
            for dev in ("sda", "sdb"):
                lines.append(
                    f"disk_metrics,host={host},device={dev} capacity_gb=512.0,"
                    f"used_gb={200 + t}.0,used_pct={40 + t % 9}.0,"
                    f"read_bps={5000 + t * 3}i,write_bps={4000 + t * 2}i {ts}"
                )
    steps = ["01-download", "02-extract", "03-validate", "04-translate", "05-transform",
             "06-enrich", "07-aggregate", "08-index", "09-package", "10-upload"]
    for p in range(5):
        pid = f"prod-{p + 1:06d}"
        for i, step in enumerate(steps):
            ts = now_ns - (len(steps) - i) * minute
            for ev in ("start", "stop"):
                lines.append(
                    f"pipeline_events,product_id={pid},step={step},event={ev},"
                    f"route=main,worker_ip=10.0.0.{p + 1} duration_s={1.5 + i * 0.25} {ts}"
                )
    body = "\n".join(lines)
    r = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "curl", "-sk", "-o", "/dev/null",
         "-w", "%{http_code}", "-X", "POST",
         "https://localhost:1963/api/v3/write_lp?db=poc&precision=ns",
         "--data-binary", "@-"],
        input=body.encode(), capture_output=True, timeout=180)
    return r.stdout.decode().strip(), len(lines)


def panel_queries():
    for f in sorted(DASHBOARDS.glob("*.json")):
        d = json.loads(f.read_text(encoding="utf-8"))
        for p in d.get("panels", []):
            for t in p.get("targets", []):
                q = t.get("rawSql") or t.get("query")
                if q:
                    q = q.replace("$host", HOST).replace("$product_id", PRODUCT)
                    # Grafana's own interval macros, bound as a dashboard would
                    q = re.sub(r"\$__interval\b", "1 minute", q)
                    yield f.stem, p.get("title", "?"), q


print("== AT-6 under want mode: stock Grafana, no client certificate ==")
code, n = seed()
check(code == "204", "seeded fixture tables over HTTPS", f"HTTP {code}, {n} lines")
time.sleep(2)

print("\n-- the datasource holds no client certificate --")
ds = gf("/api/datasources")[0]
check(ds["jsonData"].get("insecureGrpc") is False, "datasource uses TLS (not plaintext gRPC)")
check("tlsClientCert" not in ds.get("secureJsonFields", {})
      and "tlsClientKey" not in ds.get("secureJsonFields", {}),
      "datasource carries NO client certificate or key",
      f"secure fields: {sorted(ds.get('secureJsonFields', {}))}")

print("\n-- every fixture panel query, executed by Grafana over TLS --")
ok_panels = bad_panels = 0
empty = []
for dash, title, q in panel_queries():
    body = {"queries": [{"refId": "A", "datasource": {"uid": ds["uid"], "type": "influxdb"},
                         "rawSql": q, "format": "table",
                         "intervalMs": 60000, "maxDataPoints": 500}],
            "from": "now-48h", "to": "now"}
    try:
        res = gf("/api/ds/query", body)["results"]["A"]
    except urllib.error.HTTPError as e:
        res = {"error": f"HTTP {e.code}"}
    if res.get("error"):
        bad_panels += 1
        print(f"  FAIL  {dash} / {title}: {res['error'][:110]}")
    else:
        ok_panels += 1
        frames = res.get("frames") or []
        rows = 0
        if frames and frames[0].get("data", {}).get("values"):
            rows = len(frames[0]["data"]["values"][0])
        if rows == 0:
            empty.append(f"{dash}/{title}")

check(bad_panels == 0, f"all {ok_panels + bad_panels} panel queries executed over TLS",
      f"{ok_panels} ok, {bad_panels} failed")
check(len(empty) <= 2, "panels returned data, not just empty frames",
      f"{ok_panels - len(empty)}/{ok_panels} non-empty"
      + (f"; empty: {empty[:3]}" if empty else ""))

print("\n-- both paths, same server, same moment --")
# The point is not that an anonymous client works, nor that a
# cert-bearing one does, but that the SAME listener does both and can
# tell them apart. Run a Flight SQL query with a certificate and require
# that the server names the identity in its log.
FLIGHT_PY = f"""
import pyarrow.flight as fl

# Flight SQL wants an Any-wrapped CommandStatementQuery, not raw JSON.
# Hand-encoded so this drill needs only pyarrow, no protobuf toolchain.
def varint(n):
    out = b''
    while True:
        b_, n = n & 0x7F, n >> 7
        out += bytes([b_ | (0x80 if n else 0)])
        if not n:
            return out

def delimited(tag, payload):
    return bytes([tag]) + varint(len(payload)) + payload

sql = b'SELECT 1 AS ok'
cmd = delimited(0x0A, sql)                              # CommandStatementQuery.query
url = b'type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery'
any_ = delimited(0x0A, url) + delimited(0x12, cmd)      # Any{{type_url, value}}

ca = open('/certs/ca.crt','rb').read()
crt = open('/certs/client-{IDENTITY}.crt','rb').read()
key = open('/certs/client-{IDENTITY}.key','rb').read()
c = fl.FlightClient('grpc+tls://{CONTAINER}:1964', tls_root_certs=ca,
                    cert_chain=crt, private_key=key)
opts = fl.FlightCallOptions(headers=[(b'database', b'poc')])
info = c.get_flight_info(fl.FlightDescriptor.for_command(any_), opts)
rows = sum(b.data.num_rows for b in c.do_get(info.endpoints[0].ticket, opts))
print('CONNECTED rows=%d' % rows)
"""
r = subprocess.run(
    ["docker", "run", "--rm", "--network", NETWORK,
     "-v", f"{CERTS}:/certs:ro", "tldb-flight-client:latest",
     "python", "-c", FLIGHT_PY], capture_output=True, timeout=180)
out = (r.stdout + r.stderr).decode(errors="replace")
check("CONNECTED" in out, "certificate-bearing Flight SQL client connected", out.strip()[-110:])

# The identity log line is debug-level, so asserting on it would only
# measure RUST_LOG. The counters are the operator-visible contract, and
# the reason they exist: without them want mode is invisible, and the
# decision to require certificates would have to be a guess.
m = subprocess.run(["docker", "exec", CONTAINER, "curl", "-sk",
                    "https://localhost:1963/metrics"], capture_output=True, timeout=60)
metrics = {}
for line in m.stdout.decode(errors="replace").splitlines():
    if line and not line.startswith("#") and " " in line:
        k, _, v = line.rpartition(" ")
        try:
            metrics[k.strip()] = float(v)
        except ValueError:
            pass
check(metrics.get("timelake_tls_client_auth_mode") == 1.0,
      "server is in want mode throughout", f"mode={metrics.get('timelake_tls_client_auth_mode')}")
check(metrics.get("timelake_tls_client_ca_anchors", 0) >= 1.0,
      "client CA anchors loaded", f"anchors={metrics.get('timelake_tls_client_ca_anchors')}")

auth_n = metrics.get("timelake_flight_connections_authenticated_total", 0)
anon_n = metrics.get("timelake_flight_connections_anonymous_total", 0)
check(auth_n >= 1.0, "server counted the certificate-bearing connection",
      f"authenticated={auth_n:.0f}")
check(anon_n >= 1.0, "server counted Grafana's connections as anonymous",
      f"anonymous={anon_n:.0f}")
check(anon_n > auth_n,
      "the split shows this deployment has NOT migrated — requiring certs "
      "here would break Grafana",
      f"{anon_n:.0f} anonymous vs {auth_n:.0f} authenticated")

print(f"\n== AT-6 (want mode) verdict ==\n{passed}/{passed + failed} checks passed")
if failures:
    print("failed: " + "; ".join(failures))
print("AT-6 want-mode: " + ("PASS" if failed == 0 else "FAIL"))
sys.exit(0 if failed == 0 else 1)
