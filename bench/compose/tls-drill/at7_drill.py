#!/usr/bin/env python3
"""AT-7: certificate rotation drill (REQUIREMENTS.md SEC-3).

Under sustained writes and a long-running Flight SQL query, rotate to a
fresh 24 h cert. Pass: zero dropped connections, zero write errors, the
in-flight query completes, and the next new connection presents the new
certificate. Then repeat with a deliberately corrupt renewal: the server
keeps serving on the last-good cert and raises the SEC-3 alarm.

Run from bench/:  python compose/tls-drill/at7_drill.py
Prereqs: gen-certs.sh initial + the timelorddb-tls compose stack up.
"""

import shutil
import ssl
import subprocess
import sys
import threading
import time
from pathlib import Path

import requests
import urllib3

HERE = Path(__file__).parent
CERTS = HERE / "certs"
RUN_TAG = f"r{int(time.time())}"  # rerun-safe: this run's writes are countable in isolation
CA = str(CERTS / "ca.crt")
BASE = "https://localhost:2963"
FLIGHT = "grpc+tls://localhost:2964"
CONTAINER = "timelorddb-tls"
TELEGRAF = "timelord-telegraf-tls"

results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = ""):
    results.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}" + (f"  ({detail})" if detail else ""))


def serving_serial(port: int) -> str:
    """Open a NEW TLS connection and return the presented cert's serial."""
    ctx = ssl.create_default_context(cafile=CA)
    with ctx.wrap_socket(
        __import__("socket").create_connection(("localhost", port), timeout=5),
        server_hostname="localhost",
    ) as s:
        cert = s.getpeercert()
        assert cert is not None
        return str(cert["serialNumber"])


def tls_version(port: int) -> str:
    ctx = ssl.create_default_context(cafile=CA)
    with ctx.wrap_socket(
        __import__("socket").create_connection(("localhost", port), timeout=5),
        server_hostname="localhost",
    ) as s:
        return s.version() or "?"


def metrics() -> dict[str, float]:
    out = {}
    for line in requests.get(f"{BASE}/metrics", verify=CA, timeout=10).text.splitlines():
        if line and not line.startswith("#"):
            k, _, v = line.partition(" ")
            try:
                out[k] = float(v)
            except ValueError:
                pass
    return out


def sql(query: str):
    r = requests.post(f"{BASE}/api/sql", json={"db": "poc", "sql": query}, verify=CA, timeout=120)
    r.raise_for_status()
    return r.json()


# --- Flight SQL client (hand-encoded FlightSQL protobufs; pyarrow.flight
# has no FlightSQL helpers and the drill shouldn't drag in a driver). ---

def _varint(n: int) -> bytes:
    out = b""
    while True:
        b7 = n & 0x7F
        n >>= 7
        out += bytes([b7 | (0x80 if n else 0)])
        if not n:
            return out


def _field(no: int, payload: bytes) -> bytes:
    return _varint((no << 3) | 2) + _varint(len(payload)) + payload


def flight_sql(query: str):
    """Run one statement over Flight SQL/TLS; returns (row_count, first_value)."""
    import pyarrow.flight as flight

    cmd = _field(1, query.encode())  # CommandStatementQuery.query = 1
    any_cmd = _field(1, b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery") + _field(2, cmd)
    client = flight.FlightClient(FLIGHT, tls_root_certs=Path(CA).read_bytes())
    opts = flight.FlightCallOptions(headers=[(b"database", b"poc")], timeout=600)
    info = client.get_flight_info(flight.FlightDescriptor.for_command(any_cmd), opts)
    table = client.do_get(info.endpoints[0].ticket, opts).read_all()
    client.close()
    first = table.column(0)[0].as_py() if table.num_rows else None
    return table.num_rows, first


class Writer(threading.Thread):
    """Sustained line-protocol writes over HTTPS on a keep-alive session —
    a dropped established connection surfaces as a failure here."""

    def __init__(self):
        super().__init__(daemon=True)
        self.ok = 0
        self.failed = 0
        self.errors: list[str] = []
        self.stop = threading.Event()

    def run(self):
        s = requests.Session()
        i = 0
        while not self.stop.is_set():
            i += 1
            try:
                r = s.post(
                    f"{BASE}/api/v2/write?bucket=poc",
                    data=f"drill_writes,leg={RUN_TAG} seq={i}i",
                    verify=CA,
                    timeout=10,
                )
                if r.status_code == 204:
                    self.ok += 1
                else:
                    self.failed += 1
                    self.errors.append(f"HTTP {r.status_code}: {r.text[:100]}")
            except Exception as e:  # noqa: BLE001 — any transport error is a drill failure
                self.failed += 1
                self.errors.append(repr(e)[:150])
            time.sleep(0.1)


def rotate_files(cert_src: str, key_src: str):
    """Overwrite the watched files the way a certbot deploy hook would:
    key first, then cert; the server's debounce covers the tear window."""
    shutil.copyfile(CERTS / key_src, CERTS / "server.key")
    shutil.copyfile(CERTS / cert_src, CERTS / "server.crt")


def docker_logs(name: str, since: str) -> str:
    p = subprocess.run(
        ["docker", "logs", name, "--since", since],
        capture_output=True, text=True, timeout=30,
    )
    return p.stdout + p.stderr


def main():
    urllib3.disable_warnings()
    t0 = time.strftime("%Y-%m-%dT%H:%M:%S")

    print("== preflight ==")
    health = requests.get(f"{BASE}/health", verify=CA, timeout=10).json()
    check(health.get("status") == "pass", "health over TLS", str(health.get("status")))
    v = tls_version(2963)
    check(v == "TLSv1.3", "TLS 1.3 negotiated (default floor)", v)
    m = metrics()
    exp = m.get("timelord_tls_cert_expiry_seconds", -1)
    check(0 < exp <= 86400, "expiry gauge ~24h", f"{exp:.0f}s")
    check(m.get("timelord_tls_last_reload_ok") == 1.0, "reload-ok gauge starts 1")

    serial_http_0 = serving_serial(2963)
    serial_flight_0 = serving_serial(2964)
    check(serial_http_0 == serial_flight_0, "both listeners serve the same cert", serial_http_0)

    # Seed rows for the long cross-join query, then calibrate its size to
    # ~25 s so the rotation demonstrably lands mid-flight.
    print("== seed + calibrate long query ==")
    lines = "\n".join(f"drill_m,src=seed v={i}i {1754600000000000000 + i}" for i in range(1500))
    r = requests.post(f"{BASE}/api/v2/write?bucket=poc", data=lines, verify=CA, timeout=30)
    assert r.status_code == 204, r.text
    # SUM over the join values — COUNT(*) is answered from cardinality
    # statistics without materializing, but this evaluates every combo
    # (measured ~11B combos/s on this box, so a fourth leg scales the
    # runtime). Sum over all n^3*m tuples of (va+vb+vc+vd) with v=0..k-1:
    #   m * 3n^2 * S(n) + n^3 * S(m),  S(k)=k(k-1)/2 — exactly checkable.
    N = 1500

    def q(n, m):
        return (
            f"SELECT SUM(a.v + b.v + c.v + d.v) AS s "
            f"FROM (SELECT v FROM drill_m LIMIT {n}) a "
            f"CROSS JOIN (SELECT v FROM drill_m LIMIT {n}) b "
            f"CROSS JOIN (SELECT v FROM drill_m LIMIT {n}) c "
            f"CROSS JOIN (SELECT v FROM drill_m LIMIT {m}) d"
        )

    def expected(n, m):
        s = lambda k: k * (k - 1) // 2  # noqa: E731
        return m * 3 * n * n * s(n) + n**3 * s(m)

    _, val = flight_sql(q(300, 1))
    assert val == expected(300, 1), f"calibration sum wrong: {val}"
    t = time.time()
    _, val = flight_sql(q(N, 1))
    t_full = time.time() - t
    assert val == expected(N, 1), f"calibration sum wrong at N: {val}"
    rate = N**3 / max(t_full, 0.05)
    m = max(1, min(1500, int(rate * 25 / N**3)))
    n = N
    print(f"  calibration: {N**3/1e9:.2f}B combos in {t_full:.2f}s -> 4th leg m={m} "
          f"(~{n**3*m/1e9:.0f}B combos, target ~25s)")

    print("== leg 1: rotate under load ==")
    writer = Writer()
    writer.start()
    qresult: dict = {}

    def long_query():
        qresult["start"] = time.time()
        try:
            _, v = flight_sql(q(n, m))
            qresult["value"] = v
        except Exception as e:  # noqa: BLE001
            qresult["error"] = repr(e)
        qresult["end"] = time.time()

    qt = threading.Thread(target=long_query, daemon=True)
    qt.start()
    time.sleep(5)  # let the query get well underway, writers flowing

    sh = next(
        p for p in (
            shutil.which("sh"), shutil.which("bash"),
            r"C:\Program Files\Git\usr\bin\sh.exe", r"C:\Program Files\Git\bin\bash.exe",
        ) if p and Path(p).exists()
    )
    subprocess.run([sh, "gen-certs.sh", "renewal"], cwd=HERE, check=True, capture_output=True)
    rotate_files("renewal.crt", "renewal.key")
    rotated_at = time.time()

    # The file watcher should pick this up (2 s poll + debounce). Admin
    # endpoint is the deterministic fallback trigger; both are SEC-3
    # mechanisms, and the corrupt leg exercises the admin path anyway.
    new_serial = None
    trigger = "file watcher"
    for _ in range(30):
        time.sleep(0.5)
        s_now = serving_serial(2963)
        if s_now != serial_http_0:
            new_serial = s_now
            break
    if new_serial is None:
        trigger = "admin endpoint (watcher missed bind-mount mtime)"
        rr = requests.post(f"{BASE}/admin/tls/reload", verify=CA, timeout=10)
        check(rr.status_code == 200, "admin reload accepted", rr.text[:120])
        new_serial = serving_serial(2963)
    check(new_serial != serial_http_0, f"new HTTPS connections present the renewed cert [{trigger}]",
          f"{serial_http_0} -> {new_serial}")
    check(serving_serial(2964) == new_serial, "Flight listener rotated in lockstep")

    qt.join(timeout=600)
    check("value" in qresult and qresult["value"] == expected(n, m),
          "in-flight Flight SQL query completed correctly across rotation",
          f"{qresult.get('value', qresult.get('error'))} in {qresult.get('end', 0) - qresult['start']:.1f}s")
    check(qresult["start"] < rotated_at < qresult.get("end", 0),
          "rotation landed while the query was in flight",
          f"query {qresult.get('end', 0) - qresult['start']:.1f}s, rotated at T+{rotated_at - qresult['start']:.1f}s")

    time.sleep(2)
    writer.stop.set()
    writer.join(timeout=10)
    check(writer.failed == 0, f"zero write errors across rotation ({writer.ok} writes)",
          "; ".join(writer.errors[:3]))
    counted = sql(f"SELECT COUNT(*) AS n FROM drill_writes WHERE leg = '{RUN_TAG}'")[0]["n"]
    check(counted == writer.ok, "every acknowledged write is queryable", f"{counted} == {writer.ok}")

    tlogs = docker_logs(TELEGRAF, t0)
    terr = [l for l in tlogs.splitlines() if "E!" in l]
    check(not terr, "Telegraf (TLS) logged zero output errors", terr[0] if terr else "")

    m = metrics()
    check(m.get("timelord_tls_last_reload_ok") == 1.0, "reload-ok gauge still 1 after rotation")

    print("== leg 2: corrupt renewal ==")
    writer2 = Writer()
    writer2.start()
    (CERTS / "server.crt").write_text("-----BEGIN GARBAGE-----\nzzzz\n-----END GARBAGE-----\n")
    time.sleep(4)  # give the watcher a chance to trip the alarm too
    rr = requests.post(f"{BASE}/admin/tls/reload", verify=CA, timeout=10)
    body = rr.json() if rr.headers.get("content-type", "").startswith("application/json") else {}
    check(rr.status_code == 422 and body.get("alarm") == "SEC3_CERT_RENEWAL_FAILED",
          "admin reload rejects corrupt pair with named alarm", f"HTTP {rr.status_code} {body}")
    check(serving_serial(2963) == new_serial, "still serving last-good cert")
    m = metrics()
    check(m.get("timelord_tls_last_reload_ok") == 0.0, "reload-ok gauge dropped to 0")
    slogs = docker_logs(CONTAINER, t0)
    check("SEC3_CERT_RENEWAL_FAILED" in slogs, "SEC3_CERT_RENEWAL_FAILED alarm in server logs")

    time.sleep(3)
    writer2.stop.set()
    writer2.join(timeout=10)
    check(writer2.failed == 0, f"writes kept flowing on last-good ({writer2.ok} writes)",
          "; ".join(writer2.errors[:3]))

    # Recovery: a good renewal after the bad one.
    rotate_files("renewal.crt", "renewal.key")
    ok = False
    for _ in range(20):
        time.sleep(0.5)
        if metrics().get("timelord_tls_last_reload_ok") == 1.0:
            ok = True
            break
    if not ok:
        requests.post(f"{BASE}/admin/tls/reload", verify=CA, timeout=10)
        ok = metrics().get("timelord_tls_last_reload_ok") == 1.0
    check(ok, "good renewal after the bad one restores health")

    print("\n== AT-7 verdict ==")
    failed = [label for good, label in results if not good]
    print(f"{len(results) - len(failed)}/{len(results)} checks passed")
    if failed:
        for f in failed:
            print(f"  FAILED: {f}")
        sys.exit(1)
    print("AT-7: PASS")


if __name__ == "__main__":
    main()
