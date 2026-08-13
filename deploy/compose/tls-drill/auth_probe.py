#!/usr/bin/env python3
"""A Flight SQL server that answers nothing and records everything.

Data-plane auth has to be designed around what stock clients can
actually be configured to send, not what would be convenient. This
stands in for TimeLakeDB, logs the exact gRPC metadata Grafana attaches
under each datasource auth configuration, and refuses the call. Point a
Grafana datasource at it, run a query, read the log.

  python auth_probe.py            # listens on 0.0.0.0:8815, plaintext
"""
import json
import sys

import pyarrow.flight as fl

SEEN = []


class Recorder(fl.ServerMiddlewareFactory):
    def start_call(self, info, headers):
        # Keys arrive lowercased. gRPC adds its own (:authority, te,
        # user-agent, grpc-*); those are noise, but a header the client
        # was configured to send is exactly what we are looking for.
        interesting = {
            k: [str(v)[:120] for v in vs]
            for k, vs in headers.items()
            if not k.startswith((":", "grpc-")) and k not in ("te", "content-type")
        }
        rec = {"method": str(getattr(info, "method", "?")), "headers": interesting}
        SEEN.append(rec)
        print("PROBE " + json.dumps(rec), flush=True)
        return None


class Probe(fl.FlightServerBase):
    def get_flight_info(self, context, descriptor):
        raise fl.FlightUnauthenticatedError("probe: recorded, not serving")

    def do_get(self, context, ticket):
        raise fl.FlightUnauthenticatedError("probe: recorded, not serving")


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8815
    print(f"auth probe listening on 0.0.0.0:{port}", flush=True)
    Probe(location=f"grpc://0.0.0.0:{port}",
          middleware={"rec": Recorder()}).serve()
