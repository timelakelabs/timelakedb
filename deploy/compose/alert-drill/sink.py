#!/usr/bin/env python3
"""A webhook recorder standing in for whatever really receives alerts.

Rule state is not delivery. Grafana can move a rule to Alerting and still
deliver nothing — a contact point pointed at a host that does not resolve,
a notification policy whose matchers exclude the rule, a `group_wait` long
enough that an operator watching the UI concludes it worked. Each of those
leaves the rule showing `firing` and the on-call phone silent, so a drill
that stops at rule state has checked the half that is easy to check.

This records what actually arrives and serves it back, which turns the
delivery question into an assertion. It is deliberately not a notification
system: no retries, no auth, no persistence beyond the container. It exists
so `alert_drill.sh` can ask "what did Grafana send, and when".

    POST /hook    record one Grafana webhook payload
    GET  /alerts  everything recorded so far, newest last
    POST /reset   forget everything (the drill calls this between phases)
    GET  /health  liveness for compose and the drill's wait loop
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 9099

_lock = threading.Lock()
_received: list[dict] = []


class Handler(BaseHTTPRequestHandler):
    # The default logger writes a line per request to stderr, which buries
    # the drill's own output in the compose logs for no benefit.
    def log_message(self, *_args):
        pass

    def _send(self, code: int, payload: object) -> None:
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's name
        length = int(self.headers.get("content-length") or 0)
        raw = self.rfile.read(length) if length else b""

        if self.path.startswith("/reset"):
            with _lock:
                _received.clear()
            self._send(200, {"reset": True})
            return

        if not self.path.startswith("/hook"):
            self._send(404, {"error": "no such path"})
            return

        try:
            payload = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            # Record it anyway. A payload this cannot parse is a finding
            # about Grafana's webhook format, and dropping it on the floor
            # would present as "no notification was delivered" — the one
            # conclusion that would definitely be wrong.
            payload = {"unparseable": raw.decode("utf-8", "replace")}

        with _lock:
            _received.append(payload)
        self._send(200, {"recorded": True})

    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/health"):
            self._send(200, {"ok": True})
            return
        if self.path.startswith("/alerts"):
            with _lock:
                snapshot = list(_received)
            # Flattened to the fields the drill asserts on, with the raw
            # payload kept alongside so a surprise is still inspectable.
            flat = [
                {
                    "status": group.get("status"),
                    "alerts": [
                        {
                            "status": a.get("status"),
                            "alertname": (a.get("labels") or {}).get("alertname"),
                            "values": a.get("values"),
                        }
                        for a in (group.get("alerts") or [])
                    ],
                }
                for group in snapshot
            ]
            self._send(200, {"count": len(snapshot), "groups": flat, "raw": snapshot})
            return
        self._send(404, {"error": "no such path"})


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
