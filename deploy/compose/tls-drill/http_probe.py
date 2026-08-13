#!/usr/bin/env python3
"""Records the Authorization header stock write clients send, then 204s.

The read half (Grafana) and the write half (Telegraf) are different
clients with different plugins, and there is no reason to assume they
spell the same credential the same way. This finds out.
"""
import http.server


class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        auth = self.headers.get("Authorization")
        ua = (self.headers.get("User-Agent") or "?")[:40]
        print(f"PROBE path={self.path.split('?')[0]} ua={ua} authorization={auth!r}",
              flush=True)
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        self.send_response(204)
        self.end_headers()

    def log_message(self, *a):
        pass


http.server.HTTPServer(("0.0.0.0", 8086), H).serve_forever()
