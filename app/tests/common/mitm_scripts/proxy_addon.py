import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from mitmproxy import ctx, http

STATE = {
    "target_key": None,
    "method": "GET",
    "remaining": 0,
    "get_count": 0,
}


class ControlHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/configure":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)

        try:
            cfg = json.loads(body)

            STATE["target_key"] = cfg["target_key"]
            STATE["method"] = cfg.get("method", "GET").upper()
            STATE["remaining"] = int(cfg["count"])

            ctx.log.info(
                f"Updated config: "
                f"target={STATE['target_key']} "
                f"method={STATE['method']} "
                f"remaining={STATE['remaining']}"
            )

            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")

        except Exception as e:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(str(e).encode())

    def log_message(self, format, *args):
        # suppress http.server logging
        pass


class Corruptor:
    def load(self, loader):
        def run_server():
            server = HTTPServer(("0.0.0.0", 8081), ControlHandler)
            ctx.log.info("Control API listening on :8081")
            server.serve_forever()

        threading.Thread(target=run_server, daemon=True).start()

        print("Proxy addon started", flush=True)

    def request(self, flow: http.HTTPFlow):
        if flow.request.method.upper() == "GET":
            STATE["get_count"] += 1

    def response(self, flow: http.HTTPFlow):
        if STATE["remaining"] <= 0:
            return

        if flow.request.method.upper() != STATE["method"]:
            return

        target = STATE["target_key"]
        if not target:
            return

        if target not in flow.request.pretty_url:
            return

        body = flow.response.content
        if not body:
            return

        data = bytearray(body)
        data[0] ^= 0x01

        flow.response.content = bytes(data)

        STATE["remaining"] -= 1

        ctx.log.info(
            f"Corrupted {flow.request.pretty_url}. "
            f"remaining={STATE['remaining']}"
        )


addons = [Corruptor()]