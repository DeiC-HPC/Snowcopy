import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from mitmproxy import ctx, http

STATE_LOCK = threading.Lock()

STATE = {
    "counts": {
        "GET": 0,
        "PUT": 0,
        "POST": 0,
        "HEAD": 0,
    },

    # Response corruption
    "corrupt": {
        "target_key": None,
        "method": "GET",
        "remaining": 0,
    },

    # Fake responses
    "fake_response": {
        "target_key": None,
        "method": "GET",
        "remaining": 0,
        "status_code": 500,
        "headers": {},
        "body": "",
    },

    # Connection aborts
    "cut_connection": {
        "target_key": None,
        "method": "GET",
        "remaining": 0,
    },
}


def json_response(handler, status, payload):
    data = json.dumps(payload).encode("utf-8")

    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


class ControlHandler(BaseHTTPRequestHandler):
    def do_GET(self):

        if self.path == "/counts":
            with STATE_LOCK:
                counts = dict(STATE["counts"])

            json_response(self, 200, counts)
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):

        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)

            if body:
                payload = json.loads(body)
            else:
                payload = {}

            if self.path == "/configure-corrupt":
                self.configure_corrupt(payload)
                return

            if self.path == "/configure-fake-response":
                self.configure_fake_response(payload)
                return

            if self.path == "/configure-cut-connection":
                self.configure_cut_connection(payload)
                return

            if self.path == "/reset-counts":
                self.reset_counts()
                return

            self.send_response(404)
            self.end_headers()

        except Exception as ex:
            json_response(self, 400, {"error": str(ex)})

    def configure_corrupt(self, payload):

        with STATE_LOCK:
            STATE["corrupt"] = {
                "target_key": payload["target_key"],
                "method": payload.get("method", "GET").upper(),
                "remaining": int(payload.get("count", 1)),
            }

        ctx.log.info(
            f"Configured corruption: "
            f"{STATE['corrupt']}"
        )

        json_response(self, 200, {"status": "ok"})

    def configure_fake_response(self, payload):

        with STATE_LOCK:
            STATE["fake_response"] = {
                "target_key": payload["target_key"],
                "method": payload.get("method", "GET").upper(),
                "remaining": int(payload.get("count", 1)),
                "status_code": int(payload.get("status_code", 500)),
                "headers": payload.get("headers", {}),
                "body": payload.get(
                    "body",
                    "<Error><Code>InternalError</Code></Error>"
                ),
            }

        ctx.log.info(
            f"Configured fake response: "
            f"{STATE['fake_response']}"
        )

        json_response(self, 200, {"status": "ok"})

    def configure_cut_connection(self, payload):

        with STATE_LOCK:
            STATE["cut_connection"] = {
                "target_key": payload["target_key"],
                "method": payload.get("method", "GET").upper(),
                "remaining": int(payload.get("count", 1)),
            }

        ctx.log.info(
            f"Configured connection cut: "
            f"{STATE['cut_connection']}"
        )

        json_response(self, 200, {"status": "ok"})

    def reset_counts(self):

        with STATE_LOCK:
            for key in STATE["counts"]:
                STATE["counts"][key] = 0

        json_response(self, 200, {"status": "reset"})

    def log_message(self, format, *args):
        pass


class Corruptor:

    def load(self, loader):

        def run_server():
            server = HTTPServer(("0.0.0.0", 8081), ControlHandler)
            ctx.log.info("Control API listening on :8081")
            server.serve_forever()

        threading.Thread(
            target=run_server,
            daemon=True
        ).start()

        print("Proxy addon started", flush=True)

    def request(self, flow: http.HTTPFlow):

        method = flow.request.method.upper()

        with STATE_LOCK:
            if method in STATE["counts"]:
                STATE["counts"][method] += 1

        self.handle_fake_response(flow)

        self.handle_cut_connection(flow)

    def response(self, flow: http.HTTPFlow):

        self.handle_corruption(flow)

    def matches(self, flow, config):

        if config["remaining"] <= 0:
            return False

        if flow.request.method.upper() != config["method"]:
            return False

        target = config["target_key"]

        if not target:
            return False

        return target in flow.request.pretty_url

    def handle_corruption(self, flow):

        with STATE_LOCK:

            config = STATE["corrupt"].copy()

            if not self.matches(flow, config):
                return

            body = flow.response.content

            if not body:
                return

            data = bytearray(body)

            if len(data) > 0:
                data[0] ^= 0x01

            flow.response.content = bytes(data)

            STATE["corrupt"]["remaining"] -= 1

            remaining = STATE["corrupt"]["remaining"]

        ctx.log.info(
            f"Corrupted response for "
            f"{flow.request.pretty_url} "
            f"remaining={remaining}"
        )

    def handle_fake_response(self, flow):

        with STATE_LOCK:

            cfg = STATE["fake_response"].copy()

            if not self.matches(flow, cfg):
                return

            STATE["fake_response"]["remaining"] -= 1

        response = http.Response.make(
            cfg["status_code"],
            cfg["body"].encode("utf-8"),
            cfg["headers"],
        )

        flow.response = response

        ctx.log.info(
            f"Returned fake response for "
            f"{flow.request.pretty_url} "
            f"status={cfg['status_code']}"
        )

    def handle_cut_connection(self, flow):

        with STATE_LOCK:

            cfg = STATE["cut_connection"].copy()

            if not self.matches(flow, cfg):
                return

            STATE["cut_connection"]["remaining"] -= 1

        flow.error = http.Error(
            "Simulated connection termination"
        )

        ctx.log.info(
            f"Cut connection for "
            f"{flow.request.pretty_url}"
        )


addons = [Corruptor()]