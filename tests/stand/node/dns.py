import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

records = {}


class Handler(BaseHTTPRequestHandler):
    def do_PUT(self):
        name = self.path.strip("/")
        value = self.rfile.read(int(self.headers["Content-Length"])).decode()
        records[name] = value
        print(json.dumps({"ts": round(time.time(), 3), "event": "dns", "name": name, "value": value}), flush=True)
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        body = json.dumps(records).encode()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


HTTPServer(("0.0.0.0", 8053), Handler).serve_forever()
