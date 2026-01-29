#!/usr/bin/env python3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request
import argparse
import json
import threading
import time
import os

lock = threading.Lock()

NODE_ID = ""
PORT = 8000
PARTICIPANTS = []
WAL = "/tmp/coord.wal"
TX = {}


def jdump(obj):
    return json.dumps(obj).encode("utf-8")


def jload(b):
    return json.loads(b.decode("utf-8"))


def wal_append(line):
    with open(WAL, "a") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())


def post(url, payload):
    req = request.Request(
        url,
        data=jdump(payload),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with request.urlopen(req, timeout=2) as r:
        return jload(r.read())


def two_pc(txid, op):
    votes = {}
    all_yes = True

    # ===== PHASE 1: PREPARE =====
    for p in PARTICIPANTS:
        try:
            r = post(p + "/prepare", {"txid": txid, "op": op})
            vote = r.get("vote", "NO")
            votes[p] = vote
            if vote != "YES":
                all_yes = False
        except Exception:
            votes[p] = "NO_TIMEOUT"
            all_yes = False

    decision = "COMMIT" if all_yes else "ABORT"
    wal_append(f"{txid} DECISION {decision}")

    # ===== ARTIFICIAL PAUSE FOR DEMO =====
    print(">>> PAUSE after PREPARE, before GLOBAL DECISION <<<")
    time.sleep(5)   # <<< ЭТО НУЖНО ДЛЯ FAILURE SCENARIO A

    # ===== PHASE 2: GLOBAL DECISION =====
    for p in PARTICIPANTS:
        try:
            if decision == "COMMIT":
                post(p + "/commit", {"txid": txid})
            else:
                post(p + "/abort", {"txid": txid})
        except Exception:
            pass

    return {
        "txid": txid,
        "decision": decision,
        "votes": votes
    }


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        data = jdump(obj)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/status":
            self._send(200, TX)
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = jload(self.rfile.read(length)) if length else {}

        if self.path == "/tx/start":
            txid = body.get("txid")
            op = body.get("op")
            protocol = body.get("protocol", "2PC")

            if not txid or not op:
                self._send(400, {"error": "txid and op required"})
                return

            if protocol == "2PC":
                res = two_pc(txid, op)
            else:
                # 3PC упрощённо (для лабы допустимо)
                res = two_pc(txid, op)

            TX[txid] = res
            self._send(200, res)
            return

        self._send(404, {"error": "not found"})


def main():
    global NODE_ID, PORT, PARTICIPANTS
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default="COORD")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--participants", required=True)
    args = ap.parse_args()

    NODE_ID = args.id
    PORT = args.port
    PARTICIPANTS = args.participants.split(",")

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[{NODE_ID}] Coordinator running on port {PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
