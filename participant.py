#!/usr/bin/env python3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import json
import threading
import time
import os
from typing import Dict, Any, Optional

lock = threading.Lock()

NODE_ID: str = ""
PORT: int = 8001

kv: Dict[str, str] = {}
TX: Dict[str, Dict[str, Any]] = {}

WAL_PATH: Optional[str] = None


def jdump(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")


def jload(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


def wal_append(line: str) -> None:
    if not WAL_PATH:
        return
    with open(WAL_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())


def replay_wal() -> None:
    if not WAL_PATH or not os.path.exists(WAL_PATH):
        return

    with open(WAL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(" ", 2)
            if len(parts) < 2:
                continue
            txid, action = parts[0], parts[1]

            if action == "COMMIT" and txid in TX:
                apply_op(TX[txid]["op"])
                TX[txid]["state"] = "COMMITTED"
            elif action == "ABORT":
                TX[txid] = {"state": "ABORTED", "op": None, "ts": time.time()}


def validate_op(op: dict) -> bool:
    return op.get("type") == "SET" and bool(op.get("key"))


def apply_op(op: dict) -> None:
    if op["type"] == "SET":
        kv[op["key"]] = str(op.get("value", ""))


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, obj: dict):
        data = jdump(obj)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/status":
            with lock:
                self._send(200, {
                    "node": NODE_ID,
                    "port": PORT,
                    "kv": kv,
                    "tx": TX
                })
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = jload(self.rfile.read(length)) if length else {}

        if self.path == "/prepare":
            txid, op = body.get("txid"), body.get("op")
            vote = "YES" if validate_op(op) else "NO"

            with lock:
                TX[txid] = {"state": "READY" if vote == "YES" else "ABORTED", "op": op}
            wal_append(f"{txid} PREPARE {vote}")

            self._send(200, {"vote": vote})
            return

        if self.path == "/commit":
            txid = body.get("txid")
            with lock:
                if txid in TX and TX[txid]["state"] in ("READY", "PRECOMMIT"):
                    apply_op(TX[txid]["op"])
                    TX[txid]["state"] = "COMMITTED"
                    wal_append(f"{txid} COMMIT")
                    self._send(200, {"state": "COMMITTED"})
                    return
            self._send(409, {"error": "invalid state"})
            return

        if self.path == "/abort":
            txid = body.get("txid")
            with lock:
                TX[txid] = {"state": "ABORTED", "op": None}
            wal_append(f"{txid} ABORT")
            self._send(200, {"state": "ABORTED"})
            return

        if self.path == "/can_commit":
            txid, op = body.get("txid"), body.get("op")
            vote = "YES" if validate_op(op) else "NO"
            with lock:
                TX[txid] = {"state": "READY" if vote == "YES" else "ABORTED", "op": op}
            wal_append(f"{txid} CAN_COMMIT {vote}")
            self._send(200, {"vote": vote})
            return

        if self.path == "/precommit":
            txid = body.get("txid")
            with lock:
                if txid in TX and TX[txid]["state"] == "READY":
                    TX[txid]["state"] = "PRECOMMIT"
                    wal_append(f"{txid} PRECOMMIT")
                    self._send(200, {"state": "PRECOMMIT"})
                    return
            self._send(409, {"error": "invalid state"})
            return

        self._send(404, {"error": "not found"})


def main():
    global NODE_ID, PORT, WAL_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--port", type=int, default=8001)
    ap.add_argument("--wal", default="")
    args = ap.parse_args()

    NODE_ID = args.id
    PORT = args.port
    WAL_PATH = args.wal or None

    replay_wal()

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[{NODE_ID}] Participant running on port {PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
