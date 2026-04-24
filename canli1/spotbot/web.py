from __future__ import annotations

import json
import mimetypes
import os
import threading
from functools import partial
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable
from urllib.parse import urlparse

from .engine import BotRuntime


class DashboardHandler(BaseHTTPRequestHandler):
    runtime: BotRuntime
    web_root: str
    shutdown_fn: Callable[[], None] | None = None

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/state":
            self._send_json({"ok": True, "state": self.runtime.get_state()})
            return
        if parsed.path == "/api/secrets":
            self._send_json({"ok": True, "secrets": self.runtime.get_state().get("secrets", {})})
            return
        if parsed.path in {"/", "/index.html"}:
            self._serve_static("index.html")
            return
        if parsed.path == "/app.js":
            self._serve_static("app.js")
            return
        if parsed.path == "/style.css":
            self._serve_static("style.css")
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        payload = self._read_json_body()
        if parsed.path == "/api/start":
            self._send_json(self.runtime.start())
            return
        if parsed.path == "/api/stop":
            self._send_json(self.runtime.stop())
            return
        if parsed.path == "/api/run-once":
            self._send_json({"ok": True, "cycle": self.runtime.run_once()})
            return
        if parsed.path == "/api/reset":
            self._send_json(self.runtime.reset_state())
            return
        if parsed.path == "/api/reset-balance":
            self._send_json(self.runtime.reset_balance())
            return
        if parsed.path == "/api/reset-pnl":
            self._send_json(self.runtime.reset_pnl())
            return
        if parsed.path == "/api/config":
            self._send_json(self.runtime.update_config(payload))
            return
        if parsed.path == "/api/secrets":
            self._send_json(self.runtime.update_secrets(payload))
            return
        if parsed.path == "/api/test-telegram":
            self._send_json(self.runtime.test_telegram())
            return
        if parsed.path == "/api/test-polymarket":
            self._send_json(self.runtime.test_polymarket())
            return
        if parsed.path == "/api/shutdown":
            stop_result = self.runtime.stop()
            self._send_json({"ok": True, "message": "Yazılım kapatılıyor", "bot": stop_result})
            if self.shutdown_fn:
                threading.Thread(target=self.shutdown_fn, name="dashboard-shutdown", daemon=True).start()
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length <= 0:
            return {}
        body = self.rfile.read(length)
        if not body:
            return {}
        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    def _send_json(self, payload: dict, status: int = 200) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self._safe_write(raw)

    def _serve_static(self, filename: str) -> None:
        path = os.path.join(self.web_root, filename)
        if not os.path.exists(path):
            self.send_error(HTTPStatus.NOT_FOUND, "Missing asset")
            return
        mime, _ = mimetypes.guess_type(path)
        with open(path, "rb") as handle:
            payload = handle.read()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime or "application/octet-stream")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self._safe_write(payload)

    def _safe_write(self, payload: bytes) -> None:
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError, OSError):
            return

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def create_server(runtime: BotRuntime, host: str, port: int, web_root: str, shutdown_fn: Callable[[], None] | None = None) -> ThreadingHTTPServer:
    handler = partial(DashboardHandler)
    server = ThreadingHTTPServer((host, port), handler)
    DashboardHandler.runtime = runtime
    DashboardHandler.web_root = web_root
    DashboardHandler.shutdown_fn = shutdown_fn or server.shutdown
    return server
