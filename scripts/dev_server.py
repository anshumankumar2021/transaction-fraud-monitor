"""Serve the Vercel demo locally: public/ as static files, /api/simulate via the real handler.

    python -m scripts.dev_server   # then open http://localhost:3001
"""
from __future__ import annotations

import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

from api.simulate import handler as ApiHandler

ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "public")


class Dev(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=ROOT, **kw)

    def do_GET(self):
        if self.path.startswith("/api/simulate"):
            return ApiHandler.do_GET(self)
        return super().do_GET()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3001"))
    print(f"http://localhost:{port}")
    ThreadingHTTPServer(("", port), Dev).serve_forever()
