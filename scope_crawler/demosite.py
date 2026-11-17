"""A small static site used by the tests and the local demo.

The site deliberately contains the awkward cases the crawler must handle:

* ``/site/news/`` links to ``/site/docs/`` (allowed cross-column reference) and
  ``/site/store/`` (not allowed from news).
* ``/site/docs/`` links back to news (would allow A→B→A recursion).
* ``/site/latest/`` is an alias of ``/site/news/`` and duplicates one page.
* ``/site/news/private/`` is excluded by pattern.
* ``/outside/`` is on the same server but outside the site root.
* ``/site/news/leak`` links up with ``../..`` and to ``http://elsewhere.invalid/``.
* ``/site/news/restricted`` returns 403 (never bypassed).
* ``/robots.txt`` disallows ``/site/docs/secret``.
"""

from __future__ import annotations

import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

PAGES: dict[str, tuple[int, str, str]] = {
    "/robots.txt": (200, "text/plain; charset=utf-8", "User-agent: *\nDisallow: /site/docs/secret\n"),
    "/site/news/": (
        200,
        "text/html; charset=utf-8",
        """<!doctype html><html><head><title>新闻首页</title></head><body>
        <a href="/site/news/2024/spring.html">春天</a>
        <a href="/site/latest/2024/summer.html">夏天（别名）</a>
        <a href="/site/docs/guide.html">文档指引</a>
        <a href="/site/docs/deep/ref-a.html">深层文档</a>
        <a href="/site/store/">商店</a>
        <a href="/outside/">站外</a>
        <a href="/site/news/private/draft.html">草稿</a>
        <a href="../news/leak">越界</a>
        <a href="http://elsewhere.invalid/x">外部</a>
        <a href="/site/news/restricted">受限</a>
        <a href="/site/news/bounce">跳转</a>
        </body></html>""",
    ),
    "/site/news/2024/spring.html": (
        200,
        "text/html; charset=utf-8",
        """<html><head><title>春天</title></head><body>
        <a href="/site/news/2024/summer.html">下一页</a>
        <a href="/site/news/">返回</a>
        <a href="/site/docs/guide.html">文档</a>
        </body></html>""",
    ),
    "/site/news/2024/summer.html": (
        200,
        "text/html; charset=utf-8",
        """<html><head><title>夏天</title></head><body>
        <a href="/site/news/2024/spring.html">上一页</a>
        </body></html>""",
    ),
    "/site/latest/2024/summer.html": (
        200,
        "text/html; charset=utf-8",
        """<html><head><title>夏天（别名副本）</title></head><body>
        <a href="/site/news/2024/summer.html">规范链接</a>
        <a href="/site/docs/guide.html">文档</a>
        </body></html>""",
    ),
    "/site/news/private/draft.html": (
        200,
        "text/html; charset=utf-8",
        "<html><head><title>草稿</title></head><body>不应发布</body></html>",
    ),
    "/site/news/restricted": (403, "text/html; charset=utf-8", "<html>403</html>"),
    "/site/news/leak": (
        200,
        "text/html; charset=utf-8",
        """<html><head><title>越界</title></head><body>
        <a href="../../outside/">越界链接</a>
        </body></html>""",
    ),
    "/site/docs/": (
        200,
        "text/html; charset=utf-8",
        """<html><head><title>文档首页</title></head><body>
        <a href="/site/docs/guide.html">指引</a>
        <a href="/site/news/">回到新闻</a>
        <a href="/site/docs/secret.html">秘密</a>
        <a href="/site/docs/deep/ref-a.html">深层</a>
        </body></html>""",
    ),
    "/site/docs/guide.html": (
        200,
        "text/html; charset=utf-8",
        """<html><head><title>指引</title></head><body>
        <a href="/site/docs/">目录</a>
        <a href="/site/news/2024/spring.html">新闻</a>
        </body></html>""",
    ),
    "/site/docs/secret.html": (
        200,
        "text/html; charset=utf-8",
        "<html><head><title>秘密</title></head><body>robots 禁止</body></html>",
    ),
    "/site/docs/deep/ref-a.html": (
        200,
        "text/html; charset=utf-8",
        """<html><head><title>深层 A</title></head><body>
        <a href="/site/news/2024/summer.html">新闻</a>
        <a href="/site/docs/deep/ref-b.html">深层 B</a>
        </body></html>""",
    ),
    "/site/docs/deep/ref-b.html": (
        200,
        "text/html; charset=utf-8",
        """<html><head><title>深层 B</title></head><body>
        <a href="/site/docs/deep/ref-a.html">深层 A</a>
        <a href="/site/news/">新闻</a>
        </body></html>""",
    ),
    "/site/store/": (
        200,
        "text/html; charset=utf-8",
        """<html><head><title>商店</title></head><body>
        <a href="/site/store/item.html">商品</a>
        </body></html>""",
    ),
    "/site/store/item.html": (
        200,
        "text/html; charset=utf-8",
        "<html><head><title>商品</title></head><body>商品</body></html>",
    ),
    "/outside/": (
        200,
        "text/html; charset=utf-8",
        "<html><head><title>站外</title></head><body>站外页面</body></html>",
    ),
    # A link that stays in scope but whose response redirects to the outside.
    # The crawler must refuse the hop rather than request /outside/.
    "/site/news/bounce": (
        302,
        "text/html; charset=utf-8",
        "",
    ),
}

REDIRECTS: dict[str, str] = {
    "/site/news/bounce": "/outside/",
}


class SiteHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        if path in REDIRECTS:
            self.send_response(302)
            self.send_header("Location", REDIRECTS[path])
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        body_path = path
        if path.endswith("/") and path + "index.html" in PAGES:
            body_path = path + "index.html"
        entry = PAGES.get(body_path)
        if entry is None:
            status, ctype, body = 404, "text/html; charset=utf-8", "<html>404</html>"
        else:
            status, ctype, body = entry
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args) -> None:  # silence test output
        pass


class Site:
    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.httpd = ThreadingHTTPServer((host, port), SiteHandler)
        self.host, self.port = self.httpd.server_address
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.counter: dict[str, int] = {}

    @property
    def base(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def site_root(self) -> str:
        return f"{self.base}/site/"

    def start(self) -> "Site":
        self.thread.start()
        return self

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()

    def __enter__(self) -> "Site":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()


def instrument(handler_cls=SiteHandler):
    """Return a handler class that counts requests per path.

    Used by tests that need to prove scope widening does not re-crawl pages
    that were already fetched.
    """
    counter: dict[str, int] = {}

    class Counting(SiteHandler):
        def do_GET(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            counter[path] = counter.get(path, 0) + 1
            super().do_GET()

    Counting.counter = counter
    return Counting


if __name__ == "__main__":  # pragma: no cover - manual demo helper
    site = Site(port=int(os.environ.get("PORT", "8000"))).start()
    print(f"serving demo site at {site.base}/site/")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        site.stop()
