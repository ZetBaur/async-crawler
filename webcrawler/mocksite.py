"""Локальный тестовый сайт для демо и тестов.

Нужен, чтобы краулер можно было проверять быстро, без интернета и не
нагружая чужие сайты. Запускается на 127.0.0.1 на свободном порту.
"""
import asyncio
import time
from collections import Counter

from aiohttp import web

HOME_HTML = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<title>Mock Home</title>
<meta name="description" content="Тестовый сайт для краулера">
<meta name="keywords" content="crawler, test, asyncio">
<link rel="canonical" href="/">
</head><body>
<h1>Mock Home</h1><h2>Разделы</h2><h3>Подраздел</h3>
<p>Текст главной страницы.</p>
<script>var hidden = "не должно попасть в текст";</script>
<ul><li>Первый</li><li>Второй</li></ul>
<ol><li>Раз</li><li>Два</li></ol>
<table><tr><th>Имя</th><th>Значение</th></tr>
<tr><td>a</td><td>1</td></tr><tr><td>b</td><td>2</td></tr></table>
<img src="/static/logo.png" alt="Логотип"><img src="img/photo.jpg">
<a href="/page/1">Страница 1</a>
<a href="page/2">Относительная 2</a>
<a href="/about">О нас</a>
<a href="https://example.org/external">Внешняя</a>
<a href="/private/secret">Секрет</a>
<a href="mailto:a@b.c">почта</a>
<a href="javascript:void(0)">js</a>
<a href="#top">якорь</a>
<a href="/page/1#frag">дубль с якорем</a>
</body></html>"""

BROKEN_HTML = ("<html><head><title>Битая страница</title><body><h1>Заголовок без закрытия"
               "<p>Абзац <b>жирный <i>курсив</p><a href='/page/1'>ссылка"
               "<table><tr><td>ячейка<ul><li>пункт")

ROBOTS_TXT = """User-agent: *
Disallow: /private/
Crawl-delay: 0.2

User-agent: BadBot
Disallow: /
"""

MAX_PAGE = 30


def _page_html(n: int) -> str:
    links = [f'<a href="/page/{k}">Страница {k}</a>' for k in (2 * n, 2 * n + 1)
             if k <= MAX_PAGE]
    links.append('<a href="/">Домой</a>')
    links.append('<a href="../about">О нас (относительная)</a>')
    return (f'<!doctype html><html><head><title>Страница {n}</title></head><body>'
            f'<h1>Страница {n}</h1><p>Содержимое страницы {n}.</p>'
            + " ".join(links) + "</body></html>")


class MockSite:
    """Тестовый сайт. Использование: `async with MockSite() as site: site.url`."""

    def __init__(self, host: str = "127.0.0.1", flaky_failures: int = 2,
                 latency: float = 0.0) -> None:
        self.host = host
        self.latency = latency
        self.flaky_failures = flaky_failures
        self.requests: list[tuple[float, str]] = []
        self.agents: list[str] = []
        self.active = 0
        self.max_active = 0
        self.flaky_counter: Counter = Counter()
        self.port = 0
        self._runner: web.AppRunner | None = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def paths(self) -> list[str]:
        return [path for _, path in self.requests]

    @web.middleware
    async def _track(self, request: web.Request, handler):
        self.requests.append((time.monotonic(), request.path))
        self.agents.append(request.headers.get("User-Agent", ""))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            return await handler(request)
        finally:
            self.active -= 1

    def _build_app(self) -> web.Application:
        app = web.Application(middlewares=[self._track])
        app.router.add_get("/", self._home)
        app.router.add_get("/about", self._simple("О нас", "Страница о нас."))
        app.router.add_get("/contact", self._simple("Контакты", "Страница контактов."))
        app.router.add_get("/page/{n:\\d+}", self._page)
        app.router.add_get("/private/{tail:.*}", self._simple("Секрет", "Закрытая зона."))
        app.router.add_get("/delay/{seconds}", self._delay)
        app.router.add_get("/status/{code:\\d+}", self._status)
        app.router.add_get("/flaky/{key}", self._flaky)
        app.router.add_get("/broken", self._broken)
        app.router.add_get("/plain", self._plain)
        app.router.add_get("/binary", self._binary)
        app.router.add_get("/big/{n:\\d+}", self._big)
        app.router.add_get("/bulk/{n:\\d+}", self._bulk)
        app.router.add_get("/robots.txt", self._robots)
        app.router.add_get("/sitemap.xml", self._sitemap)
        app.router.add_get("/sitemap2.xml", self._sitemap2)
        app.router.add_get("/sitemap_index.xml", self._sitemap_index)
        return app

    async def _home(self, request):
        await asyncio.sleep(self.latency)
        return web.Response(text=HOME_HTML, content_type="text/html")

    def _simple(self, title: str, text: str):
        async def handler(request):
            await asyncio.sleep(self.latency)
            return web.Response(
                text=f"<html><head><title>{title}</title></head>"
                     f"<body><h1>{title}</h1><p>{text}</p></body></html>",
                content_type="text/html")
        return handler

    async def _page(self, request):
        await asyncio.sleep(self.latency)
        n = int(request.match_info["n"])
        if not 1 <= n <= MAX_PAGE:
            return web.Response(status=404, text="нет такой страницы")
        return web.Response(text=_page_html(n), content_type="text/html")

    async def _delay(self, request):
        seconds = min(float(request.match_info["seconds"]), 30.0)
        await asyncio.sleep(seconds)
        return web.Response(text=f"задержка {seconds} сек",
                            content_type="text/html")

    async def _status(self, request):
        code = int(request.match_info["code"])
        headers = {"Retry-After": "1"} if code in (429, 503) else {}
        return web.Response(status=code, text=f"status {code}", headers=headers)

    async def _flaky(self, request):
        key = request.match_info["key"]
        self.flaky_counter[key] += 1
        if self.flaky_counter[key] <= self.flaky_failures:
            return web.Response(status=503, text="временно недоступно",
                                headers={"Retry-After": "0"})
        return web.Response(text=f"<html><title>Flaky {key}</title>ok</html>",
                            content_type="text/html")

    async def _broken(self, request):
        return web.Response(text=BROKEN_HTML, content_type="text/html")

    async def _plain(self, request):
        return web.Response(text="просто текст", content_type="text/plain")

    async def _binary(self, request):
        return web.Response(body=b"\x89PNG\r\n\x1a\n" + bytes(range(256)),
                            content_type="image/png")

    async def _big(self, request):
        """Большая страница (около 100 КБ) для проверки расхода памяти."""
        n = int(request.match_info["n"])
        paragraphs = "".join(f"<p>Абзац {i} страницы {n}: " + "слово " * 30 + "</p>"
                             for i in range(400))
        return web.Response(
            text=f"<html><head><title>Big {n}</title></head><body><h1>Big {n}</h1>"
                 f"{paragraphs}</body></html>", content_type="text/html")

    async def _bulk(self, request):
        n = int(request.match_info["n"])
        delay = float(request.query.get("delay", "0.01"))
        await asyncio.sleep(delay)
        return web.Response(
            text=f"<html><head><title>Bulk {n}</title></head>"
                 f"<body><h1>Bulk {n}</h1><p>{'слово ' * 50}</p></body></html>",
            content_type="text/html")

    async def _robots(self, request):
        text = ROBOTS_TXT + f"\nSitemap: {self.url}/sitemap_index.xml\n"
        return web.Response(text=text, content_type="text/plain")

    def _urlset(self, urls: list[str]) -> str:
        items = "".join(f"<url><loc>{self.url}{u}</loc></url>" for u in urls)
        return ('<?xml version="1.0" encoding="UTF-8"?>'
                '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                f"{items}</urlset>")

    async def _sitemap(self, request):
        return web.Response(text=self._urlset(["/", "/page/1", "/page/2"]),
                            content_type="application/xml")

    async def _sitemap2(self, request):
        return web.Response(text=self._urlset(["/page/3", "/about"]),
                            content_type="application/xml")

    async def _sitemap_index(self, request):
        return web.Response(
            text=('<?xml version="1.0" encoding="UTF-8"?>'
                  '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                  f"<sitemap><loc>{self.url}/sitemap.xml</loc></sitemap>"
                  f"<sitemap><loc>{self.url}/sitemap2.xml</loc></sitemap>"
                  "</sitemapindex>"),
            content_type="application/xml")

    async def start(self) -> "MockSite":
        self._runner = web.AppRunner(self._build_app(), access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, 0)
        await site.start()
        self.port = self._runner.addresses[0][1]
        return self

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def __aenter__(self) -> "MockSite":
        return await self.start()

    async def __aexit__(self, *exc_info) -> None:
        await self.stop()


async def _serve(latency: float, port: int) -> None:
    """Запуск сайта как отдельного процесса: python -m webcrawler.mocksite"""
    site = MockSite(latency=latency)
    runner = web.AppRunner(site._build_app(), access_log=None)
    await runner.setup()
    tcp = web.TCPSite(runner, site.host, port)
    await tcp.start()
    print(f"PORT={runner.addresses[0][1]}", flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Локальный тестовый сайт для краулера")
    parser.add_argument("--latency", type=float, default=0.0)
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    try:
        asyncio.run(_serve(args.latency, args.port))
    except KeyboardInterrupt:
        pass
