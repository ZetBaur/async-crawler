"""Асинхронный краулер: HTTP-клиент (Day 1), разбор (Day 2), обход сайта (Day 3)."""
import asyncio
import logging
import time
from typing import Callable
from urllib.parse import urlparse

import aiohttp

from webcrawler.crawl_queue import CrawlerQueue
from webcrawler.filters import UrlFilter
from webcrawler.limits import SemaphoreManager
from webcrawler.parser import HTMLParser, normalize_url

logger = logging.getLogger("webcrawler")


class AsyncCrawler:
    """Загружает страницы параллельно через aiohttp и обходит сайты по ссылкам.

    Одна сессия на весь краулер (connection pooling), число одновременных
    запросов ограничено семафорами (всего и на один домен).
    """

    def __init__(self, max_concurrent: int = 10, max_depth: int = 3,
                 max_per_domain: int | None = None,
                 connect_timeout: float = 5.0, read_timeout: float = 15.0,
                 total_timeout: float = 60.0, user_agent: str = "AsyncCrawler/1.0",
                 progress_interval: float = 1.0,
                 on_progress: Callable[[dict], None] | None = None) -> None:
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.total_timeout = total_timeout
        self.user_agent = user_agent
        self.progress_interval = progress_interval
        self.on_progress = on_progress
        self.parser = HTMLParser()
        self.limits = SemaphoreManager(max_concurrent, max_per_domain)
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}
        self.queue: CrawlerQueue | None = None
        self._started_at: float | None = None
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(
                total=self.total_timeout,
                sock_connect=self.connect_timeout,
                sock_read=self.read_timeout,
            )
            connector = aiohttp.TCPConnector(
                limit=self.max_concurrent,
                limit_per_host=self.limits.max_per_domain,
                ttl_dns_cache=300,
            )
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={"User-Agent": self.user_agent},
                raise_for_status=True,
            )
        return self._session

    async def fetch_url(self, url: str) -> str:
        session = await self._get_session()
        domain = urlparse(url).netloc.lower()
        async with self.limits.limit(domain):
            logger.info("start url=%s", url)
            started = time.perf_counter()
            try:
                async with session.get(url) as response:
                    status = response.status
                    text = await response.text(errors="replace")
            except aiohttp.ClientResponseError as error:
                logger.warning("error url=%s type=ClientResponseError status=%s",
                               url, error.status)
                raise
            except asyncio.TimeoutError:
                logger.warning("error url=%s type=TimeoutError", url)
                raise
            except aiohttp.ClientError as error:
                logger.warning("error url=%s type=%s message=%s",
                               url, type(error).__name__, error)
                raise
            logger.info("done url=%s status=%s time=%.2fs",
                        url, status, time.perf_counter() - started)
            return text

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        unique = list(dict.fromkeys(urls))
        results = await asyncio.gather(*(self.fetch_url(u) for u in unique),
                                       return_exceptions=True)
        pages: dict[str, str] = {}
        for url, result in zip(unique, results):
            if isinstance(result, Exception):
                self.failed_urls[url] = f"{type(result).__name__}: {result}"
            elif isinstance(result, BaseException):
                raise result
            else:
                pages[url] = result
        return pages

    async def fetch_and_parse(self, url: str) -> dict:
        """Загружает страницу и возвращает разобранные данные (Day 2)."""
        html = await self.fetch_url(url)
        parsed = await self.parser.parse_html(html, url)
        if parsed["errors"]:
            logger.warning("parse url=%s частичный результат: %s", url, parsed["errors"])
        return parsed

    # ---------- Day 3: обход сайта ----------

    async def crawl(self, start_urls: list[str], max_pages: int = 100,
                    same_domain_only: bool = True,
                    exclude_patterns: list[str] | None = None,
                    include_patterns: list[str] | None = None) -> dict[str, dict]:
        """Обходит сайт по ссылкам и возвращает {url: разобранная страница}."""
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        queue = self.queue = CrawlerQueue(max_urls=max_pages)
        hosts: set[str] = set()
        for url in start_urls:
            normalized = normalize_url(url, url)
            if normalized is None:
                logger.warning("пропускаю некорректный стартовый URL: %r", url)
                continue
            hosts.add(urlparse(normalized).netloc)
            queue.add_url(normalized, priority=0, depth=0)
        url_filter = UrlFilter(hosts, same_domain_only, exclude_patterns, include_patterns)

        self._started_at = time.perf_counter()
        workers = [asyncio.create_task(self._worker(queue, url_filter))
                   for _ in range(self.max_concurrent)]
        reporter = asyncio.create_task(self._report_progress())
        try:
            await asyncio.gather(*workers)
        finally:
            for task in (*workers, reporter):
                task.cancel()
            await asyncio.gather(*workers, reporter, return_exceptions=True)
        self._emit_progress()
        return dict(self.processed_urls)

    async def _worker(self, queue: CrawlerQueue, url_filter: UrlFilter) -> None:
        while True:
            url = await queue.get_next()
            if url is None:
                return
            self.visited_urls.add(url)
            try:
                parsed = await self.fetch_and_parse(url)
            except Exception as error:
                message = f"{type(error).__name__}: {error}"
                self.failed_urls[url] = message
                queue.mark_failed(url, message)
                continue
            self.processed_urls[url] = parsed
            depth = queue.depth(url)
            if depth < self.max_depth:
                for link in parsed["links"]:
                    if url_filter.allows(link):
                        queue.add_url(link, priority=-(depth + 1), depth=depth + 1)
            queue.mark_processed(url)

    def get_progress(self) -> dict:
        stats = self.queue.get_stats() if self.queue else {}
        elapsed = (time.perf_counter() - self._started_at) if self._started_at else 0.0
        done = stats.get("processed", 0) + stats.get("failed", 0) + stats.get("skipped", 0)
        return {
            **stats,
            "done": done,
            "elapsed": elapsed,
            "pages_per_second": done / elapsed if elapsed > 0 else 0.0,
            "active": self.limits.active_total,
        }

    def _emit_progress(self) -> None:
        progress = self.get_progress()
        if self.on_progress:
            self.on_progress(progress)
        else:
            logger.info(
                "progress processed=%s queued=%s failed=%s speed=%.1f стр/сек",
                progress.get("processed", 0), progress.get("queued", 0),
                progress.get("failed", 0), progress["pages_per_second"])

    async def _report_progress(self) -> None:
        while True:
            await asyncio.sleep(self.progress_interval)
            self._emit_progress()

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def __aenter__(self) -> "AsyncCrawler":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()
