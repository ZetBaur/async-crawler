"""Асинхронный краулер: загрузка (Day 1), разбор (Day 2), обход (Day 3), вежливость (Day 4)."""
import asyncio
import itertools
import json
import logging
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, NamedTuple
from urllib.parse import urlparse

import aiohttp

from webcrawler.crawl_queue import CrawlerQueue
from webcrawler.errors import (
    NetworkError,
    RobotsBlockedError,
    TransientError,
    classify_exception,
)
from webcrawler.filters import UrlFilter
from webcrawler.limits import RateLimiter, SemaphoreManager
from webcrawler.parser import HTMLParser, normalize_url
from webcrawler.retry import CircuitBreaker, RetryStrategy
from webcrawler.robots import RobotsParser, origin_of
from webcrawler.storage import DataStorage, make_record

logger = logging.getLogger("webcrawler")


def domain_of(url: str) -> str:
    return urlparse(url).netloc.lower()


class FetchResult(NamedTuple):
    status: int
    text: str
    content_type: str


TEXTUAL_TYPES = {"application/xhtml+xml", "application/xml", "application/json",
                 "application/javascript", "image/svg+xml"}


def is_textual(content_type: str) -> bool:
    """Картинки, PDF и прочее двоичное не читаем как текст."""
    return (not content_type or content_type.startswith("text/")
            or content_type in TEXTUAL_TYPES
            or content_type.endswith(("+xml", "+json")))


class AsyncCrawler:
    """Загружает страницы параллельно через aiohttp и обходит сайты по ссылкам.

    Одна сессия на весь краулер (connection pooling), число одновременных
    запросов ограничено семафорами (всего и на один домен). Вежливость:
    rate limiting, robots.txt, паузы и ротация User-Agent.
    """

    def __init__(self, max_concurrent: int = 10, max_depth: int = 3,
                 max_per_domain: int | None = None,
                 connect_timeout: float = 5.0, read_timeout: float = 15.0,
                 total_timeout: float = 60.0, user_agent: str = "AsyncCrawler/1.0",
                 user_agents: list[str] | None = None,
                 requests_per_second: float | None = None, per_domain_rate: bool = True,
                 min_delay: float = 0.0, jitter: float = 0.0,
                 respect_robots: bool = False,
                 retry_strategy: RetryStrategy | None = None,
                 circuit_breaker: CircuitBreaker | None = None,
                 timeout_growth: float = 0.5,
                 parse_mode: str = "thread", parse_workers: int | None = None,
                 keep_results: bool = True,
                 storage: DataStorage | None = None, storage_retries: int = 2,
                 storage_retry_delay: float = 0.05,
                 progress_interval: float = 1.0,
                 on_progress: Callable[[dict], None] | None = None) -> None:
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.total_timeout = total_timeout
        self.user_agent = user_agent
        self.respect_robots = respect_robots
        self.retry_strategy = retry_strategy
        self.circuit_breaker = circuit_breaker
        self.timeout_growth = timeout_growth
        self.keep_results = keep_results
        self.storage = storage
        self.storage_retries = storage_retries
        self.storage_retry_delay = storage_retry_delay
        self.storage_saved = 0
        self.storage_errors: list[dict] = []
        self.progress_interval = progress_interval
        self.on_progress = on_progress
        self.parser = HTMLParser(parse_mode=parse_mode, workers=parse_workers)
        self.limits = SemaphoreManager(max_concurrent, max_per_domain)
        self.rate_limiter: RateLimiter | None = None
        if requests_per_second or min_delay or jitter or respect_robots:
            self.rate_limiter = RateLimiter(
                requests_per_second=requests_per_second or 1000.0,
                per_domain=per_domain_rate, min_delay=min_delay, jitter=jitter)
        self.robots = RobotsParser(self._raw_get, user_agent) if respect_robots else None
        self._agents = itertools.cycle(user_agents) if user_agents else None
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}
        self.robots_blocked: list[str] = []
        self.error_counts: Counter = Counter()
        self.error_log: list[dict] = []
        self.queue: CrawlerQueue | None = None
        self._started_at: float | None = None
        self._session: aiohttp.ClientSession | None = None

    # ---------- Day 1: загрузка ----------

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
                timeout=timeout, connector=connector, raise_for_status=True)
        return self._session

    def _timeout_for(self, attempt: int) -> aiohttp.ClientTimeout:
        """При повторах таймауты растут: попытка 0 — базовые, дальше + timeout_growth за раз."""
        growth = 1.0 + self.timeout_growth * attempt
        return aiohttp.ClientTimeout(
            total=self.total_timeout * growth,
            sock_connect=self.connect_timeout * growth,
            sock_read=self.read_timeout * growth,
        )

    def _pick_user_agent(self) -> str:
        return next(self._agents) if self._agents else self.user_agent

    def _note_failure(self, domain: str, status: int | None) -> None:
        """Ошибки сервера, 429, таймауты и обрывы замедляют запросы к домену."""
        if self.rate_limiter and (status is None or status == 429 or status >= 500):
            self.rate_limiter.report_error(domain)

    async def _request(self, url: str, user_agent: str, raise_for_status: bool = True,
                       attempt: int = 0) -> FetchResult:
        session = await self._get_session()
        domain = domain_of(url)
        if self.rate_limiter:
            await self.rate_limiter.acquire(domain)
        async with self.limits.limit(domain):
            logger.info("start url=%s", url)
            started = time.perf_counter()
            try:
                async with session.get(url, headers={"User-Agent": user_agent},
                                       raise_for_status=raise_for_status,
                                       timeout=self._timeout_for(attempt)) as response:
                    status = response.status
                    content_type = response.content_type or ""
                    text = (await response.text(errors="replace")
                            if is_textual(content_type) else "")
            except aiohttp.ClientResponseError as error:
                self._note_failure(domain, error.status)
                logger.warning("error url=%s type=ClientResponseError status=%s",
                               url, error.status)
                raise
            except asyncio.TimeoutError:
                self._note_failure(domain, None)
                logger.warning("error url=%s type=TimeoutError", url)
                raise
            except aiohttp.ClientError as error:
                self._note_failure(domain, None)
                logger.warning("error url=%s type=%s message=%s",
                               url, type(error).__name__, error)
                raise
            self._note_failure(domain, status)
            if self.rate_limiter and status < 500 and status != 429:
                self.rate_limiter.report_success(domain)
            logger.info("done url=%s status=%s time=%.2fs",
                        url, status, time.perf_counter() - started)
            return FetchResult(status, text, content_type)

    async def _raw_get(self, url: str) -> tuple[int, str]:
        """Загрузка без исключений на 4xx и 5xx (для robots.txt и sitemap)."""
        result = await self._request(url, self.user_agent, raise_for_status=False)
        return result.status, result.text

    async def fetch_raw(self, url: str) -> tuple[int, str]:
        """Публичная загрузка без исключений на 4xx и 5xx: (статус, текст)."""
        return await self._raw_get(url)

    async def _check_robots(self, url: str, user_agent: str) -> None:
        await self.robots.fetch_robots(url)
        delay = self.robots.get_crawl_delay(user_agent, origin=origin_of(url))
        if delay and self.rate_limiter:
            self.rate_limiter.set_crawl_delay(domain_of(url), delay)
        if not self.robots.can_fetch(url, user_agent):
            self.robots_blocked.append(url)
            logger.warning("blocked by robots.txt url=%s", url)
            raise RobotsBlockedError("запрещено robots.txt", url)

    async def _attempt(self, url: str, user_agent: str, attempt: int = 0) -> FetchResult:
        """Одна попытка загрузки с учётом circuit breaker."""
        domain = domain_of(url)
        if self.circuit_breaker:
            self.circuit_breaker.check(domain)
        try:
            result = await self._request(url, user_agent, True, attempt=attempt)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self.circuit_breaker and isinstance(
                    classify_exception(error, url), (TransientError, NetworkError)):
                self.circuit_breaker.record_failure(domain)
            raise
        if self.circuit_breaker:
            self.circuit_breaker.record_success(domain)
        return result

    async def fetch_url(self, url: str) -> str:
        """Загружает страницу и возвращает её текст."""
        return (await self.fetch_page(url)).text

    async def fetch_page(self, url: str) -> FetchResult:
        """Загружает страницу: статус, текст и тип содержимого.

        Без retry_strategy ошибки aiohttp пробрасываются как есть (Day 1). С ней
        временные ошибки повторяются, а итоговая ошибка приходит нашим типом
        (TransientError, PermanentError, NetworkError, ...).
        """
        user_agent = self._pick_user_agent()
        if self.respect_robots:
            await self._check_robots(url, user_agent)
        if self.retry_strategy:
            return await self.retry_strategy.execute_with_retry(
                self._attempt, url, user_agent, with_attempt=True)
        return await self._attempt(url, user_agent)

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        unique = list(dict.fromkeys(urls))
        results = await asyncio.gather(*(self.fetch_url(u) for u in unique),
                                       return_exceptions=True)
        pages: dict[str, str] = {}
        for url, result in zip(unique, results):
            if isinstance(result, Exception):
                self._record_error(url, result)
            elif isinstance(result, BaseException):
                raise result
            else:
                pages[url] = result
        return pages

    # ---------- Day 6: сохранение ----------

    async def _store(self, url: str, parsed: dict) -> None:
        """Сохраняет страницу в хранилище. Ошибки записи не останавливают краулер."""
        if self.storage is None:
            return
        record = make_record(parsed)
        for attempt in range(self.storage_retries + 1):
            try:
                await self.storage.save(record)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning("storage error url=%s attempt=%s/%s: %s: %s", url,
                               attempt + 1, self.storage_retries + 1,
                               type(error).__name__, error)
                if attempt < self.storage_retries:
                    await asyncio.sleep(self.storage_retry_delay * (2 ** attempt))
                    continue
                self.storage_errors.append(
                    {"url": url, "type": type(error).__name__, "message": str(error)})
            else:
                self.storage_saved += 1
                return

    async def _flush_storage(self) -> None:
        """Финальный сброс буфера с повторами. Окончательная ошибка попадает в storage_errors."""
        if self.storage is None:
            return
        for attempt in range(self.storage_retries + 1):
            try:
                await self.storage.flush()
                return
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning("storage flush attempt=%s/%s: %s: %s", attempt + 1,
                               self.storage_retries + 1, type(error).__name__, error)
                if attempt < self.storage_retries:
                    await asyncio.sleep(self.storage_retry_delay * (2 ** attempt))
                    continue
                lost = self.storage.pending()
                self.storage_saved = max(0, self.storage_saved - lost)
                self.storage_errors.append(
                    {"url": None, "type": type(error).__name__, "message": str(error),
                     "lost_records": lost})
                logger.error("не удалось сбросить хранилище, не записано записей: %s", lost)

    @staticmethod
    def _brief(parsed: dict) -> dict:
        """Краткая сводка вместо полной страницы (keep_results=False): память не растёт."""
        return {
            "url": parsed["url"], "title": parsed.get("title", ""),
            "status_code": parsed.get("status_code"),
            "content_type": parsed.get("content_type", ""),
            "links_count": len(parsed.get("links", [])),
            "text_length": len(parsed.get("text", "")),
        }

    def get_storage_stats(self) -> dict:
        stats = self.storage.stats() if self.storage else {}
        return {**stats, "crawler_saved": self.storage_saved,
                "errors": len(self.storage_errors)}

    # ---------- Day 5: учёт ошибок ----------

    def _record_error(self, url: str, error: Exception) -> str:
        classified = classify_exception(error, url)
        kind = type(classified or error).__name__
        message = f"{type(error).__name__}: {error}"
        self.failed_urls[url] = message
        self.error_counts[kind] += 1
        self.error_log.append({"url": url, "type": kind, "message": str(classified or error),
                               "status": getattr(classified, "status", None)})
        return message

    def error_report(self) -> dict:
        report = {
            "errors_by_type": dict(self.error_counts),
            "failed_urls": dict(self.failed_urls),
            "error_log": list(self.error_log),
        }
        if self.retry_strategy:
            report["retry"] = self.retry_strategy.stats.as_dict()
        if self.circuit_breaker:
            report["open_domains"] = self.circuit_breaker.open_domains()
        return report

    def export_error_report(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.error_report(), ensure_ascii=False, indent=2),
                        encoding="utf-8")
        return path

    # ---------- Day 2: разбор ----------

    async def fetch_and_parse(self, url: str) -> dict:
        page = await self.fetch_page(url)
        parsed = await self.parser.parse_html(page.text, url)
        parsed["status_code"] = page.status
        parsed["content_type"] = page.content_type
        parsed["crawled_at"] = datetime.now(timezone.utc)
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
        self.robots_blocked = []
        self.storage_saved = 0
        self.storage_errors = []
        self.error_counts = Counter()
        self.error_log = []
        queue = self.queue = CrawlerQueue(max_urls=max_pages)
        hosts: set[str] = set()
        for url in start_urls:
            normalized = normalize_url(url, url)
            if normalized is None:
                logger.warning("пропускаю некорректный стартовый URL: %r", url)
                continue
            hosts.add(domain_of(normalized))
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
        await self._flush_storage()
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
            except RobotsBlockedError:
                queue.mark_skipped(url, "robots.txt")
                continue
            except Exception as error:
                queue.mark_failed(url, self._record_error(url, error))
                continue
            self.processed_urls[url] = parsed if self.keep_results else self._brief(parsed)
            await self._store(url, parsed)
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
            "robots_blocked": len(self.robots_blocked),
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

    # ---------- Day 4: мониторинг скорости ----------

    def get_rate_stats(self) -> dict:
        stats = self.rate_limiter.stats() if self.rate_limiter else {}
        stats["robots_blocked"] = len(self.robots_blocked)
        return stats

    async def close(self) -> None:
        self.parser.close()
        if self.storage is not None:
            try:
                await self.storage.close()
            except Exception as error:
                logger.error("не удалось закрыть хранилище: %s: %s",
                             type(error).__name__, error)
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def __aenter__(self) -> "AsyncCrawler":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()
