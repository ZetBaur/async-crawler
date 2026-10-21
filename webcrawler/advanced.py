"""AdvancedCrawler: все возможности вместе (Day 7)."""
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import TextIO
from urllib.parse import urlparse

from webcrawler.client import AsyncCrawler
from webcrawler.config import ConfigError, CrawlerConfig, StorageConfig, load_config
from webcrawler.filters import UrlFilter
from webcrawler.logging_setup import setup_logging, teardown_logging
from webcrawler.monitor import ProgressMonitor
from webcrawler.parser import normalize_url
from webcrawler.report import render_html_report
from webcrawler.retry import CircuitBreaker, RetryStrategy
from webcrawler.robots import RobotsParser
from webcrawler.sitemap import SitemapParser
from webcrawler.stats import CrawlerStats
from webcrawler.storage import CSVStorage, DataStorage, JSONStorage, SQLiteStorage

logger = logging.getLogger("webcrawler")


def make_storage(config: StorageConfig) -> DataStorage | None:
    """Создаёт хранилище по настройкам storage."""
    if config.type == "none":
        return None
    if config.type == "json":
        return JSONStorage(config.path, mode="array", indent=2, buffer_size=config.buffer_size)
    if config.type == "jsonl":
        return JSONStorage(config.path, buffer_size=config.buffer_size)
    if config.type == "csv":
        return CSVStorage(config.path, buffer_size=config.buffer_size)
    if config.type == "sqlite":
        return SQLiteStorage(config.path, batch_size=config.batch_size)
    raise ConfigError(f"Неизвестный тип хранилища: {config.type}")


class AdvancedCrawler:
    """Собирает вместе настройки, краулер, повторы, хранилище, статистику и отчёты."""

    def __init__(self, config: CrawlerConfig | dict | None = None,
                 configure_logging: bool = True, progress_stream: TextIO | None = None) -> None:
        if config is None:
            config = CrawlerConfig()
        elif isinstance(config, dict):
            config = CrawlerConfig.from_dict(config)
        config.validate()
        self.config = config
        self._logging_configured = configure_logging
        if configure_logging:
            log = config.logging
            setup_logging(log.level, log.file, log.max_bytes, log.backup_count, log.json)

        retry = None
        if config.retry.enabled:
            retry = RetryStrategy(
                max_retries=config.retry.max_retries,
                backoff_factor=config.retry.backoff_factor,
                base_delay=config.retry.base_delay, max_delay=config.retry.max_delay)
        breaker = None
        if config.circuit_breaker.enabled:
            cb = config.circuit_breaker
            breaker = CircuitBreaker(cb.failure_threshold, cb.window, cb.recovery_time)
        self.monitor = (ProgressMonitor(config.max_pages, progress_stream)
                        if config.progress else None)
        self.crawler = AsyncCrawler(
            max_concurrent=config.max_concurrent, max_depth=config.max_depth,
            max_per_domain=config.max_per_domain,
            connect_timeout=config.connect_timeout, read_timeout=config.read_timeout,
            total_timeout=config.total_timeout, user_agent=config.user_agent,
            user_agents=config.user_agents or None,
            requests_per_second=config.requests_per_second, min_delay=config.min_delay,
            jitter=config.jitter, respect_robots=config.respect_robots,
            retry_strategy=retry, circuit_breaker=breaker,
            storage=make_storage(config.storage), keep_results=config.keep_results,
            parse_mode=config.parse_mode, parse_workers=config.parse_workers,
            progress_interval=0.5, on_progress=self.monitor)
        self.results: dict[str, dict] = {}
        self.stats: CrawlerStats | None = None

    @classmethod
    def from_config(cls, source: str | Path | dict | CrawlerConfig, **kwargs) -> "AdvancedCrawler":
        """Создаёт краулер из файла (.yaml, .yml, .json), словаря или готового CrawlerConfig."""
        if isinstance(source, (str, Path)):
            source = load_config(source)
        return cls(source, **kwargs)

    # ---------- работа ----------

    async def crawl(self) -> dict[str, dict]:
        config = self.config
        started = time.perf_counter()
        start_urls = list(config.start_urls)
        start_urls += [u for u in await self._collect_sitemap_urls() if u not in start_urls]
        if not start_urls:
            raise ConfigError("Нет адресов для обхода: задайте start_urls или sitemap_urls")
        logger.info("crawl start urls=%s max_pages=%s max_depth=%s",
                    len(start_urls), config.max_pages, config.max_depth)
        self.results = await self.crawler.crawl(
            start_urls, max_pages=config.max_pages, same_domain_only=config.same_domain_only,
            exclude_patterns=config.exclude_patterns or None,
            include_patterns=config.include_patterns or None)
        self.stats = CrawlerStats.from_crawler(self.crawler, time.perf_counter() - started)
        if self.monitor:
            self.monitor.finish(self.crawler.get_progress())
        logger.info("crawl done pages=%s failed=%s time=%.2fs", self.stats.successful,
                    self.stats.failed, self.stats.elapsed)
        self._auto_export()
        return self.results

    async def _collect_sitemap_urls(self) -> list[str]:
        config = self.config
        sitemap_urls = list(config.sitemap_urls)
        if config.sitemap_from_robots:
            robots = self.crawler.robots or RobotsParser(self.crawler.fetch_raw,
                                                         config.user_agent)
            for url in config.start_urls:
                info = await robots.fetch_robots(url)
                sitemap_urls += [u for u in info.get("sitemaps", []) if u not in sitemap_urls]
        if not sitemap_urls:
            return []
        parser = SitemapParser(self.crawler.fetch_raw)
        found: list[str] = []
        for sitemap_url in sitemap_urls:
            found += await parser.fetch_sitemap(sitemap_url)
        hosts = {urlparse(normalize_url(u, u) or "").netloc
                 for u in (*config.start_urls, *sitemap_urls)} - {""}
        url_filter = UrlFilter(hosts, config.same_domain_only,
                               config.exclude_patterns, config.include_patterns)
        allowed = [u for u in dict.fromkeys(found) if url_filter.allows(u)]
        logger.info("sitemap: найдено %s адресов, подходит %s", len(found), len(allowed))
        return allowed

    # ---------- статистика и отчёты ----------

    def get_stats(self) -> dict:
        stats = (self.stats or CrawlerStats()).summary()
        stats["rate"] = self.crawler.get_rate_stats()
        stats["storage"] = self.crawler.get_storage_stats()
        if self.crawler.retry_strategy:
            stats["retry"] = self.crawler.retry_strategy.stats.as_dict()
        return stats

    def export_to_json(self, filename: str | Path) -> Path:
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "config": self.config.to_dict(),
            "summary": self.get_stats(),
            "errors": self.crawler.error_report(),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                        encoding="utf-8")
        return path

    def export_to_html_report(self, filename: str | Path) -> Path:
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        html = render_html_report(self.get_stats(), dict(self.crawler.failed_urls),
                                  list(self.results.values()))
        path.write_text(html, encoding="utf-8")
        return path

    def _auto_export(self) -> None:
        output = self.config.output
        try:
            if output.stats_json:
                self.export_to_json(output.stats_json)
            if output.html_report:
                self.export_to_html_report(output.html_report)
            if output.error_report:
                self.crawler.export_error_report(output.error_report)
        except OSError as error:
            logger.error("не удалось сохранить отчёт: %s", error)

    async def close(self) -> None:
        await self.crawler.close()
        if self._logging_configured:
            teardown_logging()

    async def __aenter__(self) -> "AdvancedCrawler":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()
