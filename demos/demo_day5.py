"""Демонстрация Day5: ошибки, повторы, backoff, circuit breaker, отчёт об ошибках."""
import _common  # noqa: F401

import asyncio
import json

from webcrawler import AsyncCrawler
from webcrawler.mocksite import MockSite
from webcrawler.retry import CircuitBreaker, RetryStrategy


async def run(site: MockSite) -> None:
    strategy = RetryStrategy(max_retries=3, backoff_factor=2.0, base_delay=0.1)
    breaker = CircuitBreaker(failure_threshold=50)
    urls = [
        f"{site.url}/",                 # обычная страница
        f"{site.url}/flaky/demo",       # 2 раза 503, потом успех: повторы работают
        f"{site.url}/status/404",       # постоянная ошибка: без повторов
        f"{site.url}/status/403",       # постоянная ошибка: без повторов
        f"{site.url}/status/503",       # всегда 503: повторы и сдаёмся
        f"{site.url}/status/429",       # слишком много запросов: ждём по Retry-After
        f"{site.url}/delay/0.6",        # медленная: таймаут растёт между попытками
        "http://127.0.0.1:1/",          # сеть: отказ в соединении
    ]
    async with AsyncCrawler(max_depth=0, read_timeout=0.3, timeout_growth=1.0,
                            retry_strategy=strategy, circuit_breaker=breaker,
                            progress_interval=60) as crawler:
        results = await crawler.crawl(urls)

        print(f"\nУспешно: {len(results)}, ошибок: {len(crawler.failed_urls)}")
        print("\n--- Итог по каждому адресу ---")
        for url in urls:
            if url in results:
                print(f"OK      {url}")
            else:
                print(f"ОШИБКА  {url}  -> {crawler.failed_urls[url][:90]}")

        stats = strategy.stats.as_dict()
        print("\n--- Статистика ошибок и повторов ---")
        print("Ошибок всего:          ", stats["total_errors"])
        print("По типам:              ", stats["errors_by_type"])
        print("Повторных попыток:     ", stats["retries"])
        print("Успешных повторов:     ", stats["successful_retries"])
        print("Сдались после повторов:", stats["gave_up"])
        print("Среднее время на повтор:", stats["avg_retry_time"], "сек")
        print("Постоянные ошибки (без повторов):")
        for url, text in stats["permanent_urls"].items():
            print("  -", url, "->", text)

        path = crawler.export_error_report(_common.OUTPUT_DIR / "error_report.json")
        print(f"\nОтчёт сохранён: {path}")
        print(json.dumps(crawler.error_report()["errors_by_type"], ensure_ascii=False))


async def main() -> None:
    _common.setup_demo_logging(level=30)
    async with MockSite() as site:
        await run(site)


if __name__ == "__main__":
    asyncio.run(main())
