"""Демонстрация Day4: rate limiting, robots.txt, задержки, User-Agent."""
import _common  # noqa: F401

import argparse
import asyncio
import time

from webcrawler import AsyncCrawler
from webcrawler.mocksite import MockSite


async def run(start: str, max_depth: int) -> None:
    async with AsyncCrawler(
        max_concurrent=5,
        max_depth=max_depth,
        requests_per_second=5.0,       # не чаще 5 запросов в секунду на домен
        respect_robots=True,           # читаем и соблюдаем robots.txt
        min_delay=0.0,
        jitter=0.05,                   # случайная добавка, как у человека
        user_agent="DemoBot/1.0",
        progress_interval=60,
    ) as crawler:
        started = time.perf_counter()
        results = await crawler.crawl([start], max_pages=50)
        elapsed = time.perf_counter() - started

        print(f"\nОбработано страниц: {len(results)} за {elapsed:.2f} сек")
        print("Заблокировано robots.txt:")
        for url in crawler.robots_blocked:
            print("  -", url)
        stats = crawler.get_rate_stats()
        print("\n--- Статистика скорости ---")
        print(f"Всего запросов:               {stats['total_requests']}")
        print(f"Текущая скорость:             {stats['current_rps']} запросов/сек")
        print(f"Средняя пауза между запросами: {stats['avg_delay_between_requests']} сек")
        print(f"Среднее ожидание в очереди:   {stats['avg_wait']} сек")
        print(f"Запросов по доменам:          {stats['requests_by_domain']}")
        print(f"Заблокировано robots.txt:     {stats['robots_blocked']}")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--external", action="store_true")
    args = parser.parse_args()
    _common.setup_demo_logging(level=30)
    if args.external:
        await run("https://example.com", 1)
    else:
        async with MockSite(latency=0.05) as site:
            await run(site.url + "/", 2)
            print("\nЧто видел сервер (первые 12 запросов):")
            for _, path in site.requests[:12]:
                print("  ", path)
            print("User-Agent у всех запросов:", set(site.agents))


if __name__ == "__main__":
    asyncio.run(main())
