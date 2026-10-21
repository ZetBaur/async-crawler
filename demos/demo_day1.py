"""Демонстрация Day1: параллельная загрузка и обработка ошибок.

По умолчанию работает с локальным тестовым сайтом (быстро, без интернета).
Флаг --external берёт настоящие адреса (example.com и httpbin.org).
"""
import _common  # noqa: F401  (добавляет корень проекта в sys.path)

import argparse
import asyncio
import time

from webcrawler import AsyncCrawler
from webcrawler.mocksite import MockSite

EXTERNAL_URLS = [
    "https://example.com",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/status/404",
]


def local_urls(base: str) -> list[str]:
    return [
        f"{base}/",
        f"{base}/delay/0.5",
        f"{base}/delay/1",
        f"{base}/delay/1.5",
        f"{base}/page/1",
        f"{base}/status/404",
        f"{base}/status/500",
        "http://127.0.0.1:1/",
    ]


async def sequential(urls: list[str]) -> tuple[float, int]:
    ok = 0
    started = time.perf_counter()
    async with AsyncCrawler(max_concurrent=1) as crawler:
        for url in urls:
            try:
                await crawler.fetch_url(url)
                ok += 1
            except Exception:
                pass
    return time.perf_counter() - started, ok


async def parallel(urls: list[str]) -> tuple[float, dict, dict]:
    started = time.perf_counter()
    async with AsyncCrawler(max_concurrent=5) as crawler:
        pages = await crawler.fetch_urls(urls)
        failed = dict(crawler.failed_urls)
    return time.perf_counter() - started, pages, failed


async def run(urls: list[str]) -> None:
    seq_time, seq_ok = await sequential(urls)
    par_time, pages, failed = await parallel(urls)

    print("\n--- Статус каждого запроса (параллельная загрузка) ---")
    for url in urls:
        if url in pages:
            print(f"OK      {url}  ({len(pages[url])} символов)")
        else:
            print(f"ОШИБКА  {url}  -> {failed.get(url)}")

    print("\n--- Итог ---")
    print(f"Загружено: {len(pages)} из {len(urls)}")
    print(f"Последовательно: {seq_time:.2f} сек")
    print(f"Параллельно:     {par_time:.2f} сек")
    print(f"Ускорение:       в {seq_time / par_time:.1f} раза")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--external", action="store_true",
                        help="использовать настоящие сайты вместо локального")
    args = parser.parse_args()
    _common.setup_demo_logging()
    if args.external:
        await run(EXTERNAL_URLS)
    else:
        async with MockSite() as site:
            await run(local_urls(site.url))


if __name__ == "__main__":
    asyncio.run(main())
