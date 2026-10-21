"""Демонстрация Day3: обход сайта с очередью, глубиной и фильтрами."""
import _common  # noqa: F401

import argparse
import asyncio
import json

from webcrawler import AsyncCrawler
from webcrawler.mocksite import MockSite


def print_progress(progress: dict) -> None:
    print(f"  [прогресс] обработано={progress.get('processed', 0)} "
          f"в очереди={progress.get('queued', 0)} "
          f"ошибок={progress.get('failed', 0)} "
          f"активных={progress['active']} "
          f"скорость={progress['pages_per_second']:.1f} стр/сек")


async def run(start_urls: list[str], max_pages: int, max_depth: int) -> None:
    async with AsyncCrawler(max_concurrent=10, max_depth=max_depth,
                            progress_interval=0.3, on_progress=print_progress) as crawler:
        results = await crawler.crawl(start_urls, max_pages=max_pages,
                                      same_domain_only=True)
        print(f"\nОбработано: {len(results)} страниц, ошибок: {len(crawler.failed_urls)}")
        print("Статистика очереди:", crawler.queue.get_stats())
        print("Одновременно максимум:", crawler.limits.peak_total)
        by_depth: dict[int, int] = {}
        for url in results:
            depth = crawler.queue.depth(url)
            by_depth[depth] = by_depth.get(depth, 0) + 1
        print("Страниц по глубине:", dict(sorted(by_depth.items())))
        print("Ошибки:", json.dumps(crawler.failed_urls, ensure_ascii=False, indent=2))

        out = _common.OUTPUT_DIR / "day3_results.json"
        out.parent.mkdir(exist_ok=True)
        out.write_text(json.dumps(
            {u: {"title": r["title"], "links": len(r["links"])} for u, r in results.items()},
            ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Сохранено в {out}")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--external", action="store_true")
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument("--max-depth", type=int, default=3)
    args = parser.parse_args()
    _common.setup_demo_logging(level=40)
    if args.external:
        await run(["https://example.com"], args.max_pages, args.max_depth)
    else:
        async with MockSite(latency=0.2) as site:
            await run([site.url + "/"], args.max_pages, args.max_depth)


if __name__ == "__main__":
    asyncio.run(main())
