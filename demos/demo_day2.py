"""Демонстрация Day2: загрузка и разбор страниц."""
import _common  # noqa: F401

import argparse
import asyncio
import json

from webcrawler import AsyncCrawler
from webcrawler.mocksite import MockSite


def summary(parsed: dict) -> dict:
    return {
        "url": parsed["url"],
        "title": parsed["title"],
        "text_length": len(parsed["text"]),
        "links_count": len(parsed["links"]),
        "links": parsed["links"][:5],
        "images_count": len(parsed["images"]),
    }


async def run(urls: list[str]) -> None:
    async with AsyncCrawler(max_concurrent=5) as crawler:
        results = await asyncio.gather(*(crawler.fetch_and_parse(u) for u in urls),
                                       return_exceptions=True)
    total_links = 0
    for url, result in zip(urls, results):
        print(f"\n=== {url} ===")
        if isinstance(result, Exception):
            print(f"ОШИБКА: {type(result).__name__}: {result}")
            continue
        print(json.dumps(summary(result), ensure_ascii=False, indent=2))
        print("заголовки:", result["headings"])
        if result["errors"]:
            print("предупреждения парсинга:", result["errors"])
        total_links += len(result["links"])
    print(f"\nВсего ссылок на всех страницах: {total_links}")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--external", action="store_true")
    args = parser.parse_args()
    _common.setup_demo_logging()
    if args.external:
        await run(["https://example.com", "https://httpbin.org/html"])
    else:
        async with MockSite() as site:
            await run([f"{site.url}/", f"{site.url}/page/1", f"{site.url}/broken",
                       f"{site.url}/plain", f"{site.url}/status/404"])


if __name__ == "__main__":
    asyncio.run(main())
