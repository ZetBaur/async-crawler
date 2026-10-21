"""Демонстрация Day6: сохранение собранных данных в JSON, CSV и SQLite."""
import _common  # noqa: F401

import argparse
import asyncio
import csv
import time

from webcrawler import AsyncCrawler
from webcrawler.mocksite import MockSite
from webcrawler.storage import (
    CSVStorage,
    JSONStorage,
    MultiStorage,
    SQLiteStorage,
    read_json_records,
)


async def run(start: str, max_depth: int) -> None:
    out = _common.OUTPUT_DIR / "day6"
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "results.jsonl"
    pretty_path = out / "results_pretty.json"
    csv_path = out / "results.csv"
    db_path = out / "crawler.db"

    db_path.unlink(missing_ok=True)  # чтобы в демо считалась только свежая загрузка
    sqlite = SQLiteStorage(db_path, batch_size=10)
    storage = MultiStorage(
        JSONStorage(json_path),
        JSONStorage(pretty_path, mode="array", indent=2),
        CSVStorage(csv_path, encoding="utf-8-sig"),
        sqlite,
    )
    async with AsyncCrawler(max_concurrent=5, max_depth=max_depth, storage=storage,
                            progress_interval=60) as crawler:
        started = time.perf_counter()
        results = await crawler.crawl([start], max_pages=60)
        elapsed = time.perf_counter() - started
        print(f"\nОбойдено страниц: {len(results)} за {elapsed:.2f} сек")
        print("Статистика хранилища:", crawler.get_storage_stats())
        print("Ошибок сохранения:", len(crawler.storage_errors))

        print("\n--- Чтение сохранённого ---")
        records = read_json_records(json_path)
        print(f"JSON Lines: {len(records)} записей, первая: {records[0]['url']}")
        with open(csv_path, newline="", encoding="utf-8-sig") as file:
            rows = list(csv.DictReader(file))
        print(f"CSV:        {len(rows)} строк, колонки: {list(rows[0])}")
        print(f"SQLite:     {await sqlite.count()} строк")
        found = await sqlite.search_title("Страница 2")
        print("Поиск по заголовку 'Страница 2' в SQLite:", len(found), "шт.,",
              [r["title"] for r in found][:5])

    print("\nФайлы:")
    for path in (json_path, pretty_path, csv_path, db_path):
        print(f"  {path}  ({path.stat().st_size} байт)")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--external", action="store_true")
    args = parser.parse_args()
    _common.setup_demo_logging(level=30)
    if args.external:
        await run("https://example.com", 1)
    else:
        async with MockSite(latency=0.02) as site:
            await run(site.url + "/", 4)


if __name__ == "__main__":
    asyncio.run(main())
