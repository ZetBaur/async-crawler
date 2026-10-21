"""Тест производительности: синхронная и асинхронная загрузка (Day 7).

Что измеряется (всё на локальном тестовом сайте, где каждая страница отвечает
с задержкой --latency, как реальный сервер):
  1. синхронный краулер (urllib, по одной странице) и асинхронный на 100, 500, 1000 страниц;
  2. пиковая память Python-объектов (tracemalloc) для обоих;
  3. как скорость зависит от числа одновременных запросов;
  4. разбор HTML: в потоке, прямо в event loop и в отдельных процессах;
  5. расход памяти при обходе больших страниц с keep_results=True и False.

Запуск: python benchmarks/compare.py [--sizes 100 500 1000] [--latency 0.02]
"""
import argparse
import asyncio
import json
import subprocess
import sys
import time
import tracemalloc
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from webcrawler import AsyncCrawler  # noqa: E402
from webcrawler.storage import DataStorage  # noqa: E402


class NullStorage(DataStorage):
    """Хранилище-пустышка: записи принимаются и сразу выбрасываются."""

    async def save(self, data: dict) -> None:
        pass

    async def close(self) -> None:
        pass


async def big_crawl(base: str, count: int, keep_results: bool) -> int:
    urls = [f"{base}/big/{i}" for i in range(count)]
    async with AsyncCrawler(max_concurrent=20, max_depth=0, keep_results=keep_results,
                            storage=NullStorage(), progress_interval=60) as crawler:
        results = await crawler.crawl(urls, max_pages=count)
    return len(results)


def start_server(latency: float) -> tuple[subprocess.Popen, str]:
    process = subprocess.Popen(
        [sys.executable, "-m", "webcrawler.mocksite", "--latency", str(latency)],
        cwd=ROOT, stdout=subprocess.PIPE, text=True)
    line = process.stdout.readline().strip()
    if not line.startswith("PORT="):
        process.kill()
        raise RuntimeError(f"Тестовый сайт не запустился: {line!r}")
    return process, f"http://127.0.0.1:{line.split('=')[1]}"


def make_urls(base: str, count: int, delay: float) -> list[str]:
    return [f"{base}/bulk/{i}?delay={delay}" for i in range(count)]


def sync_crawl(urls: list[str]) -> int:
    ok = 0
    for url in urls:
        with urllib.request.urlopen(url, timeout=30) as response:
            response.read()
            ok += 1
    return ok


async def async_crawl(urls: list[str], concurrency: int) -> int:
    async with AsyncCrawler(max_concurrent=concurrency) as crawler:
        pages = await crawler.fetch_urls(urls)
    return len(pages)


async def parse_crawl(urls: list[str], concurrency: int, mode: str) -> int:
    async with AsyncCrawler(max_concurrent=concurrency, parse_mode=mode) as crawler:
        results = await asyncio.gather(*(crawler.fetch_and_parse(u) for u in urls))
    return len(results)


def timed(function, *args) -> float:
    started = time.perf_counter()
    function(*args)
    return time.perf_counter() - started


def timed_async(coro_fn, *args) -> float:
    started = time.perf_counter()
    asyncio.run(coro_fn(*args))
    return time.perf_counter() - started


def peak_memory_mb(function, *args, is_async: bool = False) -> float:
    tracemalloc.start()
    try:
        if is_async:
            asyncio.run(function(*args))
        else:
            function(*args)
        return tracemalloc.get_traced_memory()[1] / 1024 / 1024
    finally:
        tracemalloc.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--sizes", type=int, nargs="+", default=[100, 500, 1000])
    parser.add_argument("--latency", type=float, default=0.02,
                        help="задержка ответа сервера, сек (по умолчанию 0.02)")
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--sync-max", type=int, default=1000,
                        help="для больших размеров синхронный краулер не запускать")
    parser.add_argument("--sweep-size", type=int, default=500)
    args = parser.parse_args()

    process, base = start_server(args.latency)
    report: dict = {"latency": args.latency, "concurrency": args.concurrency,
                    "scale": [], "concurrency_sweep": [], "parsing": [], "memory": []}
    try:
        print(f"Тестовый сайт: {base}, задержка ответа {args.latency} сек\n")
        print("1) Масштабирование и память")
        print(f"{'страниц':>8} {'sync, сек':>10} {'async, сек':>11} {'ускорение':>10} "
              f"{'async стр/сек':>14} {'sync память':>12} {'async память':>13}")
        for size in args.sizes:
            urls = make_urls(base, size, args.latency)
            async_time = timed_async(async_crawl, urls, args.concurrency)
            async_mem = peak_memory_mb(async_crawl, urls, args.concurrency, is_async=True)
            row = {"pages": size, "async_seconds": round(async_time, 3),
                   "async_pages_per_second": round(size / async_time, 1),
                   "async_peak_mb": round(async_mem, 2)}
            if size <= args.sync_max:
                sync_time = timed(sync_crawl, urls)
                sync_mem = peak_memory_mb(sync_crawl, urls)
                row.update(sync_seconds=round(sync_time, 3), speedup=round(sync_time / async_time, 1),
                           sync_peak_mb=round(sync_mem, 2))
                print(f"{size:>8} {sync_time:>10.2f} {async_time:>11.2f} "
                      f"{sync_time / async_time:>9.1f}x {size / async_time:>14.1f} "
                      f"{sync_mem:>10.2f}MB {async_mem:>11.2f}MB")
            else:
                print(f"{size:>8} {'-':>10} {async_time:>11.2f} {'-':>10} "
                      f"{size / async_time:>14.1f} {'-':>12} {async_mem:>11.2f}MB")
            report["scale"].append(row)

        print(f"\n2) Зависимость скорости от числа одновременных запросов ({args.sweep_size} страниц)")
        print(f"{'одновременно':>13} {'время, сек':>11} {'стр/сек':>9}")
        urls = make_urls(base, args.sweep_size, args.latency)
        for concurrency in (1, 5, 10, 25, 50, 100, 200):
            elapsed = timed_async(async_crawl, urls, concurrency)
            print(f"{concurrency:>13} {elapsed:>11.2f} {args.sweep_size / elapsed:>9.1f}")
            report["concurrency_sweep"].append(
                {"concurrency": concurrency, "seconds": round(elapsed, 3),
                 "pages_per_second": round(args.sweep_size / elapsed, 1)})

        print(f"\n3) Режимы разбора HTML: маленькие страницы ({args.sweep_size} шт.) и большие (200 шт.)")
        print(f"{'страницы':>10} {'режим':>9} {'время, сек':>11} {'стр/сек':>9}")
        small_urls = make_urls(base, args.sweep_size, args.latency)
        big_urls = [f"{base}/big/{i}" for i in range(200)]
        for label, group in (("маленькие", small_urls), ("большие", big_urls)):
            for mode in ("thread", "inline", "process"):
                best = min(timed_async(parse_crawl, group, args.concurrency, mode)
                           for _ in range(2))
                print(f"{label:>10} {mode:>9} {best:>11.2f} {len(group) / best:>9.1f}")
                report["parsing"].append({"pages": label, "mode": mode,
                                          "seconds": round(best, 3),
                                          "pages_per_second": round(len(group) / best, 1)})

        big = 300
        print(f"\n4) Память при обходе больших страниц ({big} страниц по ~100 КБ)")
        print(f"{'режим':>34} {'пик памяти':>12} {'время, сек':>11}")
        for label, keep in (("keep_results=True (по умолчанию)", True),
                            ("keep_results=False + хранилище", False)):
            elapsed = timed_async(big_crawl, base, big, keep)
            peak = peak_memory_mb(big_crawl, base, big, keep, is_async=True)
            print(f"{label:>34} {peak:>10.1f}MB {elapsed:>11.2f}")
            report["memory"].append({"mode": label, "peak_mb": round(peak, 1),
                                     "seconds": round(elapsed, 3)})
    finally:
        process.kill()
        process.wait()

    out = ROOT / "output" / "benchmark.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nРезультаты сохранены: {out}")


if __name__ == "__main__":
    main()
