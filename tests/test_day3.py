import asyncio
from collections import Counter

import pytest

from webcrawler import AsyncCrawler
from webcrawler.crawl_queue import CrawlerQueue
from webcrawler.filters import UrlFilter
from webcrawler.limits import SemaphoreManager


# ---------- очередь ----------

async def test_queue_priority_then_fifo():
    queue = CrawlerQueue()
    queue.add_url("low", priority=1)
    queue.add_url("high", priority=10)
    queue.add_url("mid-a", priority=5)
    queue.add_url("mid-b", priority=5)
    order = [await queue.get_next() for _ in range(4)]
    assert order == ["high", "mid-a", "mid-b", "low"]


async def test_queue_rejects_duplicates():
    queue = CrawlerQueue()
    assert queue.add_url("a") is True
    assert queue.add_url("a") is False
    assert queue.get_stats()["seen"] == 1


async def test_queue_max_urls_limit():
    queue = CrawlerQueue(max_urls=2)
    assert queue.add_url("a") and queue.add_url("b")
    assert queue.add_url("c") is False
    assert queue.get_stats()["seen"] == 2


async def test_queue_returns_none_when_all_work_done():
    queue = CrawlerQueue()
    assert await queue.get_next() is None
    queue.add_url("a")
    url = await queue.get_next()
    queue.mark_processed(url)
    assert await queue.get_next() is None


async def test_queue_waits_for_in_progress_work():
    queue = CrawlerQueue()
    queue.add_url("a")
    first = await queue.get_next()
    waiter = asyncio.create_task(queue.get_next())
    await asyncio.sleep(0.05)
    assert not waiter.done()
    queue.add_url("b")
    queue.mark_processed(first)
    assert await asyncio.wait_for(waiter, 1) == "b"


async def test_queue_waiter_gets_none_when_nothing_more_comes():
    queue = CrawlerQueue()
    queue.add_url("a")
    first = await queue.get_next()
    waiter = asyncio.create_task(queue.get_next())
    await asyncio.sleep(0.05)
    queue.mark_failed(first, "boom")
    assert await asyncio.wait_for(waiter, 1) is None


async def test_queue_stats_and_depth():
    queue = CrawlerQueue()
    queue.add_url("a", depth=0)
    queue.add_url("b", depth=2)
    queue.add_url("c")
    a = await queue.get_next()
    b = await queue.get_next()
    c = await queue.get_next()
    queue.mark_processed(a)
    queue.mark_failed(b, "err")
    queue.mark_skipped(c, "robots")
    stats = queue.get_stats()
    assert stats == {"seen": 3, "queued": 0, "in_progress": 0,
                     "processed": 1, "failed": 1, "skipped": 1}
    assert queue.depth("b") == 2


# ---------- семафоры ----------

async def test_semaphore_manager_limits_per_domain_and_total():
    manager = SemaphoreManager(max_concurrent=4, max_per_domain=2)

    async def job(domain):
        async with manager.limit(domain):
            await asyncio.sleep(0.05)

    await asyncio.gather(*[job("a.test") for _ in range(6)],
                         *[job("b.test") for _ in range(6)])
    assert manager.peak_by_domain["a.test"] <= 2
    assert manager.peak_by_domain["b.test"] <= 2
    assert manager.peak_total <= 4
    assert manager.active_total == 0


# ---------- фильтры ----------

def test_url_filter():
    hosts = {"a.test"}
    assert UrlFilter(hosts).allows("http://a.test/x")
    assert not UrlFilter(hosts).allows("http://b.test/x")
    assert UrlFilter(hosts, same_domain_only=False).allows("http://b.test/x")
    flt = UrlFilter(hosts, exclude_patterns=[r"/private/"], include_patterns=[r"/page/"])
    assert flt.allows("http://a.test/page/1")
    assert not flt.allows("http://a.test/about")
    assert not flt.allows("http://a.test/private/page/1")


# ---------- обход сайта ----------

async def test_crawl_full_site_without_duplicates(site):
    async with AsyncCrawler(max_concurrent=5, max_depth=10, progress_interval=60) as crawler:
        results = await crawler.crawl([site.url + "/"], max_pages=100)
    # главная + 30 страниц + about + private/secret
    assert len(results) == 33
    counts = Counter(site.paths())
    assert max(counts.values()) == 1
    assert crawler.visited_urls == set(results)
    assert not any("example.org" in url for url in results)


async def test_crawl_respects_max_depth(site):
    async with AsyncCrawler(max_depth=0, progress_interval=60) as crawler:
        results = await crawler.crawl([site.url + "/"])
    assert list(results) == [site.url + "/"]

    async with AsyncCrawler(max_depth=1, progress_interval=60) as crawler:
        results = await crawler.crawl([site.url + "/"])
    assert set(results) == {site.url + p for p in
                            ("/", "/page/1", "/page/2", "/about", "/private/secret")}


async def test_crawl_respects_max_pages(site):
    async with AsyncCrawler(max_depth=10, progress_interval=60) as crawler:
        results = await crawler.crawl([site.url + "/"], max_pages=5)
    assert len(results) == 5


async def test_crawl_filters(site):
    async with AsyncCrawler(max_depth=10, progress_interval=60) as crawler:
        results = await crawler.crawl([site.url + "/"], exclude_patterns=[r"/private/"])
    assert not any("/private/" in url for url in results)

    async with AsyncCrawler(max_depth=10, progress_interval=60) as crawler:
        results = await crawler.crawl([site.url + "/"], include_patterns=[r"/page/\d+$"])
    assert site.url + "/" in results
    assert all("/page/" in url for url in results if url != site.url + "/")
    assert site.url + "/about" not in results


async def test_crawl_records_failures_and_continues(site):
    start = [site.url + "/", site.url + "/status/404"]
    async with AsyncCrawler(max_depth=1, progress_interval=60) as crawler:
        results = await crawler.crawl(start)
    assert site.url + "/status/404" in crawler.failed_urls
    assert site.url + "/" in results


async def test_crawl_normalizes_urls_to_avoid_duplicates(site):
    start = [site.url + "/page/1", site.url + "/page/1#frag", site.url]
    async with AsyncCrawler(max_depth=0, progress_interval=60) as crawler:
        results = await crawler.crawl(start)
    assert set(results) == {site.url + "/page/1", site.url + "/"}


async def test_crawl_reports_progress(site):
    seen = []
    async with AsyncCrawler(max_depth=2, progress_interval=0.05,
                            on_progress=seen.append) as crawler:
        await crawler.crawl([site.url + "/delay/0.3"], max_pages=10)
    assert seen
    last = seen[-1]
    assert {"processed", "queued", "failed", "pages_per_second"} <= set(last)


async def test_crawl_ignores_invalid_start_urls(site):
    async with AsyncCrawler(max_depth=0, progress_interval=60) as crawler:
        results = await crawler.crawl(["mailto:a@b.c", "", site.url + "/about"])
    assert list(results) == [site.url + "/about"]


async def test_crawl_cancellation_does_not_hang(site):
    async with AsyncCrawler(max_depth=5, progress_interval=60) as crawler:
        task = asyncio.create_task(crawler.crawl([site.url + "/delay/5"]))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
