import asyncio
import time

import aiohttp
import pytest

from webcrawler import AsyncCrawler


async def test_fetch_valid_url(site):
    async with AsyncCrawler() as crawler:
        html = await crawler.fetch_url(site.url + "/")
    assert "Mock Home" in html


async def test_http_404_raises_client_response_error(site):
    async with AsyncCrawler() as crawler:
        with pytest.raises(aiohttp.ClientResponseError) as info:
            await crawler.fetch_url(site.url + "/status/404")
    assert info.value.status == 404


async def test_http_500_raises_client_response_error(site):
    async with AsyncCrawler() as crawler:
        with pytest.raises(aiohttp.ClientResponseError) as info:
            await crawler.fetch_url(site.url + "/status/500")
    assert info.value.status == 500


async def test_timeout(site):
    async with AsyncCrawler(read_timeout=0.2) as crawler:
        with pytest.raises(asyncio.TimeoutError):
            await crawler.fetch_url(site.url + "/delay/2")


async def test_connection_refused():
    async with AsyncCrawler() as crawler:
        with pytest.raises(aiohttp.ClientConnectorError):
            await crawler.fetch_url("http://127.0.0.1:1/")


async def test_fetch_urls_collects_pages_and_failures(site):
    urls = [site.url + "/", site.url + "/about", site.url + "/status/404",
            "http://127.0.0.1:1/"]
    async with AsyncCrawler() as crawler:
        pages = await crawler.fetch_urls(urls)
        failed = dict(crawler.failed_urls)
    assert set(pages) == {urls[0], urls[1]}
    assert set(failed) == {urls[2], urls[3]}
    assert "ClientResponseError" in failed[urls[2]]


async def test_fetch_urls_removes_duplicates(site):
    url = site.url + "/about"
    async with AsyncCrawler() as crawler:
        pages = await crawler.fetch_urls([url, url, url])
    assert list(pages) == [url]
    assert site.paths().count("/about") == 1


async def test_parallel_faster_than_sequential(site):
    urls = [site.url + "/delay/0.3" for _ in range(5)]
    urls = [u + f"?i={i}" for i, u in enumerate(urls)]
    async with AsyncCrawler(max_concurrent=5) as crawler:
        started = time.perf_counter()
        for url in urls:
            await crawler.fetch_url(url)
        sequential = time.perf_counter() - started

        started = time.perf_counter()
        await crawler.fetch_urls(urls)
        parallel = time.perf_counter() - started
    assert parallel < sequential * 0.6


async def test_concurrency_is_limited(site):
    urls = [site.url + f"/delay/0.2?i={i}" for i in range(8)]
    async with AsyncCrawler(max_concurrent=2) as crawler:
        await crawler.fetch_urls(urls)
    assert site.max_active <= 2


async def test_close_is_idempotent(site):
    crawler = AsyncCrawler()
    await crawler.fetch_url(site.url + "/")
    await crawler.close()
    await crawler.close()


async def test_invalid_max_concurrent():
    with pytest.raises(ValueError):
        AsyncCrawler(max_concurrent=0)
