import asyncio
import random
import time

import pytest

from webcrawler import AsyncCrawler
from webcrawler.limits import RateLimiter
from webcrawler.robots import RobotsParser, agent_token, parse_crawl_delays

ROBOTS = """# комментарий
User-agent: *
Disallow: /private/
Crawl-delay: 0.2

User-agent: BadBot
Disallow: /
"""


def gaps(times):
    return [b - a for a, b in zip(times, times[1:])]


# ---------- RateLimiter ----------

async def test_rate_limiter_spaces_requests_for_one_domain():
    limiter = RateLimiter(requests_per_second=20)
    times = []
    for _ in range(5):
        await limiter.acquire("a.test")
        times.append(time.monotonic())
    assert all(g >= 0.045 for g in gaps(times))


async def test_rate_limiter_domains_are_independent():
    limiter = RateLimiter(requests_per_second=10, per_domain=True)
    started = time.monotonic()
    await asyncio.gather(*[limiter.acquire("a.test") for _ in range(4)],
                         *[limiter.acquire("b.test") for _ in range(4)])
    elapsed = time.monotonic() - started
    assert 0.25 <= elapsed < 0.5  # 3 интервала по 0.1, а не 7


async def test_rate_limiter_global_limit_when_per_domain_off():
    limiter = RateLimiter(requests_per_second=10, per_domain=False)
    started = time.monotonic()
    await asyncio.gather(*[limiter.acquire("a.test") for _ in range(4)],
                         *[limiter.acquire("b.test") for _ in range(4)])
    assert time.monotonic() - started >= 0.65  # 7 интервалов по 0.1


async def test_min_delay_dominates_when_larger():
    limiter = RateLimiter(requests_per_second=1000, min_delay=0.1)
    assert limiter.current_interval("a.test") == pytest.approx(0.1)


async def test_jitter_adds_random_extra_delay():
    limiter = RateLimiter(requests_per_second=50, jitter=0.05, rng=random.Random(1))
    times = []
    for _ in range(6):
        await limiter.acquire("a.test")
        times.append(time.monotonic())
    spacing = gaps(times)
    assert all(g >= 0.018 for g in spacing)
    assert max(spacing) > 0.02 + 0.005


def test_exponential_backoff_and_reset():
    limiter = RateLimiter(requests_per_second=10)
    assert limiter.current_interval("a.test") == pytest.approx(0.1)
    limiter.report_error("a.test")
    assert limiter.current_interval("a.test") == pytest.approx(0.2)
    limiter.report_error("a.test")
    assert limiter.current_interval("a.test") == pytest.approx(0.4)
    assert limiter.current_interval("b.test") == pytest.approx(0.1)
    limiter.report_success("a.test")
    assert limiter.current_interval("a.test") == pytest.approx(0.1)


def test_backoff_is_capped():
    limiter = RateLimiter(requests_per_second=10, max_backoff=1.0)
    for _ in range(20):
        limiter.report_error("a.test")
    assert limiter.current_interval("a.test") == pytest.approx(1.0)


def test_crawl_delay_overrides_rate():
    limiter = RateLimiter(requests_per_second=10)
    limiter.set_crawl_delay("a.test", 0.5)
    assert limiter.current_interval("a.test") == pytest.approx(0.5)
    assert limiter.current_interval("b.test") == pytest.approx(0.1)


@pytest.mark.parametrize("kwargs", [{"requests_per_second": 0},
                                    {"requests_per_second": -1},
                                    {"min_delay": -1}, {"jitter": -0.1}])
def test_rate_limiter_validates_arguments(kwargs):
    with pytest.raises(ValueError):
        RateLimiter(**kwargs)


async def test_rate_limiter_stats():
    limiter = RateLimiter(requests_per_second=50)
    for _ in range(5):
        await limiter.acquire("a.test")
    stats = limiter.stats()
    assert stats["total_requests"] == 5
    assert stats["requests_by_domain"] == {"a.test": 5}
    assert stats["avg_delay_between_requests"] >= 0.018
    assert 10 < stats["current_rps"] < 80


# ---------- robots.txt ----------

def fake_fetcher(status=200, text=ROBOTS, error=None):
    calls = []

    async def fetch(url):
        calls.append(url)
        await asyncio.sleep(0.01)
        if error:
            raise error
        return status, text

    fetch.calls = calls
    return fetch


async def test_robots_rules_and_user_agents():
    robots = RobotsParser(fake_fetcher())
    info = await robots.fetch_robots("http://a.test/some/page")
    assert info["found"] and info["status"] == 200
    assert robots.can_fetch("http://a.test/public", "MyBot/1.0")
    assert not robots.can_fetch("http://a.test/private/x", "MyBot/1.0")
    assert not robots.can_fetch("http://a.test/anything", "BadBot/2.0")
    assert robots.can_fetch("http://a.test/anything", "GoodBot/1.0")


async def test_robots_crawl_delay_supports_fractions():
    robots = RobotsParser(fake_fetcher())
    await robots.fetch_robots("http://a.test/")
    assert robots.get_crawl_delay("MyBot/1.0", origin="http://a.test") == 0.2
    assert robots.get_crawl_delay("BadBot/1.0", origin="http://a.test") == 0.0
    assert robots.get_crawl_delay() == 0.2


async def test_robots_cached_and_downloaded_once_for_concurrent_calls():
    fetcher = fake_fetcher()
    robots = RobotsParser(fetcher)
    await asyncio.gather(*[robots.fetch_robots(f"http://a.test/p{i}") for i in range(10)])
    await robots.fetch_robots("http://a.test/other")
    assert fetcher.calls == ["http://a.test/robots.txt"]


async def test_robots_each_domain_has_own_rules():
    robots = RobotsParser(fake_fetcher())
    await robots.fetch_robots("http://a.test/")
    assert not robots.can_fetch("http://a.test/private/x")
    assert robots.can_fetch("http://b.test/private/x")  # b.test ещё не загружен
    await robots.fetch_robots("http://b.test/")
    assert not robots.can_fetch("http://b.test/private/x")


@pytest.mark.parametrize("fetcher", [
    fake_fetcher(status=404, text="not found"),
    fake_fetcher(status=500, text="oops"),
    fake_fetcher(error=ConnectionError("нет связи")),
    fake_fetcher(error=asyncio.TimeoutError()),
])
async def test_robots_missing_or_broken_means_allowed(fetcher):
    robots = RobotsParser(fetcher)
    info = await robots.fetch_robots("http://a.test/")
    assert info["found"] is False
    assert robots.can_fetch("http://a.test/private/x")
    assert robots.get_crawl_delay(origin="http://a.test") == 0.0


def test_robots_helpers():
    assert agent_token("MyBot/1.0 (+http://x)") == "MyBot"
    assert agent_token("*") == "*"
    assert agent_token(None) == "*"
    delays = parse_crawl_delays("User-agent: a\nUser-agent: b\nCrawl-delay: 3\n"
                                "User-agent: *\nCrawl-delay: bad\nDisallow: /x\n")
    assert delays == {"a": 3.0, "b": 3.0, "*": 0.0}


# ---------- краулер ----------

async def test_crawl_respects_robots_txt(site):
    async with AsyncCrawler(max_depth=1, respect_robots=True,
                            progress_interval=60) as crawler:
        results = await crawler.crawl([site.url + "/"])
    assert site.url + "/private/secret" not in results
    assert "/private/secret" not in site.paths()
    assert site.paths().count("/robots.txt") == 1
    assert crawler.robots_blocked == [site.url + "/private/secret"]
    assert crawler.queue.get_stats()["skipped"] == 1
    assert crawler.failed_urls == {}


async def test_without_robots_private_pages_are_fetched(site):
    async with AsyncCrawler(max_depth=1, progress_interval=60) as crawler:
        results = await crawler.crawl([site.url + "/"])
    assert site.url + "/private/secret" in results


async def test_crawl_delay_from_robots_is_applied(site):
    async with AsyncCrawler(max_depth=1, respect_robots=True, max_concurrent=5,
                            progress_interval=60) as crawler:
        await crawler.crawl([site.url + "/"])
    times = [t for t, path in site.requests if path != "/robots.txt"]
    assert len(times) >= 4
    assert all(g >= 0.17 for g in gaps(times))


async def test_blocked_user_agent_fetches_nothing(site):
    async with AsyncCrawler(max_depth=2, respect_robots=True, user_agent="BadBot/1.0",
                            progress_interval=60) as crawler:
        results = await crawler.crawl([site.url + "/"])
    assert results == {}
    assert site.paths() == ["/robots.txt"]
    assert crawler.robots_blocked == [site.url + "/"]


async def test_fetch_url_raises_when_blocked(site):
    from webcrawler.errors import RobotsBlockedError
    async with AsyncCrawler(respect_robots=True) as crawler:
        with pytest.raises(RobotsBlockedError):
            await crawler.fetch_url(site.url + "/private/secret")
        assert "Mock Home" in await crawler.fetch_url(site.url + "/")


async def test_requests_per_second_limit(site):
    urls = [site.url + f"/bulk/{i}" for i in range(6)]
    async with AsyncCrawler(requests_per_second=20, max_concurrent=6) as crawler:
        started = time.perf_counter()
        pages = await crawler.fetch_urls(urls)
        elapsed = time.perf_counter() - started
    assert len(pages) == 6
    assert elapsed >= 0.22  # 5 интервалов по 0.05


async def test_user_agent_is_sent_and_rotated(site):
    async with AsyncCrawler(user_agent="MyBot/1.0") as crawler:
        await crawler.fetch_url(site.url + "/about")
    assert site.agents == ["MyBot/1.0"]

    site.agents.clear()
    agents = ["A/1", "B/1", "C/1"]
    async with AsyncCrawler(user_agents=agents, max_concurrent=1) as crawler:
        for i in range(5):
            await crawler.fetch_url(site.url + f"/bulk/{i}")
    assert site.agents == ["A/1", "B/1", "C/1", "A/1", "B/1"]


async def test_server_errors_slow_down_the_domain_and_success_resets(site):
    async with AsyncCrawler(requests_per_second=100) as crawler:
        domain = site.url.split("//")[1]
        base = crawler.rate_limiter.current_interval(domain)
        for _ in range(2):
            with pytest.raises(Exception):
                await crawler.fetch_url(site.url + "/status/503")
        assert crawler.rate_limiter.current_interval(domain) == pytest.approx(base * 4)
        await crawler.fetch_url(site.url + "/about")
        assert crawler.rate_limiter.current_interval(domain) == pytest.approx(base)


async def test_not_found_does_not_slow_down(site):
    async with AsyncCrawler(requests_per_second=100) as crawler:
        domain = site.url.split("//")[1]
        base = crawler.rate_limiter.current_interval(domain)
        with pytest.raises(Exception):
            await crawler.fetch_url(site.url + "/status/404")
        assert crawler.rate_limiter.current_interval(domain) == pytest.approx(base)


async def test_rate_stats_include_blocked(site):
    async with AsyncCrawler(max_depth=1, respect_robots=True, progress_interval=60) as crawler:
        await crawler.crawl([site.url + "/"])
        stats = crawler.get_rate_stats()
    assert stats["robots_blocked"] == 1
    assert stats["total_requests"] >= 5
    assert stats["avg_delay_between_requests"] >= 0.15
