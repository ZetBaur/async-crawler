import asyncio
import json

import aiohttp
import pytest
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from webcrawler import AsyncCrawler
from webcrawler.errors import (
    CircuitOpenError,
    CrawlerError,
    NetworkError,
    ParseError,
    PermanentError,
    RobotsBlockedError,
    TransientError,
    classify_exception,
)
from webcrawler.retry import CircuitBreaker, RetryStrategy


def response_error(status, headers=None):
    info = aiohttp.RequestInfo(url=URL("http://x.test/"), method="GET",
                               headers=CIMultiDictProxy(CIMultiDict()),
                               real_url=URL("http://x.test/"))
    return aiohttp.ClientResponseError(
        info, (), status=status, message="msg",
        headers=CIMultiDictProxy(CIMultiDict(headers or {})))


class Recorder:
    """Подменяет asyncio.sleep: ничего не ждёт, но запоминает паузы."""

    def __init__(self):
        self.delays = []

    async def __call__(self, delay):
        self.delays.append(delay)


def flaky_function(errors):
    """Функция, которая по очереди бросает ошибки из списка, потом возвращает 'ok'."""
    calls = []

    async def run(url, attempt=0):
        calls.append(attempt)
        if len(calls) <= len(errors):
            raise errors[len(calls) - 1]
        return "ok"

    run.calls = calls
    return run


# ---------- классификация ----------

@pytest.mark.parametrize("status", [404, 403, 401, 410, 400])
def test_client_errors_are_permanent(status):
    error = classify_exception(response_error(status), "http://x.test/")
    assert isinstance(error, PermanentError) and error.status == status


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_server_errors_are_transient(status):
    error = classify_exception(response_error(status), "http://x.test/")
    assert isinstance(error, TransientError) and error.status == status


def test_retry_after_header_is_parsed():
    error = classify_exception(response_error(429, {"Retry-After": "7"}))
    assert error.retry_after == 7.0
    error = classify_exception(response_error(503, {"Retry-After": "завтра"}))
    assert error.retry_after is None


def test_timeouts_network_and_unknown_errors():
    assert isinstance(classify_exception(asyncio.TimeoutError()), TransientError)
    assert isinstance(classify_exception(ConnectionRefusedError()), NetworkError)
    assert isinstance(classify_exception(aiohttp.ServerDisconnectedError()), NetworkError)
    assert isinstance(classify_exception(aiohttp.InvalidURL("bad")), PermanentError)
    assert classify_exception(ValueError("что-то своё")) is None
    own = ParseError("битый html")
    assert classify_exception(own) is own
    assert issubclass(RobotsBlockedError, PermanentError)
    assert issubclass(CircuitOpenError, CrawlerError)


# ---------- RetryStrategy ----------

async def test_retries_transient_then_succeeds_with_exponential_backoff():
    sleeper = Recorder()
    strategy = RetryStrategy(max_retries=3, backoff_factor=2.0, base_delay=0.5, sleep=sleeper)
    run = flaky_function([response_error(503), response_error(503)])
    assert await strategy.execute_with_retry(run, "http://x.test/") == "ok"
    assert len(run.calls) == 3
    assert sleeper.delays == [0.5, 1.0]
    assert strategy.stats.retries == 2 and strategy.stats.successful_retries == 1
    assert strategy.stats.errors_by_type == {"TransientError": 2}


async def test_permanent_errors_are_not_retried():
    sleeper = Recorder()
    strategy = RetryStrategy(sleep=sleeper)
    run = flaky_function([response_error(404)])
    with pytest.raises(PermanentError):
        await strategy.execute_with_retry(run, "http://x.test/gone")
    assert len(run.calls) == 1 and sleeper.delays == []
    assert "http://x.test/gone" in strategy.stats.permanent_urls


@pytest.mark.parametrize("status", [403, 401])
async def test_forbidden_and_unauthorized_not_retried(status):
    strategy = RetryStrategy(sleep=Recorder())
    run = flaky_function([response_error(status)])
    with pytest.raises(PermanentError):
        await strategy.execute_with_retry(run, "http://x.test/")
    assert len(run.calls) == 1


async def test_gives_up_after_max_retries():
    strategy = RetryStrategy(max_retries=2, sleep=Recorder())
    run = flaky_function([response_error(500)] * 10)
    with pytest.raises(TransientError):
        await strategy.execute_with_retry(run, "http://x.test/")
    assert len(run.calls) == 3
    assert strategy.stats.gave_up == 1


async def test_zero_retries_means_single_attempt():
    strategy = RetryStrategy(max_retries=0, sleep=Recorder())
    run = flaky_function([response_error(503)])
    with pytest.raises(TransientError):
        await strategy.execute_with_retry(run, "http://x.test/")
    assert len(run.calls) == 1


async def test_retry_on_can_be_customized():
    strategy = RetryStrategy(retry_on=[TransientError], sleep=Recorder())
    run = flaky_function([ConnectionRefusedError()])
    with pytest.raises(NetworkError):
        await strategy.execute_with_retry(run, "http://x.test/")
    assert len(run.calls) == 1


async def test_per_type_limits_and_backoff():
    sleeper = Recorder()
    strategy = RetryStrategy(max_retries=5, base_delay=1.0, sleep=sleeper,
                             per_type_retries={NetworkError: 1},
                             backoff_by_type={NetworkError: 3.0})
    run = flaky_function([ConnectionRefusedError()] * 5)
    with pytest.raises(NetworkError):
        await strategy.execute_with_retry(run, "http://x.test/")
    assert len(run.calls) == 2
    assert sleeper.delays == [1.0]

    sleeper.delays.clear()
    run = flaky_function([ConnectionRefusedError(), ConnectionRefusedError(),
                          ConnectionRefusedError()])
    strategy = RetryStrategy(max_retries=5, base_delay=1.0, sleep=sleeper,
                             backoff_by_type={NetworkError: 3.0})
    await strategy.execute_with_retry(run, "http://x.test/")
    assert sleeper.delays == [1.0, 3.0, 9.0]


async def test_429_waits_longer_and_respects_retry_after():
    sleeper = Recorder()
    strategy = RetryStrategy(base_delay=1.0, rate_limit_multiplier=2.0, sleep=sleeper)
    run = flaky_function([response_error(429), response_error(429, {"Retry-After": "10"})])
    await strategy.execute_with_retry(run, "http://x.test/")
    assert sleeper.delays == [2.0, 10.0]


async def test_delay_is_capped_by_max_delay():
    sleeper = Recorder()
    strategy = RetryStrategy(max_retries=10, base_delay=1.0, backoff_factor=10,
                             max_delay=5.0, sleep=sleeper)
    run = flaky_function([response_error(503)] * 4)
    await strategy.execute_with_retry(run, "http://x.test/")
    assert max(sleeper.delays) == 5.0


async def test_unknown_errors_pass_through_unchanged():
    strategy = RetryStrategy(sleep=Recorder())
    run = flaky_function([ValueError("свой баг")])
    with pytest.raises(ValueError):
        await strategy.execute_with_retry(run, "http://x.test/")
    assert len(run.calls) == 1


async def test_cancellation_is_not_swallowed():
    strategy = RetryStrategy()

    async def hang(url):
        await asyncio.sleep(10)

    task = asyncio.create_task(strategy.execute_with_retry(hang, "http://x.test/"))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_with_attempt_passes_attempt_number():
    strategy = RetryStrategy(sleep=Recorder())
    run = flaky_function([response_error(503), response_error(503)])
    await strategy.execute_with_retry(run, "http://x.test/", with_attempt=True)
    assert run.calls == [0, 1, 2]


@pytest.mark.parametrize("kwargs", [{"max_retries": -1}, {"backoff_factor": 0.5}])
def test_retry_strategy_validates_arguments(kwargs):
    with pytest.raises(ValueError):
        RetryStrategy(**kwargs)


# ---------- CircuitBreaker ----------

class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def test_circuit_breaker_opens_recovers_and_closes():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=3, window=10, recovery_time=5, clock=clock)
    for _ in range(2):
        breaker.check("a.test")
        breaker.record_failure("a.test")
    assert breaker.state("a.test") == "closed"
    breaker.record_failure("a.test")
    assert breaker.state("a.test") == "open"
    with pytest.raises(CircuitOpenError):
        breaker.check("a.test")
    breaker.check("b.test")  # другие домены не затронуты

    clock.now = 5.0
    assert breaker.state("a.test") == "half_open"
    breaker.check("a.test")  # одна пробная попытка
    with pytest.raises(CircuitOpenError):
        breaker.check("a.test")  # остальные ждут результата пробы
    breaker.record_success("a.test")
    assert breaker.state("a.test") == "closed"
    breaker.check("a.test")


def test_circuit_breaker_failed_trial_reopens():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, recovery_time=5, clock=clock)
    breaker.record_failure("a.test")
    clock.now = 6.0
    breaker.check("a.test")
    breaker.record_failure("a.test")
    assert breaker.state("a.test") == "open"
    clock.now = 10.0
    assert breaker.state("a.test") == "open"
    clock.now = 11.0
    assert breaker.state("a.test") == "half_open"


def test_circuit_breaker_ignores_old_failures_and_success_resets():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=3, window=10, clock=clock)
    breaker.record_failure("a.test")
    breaker.record_failure("a.test")
    clock.now = 20.0
    breaker.record_failure("a.test")
    assert breaker.state("a.test") == "closed"
    breaker.record_failure("a.test")
    breaker.record_success("a.test")
    breaker.record_failure("a.test")
    breaker.record_failure("a.test")
    assert breaker.state("a.test") == "closed"


# ---------- интеграция с краулером ----------

async def test_flaky_page_succeeds_after_retries(site):
    strategy = RetryStrategy(max_retries=3, base_delay=0.01)
    async with AsyncCrawler(retry_strategy=strategy) as crawler:
        html = await crawler.fetch_url(site.url + "/flaky/a")
    assert "Flaky a" in html
    assert site.flaky_counter["a"] == 3
    assert strategy.stats.successful_retries == 1


async def test_404_is_not_retried_and_5xx_is_retried(site):
    strategy = RetryStrategy(max_retries=2, base_delay=0.01)
    async with AsyncCrawler(retry_strategy=strategy) as crawler:
        with pytest.raises(PermanentError):
            await crawler.fetch_url(site.url + "/status/404")
        with pytest.raises(TransientError):
            await crawler.fetch_url(site.url + "/status/503")
    assert site.paths().count("/status/404") == 1
    assert site.paths().count("/status/503") == 3


async def test_connection_refused_is_network_error_and_retried():
    strategy = RetryStrategy(max_retries=1, base_delay=0.01)
    async with AsyncCrawler(retry_strategy=strategy) as crawler:
        with pytest.raises(NetworkError):
            await crawler.fetch_url("http://127.0.0.1:1/")
    assert strategy.stats.errors_by_type == {"NetworkError": 2}


async def test_timeout_grows_between_attempts(site):
    strategy = RetryStrategy(max_retries=3, base_delay=0.01)
    async with AsyncCrawler(read_timeout=0.15, timeout_growth=1.0,
                            retry_strategy=strategy) as crawler:
        html = await crawler.fetch_url(site.url + "/delay/0.35")
    assert "задержка" in html
    assert site.paths().count("/delay/0.35") == 3
    assert strategy.stats.errors_by_type == {"TransientError": 2}


async def test_429_uses_retry_after_from_server(site):
    sleeper = Recorder()
    strategy = RetryStrategy(max_retries=1, base_delay=0.1, sleep=sleeper)
    async with AsyncCrawler(retry_strategy=strategy) as crawler:
        with pytest.raises(TransientError):
            await crawler.fetch_url(site.url + "/status/429")
    assert sleeper.delays == [1.0]


async def test_circuit_breaker_stops_requests_to_broken_domain(site):
    breaker = CircuitBreaker(failure_threshold=3, window=30, recovery_time=0.3)
    async with AsyncCrawler(circuit_breaker=breaker) as crawler:
        for _ in range(3):
            with pytest.raises(aiohttp.ClientResponseError):
                await crawler.fetch_url(site.url + "/status/503")
        before = site.paths().count("/status/503")
        with pytest.raises(CircuitOpenError):
            await crawler.fetch_url(site.url + "/about")
        assert site.paths().count("/status/503") == before
        assert "/about" not in site.paths()

        await asyncio.sleep(0.35)
        assert "О нас" in await crawler.fetch_url(site.url + "/about")
        assert breaker.state(site.url.split("//")[1]) == "closed"


async def test_404_does_not_open_circuit(site):
    breaker = CircuitBreaker(failure_threshold=2)
    async with AsyncCrawler(circuit_breaker=breaker) as crawler:
        for _ in range(5):
            with pytest.raises(aiohttp.ClientResponseError):
                await crawler.fetch_url(site.url + "/status/404")
        assert "О нас" in await crawler.fetch_url(site.url + "/about")


async def test_crawl_collects_error_stats_and_exports_report(site, tmp_path):
    strategy = RetryStrategy(max_retries=2, base_delay=0.01)
    start = [site.url + "/flaky/b", site.url + "/status/404", site.url + "/status/403",
             site.url + "/status/503", "http://127.0.0.1:1/"]
    async with AsyncCrawler(max_depth=0, retry_strategy=strategy,
                            progress_interval=60) as crawler:
        results = await crawler.crawl(start)
        path = crawler.export_error_report(tmp_path / "out" / "errors.json")
    assert set(results) == {site.url + "/flaky/b"}
    report = json.loads(path.read_text(encoding="utf-8"))
    assert report["errors_by_type"] == {"PermanentError": 2, "TransientError": 1,
                                        "NetworkError": 1}
    assert len(report["failed_urls"]) == 4
    assert report["retry"]["successful_retries"] == 1
    assert set(report["retry"]["permanent_urls"]) == {site.url + "/status/404",
                                                      site.url + "/status/403"}
    assert report["retry"]["retries"] >= 4
