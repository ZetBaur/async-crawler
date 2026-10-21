"""Ограничения конкурентности и скорости (Day 3 и Day 4)."""
import asyncio
import random
from collections import Counter
from contextlib import asynccontextmanager


class SemaphoreManager:
    """Ограничивает число одновременных запросов: всего и на один домен."""

    def __init__(self, max_concurrent: int = 10, max_per_domain: int | None = None) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent должен быть не меньше 1")
        self.max_concurrent = max_concurrent
        self.max_per_domain = max_per_domain or max_concurrent
        self._global = asyncio.Semaphore(max_concurrent)
        self._domains: dict[str, asyncio.Semaphore] = {}
        self.active_total = 0
        self.active_by_domain: Counter = Counter()
        self.peak_total = 0
        self.peak_by_domain: Counter = Counter()

    @asynccontextmanager
    async def limit(self, domain: str):
        domain_sem = self._domains.setdefault(domain, asyncio.Semaphore(self.max_per_domain))
        async with domain_sem:
            async with self._global:
                self.active_total += 1
                self.active_by_domain[domain] += 1
                self.peak_total = max(self.peak_total, self.active_total)
                self.peak_by_domain[domain] = max(self.peak_by_domain[domain],
                                                  self.active_by_domain[domain])
                try:
                    yield
                finally:
                    self.active_total -= 1
                    self.active_by_domain[domain] -= 1

    def stats(self) -> dict:
        return {
            "active_total": self.active_total,
            "active_by_domain": {d: n for d, n in self.active_by_domain.items() if n},
            "peak_total": self.peak_total,
        }


class RateLimiter:
    """Ограничивает скорость запросов (правило вежливости).

    - requests_per_second: не чаще одного запроса за 1/rps секунды;
    - per_domain=True: отдельный лимит на каждый домен, иначе один общий;
    - min_delay: минимальная пауза между запросами (сек);
    - jitter: случайная добавка к паузе до этого значения (имитация человека);
    - set_crawl_delay: задержка из robots.txt для домена;
    - report_error / report_success: после ошибок пауза растёт вдвое с каждой
      подряд идущей ошибкой (экспоненциальный backoff) и сбрасывается после успеха.
    """

    def __init__(self, requests_per_second: float = 1.0, per_domain: bool = True,
                 min_delay: float = 0.0, jitter: float = 0.0,
                 max_backoff: float = 30.0, rng: random.Random | None = None) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second должен быть больше нуля")
        if min_delay < 0 or jitter < 0:
            raise ValueError("min_delay и jitter не могут быть отрицательными")
        self.requests_per_second = requests_per_second
        self.per_domain = per_domain
        self.min_delay = min_delay
        self.jitter = jitter
        self.max_backoff = max_backoff
        self._rng = rng or random.Random()
        self._lock = asyncio.Lock()
        self._next_slot: dict[str, float] = {}
        self._crawl_delays: dict[str, float] = {}
        self._error_streak: Counter = Counter()
        self._request_times: dict[str, list[float]] = {}
        self._waits: list[float] = []

    def _key(self, domain: str | None) -> str:
        return (domain or "*") if self.per_domain else "*"

    def set_crawl_delay(self, domain: str, delay: float) -> None:
        self._crawl_delays[self._key(domain)] = max(0.0, float(delay))

    def current_interval(self, domain: str | None = None) -> float:
        key = self._key(domain)
        base = max(1.0 / self.requests_per_second, self.min_delay,
                   self._crawl_delays.get(key, 0.0))
        streak = self._error_streak[key]
        if streak:
            base = min(base * (2 ** streak), max(base, self.max_backoff))
        return base

    async def acquire(self, domain: str | None = None) -> float:
        """Ждёт своей очереди на запрос. Возвращает, сколько секунд пришлось ждать."""
        key = self._key(domain)
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            slot = max(now, self._next_slot.get(key, now))
            interval = self.current_interval(domain)
            if self.jitter:
                interval += self._rng.uniform(0.0, self.jitter)
            self._next_slot[key] = slot + interval
            wait = slot - now
        if wait > 0:
            await asyncio.sleep(wait)
        self._waits.append(max(wait, 0.0))
        self._request_times.setdefault(key, []).append(loop.time())
        return wait

    def report_error(self, domain: str | None = None) -> None:
        self._error_streak[self._key(domain)] += 1

    def report_success(self, domain: str | None = None) -> None:
        self._error_streak[self._key(domain)] = 0

    def stats(self, window: float = 10.0) -> dict:
        all_times = sorted(t for times in self._request_times.values() for t in times)
        gaps = []
        for times in self._request_times.values():
            gaps.extend(b - a for a, b in zip(times, times[1:]))
        current_rps = 0.0
        if len(all_times) >= 2:
            recent = [t for t in all_times if t >= all_times[-1] - window]
            span = all_times[-1] - recent[0]
            current_rps = (len(recent) - 1) / span if span > 0 else 0.0
        return {
            "total_requests": len(all_times),
            "current_rps": round(current_rps, 2),
            "avg_delay_between_requests": round(sum(gaps) / len(gaps), 3) if gaps else 0.0,
            "avg_wait": round(sum(self._waits) / len(self._waits), 3) if self._waits else 0.0,
            "requests_by_domain": {d: len(t) for d, t in self._request_times.items()},
        }
