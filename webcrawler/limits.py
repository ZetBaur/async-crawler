"""Ограничения конкурентности и скорости (Day 3 и Day 4)."""
import asyncio
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
