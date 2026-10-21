"""Статистика работы краулера (Day 7)."""
from collections import Counter
from urllib.parse import urlparse


class CrawlerStats:
    """Сводка по итогам краулинга."""

    def __init__(self, elapsed: float = 0.0) -> None:
        self.elapsed = elapsed
        self.successful = 0
        self.failed = 0
        self.skipped = 0
        self.status_codes: Counter = Counter()
        self.domains: Counter = Counter()
        self.error_types: Counter = Counter()

    @classmethod
    def from_crawler(cls, crawler, elapsed: float) -> "CrawlerStats":
        stats = cls(elapsed)
        for url, parsed in crawler.processed_urls.items():
            stats.successful += 1
            stats.status_codes[str(parsed.get("status_code") or "?")] += 1
            stats.domains[urlparse(url).netloc.lower()] += 1
        for entry in crawler.error_log:
            stats.failed += 1
            stats.status_codes[str(entry["status"]) if entry["status"] else entry["type"]] += 1
            stats.domains[urlparse(entry["url"]).netloc.lower()] += 1
            stats.error_types[entry["type"]] += 1
        stats.skipped = len(crawler.robots_blocked)
        return stats

    @property
    def total_pages(self) -> int:
        return self.successful + self.failed

    @property
    def success_rate(self) -> float:
        return self.successful / self.total_pages if self.total_pages else 0.0

    @property
    def avg_speed(self) -> float:
        return self.total_pages / self.elapsed if self.elapsed > 0 else 0.0

    def top_domains(self, n: int = 10) -> list[tuple[str, int]]:
        return self.domains.most_common(n)

    def summary(self) -> dict:
        return {
            "total_pages": self.total_pages,
            "successful": self.successful,
            "failed": self.failed,
            "skipped": self.skipped,
            "success_rate": round(self.success_rate, 4),
            "elapsed_seconds": round(self.elapsed, 3),
            "avg_speed": round(self.avg_speed, 2),
            "status_codes": dict(sorted(self.status_codes.items())),
            "top_domains": [{"domain": d, "pages": n} for d, n in self.top_domains()],
            "errors_by_type": dict(self.error_types),
        }
