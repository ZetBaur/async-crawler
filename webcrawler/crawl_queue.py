"""Очередь URL с приоритетами (Day 3)."""
import asyncio
import heapq
import itertools


class CrawlerQueue:
    """Очередь адресов для краулинга.

    - Чем больше priority, тем раньше URL достанется (при равенстве по порядку добавления).
    - Один и тот же URL в очередь второй раз не попадает (защита от дублей).
    - max_urls ограничивает общее число адресов, которые вообще могут быть приняты.
    - get_next() ждёт, пока другие задачи ещё работают (они могут добавить новые
      ссылки), и возвращает None только когда работа точно закончилась.
    """

    def __init__(self, max_urls: int | None = None) -> None:
        self.max_urls = max_urls
        self._heap: list[tuple[int, int, str]] = []
        self._counter = itertools.count()
        self._seen: set[str] = set()
        self._depth: dict[str, int] = {}
        self._in_progress: set[str] = set()
        self._processed: set[str] = set()
        self._failed: dict[str, str] = {}
        self._skipped: dict[str, str] = {}
        self._wakeup = asyncio.Event()

    def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        """Добавляет URL. Возвращает False, если он уже был или лимит исчерпан."""
        if url in self._seen:
            return False
        if self.max_urls is not None and len(self._seen) >= self.max_urls:
            return False
        self._seen.add(url)
        self._depth[url] = depth
        heapq.heappush(self._heap, (-priority, next(self._counter), url))
        self._wakeup.set()
        return True

    async def get_next(self) -> str | None:
        while True:
            if self._heap:
                _, _, url = heapq.heappop(self._heap)
                self._in_progress.add(url)
                return url
            if not self._in_progress:
                return None
            self._wakeup.clear()
            await self._wakeup.wait()

    def depth(self, url: str) -> int:
        return self._depth.get(url, 0)

    def _finish(self, url: str) -> None:
        self._in_progress.discard(url)
        self._wakeup.set()

    def mark_processed(self, url: str) -> None:
        self._processed.add(url)
        self._finish(url)

    def mark_failed(self, url: str, error: str) -> None:
        self._failed[url] = error
        self._finish(url)

    def mark_skipped(self, url: str, reason: str) -> None:
        """Адрес не загружали осознанно (например, запрещён robots.txt)."""
        self._skipped[url] = reason
        self._finish(url)

    def get_stats(self) -> dict:
        return {
            "seen": len(self._seen),
            "queued": len(self._heap),
            "in_progress": len(self._in_progress),
            "processed": len(self._processed),
            "failed": len(self._failed),
            "skipped": len(self._skipped),
        }
