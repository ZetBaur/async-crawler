"""Мониторинг прогресса в реальном времени: полоса, скорость, оценка времени (Day 7)."""
import sys
import time
from typing import TextIO


def format_duration(seconds: float | None) -> str:
    if seconds is None or seconds != seconds or seconds < 0:
        return "--:--"
    seconds = int(round(seconds))
    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def render_progress(progress: dict, max_pages: int, width: int = 24) -> str:
    """Строка прогресса: [#####-----] 50% 12/24 | 8.3 стр/сек | осталось ~00:02 | активных 4."""
    done = progress.get("done", 0)
    seen = progress.get("seen", 0)
    total = min(max_pages, max(seen, done)) or 1
    fraction = min(1.0, done / total)
    filled = int(round(width * fraction))
    bar = "#" * filled + "-" * (width - filled)
    speed = progress.get("pages_per_second", 0.0)
    remaining = max(0, total - done)
    eta = remaining / speed if speed > 0 else None
    return (f"[{bar}] {fraction:4.0%} {done}/{total} | {speed:.1f} стр/сек | "
            f"ошибок {progress.get('failed', 0)} | в очереди {progress.get('queued', 0)} | "
            f"активных {progress.get('active', 0)} | осталось ~{format_duration(eta)}")


class ProgressMonitor:
    """Передаётся краулеру как on_progress. Рисует одну обновляемую строку (в терминале)
    или печатает отдельные строки (если вывод идёт в файл)."""

    def __init__(self, max_pages: int, stream: TextIO | None = None,
                 min_interval: float = 0.2) -> None:
        self.max_pages = max_pages
        self.stream = stream or sys.stderr
        self.min_interval = min_interval
        self._last = 0.0
        self._interactive = bool(getattr(self.stream, "isatty", lambda: False)())
        self.last_line = ""

    def __call__(self, progress: dict) -> None:
        now = time.monotonic()
        if now - self._last < self.min_interval:
            return
        self._last = now
        self.last_line = render_progress(progress, self.max_pages)
        if self._interactive:
            self.stream.write("\r" + self.last_line + "\x1b[K")
        else:
            self.stream.write(self.last_line + "\n")
        self.stream.flush()

    def finish(self, progress: dict | None = None) -> None:
        if progress is not None:
            line = render_progress(progress, self.max_pages)
            if line == self.last_line and not self._interactive:
                return
            self.last_line = line
            self.stream.write(("\r" if self._interactive else "") + line + "\n")
            self.stream.flush()
        elif self._interactive:
            self.stream.write("\n")
            self.stream.flush()
