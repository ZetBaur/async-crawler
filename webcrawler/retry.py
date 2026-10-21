"""Повторы при ошибках, экспоненциальный backoff и circuit breaker (Day 5)."""
import asyncio
import logging
import random
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from webcrawler.errors import (
    CircuitOpenError,
    CrawlerError,
    NetworkError,
    PermanentError,
    TransientError,
    classify_exception,
)

logger = logging.getLogger("webcrawler")


@dataclass
class RetryStats:
    """Статистика ошибок и повторов."""

    errors_by_type: Counter = field(default_factory=Counter)
    retries: int = 0
    successful_retries: int = 0
    retry_time: float = 0.0
    gave_up: int = 0
    permanent_urls: dict[str, str] = field(default_factory=dict)

    @property
    def total_errors(self) -> int:
        return sum(self.errors_by_type.values())

    @property
    def avg_retry_time(self) -> float:
        return self.retry_time / self.retries if self.retries else 0.0

    def as_dict(self) -> dict:
        return {
            "total_errors": self.total_errors,
            "errors_by_type": dict(self.errors_by_type),
            "retries": self.retries,
            "successful_retries": self.successful_retries,
            "gave_up": self.gave_up,
            "avg_retry_time": round(self.avg_retry_time, 3),
            "permanent_urls": dict(self.permanent_urls),
        }


class RetryStrategy:
    """Выполняет асинхронную функцию с повторами при временных ошибках.

    - повторяются только ошибки из retry_on (по умолчанию TransientError и NetworkError);
    - PermanentError (404, 403, 401...) не повторяется никогда;
    - пауза перед n-й повторной попыткой: base_delay * backoff_factor ** n;
    - per_type_retries: свой лимит повторов для типа ошибки, например {NetworkError: 1};
    - backoff_by_type: свой коэффициент роста паузы для типа ошибки;
    - ответ 429 ждёт дольше (rate_limit_multiplier) и уважает заголовок Retry-After.
    """

    def __init__(self, max_retries: int = 3, backoff_factor: float = 2.0,
                 retry_on: list[type] | None = None, base_delay: float = 0.5,
                 max_delay: float = 30.0, jitter: float = 0.0,
                 per_type_retries: dict[type, int] | None = None,
                 backoff_by_type: dict[type, float] | None = None,
                 rate_limit_multiplier: float = 2.0,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                 rng: random.Random | None = None) -> None:
        if max_retries < 0:
            raise ValueError("max_retries не может быть отрицательным")
        if backoff_factor < 1:
            raise ValueError("backoff_factor должен быть не меньше 1")
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = tuple(retry_on) if retry_on else (TransientError, NetworkError)
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter
        self.per_type_retries = per_type_retries or {}
        self.backoff_by_type = backoff_by_type or {}
        self.rate_limit_multiplier = rate_limit_multiplier
        self._sleep = sleep
        self._rng = rng or random.Random()
        self.stats = RetryStats()

    def _should_retry(self, error: CrawlerError) -> bool:
        return isinstance(error, self.retry_on) and not isinstance(error, PermanentError)

    def _limit_for(self, error: CrawlerError) -> int:
        for error_type, limit in self.per_type_retries.items():
            if isinstance(error, error_type):
                return limit
        return self.max_retries

    def delay_for(self, error: CrawlerError, attempt: int) -> float:
        """Пауза перед повтором. attempt — номер только что провалившейся попытки (с 0)."""
        factor = self.backoff_factor
        for error_type, value in self.backoff_by_type.items():
            if isinstance(error, error_type):
                factor = value
                break
        delay = self.base_delay * (factor ** attempt)
        if error.status == 429:
            delay *= self.rate_limit_multiplier
        if error.retry_after is not None:
            delay = max(delay, error.retry_after)
        if self.jitter:
            delay += self._rng.uniform(0.0, self.jitter)
        return min(delay, max(self.max_delay, error.retry_after or 0.0))

    async def execute_with_retry(self, coro_fn: Callable[..., Awaitable],
                                 *args, with_attempt: bool = False, **kwargs):
        """Вызывает coro_fn(*args, **kwargs) и повторяет при временных ошибках.

        with_attempt=True: в функцию дополнительно передаётся attempt=N (номер попытки с 0),
        чтобы, например, увеличивать таймаут. Ошибки возвращаются нашими типами
        (TransientError, PermanentError, ...). Незнакомые ошибки пробрасываются как есть.
        """
        url = args[0] if args and isinstance(args[0], str) else None
        attempt = 0
        while True:
            call_kwargs = dict(kwargs, attempt=attempt) if with_attempt else kwargs
            try:
                result = await coro_fn(*args, **call_kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as raw:
                error = classify_exception(raw, url)
                if error is None:
                    raise
                if error.url is None:
                    error.url = url
                self.stats.errors_by_type[type(error).__name__] += 1
                if not self._should_retry(error) or attempt >= self._limit_for(error):
                    self._give_up(url, error, attempt)
                    if error is raw:
                        raise
                    raise error from raw
                delay = self.delay_for(error, attempt)
                logger.warning(
                    "retry type=%s url=%s attempt=%s/%s next_in=%.2fs reason=%s",
                    type(error).__name__, url, attempt + 1, self._limit_for(error) + 1,
                    delay, error)
                self.stats.retries += 1
                self.stats.retry_time += delay
                await self._sleep(delay)
                attempt += 1
                continue
            if attempt > 0:
                self.stats.successful_retries += 1
                logger.info("retry ok url=%s после попыток: %s", url, attempt + 1)
            return result

    def _give_up(self, url: str | None, error: CrawlerError, attempt: int) -> None:
        if isinstance(error, PermanentError):
            if url:
                self.stats.permanent_urls[url] = f"{type(error).__name__}: {error}"
            logger.warning("no retry type=%s url=%s reason=%s",
                           type(error).__name__, url, error)
        else:
            self.stats.gave_up += 1
            logger.error("gave up type=%s url=%s attempts=%s reason=%s",
                         type(error).__name__, url, attempt + 1, error)


class CircuitBreaker:
    """Временно блокирует домен, если с ним слишком много сбоев подряд.

    closed -> open: failure_threshold сбоев за последние window секунд.
    open -> half-open: через recovery_time секунд пропускается одна пробная попытка.
    half-open -> closed при успехе пробы, обратно в open при её провале.
    """

    def __init__(self, failure_threshold: int = 5, window: float = 30.0,
                 recovery_time: float = 10.0,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold должен быть не меньше 1")
        self.failure_threshold = failure_threshold
        self.window = window
        self.recovery_time = recovery_time
        self._clock = clock
        self._failures: dict[str, deque[float]] = {}
        self._opened_at: dict[str, float] = {}
        self._trial: set[str] = set()

    def state(self, domain: str) -> str:
        if domain not in self._opened_at:
            return "closed"
        if self._clock() - self._opened_at[domain] >= self.recovery_time:
            return "half_open"
        return "open"

    def check(self, domain: str) -> None:
        """Бросает CircuitOpenError, если запрос к домену сейчас запрещён."""
        state = self.state(domain)
        if state == "closed":
            return
        if state == "half_open" and domain not in self._trial:
            self._trial.add(domain)
            logger.info("circuit half-open domain=%s: пробный запрос", domain)
            return
        raise CircuitOpenError(f"домен {domain} временно заблокирован из-за ошибок")

    def record_failure(self, domain: str) -> None:
        now = self._clock()
        if domain in self._opened_at:
            if domain in self._trial:
                self._trial.discard(domain)
                self._opened_at[domain] = now
                logger.warning("circuit снова открыт domain=%s (проба не удалась)", domain)
            return
        failures = self._failures.setdefault(domain, deque())
        failures.append(now)
        while failures and now - failures[0] > self.window:
            failures.popleft()
        if len(failures) >= self.failure_threshold:
            self._opened_at[domain] = now
            failures.clear()
            logger.warning("circuit открыт domain=%s на %.1f сек", domain, self.recovery_time)

    def record_success(self, domain: str) -> None:
        if domain in self._opened_at and domain in self._trial:
            del self._opened_at[domain]
            self._trial.discard(domain)
            logger.info("circuit закрыт domain=%s: домен снова доступен", domain)
        self._failures.pop(domain, None)

    def open_domains(self) -> list[str]:
        return [d for d in self._opened_at if self.state(d) != "closed"]
