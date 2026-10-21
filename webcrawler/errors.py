"""Типы ошибок краулера (Day 4 и Day 5)."""
import asyncio

import aiohttp


class CrawlerError(Exception):
    """Базовая ошибка краулера."""

    def __init__(self, message: str, url: str | None = None, status: int | None = None,
                 retry_after: float | None = None) -> None:
        super().__init__(message)
        self.url = url
        self.status = status
        self.retry_after = retry_after


class TransientError(CrawlerError):
    """Временная ошибка: таймаут, 5xx, 429. Имеет смысл повторить."""


class PermanentError(CrawlerError):
    """Постоянная ошибка: 404, 403, 401. Повторять бесполезно."""


class NetworkError(CrawlerError):
    """Сетевая ошибка: отказ в соединении, проблемы с DNS."""


class ParseError(CrawlerError):
    """Ошибка разбора содержимого страницы."""


class RobotsBlockedError(PermanentError):
    """Адрес запрещён правилами robots.txt."""


class CircuitOpenError(CrawlerError):
    """Домен временно заблокирован автоматом защиты (circuit breaker)."""


def _retry_after_seconds(headers) -> float | None:
    if not headers:
        return None
    value = headers.get("Retry-After")
    try:
        return max(0.0, float(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def classify_exception(error: BaseException, url: str | None = None) -> CrawlerError | None:
    """Превращает «сырую» ошибку aiohttp или asyncio в наш тип.

    Возвращает None, если ошибка нам незнакома (её лучше не глотать).
    """
    if isinstance(error, CrawlerError):
        return error
    if isinstance(error, aiohttp.ClientResponseError):
        status = error.status
        retry_after = _retry_after_seconds(error.headers)
        text = f"HTTP {status}: {error.message}"
        if status == 429 or status >= 500:
            return TransientError(text, url, status, retry_after)
        return PermanentError(text, url, status)
    if isinstance(error, asyncio.TimeoutError):
        return TransientError("таймаут", url)
    if isinstance(error, aiohttp.InvalidURL):
        return PermanentError(f"неверный адрес: {error}", url)
    if isinstance(error, (aiohttp.ClientError, OSError)):
        return NetworkError(f"{type(error).__name__}: {error}", url)
    return None
