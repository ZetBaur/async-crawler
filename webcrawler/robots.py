"""Разбор robots.txt (Day 4)."""
import asyncio
import logging
from typing import Awaitable, Callable
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

logger = logging.getLogger("webcrawler")

Fetcher = Callable[[str], Awaitable[tuple[int, str]]]


def origin_of(url: str) -> str:
    parts = urlparse(url)
    return f"{parts.scheme}://{parts.netloc}".lower()


def agent_token(user_agent: str | None) -> str:
    """'MyBot/1.0 (+site)' -> 'MyBot'; пустое значение -> '*'."""
    if not user_agent:
        return "*"
    token = user_agent.split("/")[0].split(" ")[0].strip()
    return token or "*"


def parse_crawl_delays(text: str) -> dict[str, float]:
    """Достаёт Crawl-delay по группам User-agent. Поддерживает дроби (0.5)."""
    delays: dict[str, float] = {}
    group: list[str] = []
    in_group_header = False
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if ":" not in line:
            continue
        name, value = (part.strip() for part in line.split(":", 1))
        name = name.lower()
        if name == "user-agent":
            if not in_group_header:
                group = []
            group.append(value.lower())
            delays.setdefault(value.lower(), 0.0)
            in_group_header = True
            continue
        in_group_header = False
        if name == "crawl-delay":
            try:
                delay = float(value)
            except ValueError:
                continue
            if delay >= 0:
                for agent in group:
                    delays[agent] = delay
    return delays


class RobotsParser:
    """Загружает и кэширует robots.txt для каждого сайта.

    fetcher — асинхронная функция «адрес -> (HTTP-статус, текст)».
    Если файла нет (4xx), он недоступен или ответил ошибкой сервера, считаем, что
    ограничений нет, и пишем предупреждение в лог.
    """

    def __init__(self, fetcher: Fetcher, user_agent: str = "*") -> None:
        self._fetcher = fetcher
        self.user_agent = user_agent
        self._rules: dict[str, RobotFileParser | None] = {}
        self._delays: dict[str, dict[str, float]] = {}
        self._info: dict[str, dict] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._last_origin: str | None = None

    async def fetch_robots(self, base_url: str) -> dict:
        origin = origin_of(base_url)
        self._last_origin = origin
        if origin in self._info:
            return self._info[origin]
        lock = self._locks.setdefault(origin, asyncio.Lock())
        async with lock:
            if origin not in self._info:
                self._info[origin] = await self._download(origin)
        return self._info[origin]

    async def _download(self, origin: str) -> dict:
        robots_url = f"{origin}/robots.txt"
        rules: RobotFileParser | None = None
        delays: dict[str, float] = {}
        status: int | None = None
        try:
            status, text = await self._fetcher(robots_url)
        except Exception as error:
            logger.warning("robots.txt недоступен url=%s %s: %s; считаю, что можно всё",
                           robots_url, type(error).__name__, error)
        else:
            if status == 200:
                rules = RobotFileParser()
                rules.parse(text.splitlines())
                rules.modified()
                delays = parse_crawl_delays(text)
            elif status >= 500:
                logger.warning("robots.txt ответил %s url=%s; считаю, что можно всё",
                               status, robots_url)
            else:
                logger.info("robots.txt не найден (%s) url=%s", status, robots_url)
        self._rules[origin] = rules
        self._delays[origin] = delays
        return {
            "origin": origin,
            "url": robots_url,
            "status": status,
            "found": rules is not None,
            "crawl_delays": delays,
        }

    def can_fetch(self, url: str, user_agent: str | None = None) -> bool:
        """Можно ли загружать URL. Работает по уже скачанным правилам."""
        rules = self._rules.get(origin_of(url))
        if rules is None:
            return True
        return rules.can_fetch(agent_token(user_agent or self.user_agent), url)

    def get_crawl_delay(self, user_agent: str | None = None, origin: str | None = None) -> float:
        """Задержка между запросами из robots.txt (сек), 0.0 если не задана."""
        delays = self._delays.get(origin or self._last_origin or "", {})
        token = agent_token(user_agent or self.user_agent).lower()
        return delays.get(token, delays.get("*", 0.0))

    def is_loaded(self, url: str) -> bool:
        return origin_of(url) in self._info
