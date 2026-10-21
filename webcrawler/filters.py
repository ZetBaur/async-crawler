"""Фильтры адресов (Day 3)."""
import re
from urllib.parse import urlparse


class UrlFilter:
    """Решает, можно ли добавлять ссылку в очередь."""

    def __init__(self, start_hosts: set[str], same_domain_only: bool = True,
                 exclude_patterns: list[str] | None = None,
                 include_patterns: list[str] | None = None) -> None:
        self.start_hosts = set(start_hosts)
        self.same_domain_only = same_domain_only
        self.exclude = [re.compile(p) for p in (exclude_patterns or [])]
        self.include = [re.compile(p) for p in (include_patterns or [])]

    def allows(self, url: str) -> bool:
        if self.same_domain_only and urlparse(url).netloc not in self.start_hosts:
            return False
        if any(pattern.search(url) for pattern in self.exclude):
            return False
        if self.include and not any(pattern.search(url) for pattern in self.include):
            return False
        return True
