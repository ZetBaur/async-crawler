"""Конфигурация краулера из YAML или JSON файла (Day 7)."""
import json
import logging
import re
import types
import typing
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import Any

import yaml

STORAGE_TYPES = ("none", "json", "jsonl", "csv", "sqlite")


class ConfigError(ValueError):
    """Ошибка в конфигурации."""


@dataclass
class RetryConfig:
    enabled: bool = True
    max_retries: int = 3
    backoff_factor: float = 2.0
    base_delay: float = 0.5
    max_delay: float = 30.0


@dataclass
class CircuitBreakerConfig:
    enabled: bool = False
    failure_threshold: int = 5
    window: float = 30.0
    recovery_time: float = 10.0


@dataclass
class StorageConfig:
    type: str = "none"
    path: str = "output/results.jsonl"
    buffer_size: int = 20
    batch_size: int = 20


@dataclass
class LoggingConfig:
    level: str = "INFO"
    file: str | None = None
    max_bytes: int = 1_000_000
    backup_count: int = 3
    json: bool = False


@dataclass
class OutputConfig:
    stats_json: str | None = None
    html_report: str | None = None
    error_report: str | None = None


@dataclass
class CrawlerConfig:
    start_urls: list[str] = field(default_factory=list)
    sitemap_urls: list[str] = field(default_factory=list)
    sitemap_from_robots: bool = False
    max_pages: int = 100
    max_depth: int = 3
    max_concurrent: int = 10
    max_per_domain: int | None = None
    same_domain_only: bool = True
    include_patterns: list[str] = field(default_factory=list)
    exclude_patterns: list[str] = field(default_factory=list)
    requests_per_second: float | None = None
    min_delay: float = 0.0
    jitter: float = 0.0
    respect_robots: bool = True
    user_agent: str = "AdvancedCrawler/1.0"
    user_agents: list[str] = field(default_factory=list)
    connect_timeout: float = 5.0
    read_timeout: float = 15.0
    total_timeout: float = 60.0
    progress: bool = True
    keep_results: bool = True
    parse_mode: str = "thread"
    parse_workers: int | None = None
    retry: RetryConfig = field(default_factory=RetryConfig)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    output: OutputConfig = field(default_factory=OutputConfig)

    @classmethod
    def from_dict(cls, data: dict) -> "CrawlerConfig":
        if not isinstance(data, dict):
            raise ConfigError("Конфигурация должна быть словарём (ключ: значение)")
        config = _build(cls, data, "")
        config.validate()
        return config

    def to_dict(self) -> dict:
        return asdict(self)

    def override(self, **values: Any) -> "CrawlerConfig":
        """Заменяет значения верхнего уровня (None пропускается). Возвращает копию."""
        data = self.to_dict()
        for key, value in values.items():
            if value is None:
                continue
            if key not in data:
                raise ConfigError(f"Неизвестная настройка: {key}")
            data[key] = value
        return CrawlerConfig.from_dict(data)

    def validate(self) -> None:
        problems: list[str] = []

        def need(condition: bool, message: str) -> None:
            if not condition:
                problems.append(message)

        need(self.max_pages >= 1, "max_pages должен быть не меньше 1")
        need(self.max_depth >= 0, "max_depth не может быть отрицательным")
        need(self.max_concurrent >= 1, "max_concurrent должен быть не меньше 1")
        need(self.max_per_domain is None or self.max_per_domain >= 1,
             "max_per_domain должен быть не меньше 1")
        need(self.requests_per_second is None or self.requests_per_second > 0,
             "requests_per_second должен быть больше 0")
        need(self.min_delay >= 0 and self.jitter >= 0,
             "min_delay и jitter не могут быть отрицательными")
        for name in ("connect_timeout", "read_timeout", "total_timeout"):
            need(getattr(self, name) > 0, f"{name} должен быть больше 0")
        need(self.retry.max_retries >= 0, "retry.max_retries не может быть отрицательным")
        need(self.retry.backoff_factor >= 1, "retry.backoff_factor должен быть не меньше 1")
        need(self.circuit_breaker.failure_threshold >= 1,
             "circuit_breaker.failure_threshold должен быть не меньше 1")
        need(self.storage.type in STORAGE_TYPES,
             f"storage.type должен быть одним из {STORAGE_TYPES}")
        need(self.parse_mode in ("thread", "inline", "process"),
             "parse_mode должен быть thread, inline или process")
        need(self.parse_workers is None or self.parse_workers >= 1,
             "parse_workers должен быть не меньше 1")
        need(self.keep_results or self.storage.type != "none",
             "keep_results: false нужен вместе с storage (иначе результаты потеряются)")
        need(self.storage.buffer_size >= 1 and self.storage.batch_size >= 1,
             "storage.buffer_size и storage.batch_size должны быть не меньше 1")
        need(isinstance(logging.getLevelName(self.logging.level.upper()), int),
             f"logging.level неизвестен: {self.logging.level!r}")
        for pattern_list in ("include_patterns", "exclude_patterns"):
            for pattern in getattr(self, pattern_list):
                try:
                    re.compile(pattern)
                except re.error as error:
                    problems.append(f"{pattern_list}: неверное регулярное выражение "
                                    f"{pattern!r}: {error}")
        if problems:
            raise ConfigError("Ошибки в конфигурации:\n- " + "\n- ".join(problems))


def _build(cls, data: dict, path: str):
    """Собирает dataclass из словаря, проверяя неизвестные ключи и типы по аннотациям."""
    hints = typing.get_type_hints(cls)
    unknown = sorted(set(data) - set(hints))
    if unknown:
        where = path.rstrip(".") or "верхний уровень"
        raise ConfigError(f"Неизвестные настройки ({where}): {', '.join(unknown)}")
    values = {}
    for name, value in data.items():
        hint = hints[name]
        key = f"{path}{name}"
        if isinstance(hint, type) and is_dataclass(hint):
            if value is None:
                value = {}
            if not isinstance(value, dict):
                raise ConfigError(f"{key}: ожидается словарь")
            values[name] = _build(hint, value, key + ".")
        else:
            values[name] = _coerce(key, value, hint)
    return cls(**values)


def _coerce(key: str, value: Any, hint: Any) -> Any:
    """Проверяет значение по аннотации поля (int, float, str, bool, list[str], X | None)."""
    origin = typing.get_origin(hint)
    if origin in (typing.Union, types.UnionType):
        options = typing.get_args(hint)
        if value is None:
            if type(None) in options:
                return None
            raise ConfigError(f"{key}: значение не может быть пустым")
        errors = []
        for option in options:
            if option is type(None):
                continue
            try:
                return _coerce(key, value, option)
            except ConfigError as error:
                errors.append(str(error))
        raise ConfigError(errors[0])
    if value is None:
        raise ConfigError(f"{key}: значение не может быть пустым")
    if hint is bool:
        if not isinstance(value, bool):
            raise ConfigError(f"{key}: ожидается true или false")
        return value
    if origin is list:
        if isinstance(value, str):
            return [value]
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            raise ConfigError(f"{key}: ожидается список строк")
        return value
    if hint is int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConfigError(f"{key}: ожидается целое число")
        return value
    if hint is float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfigError(f"{key}: ожидается число")
        return float(value)
    if hint is str:
        if not isinstance(value, str):
            raise ConfigError(f"{key}: ожидается строка")
        return value
    return value


def load_config(path: str | Path) -> CrawlerConfig:
    """Читает YAML (.yaml, .yml) или JSON (.json) файл."""
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Файл конфигурации не найден: {path}")
    text = path.read_text(encoding="utf-8")
    try:
        if path.suffix.lower() in (".yaml", ".yml"):
            data = yaml.safe_load(text)
        elif path.suffix.lower() == ".json":
            data = json.loads(text)
        else:
            raise ConfigError("Конфигурация должна быть в файле .yaml, .yml или .json")
    except (yaml.YAMLError, json.JSONDecodeError) as error:
        raise ConfigError(f"Не удалось прочитать {path.name}: {error}") from error
    return CrawlerConfig.from_dict(data or {})
