"""Настройка логирования: консоль, файл с ротацией, текстовый или JSON формат (Day 7)."""
import json
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOGGER_NAME = "webcrawler"
_MARK = "_webcrawler_handler"

TEXT_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"


class JsonFormatter(logging.Formatter):
    """Одна строка = один JSON-объект: удобно для поиска и разбора логов."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: str = "INFO", log_file: str | Path | None = None,
                  max_bytes: int = 1_000_000, backup_count: int = 3,
                  json_format: bool = False, console: bool = True) -> logging.Logger:
    """Настраивает логгер 'webcrawler'. Повторный вызов заменяет прежние обработчики."""
    logger = logging.getLogger(LOGGER_NAME)
    teardown_logging()
    numeric = logging.getLevelName(level.upper())
    if not isinstance(numeric, int):
        raise ValueError(f"Неизвестный уровень логирования: {level!r}")
    logger.setLevel(numeric)
    logger.propagate = False
    formatter = (JsonFormatter() if json_format
                 else logging.Formatter(TEXT_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
    handlers: list[logging.Handler] = []
    if console:
        handlers.append(logging.StreamHandler(sys.stderr))
    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(RotatingFileHandler(path, maxBytes=max_bytes,
                                            backupCount=backup_count, encoding="utf-8"))
    for handler in handlers:
        handler.setFormatter(formatter)
        setattr(handler, _MARK, True)
        logger.addHandler(handler)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    return logger


def teardown_logging() -> None:
    """Убирает обработчики, поставленные setup_logging, и закрывает файлы."""
    logger = logging.getLogger(LOGGER_NAME)
    for handler in list(logger.handlers):
        if getattr(handler, _MARK, False):
            logger.removeHandler(handler)
            handler.close()
    logger.propagate = True
