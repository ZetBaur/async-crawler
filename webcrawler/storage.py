"""Асинхронное сохранение собранных данных (Day 6): JSON, CSV, SQLite."""
import asyncio
import csv
import io
import json
import logging
import re
from abc import ABC, abstractmethod
from datetime import date, datetime, timezone
from pathlib import Path

import aiofiles
import aiosqlite

logger = logging.getLogger("webcrawler")

RECORD_FIELDS = ("url", "title", "text", "links", "metadata", "crawled_at",
                 "status_code", "content_type")


def make_record(parsed: dict) -> dict:
    """Приводит результат разбора страницы к стандартному формату записи."""
    return {
        "url": parsed["url"],
        "title": parsed.get("title", ""),
        "text": parsed.get("text", ""),
        "links": list(parsed.get("links", [])),
        "metadata": dict(parsed.get("metadata", {})),
        "crawled_at": parsed.get("crawled_at") or datetime.now(timezone.utc),
        "status_code": parsed.get("status_code"),
        "content_type": parsed.get("content_type", ""),
    }


def json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Нельзя сохранить в JSON: {type(value).__name__}")


class DataStorage(ABC):
    """Общий интерфейс хранилищ."""

    @abstractmethod
    async def save(self, data: dict) -> None:
        """Сохраняет одну запись (может буферизовать)."""

    async def flush(self) -> None:
        """Сбрасывает буфер на диск."""

    @abstractmethod
    async def close(self) -> None:
        """Сбрасывает буфер и закрывает файл или соединение."""

    def pending(self) -> int:
        """Сколько принятых записей ещё не записано на диск."""
        return 0

    def stats(self) -> dict:
        return {}

    async def __aenter__(self) -> "DataStorage":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()


class _BufferedFileStorage(DataStorage):
    """Общая часть файловых хранилищ: буфер, блокировка, асинхронная запись."""

    def __init__(self, path: str | Path, buffer_size: int = 20,
                 encoding: str = "utf-8", append: bool = False) -> None:
        if buffer_size < 1:
            raise ValueError("buffer_size должен быть не меньше 1")
        self.path = Path(path)
        self.buffer_size = buffer_size
        self.encoding = encoding
        self.append = append
        self.saved = 0
        self._buffer: list[str] = []
        self._lock = asyncio.Lock()
        self._file = None
        self._closed = False

    async def _open(self):
        if self._file is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._file = await aiofiles.open(
                self.path, "a" if self.append else "w", encoding=self.encoding,
                errors="replace", newline="")
            await self._on_open()
        return self._file

    async def _on_open(self) -> None:
        """Хук: что записать при открытии файла."""

    async def _write_buffer(self) -> None:
        if not self._buffer:
            return
        file = await self._open()
        chunk = "".join(self._buffer)
        position = await file.tell()
        try:
            await file.write(chunk)
            await file.flush()
        except Exception:
            await self._rollback(file, position)
            raise
        self.saved += len(self._buffer)
        self._buffer = []

    @staticmethod
    async def _rollback(file, position: int) -> None:
        """Убирает с диска частично записанную пачку, чтобы повтор не дал дубликатов."""
        try:
            await file.seek(position)
            await file.truncate(position)
        except Exception as error:
            logger.warning("не удалось откатить частичную запись: %s: %s",
                           type(error).__name__, error)

    async def _enqueue(self, item: str, undo=None) -> None:
        """Добавляет запись в буфер; при сбое записи пачки убирает только её саму.

        Ранее принятые записи остаются в буфере, их повторит следующий сброс.
        """
        self._buffer.append(item)
        if len(self._buffer) >= self.buffer_size:
            try:
                await self._write_buffer()
            except Exception:
                self._buffer.pop()
                if undo is not None:
                    undo()
                raise

    def pending(self) -> int:
        return len(self._buffer)

    async def flush(self) -> None:
        async with self._lock:
            await self._write_buffer()
            if self._file is not None:
                await self._file.flush()

    async def close(self) -> None:
        if self._closed:
            return
        async with self._lock:
            await self._write_buffer()
            file = await self._open()
            await self._on_close()
            await file.close()
            self._file = None
            self._closed = True

    async def _on_close(self) -> None:
        """Хук: что дописать перед закрытием."""

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("Хранилище уже закрыто")

    def stats(self) -> dict:
        return {"saved": self.saved, "path": str(self.path)}


class JSONStorage(_BufferedFileStorage):
    """JSON-хранилище.

    mode="lines": по одной записи JSON на строку (JSON Lines). Удобно для больших
    объёмов: файл дописывается построчно и остаётся читаемым даже при сбое.
    mode="array": один файл-массив JSON; indent=2 даёт форматированный вывод.
    """

    def __init__(self, path: str | Path, mode: str = "lines", indent: int | None = None,
                 buffer_size: int = 20, encoding: str = "utf-8", append: bool = False) -> None:
        if mode not in ("lines", "array"):
            raise ValueError("mode должен быть 'lines' или 'array'")
        if mode == "lines" and indent is not None:
            raise ValueError("indent работает только с mode='array'")
        if mode == "array" and append:
            raise ValueError("append не поддерживается для mode='array'")
        super().__init__(path, buffer_size, encoding, append)
        self.mode = mode
        self.indent = indent
        self._first = True

    async def _on_open(self) -> None:
        if self.mode == "array":
            await self._file.write("[")

    async def save(self, data: dict) -> None:
        self._check_open()
        text = json.dumps(data, ensure_ascii=False, default=json_default,
                          indent=self.indent if self.mode == "array" else None)
        async with self._lock:
            if self.mode == "lines":
                await self._enqueue(text + "\n")
            else:
                was_first = self._first
                self._first = False
                await self._enqueue(("" if was_first else ",") + "\n" + text,
                                    undo=lambda: setattr(self, "_first", was_first))

    async def _on_close(self) -> None:
        if self.mode == "array":
            await self._file.write("\n]\n")


def flatten_value(value, sanitize_formulas: bool = True) -> str:
    """Значение для ячейки CSV: сложные типы превращаются в JSON-строку."""
    if value is None:
        return ""
    if isinstance(value, (list, dict, tuple, set, frozenset)):
        return json.dumps(value, ensure_ascii=False, default=json_default)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    text = str(value)
    if sanitize_formulas and isinstance(value, str) and text[:1] in ("=", "+", "-", "@"):
        return "'" + text
    return text


class CSVStorage(_BufferedFileStorage):
    """CSV-хранилище. Заголовки берутся из первой записи.

    Следующие записи приводятся к этим колонкам: лишние поля отбрасываются,
    недостающие остаются пустыми. Текст, начинающийся с = + - @, получает
    префикс ' (защита от формул в Excel), отключается sanitize_formulas=False.
    """

    def __init__(self, path: str | Path, fieldnames: list[str] | None = None,
                 delimiter: str = ",", encoding: str = "utf-8", buffer_size: int = 20,
                 sanitize_formulas: bool = True, append: bool = False) -> None:
        super().__init__(path, buffer_size, encoding, append)
        self.fieldnames = list(fieldnames) if fieldnames else None
        self.delimiter = delimiter
        self.sanitize_formulas = sanitize_formulas
        self._header_written = append and self.path.exists() and self.path.stat().st_size > 0
        self._warned_extra = False

    def _encode(self, rows: list[list[str]]) -> str:
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer, delimiter=self.delimiter, lineterminator="\n")
        writer.writerows(rows)
        return buffer.getvalue()

    async def save(self, data: dict) -> None:
        self._check_open()
        async with self._lock:
            if self.fieldnames is None:
                self.fieldnames = list(data.keys())
            extra = set(data) - set(self.fieldnames)
            if extra and not self._warned_extra:
                logger.warning("CSV: поля %s не входят в заголовок и будут пропущены",
                               sorted(extra))
                self._warned_extra = True
            rows = []
            header_was_written = self._header_written
            if not header_was_written:
                rows.append(self.fieldnames)
                self._header_written = True
            rows.append([flatten_value(data.get(name), self.sanitize_formulas)
                         for name in self.fieldnames])
            await self._enqueue(self._encode(rows),
                                undo=lambda: setattr(self, "_header_written",
                                                     header_was_written))


class SQLiteStorage(DataStorage):
    """SQLite через aiosqlite: пакетные вставки, индексы, повтор записи с тем же url заменяет её."""

    def __init__(self, path: str | Path, batch_size: int = 20, table: str = "pages") -> None:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
            raise ValueError(f"Недопустимое имя таблицы: {table!r}")
        if batch_size < 1:
            raise ValueError("batch_size должен быть не меньше 1")
        self.path = Path(path)
        self.batch_size = batch_size
        self.table = table
        self.saved = 0
        self._batch: list[tuple] = []
        self._db: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()
        self._closed = False

    async def init_db(self) -> None:
        if self._db is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.path)
        await self._db.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table} ("
            "url TEXT PRIMARY KEY, title TEXT, text TEXT, links TEXT, metadata TEXT, "
            "crawled_at TEXT, status_code INTEGER, content_type TEXT)")
        await self._db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.table}_title ON {self.table}(title)")
        await self._db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.table}_crawled_at "
            f"ON {self.table}(crawled_at)")
        await self._db.commit()

    async def save(self, data: dict) -> None:
        if self._closed:
            raise RuntimeError("Хранилище уже закрыто")
        row = (
            data["url"], data.get("title", ""), data.get("text", ""),
            json.dumps(data.get("links", []), ensure_ascii=False),
            json.dumps(data.get("metadata", {}), ensure_ascii=False, default=json_default),
            flatten_value(data.get("crawled_at"), False),
            data.get("status_code"), data.get("content_type", ""),
        )
        async with self._lock:
            await self.init_db()
            self._batch.append(row)
            if len(self._batch) >= self.batch_size:
                try:
                    await self._write_batch()
                except Exception:
                    self._batch.pop()
                    raise

    async def _write_batch(self) -> None:
        if not self._batch:
            return
        rows, self._batch = self._batch, []
        try:
            await self._db.executemany(
                f"INSERT OR REPLACE INTO {self.table} "
                "(url, title, text, links, metadata, crawled_at, status_code, content_type) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
            await self._db.commit()
        except Exception:
            self._batch = rows + self._batch
            raise
        self.saved += len(rows)

    def pending(self) -> int:
        return len(self._batch)

    async def flush(self) -> None:
        async with self._lock:
            if self._db is not None:
                await self._write_batch()

    async def close(self) -> None:
        if self._closed:
            return
        async with self._lock:
            if self._db is not None:
                await self._write_batch()
                await self._db.close()
                self._db = None
            self._closed = True

    async def count(self) -> int:
        await self.flush()
        async with self._lock:
            await self.init_db()
            async with self._db.execute(f"SELECT COUNT(*) FROM {self.table}") as cursor:
                return (await cursor.fetchone())[0]

    async def fetch_all(self) -> list[dict]:
        await self.flush()
        async with self._lock:
            await self.init_db()
            self._db.row_factory = aiosqlite.Row
            async with self._db.execute(f"SELECT * FROM {self.table} ORDER BY url") as cursor:
                rows = await cursor.fetchall()
        return [self._decode(row) for row in rows]

    async def search_title(self, text: str) -> list[dict]:
        await self.flush()
        async with self._lock:
            await self.init_db()
            self._db.row_factory = aiosqlite.Row
            async with self._db.execute(
                    f"SELECT * FROM {self.table} WHERE title LIKE ? ORDER BY url",
                    (f"%{text}%",)) as cursor:
                rows = await cursor.fetchall()
        return [self._decode(row) for row in rows]

    @staticmethod
    def _decode(row) -> dict:
        record = dict(row)
        record["links"] = json.loads(record["links"] or "[]")
        record["metadata"] = json.loads(record["metadata"] or "{}")
        return record

    def stats(self) -> dict:
        return {"saved": self.saved, "path": str(self.path)}


class MultiStorage(DataStorage):
    """Пишет каждую запись сразу в несколько хранилищ (например, JSON + CSV + SQLite).

    Если упало одно из хранилищ, остальные продолжают работать (ошибка пишется в лог).
    Ошибка поднимается наружу, только если не сохранилось ни в одном.
    """

    def __init__(self, *storages: DataStorage) -> None:
        if not storages:
            raise ValueError("Нужно хотя бы одно хранилище")
        self.storages = list(storages)

    async def _run(self, method: str, *args) -> None:
        results = await asyncio.gather(
            *(getattr(storage, method)(*args) for storage in self.storages),
            return_exceptions=True)
        errors = [r for r in results if isinstance(r, Exception)]
        for storage, result in zip(self.storages, results):
            if isinstance(result, Exception):
                logger.warning("%s.%s не удалось: %s: %s", type(storage).__name__,
                               method, type(result).__name__, result)
        if errors and len(errors) == len(self.storages):
            raise errors[0]

    def pending(self) -> int:
        return min(storage.pending() for storage in self.storages)

    async def save(self, data: dict) -> None:
        await self._run("save", data)

    async def flush(self) -> None:
        await self._run("flush")

    async def close(self) -> None:
        await self._run("close")

    def stats(self) -> dict:
        return {"storages": [{"type": type(s).__name__, **s.stats()} for s in self.storages]}


def read_json_records(path: str | Path) -> list[dict]:
    """Читает файл, записанный JSONStorage (любой режим)."""
    text = Path(path).read_text(encoding="utf-8")
    stripped = text.lstrip()
    if stripped.startswith("["):
        return json.loads(text)
    return [json.loads(line) for line in text.splitlines() if line.strip()]
