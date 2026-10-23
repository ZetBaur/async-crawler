import sqlite3

import pytest

from webcrawler import AsyncCrawler
from webcrawler.storage import CSVStorage, DataStorage, JSONStorage, SQLiteStorage, read_json_records
from tests.test_day6 import read_csv, record


class FailingFile:
    def __init__(self, file, state):
        self._file = file
        self._state = state

    async def write(self, data):
        if self._state["fail_writes"] > 0:
            self._state["fail_writes"] -= 1
            if self._state["partial"]:
                await self._file.write(data[: len(data) // 2])
            raise OSError("диск переполнен")
        return await self._file.write(data)

    def __getattr__(self, name):
        return getattr(self._file, name)


def make_failing(storage, fail_writes, partial=False):
    state = {"fail_writes": fail_writes, "partial": partial}
    original_open = storage._open

    async def failing_open():
        file = await original_open()
        return file if isinstance(file, FailingFile) else FailingFile(file, state)

    async def open_once():
        storage._file = await failing_open()
        return storage._file

    storage._open = open_once
    return state


async def test_json_lines_failed_write_keeps_buffer_and_counter(tmp_path):
    path = tmp_path / "r.jsonl"
    storage = JSONStorage(path, buffer_size=3)
    make_failing(storage, fail_writes=1)
    await storage.save(record(1))
    await storage.save(record(2))
    with pytest.raises(OSError):
        await storage.save(record(3))
    assert storage.pending() == 2
    assert storage.saved == 0
    await storage.save(record(3))
    assert storage.pending() == 0
    assert storage.saved == 3
    await storage.close()
    assert [r["url"] for r in read_json_records(path)] == [f"http://a.test/{i}" for i in (1, 2, 3)]


async def test_partial_write_is_rolled_back(tmp_path):
    path = tmp_path / "r.jsonl"
    storage = JSONStorage(path, buffer_size=2)
    make_failing(storage, fail_writes=1, partial=True)
    await storage.save(record(1))
    with pytest.raises(OSError):
        await storage.save(record(2))
    await storage.save(record(2))
    await storage.close()
    assert [r["url"] for r in read_json_records(path)] == ["http://a.test/1", "http://a.test/2"]


async def test_flush_failure_keeps_records_for_next_flush(tmp_path):
    path = tmp_path / "r.jsonl"
    storage = JSONStorage(path, buffer_size=10)
    make_failing(storage, fail_writes=1)
    await storage.save(record(1))
    await storage.save(record(2))
    with pytest.raises(OSError):
        await storage.flush()
    assert storage.pending() == 2 and storage.saved == 0
    await storage.flush()
    assert storage.pending() == 0 and storage.saved == 2
    await storage.close()
    assert len(read_json_records(path)) == 2


async def test_json_array_stays_valid_after_failed_write(tmp_path):
    path = tmp_path / "r.json"
    storage = JSONStorage(path, mode="array", buffer_size=1)
    make_failing(storage, fail_writes=1)
    with pytest.raises(OSError):
        await storage.save(record(1))
    await storage.save(record(1))
    await storage.save(record(2))
    await storage.close()
    assert [r["url"] for r in read_json_records(path)] == ["http://a.test/1", "http://a.test/2"]


async def test_csv_header_written_once_after_failed_write(tmp_path):
    path = tmp_path / "r.csv"
    storage = CSVStorage(path, buffer_size=1)
    make_failing(storage, fail_writes=1)
    with pytest.raises(OSError):
        await storage.save(record(1))
    await storage.save(record(1))
    await storage.save(record(2))
    await storage.close()
    assert path.read_text(encoding="utf-8").count("url,title") == 1
    assert [r["url"] for r in read_csv(path)] == ["http://a.test/1", "http://a.test/2"]


async def test_sqlite_failed_batch_is_kept_and_retried(tmp_path):
    path = tmp_path / "r.db"
    storage = SQLiteStorage(path, batch_size=2)
    await storage.init_db()
    original = storage._db.executemany
    calls = {"n": 0}

    async def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return await original(*args, **kwargs)

    storage._db.executemany = flaky
    await storage.save(record(1))
    with pytest.raises(sqlite3.OperationalError):
        await storage.save(record(2))
    assert storage.pending() == 1 and storage.saved == 0
    await storage.save(record(2))
    assert storage.saved == 2
    assert await storage.count() == 2
    await storage.close()


class BufferingStorage(DataStorage):
    def __init__(self, flush_failures):
        self.flush_failures = flush_failures
        self.flush_calls = 0
        self.buffer = []
        self.written = []

    async def save(self, data):
        self.buffer.append(data)

    def pending(self):
        return len(self.buffer)

    async def flush(self):
        self.flush_calls += 1
        if self.flush_calls <= self.flush_failures:
            raise OSError("диск занят")
        self.written.extend(self.buffer)
        self.buffer = []

    async def close(self):
        pass


async def test_final_flush_is_retried(site):
    storage = BufferingStorage(flush_failures=2)
    async with AsyncCrawler(max_depth=0, storage=storage, storage_retries=3,
                            storage_retry_delay=0.001, progress_interval=60) as crawler:
        await crawler.crawl([site.url + "/"])
        assert crawler.storage_errors == []
        assert crawler.storage_saved == 1
    assert storage.flush_calls == 3
    assert len(storage.written) == 1


async def test_final_flush_failure_is_recorded_and_not_counted_as_saved(site):
    storage = BufferingStorage(flush_failures=10_000)
    async with AsyncCrawler(max_depth=0, storage=storage, storage_retries=1,
                            storage_retry_delay=0.001, progress_interval=60) as crawler:
        await crawler.crawl([site.url + "/"])
        assert crawler.storage_saved == 0
        assert len(crawler.storage_errors) == 1
        error = crawler.storage_errors[0]
        assert error["type"] == "OSError" and error["lost_records"] == 1
        assert crawler.get_storage_stats()["errors"] == 1
    assert storage.flush_calls == 2
