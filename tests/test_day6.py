import asyncio
import csv
import json
import sqlite3
from datetime import datetime, timezone

import aiosqlite
import pytest

from webcrawler import AsyncCrawler
from webcrawler.storage import (
    CSVStorage,
    DataStorage,
    JSONStorage,
    SQLiteStorage,
    flatten_value,
    make_record,
    read_json_records,
)

NOW = datetime(2026, 10, 21, 12, 0, tzinfo=timezone.utc)


def record(i=0, **extra):
    data = {
        "url": f"http://a.test/{i}",
        "title": f"Страница {i}",
        "text": f"Текст {i}",
        "links": [f"http://a.test/{i + 1}"],
        "metadata": {"description": "описание", "keywords": ["a", "b"]},
        "crawled_at": NOW,
        "status_code": 200,
        "content_type": "text/html",
    }
    data.update(extra)
    return data


def read_csv(path, encoding="utf-8", delimiter=","):
    with open(path, newline="", encoding=encoding) as file:
        return list(csv.DictReader(file, delimiter=delimiter))


# ---------- формат записи ----------

def test_make_record_has_standard_fields():
    parsed = {"url": "http://a.test/", "title": "T", "text": "x", "links": ["l"],
              "metadata": {"k": 1}, "status_code": 200, "content_type": "text/html",
              "images": [1], "errors": []}
    result = make_record(parsed)
    assert list(result) == ["url", "title", "text", "links", "metadata", "crawled_at",
                            "status_code", "content_type"]
    assert isinstance(result["crawled_at"], datetime)


def test_flatten_value():
    assert flatten_value(None) == ""
    assert flatten_value(["а", "b"]) == '["а", "b"]'
    assert flatten_value({"k": 1}) == '{"k": 1}'
    assert flatten_value(NOW) == NOW.isoformat()
    assert flatten_value(200) == "200"
    assert flatten_value("=SUM(A1)") == "'=SUM(A1)"
    assert flatten_value("-5 градусов") == "'-5 градусов"
    assert flatten_value("=SUM(A1)", sanitize_formulas=False) == "=SUM(A1)"
    assert flatten_value(-5) == "-5"


# ---------- JSON ----------

async def test_json_lines_roundtrip_and_unicode(tmp_path):
    path = tmp_path / "out" / "data.jsonl"
    async with JSONStorage(path, buffer_size=7) as storage:
        for i in range(50):
            await storage.save(record(i))
    records = read_json_records(path)
    assert len(records) == 50
    assert records[3]["title"] == "Страница 3"
    assert records[3]["crawled_at"] == NOW.isoformat()
    assert "Страница 3" in path.read_text(encoding="utf-8")  # не С...
    assert len(path.read_text(encoding="utf-8").splitlines()) == 50


async def test_json_array_pretty(tmp_path):
    path = tmp_path / "data.json"
    async with JSONStorage(path, mode="array", indent=2, buffer_size=3) as storage:
        for i in range(8):
            await storage.save(record(i))
    data = json.loads(path.read_text(encoding="utf-8"))
    assert [r["url"] for r in data] == [f"http://a.test/{i}" for i in range(8)]
    assert '\n  "url"' in path.read_text(encoding="utf-8")


@pytest.mark.parametrize("mode", ["lines", "array"])
async def test_json_empty_storage_is_valid(tmp_path, mode):
    path = tmp_path / "empty.json"
    storage = JSONStorage(path, mode=mode)
    await storage.close()
    assert read_json_records(path) == []


async def test_json_buffering_flushes_on_threshold_and_flush(tmp_path):
    path = tmp_path / "data.jsonl"
    storage = JSONStorage(path, buffer_size=10)
    for i in range(4):
        await storage.save(record(i))
    assert not path.exists() or path.read_text(encoding="utf-8") == ""
    await storage.flush()
    assert len(read_json_records(path)) == 4
    for i in range(4, 14):
        await storage.save(record(i))
    assert len(read_json_records(path)) >= 14
    await storage.close()


async def test_json_concurrent_saves_keep_lines_intact(tmp_path):
    path = tmp_path / "data.jsonl"
    async with JSONStorage(path, buffer_size=5) as storage:
        await asyncio.gather(*(storage.save(record(i)) for i in range(200)))
    records = read_json_records(path)
    assert len(records) == 200
    assert len({r["url"] for r in records}) == 200


async def test_json_append_mode_and_validation(tmp_path):
    path = tmp_path / "data.jsonl"
    async with JSONStorage(path) as storage:
        await storage.save(record(1))
    async with JSONStorage(path, append=True) as storage:
        await storage.save(record(2))
    assert len(read_json_records(path)) == 2
    with pytest.raises(ValueError):
        JSONStorage(path, mode="xml")
    with pytest.raises(ValueError):
        JSONStorage(path, mode="lines", indent=2)
    with pytest.raises(ValueError):
        JSONStorage(path, mode="array", append=True)
    with pytest.raises(ValueError):
        JSONStorage(path, buffer_size=0)


async def test_storage_rejects_save_after_close(tmp_path):
    storage = JSONStorage(tmp_path / "x.jsonl")
    await storage.close()
    await storage.close()
    with pytest.raises(RuntimeError):
        await storage.save(record())


# ---------- CSV ----------

async def test_csv_headers_special_chars_and_complex_values(tmp_path):
    path = tmp_path / "data.csv"
    nasty = 'Запятая, "кавычки"\nи перенос строки; точка с запятой'
    async with CSVStorage(path) as storage:
        await storage.save(record(1, text=nasty))
        await storage.save(record(2))
    rows = read_csv(path)
    assert list(rows[0]) == ["url", "title", "text", "links", "metadata", "crawled_at",
                             "status_code", "content_type"]
    assert rows[0]["text"] == nasty
    assert json.loads(rows[0]["links"]) == ["http://a.test/2"]
    assert json.loads(rows[0]["metadata"])["keywords"] == ["a", "b"]
    assert rows[0]["status_code"] == "200"
    assert len(rows) == 2


async def test_csv_unknown_fields_dropped_and_missing_left_empty(tmp_path):
    path = tmp_path / "data.csv"
    async with CSVStorage(path) as storage:
        await storage.save({"url": "u1", "title": "t1"})
        await storage.save({"url": "u2", "extra": "лишнее"})
    rows = read_csv(path)
    assert rows == [{"url": "u1", "title": "t1"}, {"url": "u2", "title": ""}]


async def test_csv_formula_injection_is_neutralised(tmp_path):
    path = tmp_path / "data.csv"
    async with CSVStorage(path) as storage:
        await storage.save(record(1, title="=HYPERLINK(\"http://evil\")"))
    assert read_csv(path)[0]["title"].startswith("'=")


async def test_csv_encodings_and_delimiter(tmp_path):
    path = tmp_path / "bom.csv"
    async with CSVStorage(path, encoding="utf-8-sig", delimiter=";") as storage:
        await storage.save(record(1))
    assert path.read_bytes().startswith(b"\xef\xbb\xbf")
    rows = read_csv(path, "utf-8-sig", delimiter=";")
    assert rows[0]["title"] == "Страница 1"
    assert len(rows) == 1

    path = tmp_path / "cp1251.csv"
    async with CSVStorage(path, encoding="cp1251") as storage:
        await storage.save(record(1, title="Привет 😀"))
    assert read_csv(path, "cp1251")[0]["title"].startswith("Привет")


async def test_csv_append_does_not_repeat_header(tmp_path):
    path = tmp_path / "data.csv"
    async with CSVStorage(path) as storage:
        await storage.save(record(1))
    async with CSVStorage(path, append=True, fieldnames=list(record(1))) as storage:
        await storage.save(record(2))
    assert len(read_csv(path)) == 2
    assert path.read_text(encoding="utf-8").count("url,title") == 1


# ---------- SQLite ----------

async def test_sqlite_creates_table_and_indexes(tmp_path):
    path = tmp_path / "crawler.db"
    storage = SQLiteStorage(path)
    await storage.init_db()
    await storage.close()
    with sqlite3.connect(path) as db:
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        indexes = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert "pages" in tables
    assert {"idx_pages_title", "idx_pages_crawled_at"} <= indexes


async def test_sqlite_batch_insert_and_readback(tmp_path):
    path = tmp_path / "crawler.db"
    async with SQLiteStorage(path, batch_size=10) as storage:
        for i in range(25):
            await storage.save(record(i))
        assert storage.saved == 20  # два полных пакета записаны, 5 ещё в буфере
        assert await storage.count() == 25  # count() сначала сбрасывает буфер
        rows = await storage.fetch_all()
        found = await storage.search_title("Страница 7")
    assert len(rows) == 25
    row = next(r for r in rows if r["url"] == "http://a.test/3")
    assert row["links"] == ["http://a.test/4"]
    assert row["metadata"]["keywords"] == ["a", "b"]
    assert row["status_code"] == 200 and row["content_type"] == "text/html"
    assert [r["url"] for r in found] == ["http://a.test/7"]


async def test_sqlite_same_url_replaces_row(tmp_path):
    async with SQLiteStorage(tmp_path / "c.db", batch_size=2) as storage:
        await storage.save(record(1, title="старый"))
        await storage.save(record(1, title="новый"))
        await storage.save(record(2))
        assert await storage.count() == 2
        assert (await storage.search_title("новый"))[0]["url"] == "http://a.test/1"


async def test_sqlite_concurrent_saves(tmp_path):
    async with SQLiteStorage(tmp_path / "c.db", batch_size=7) as storage:
        await asyncio.gather(*(storage.save(record(i)) for i in range(100)))
        assert await storage.count() == 100


def test_sqlite_validates_arguments(tmp_path):
    with pytest.raises(ValueError):
        SQLiteStorage(tmp_path / "c.db", table="pages; DROP TABLE x")
    with pytest.raises(ValueError):
        SQLiteStorage(tmp_path / "c.db", batch_size=0)


async def test_sql_injection_in_data_is_harmless(tmp_path):
    path = tmp_path / "c.db"
    async with SQLiteStorage(path, batch_size=1) as storage:
        await storage.save(record(1, title="x'); DROP TABLE pages; --"))
        assert await storage.count() == 1
        assert (await storage.fetch_all())[0]["title"] == "x'); DROP TABLE pages; --"


# ---------- интеграция с краулером ----------

async def run_crawl(site, storage, **kwargs):
    async with AsyncCrawler(max_depth=2, storage=storage, progress_interval=60,
                            **kwargs) as crawler:
        results = await crawler.crawl([site.url + "/"], max_pages=100)
        stats = crawler.get_storage_stats()
        errors = list(crawler.storage_errors)
    return results, stats, errors


async def test_crawl_saves_to_json(site, tmp_path):
    path = tmp_path / "r.jsonl"
    results, stats, errors = await run_crawl(site, JSONStorage(path))
    records = read_json_records(path)
    assert {r["url"] for r in records} == set(results)
    assert errors == [] and stats["crawler_saved"] == len(results)
    home = next(r for r in records if r["url"] == site.url + "/")
    assert home["title"] == "Mock Home"
    assert home["status_code"] == 200 and home["content_type"] == "text/html"
    assert datetime.fromisoformat(home["crawled_at"]).tzinfo is not None


async def test_crawl_saves_to_csv(site, tmp_path):
    path = tmp_path / "r.csv"
    results, _, _ = await run_crawl(site, CSVStorage(path))
    assert {r["url"] for r in read_csv(path)} == set(results)


async def test_crawl_saves_to_sqlite(site, tmp_path):
    path = tmp_path / "r.db"
    storage = SQLiteStorage(path, batch_size=3)
    results, _, _ = await run_crawl(site, storage)
    with sqlite3.connect(path) as db:
        urls = {r[0] for r in db.execute("SELECT url FROM pages")}
    assert urls == set(results)


async def test_failed_pages_are_not_saved(site, tmp_path):
    path = tmp_path / "r.jsonl"
    async with AsyncCrawler(max_depth=0, storage=JSONStorage(path),
                            progress_interval=60) as crawler:
        await crawler.crawl([site.url + "/", site.url + "/status/404"])
    assert [r["url"] for r in read_json_records(path)] == [site.url + "/"]


class FlakyStorage(DataStorage):
    def __init__(self, fail_times):
        self.fail_times = fail_times
        self.attempts = 0
        self.saved_records = []

    async def save(self, data):
        self.attempts += 1
        if self.attempts <= self.fail_times:
            raise OSError("диск занят")
        self.saved_records.append(data)

    async def close(self):
        pass


async def test_storage_errors_are_retried(site):
    storage = FlakyStorage(fail_times=2)
    async with AsyncCrawler(max_depth=0, storage=storage, storage_retries=3,
                            storage_retry_delay=0.001, progress_interval=60) as crawler:
        await crawler.crawl([site.url + "/"])
        assert crawler.storage_errors == [] and crawler.storage_saved == 1
    assert storage.attempts == 3


async def test_broken_storage_does_not_stop_crawling(site):
    storage = FlakyStorage(fail_times=10_000)
    async with AsyncCrawler(max_depth=1, storage=storage, storage_retries=1,
                            storage_retry_delay=0.001, progress_interval=60) as crawler:
        results = await crawler.crawl([site.url + "/"])
        assert len(results) == 5
        assert len(crawler.storage_errors) == 5
        assert crawler.storage_errors[0]["type"] == "OSError"
        assert crawler.get_storage_stats()["errors"] == 5


async def test_non_html_content_is_not_decoded(site):
    async with AsyncCrawler() as crawler:
        parsed = await crawler.fetch_and_parse(site.url + "/binary")
        plain = await crawler.fetch_and_parse(site.url + "/plain")
    assert parsed["content_type"] == "image/png"
    assert parsed["text"] == "" and parsed["links"] == []
    assert plain["content_type"] == "text/plain" and plain["text"] == "просто текст"


async def test_crawler_close_closes_storage(site, tmp_path):
    path = tmp_path / "r.json"
    crawler = AsyncCrawler(max_depth=0, storage=JSONStorage(path, mode="array"),
                           progress_interval=60)
    await crawler.crawl([site.url + "/"])
    await crawler.close()
    assert len(json.loads(path.read_text(encoding="utf-8"))) == 1


async def test_aiosqlite_is_used_asynchronously(tmp_path):
    async with aiosqlite.connect(tmp_path / "x.db") as db:
        await db.execute("CREATE TABLE t (a)")
        await db.commit()


async def test_multi_storage_writes_everywhere_and_survives_partial_failure(site, tmp_path):
    from webcrawler.storage import MultiStorage
    good = JSONStorage(tmp_path / "a.jsonl")
    multi = MultiStorage(good, FlakyStorage(fail_times=10_000))
    await multi.save(record(1))
    await multi.close()
    assert len(read_json_records(tmp_path / "a.jsonl")) == 1

    only_bad = MultiStorage(FlakyStorage(fail_times=10_000))
    with pytest.raises(OSError):
        await only_bad.save(record(1))
    with pytest.raises(ValueError):
        MultiStorage()
