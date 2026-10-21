import gc
import tracemalloc

import pytest

from webcrawler import AsyncCrawler
from webcrawler.advanced import AdvancedCrawler
from webcrawler.config import ConfigError, CrawlerConfig
from webcrawler.storage import DataStorage, JSONStorage, read_json_records


class NullStorage(DataStorage):
    """Хранилище-пустышка: принимает записи и сразу выбрасывает."""

    def __init__(self):
        self.count = 0

    async def save(self, data):
        self.count += 1

    async def close(self):
        pass


async def test_keep_results_false_keeps_only_brief_and_still_stores_full_pages(site, tmp_path):
    path = tmp_path / "r.jsonl"
    async with AsyncCrawler(max_depth=1, keep_results=False, storage=JSONStorage(path),
                            progress_interval=60) as crawler:
        results = await crawler.crawl([site.url + "/"])
    home = results[site.url + "/"]
    assert set(home) == {"url", "title", "status_code", "content_type",
                         "links_count", "text_length"}
    assert home["title"] == "Mock Home" and home["links_count"] == 5
    stored = {r["url"]: r for r in read_json_records(path)}
    assert set(stored) == set(results)
    assert "Текст главной страницы." in stored[site.url + "/"]["text"]
    assert len(stored[site.url + "/"]["links"]) == 5


async def test_keep_results_false_still_follows_links(site):
    async with AsyncCrawler(max_depth=1, keep_results=False, storage=NullStorage(),
                            progress_interval=60) as crawler:
        results = await crawler.crawl([site.url + "/"])
    assert len(results) == 5


async def test_keep_results_false_retains_much_less_memory(site):
    async def retained(keep):
        """Сколько памяти остаётся занято результатами после обхода."""
        tracemalloc.start()
        try:
            async with AsyncCrawler(max_concurrent=5, max_depth=0, keep_results=keep,
                                    storage=NullStorage(), progress_interval=60) as crawler:
                results = await crawler.crawl(
                    [site.url + f"/big/{i}" for i in range(60)], max_pages=60)
                gc.collect()  # деревья разбора HTML с циклическими ссылками уходят только при сборке мусора
                size = tracemalloc.get_traced_memory()[0]
                assert len(results) == 60
            return size
        finally:
            tracemalloc.stop()

    keep_all = await retained(True)
    brief = await retained(False)
    assert brief < keep_all * 0.4, (brief, keep_all)


def test_keep_results_false_requires_storage():
    with pytest.raises(ConfigError):
        CrawlerConfig.from_dict({"keep_results": False})
    CrawlerConfig.from_dict({"keep_results": False, "storage": {"type": "csv"}})


async def test_report_works_with_brief_results(site, tmp_path):
    config = {"start_urls": [site.url + "/"], "max_depth": 1, "respect_robots": False,
              "progress": False, "keep_results": False,
              "storage": {"type": "jsonl", "path": str(tmp_path / "r.jsonl")}}
    async with AdvancedCrawler(config, configure_logging=False) as crawler:
        await crawler.crawl()
        html = crawler.export_to_html_report(tmp_path / "r.html").read_text(encoding="utf-8")
    assert "Mock Home" in html and site.url in html


async def test_parse_inline_and_in_thread_give_same_result(site):
    async with AsyncCrawler(parse_mode="thread") as threaded, \
            AsyncCrawler(parse_mode="inline") as inline, \
            AsyncCrawler(parse_mode="process", parse_workers=2) as process:
        a = await threaded.fetch_and_parse(site.url + "/")
        b = await inline.fetch_and_parse(site.url + "/")
        c = await process.fetch_and_parse(site.url + "/")
    for key in ("title", "text", "links", "metadata", "images", "headings", "errors"):
        assert a[key] == b[key] == c[key]


def test_parse_mode_is_validated():
    with pytest.raises(ValueError):
        AsyncCrawler(parse_mode="gpu")
    with pytest.raises(ConfigError):
        CrawlerConfig.from_dict({"parse_mode": "gpu"})
    with pytest.raises(ConfigError):
        CrawlerConfig.from_dict({"parse_workers": 0})
