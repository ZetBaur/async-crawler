import asyncio
import io
import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from crawler import AdvancedCrawler, amain, build_config, build_parser
from webcrawler.advanced import make_storage
from webcrawler.config import ConfigError, CrawlerConfig, StorageConfig, load_config
from webcrawler.logging_setup import JsonFormatter, setup_logging, teardown_logging
from webcrawler.monitor import ProgressMonitor, format_duration, render_progress
from webcrawler.report import bar_chart_svg, render_html_report
from webcrawler.robots import parse_sitemaps
from webcrawler.sitemap import SitemapParser
from webcrawler.stats import CrawlerStats
from webcrawler.storage import CSVStorage, JSONStorage, SQLiteStorage, read_json_records

ROOT = Path(__file__).resolve().parent.parent
NS = "http://www.sitemaps.org/schemas/sitemap/0.9"


def urlset(*locs, ns=True):
    attr = f' xmlns="{NS}"' if ns else ""
    items = "".join(f"<url><loc>{loc}</loc></url>" for loc in locs)
    return f'<?xml version="1.0" encoding="UTF-8"?><urlset{attr}>{items}</urlset>'


def index(*locs):
    items = "".join(f"<sitemap><loc>{loc}</loc></sitemap>" for loc in locs)
    return f'<?xml version="1.0" encoding="UTF-8"?><sitemapindex xmlns="{NS}">{items}</sitemapindex>'


def fake_fetcher(files: dict):
    calls = []

    async def fetch(url):
        calls.append(url)
        if url not in files:
            return 404, "нет"
        value = files[url]
        if isinstance(value, Exception):
            raise value
        return 200, value

    fetch.calls = calls
    return fetch


# ---------- sitemap ----------

async def test_sitemap_index_is_followed_recursively(site):
    from webcrawler import AsyncCrawler
    async with AsyncCrawler() as crawler:
        urls = await SitemapParser(crawler.fetch_raw).fetch_sitemap(site.url + "/sitemap_index.xml")
    assert urls == [site.url + p for p in ("/", "/page/1", "/page/2", "/page/3", "/about")]


async def test_plain_sitemap_with_and_without_namespace():
    files = {"http://a.test/s1.xml": urlset("http://a.test/x", "http://a.test/y"),
             "http://a.test/s2.xml": urlset("http://a.test/z", ns=False)}
    parser = SitemapParser(fake_fetcher(files))
    assert await parser.fetch_sitemap("http://a.test/s1.xml") == ["http://a.test/x", "http://a.test/y"]
    assert await parser.fetch_sitemap("http://a.test/s2.xml") == ["http://a.test/z"]


async def test_sitemap_cycles_depth_and_duplicates_are_safe():
    files = {
        "http://a.test/a.xml": index("http://a.test/b.xml", "http://a.test/a.xml"),
        "http://a.test/b.xml": index("http://a.test/a.xml", "http://a.test/c.xml"),
        "http://a.test/c.xml": urlset("http://a.test/p", "http://a.test/p#frag", "http://a.test/p"),
    }
    fetcher = fake_fetcher(files)
    urls = await SitemapParser(fetcher).fetch_sitemap("http://a.test/a.xml")
    assert urls == ["http://a.test/p"]
    assert sorted(fetcher.calls) == ["http://a.test/a.xml", "http://a.test/b.xml", "http://a.test/c.xml"]

    shallow = await SitemapParser(fake_fetcher(files), max_depth=1).fetch_sitemap("http://a.test/a.xml")
    assert shallow == []


async def test_sitemap_limit_and_relative_locations():
    locs = [f"http://a.test/{i}" for i in range(10)]
    parser = SitemapParser(fake_fetcher({"http://a.test/s.xml": urlset(*locs)}), max_urls=4)
    assert len(await parser.fetch_sitemap("http://a.test/s.xml")) == 4
    parser = SitemapParser(fake_fetcher({"http://a.test/dir/s.xml": urlset("/abs", "rel")}))
    assert await parser.fetch_sitemap("http://a.test/dir/s.xml") == ["http://a.test/abs", "http://a.test/dir/rel"]


@pytest.mark.parametrize("body", ["", "   ", "<urlset", "не xml вообще", "<html><body/></html>",
                                  "<?xml version='1.0'?><a><b/></a>"])
async def test_broken_sitemaps_give_empty_result(body):
    parser = SitemapParser(fake_fetcher({"http://a.test/s.xml": body}))
    assert await parser.fetch_sitemap("http://a.test/s.xml") == []


async def test_missing_or_failing_sitemap_gives_empty_result():
    files = {"http://a.test/err.xml": ConnectionError("нет связи")}
    parser = SitemapParser(fake_fetcher(files))
    assert await parser.fetch_sitemap("http://a.test/missing.xml") == []
    assert await parser.fetch_sitemap("http://a.test/err.xml") == []


async def test_sitemap_with_other_encoding_declaration_and_junk_locations():
    body = ('<?xml version="1.0" encoding="windows-1251"?>'
            f'<urlset xmlns="{NS}"><url><loc>http://a.test/привет</loc></url>'
            "<url><loc>mailto:a@b.c</loc></url><url><loc></loc></url><url/></urlset>")
    urls = await SitemapParser(fake_fetcher({"http://a.test/s.xml": body})).fetch_sitemap("http://a.test/s.xml")
    assert urls == ["http://a.test/привет"]


async def test_sitemap_does_not_expand_external_entities(tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("СЕКРЕТ", encoding="utf-8")
    body = (f'<?xml version="1.0"?><!DOCTYPE d [<!ENTITY x SYSTEM "file://{secret}">]>'
            f'<urlset xmlns="{NS}"><url><loc>http://a.test/&x;</loc></url></urlset>')
    urls = await SitemapParser(fake_fetcher({"http://a.test/s.xml": body})).fetch_sitemap("http://a.test/s.xml")
    assert all("СЕКРЕТ" not in url for url in urls)


def test_sitemaps_from_robots_txt():
    text = "User-agent: *\nDisallow: /x\nSitemap: http://a.test/s1.xml # комментарий\nsitemap: http://a.test/s2.xml\nSitemap:\n"
    assert parse_sitemaps(text) == ["http://a.test/s1.xml", "http://a.test/s2.xml"]


# ---------- статистика ----------

def fake_crawler():
    return SimpleNamespace(
        processed_urls={
            "http://a.test/1": {"status_code": 200}, "http://a.test/2": {"status_code": 200},
            "http://b.test/1": {"status_code": 200}, "http://b.test/2": {},
        },
        error_log=[{"url": "http://a.test/404", "type": "PermanentError", "status": 404},
                   {"url": "http://c.test/x", "type": "NetworkError", "status": None}],
        robots_blocked=["http://a.test/private"])


def test_crawler_stats_summary():
    stats = CrawlerStats.from_crawler(fake_crawler(), elapsed=2.0)
    summary = stats.summary()
    assert summary["total_pages"] == 6 and summary["successful"] == 4
    assert summary["failed"] == 2 and summary["skipped"] == 1
    assert summary["avg_speed"] == 3.0 and summary["elapsed_seconds"] == 2.0
    assert summary["success_rate"] == pytest.approx(0.6667, abs=1e-4)
    assert summary["status_codes"] == {"200": 3, "404": 1, "?": 1, "NetworkError": 1}
    assert summary["top_domains"][0] == {"domain": "a.test", "pages": 3}
    assert summary["errors_by_type"] == {"PermanentError": 1, "NetworkError": 1}


def test_crawler_stats_empty_is_safe():
    summary = CrawlerStats().summary()
    assert summary["total_pages"] == 0 and summary["avg_speed"] == 0.0
    assert summary["success_rate"] == 0.0 and summary["top_domains"] == []


# ---------- HTML-отчёт ----------

def test_html_report_is_escaped_and_has_charts():
    summary = CrawlerStats.from_crawler(fake_crawler(), 2.0).summary()
    html = render_html_report(
        summary, {"http://a.test/<script>alert(1)</script>": "Err & <b>bold</b>"},
        [{"url": "http://a.test/1", "title": "<img src=x onerror=alert(1)>", "links": [1, 2], "text": "abc"}])
    assert "<script>alert(1)</script>" not in html
    assert "<img src=x" not in html
    assert "&lt;script&gt;" in html and "&lt;img src=x" in html
    assert html.count("<svg") == 3 and "Отчёт краулера" in html


def test_bar_chart_handles_empty_and_long_labels():
    assert "Нет данных" in bar_chart_svg([])
    svg = bar_chart_svg([("x" * 100, 5), ("<b>", 0)])
    assert "<b>" not in svg and "…" in svg


# ---------- конфигурация ----------

def test_config_defaults_and_roundtrip():
    config = CrawlerConfig()
    assert config.max_pages == 100 and config.retry.enabled and config.storage.type == "none"
    assert CrawlerConfig.from_dict(config.to_dict()).to_dict() == config.to_dict()


def test_config_example_file_is_valid():
    config = load_config(ROOT / "config.example.yaml")
    assert config.start_urls == ["https://books.toscrape.com/"]
    assert config.requests_per_second == 2.0 and config.logging.file == "output/crawler.log"
    assert config.storage.type == "jsonl" and config.output.html_report == "output/report.html"


def test_config_loads_yaml_and_json(tmp_path):
    (tmp_path / "c.yaml").write_text("start_urls: [http://a.test/]\nmax_depth: 1\nretry: {max_retries: 5}\n", encoding="utf-8")
    (tmp_path / "c.json").write_text('{"start_urls": "http://b.test/", "storage": {"type": "csv"}}', encoding="utf-8")
    yaml_config = load_config(tmp_path / "c.yaml")
    assert yaml_config.max_depth == 1 and yaml_config.retry.max_retries == 5
    json_config = load_config(tmp_path / "c.json")
    assert json_config.start_urls == ["http://b.test/"] and json_config.storage.type == "csv"
    (tmp_path / "empty.yaml").write_text("", encoding="utf-8")
    assert load_config(tmp_path / "empty.yaml").max_pages == 100


@pytest.mark.parametrize("data, fragment", [
    ({"max_pagez": 5}, "max_pagez"),
    ({"retry": {"retries": 5}}, "retries"),
    ({"max_pages": "10"}, "max_pages"),
    ({"max_pages": True}, "max_pages"),
    ({"respect_robots": "yes"}, "respect_robots"),
    ({"requests_per_second": "fast"}, "requests_per_second"),
    ({"start_urls": [1, 2]}, "start_urls"),
    ({"retry": 5}, "retry"),
    ({"max_pages": 0}, "max_pages"),
    ({"max_depth": -1}, "max_depth"),
    ({"requests_per_second": 0}, "requests_per_second"),
    ({"storage": {"type": "excel"}}, "storage.type"),
    ({"logging": {"level": "LOUD"}}, "logging.level"),
    ({"exclude_patterns": ["["]}, "exclude_patterns"),
    ({"retry": {"backoff_factor": 0.5}}, "backoff_factor"),
])
def test_config_rejects_bad_values(data, fragment):
    with pytest.raises(ConfigError) as info:
        CrawlerConfig.from_dict(data)
    assert fragment in str(info.value)


def test_config_reports_all_problems_at_once():
    with pytest.raises(ConfigError) as info:
        CrawlerConfig.from_dict({"max_pages": 0, "max_depth": -1, "max_concurrent": 0})
    message = str(info.value)
    assert "max_pages" in message and "max_depth" in message and "max_concurrent" in message


def test_config_file_errors(tmp_path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "missing.yaml")
    (tmp_path / "bad.txt").write_text("x", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(tmp_path / "bad.txt")
    (tmp_path / "bad.yaml").write_text("a: [1, 2", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(tmp_path / "bad.yaml")
    (tmp_path / "bad.json").write_text("{", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(tmp_path / "bad.json")
    (tmp_path / "list.yaml").write_text("- 1\n- 2\n", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(tmp_path / "list.yaml")


def test_config_override_keeps_original():
    config = CrawlerConfig()
    changed = config.override(max_pages=5, max_depth=None)
    assert changed.max_pages == 5 and changed.max_depth == 3 and config.max_pages == 100
    with pytest.raises(ConfigError):
        config.override(unknown=1)


# ---------- логирование ----------

def test_logging_file_json_and_no_duplicate_handlers(tmp_path):
    path = tmp_path / "logs" / "c.log"
    try:
        setup_logging("INFO", path, json_format=True, console=False)
        setup_logging("INFO", path, json_format=True, console=False)
        logger = logging.getLogger("webcrawler")
        assert len(logger.handlers) == 1
        logger.info("привет %s", "мир")
        logger.debug("не попадёт")
        logger.warning("внимание")
    finally:
        teardown_logging()
    lines = path.read_text(encoding="utf-8").splitlines()
    records = [json.loads(line) for line in lines]
    assert [r["message"] for r in records] == ["привет мир", "внимание"]
    assert records[1]["level"] == "WARNING" and "time" in records[0]
    assert not logging.getLogger("webcrawler").handlers


def test_logging_rotation_creates_backups(tmp_path):
    path = tmp_path / "r.log"
    try:
        setup_logging("INFO", path, max_bytes=500, backup_count=2, console=False)
        for i in range(100):
            logging.getLogger("webcrawler").info("сообщение номер %s " + "x" * 40, i)
    finally:
        teardown_logging()
    names = sorted(p.name for p in tmp_path.iterdir())
    assert names == ["r.log", "r.log.1", "r.log.2"]


def test_logging_rejects_unknown_level_and_formats_exceptions():
    with pytest.raises(ValueError):
        setup_logging("LOUD", console=False)
    try:
        raise RuntimeError("беда")
    except RuntimeError:
        record = logging.LogRecord("x", logging.ERROR, __file__, 1, "ой", (), sys.exc_info())
    assert "RuntimeError" in json.loads(JsonFormatter().format(record))["exception"]


# ---------- монитор ----------

def test_render_progress_and_duration():
    line = render_progress({"done": 5, "seen": 10, "pages_per_second": 5.0, "failed": 1,
                            "queued": 3, "active": 2}, max_pages=100)
    assert " 50%" in line and "5/10" in line and "5.0 стр/сек" in line
    assert "осталось ~00:01" in line and line.startswith("[############")
    done = render_progress({"done": 10, "seen": 10, "pages_per_second": 2.0}, 10)
    assert "100%" in done and "[" + "#" * 24 + "]" in done
    assert "--:--" in render_progress({"done": 0, "seen": 5}, 10)
    assert format_duration(75) == "01:15" and format_duration(None) == "--:--"
    assert format_duration(-1) == "--:--"


def test_progress_monitor_prints_lines_and_throttles():
    stream = io.StringIO()
    monitor = ProgressMonitor(max_pages=10, stream=stream, min_interval=10)
    monitor({"done": 1, "seen": 5, "pages_per_second": 1.0})
    monitor({"done": 2, "seen": 5, "pages_per_second": 1.0})
    assert len(stream.getvalue().splitlines()) == 1
    monitor.finish({"done": 5, "seen": 5, "pages_per_second": 1.0})
    assert len(stream.getvalue().splitlines()) == 2 and "100%" in monitor.last_line


# ---------- хранилище из настроек ----------

@pytest.mark.parametrize("kind, cls", [("json", JSONStorage), ("jsonl", JSONStorage),
                                       ("csv", CSVStorage), ("sqlite", SQLiteStorage)])
def test_make_storage(tmp_path, kind, cls):
    assert isinstance(make_storage(StorageConfig(type=kind, path=str(tmp_path / "x"))), cls)
    assert make_storage(StorageConfig(type="none")) is None


# ---------- AdvancedCrawler ----------

def base_config(site, **extra):
    config = {"start_urls": [site.url + "/"], "max_depth": 2, "respect_robots": False,
              "progress": False, "retry": {"enabled": True, "base_delay": 0.01}}
    config.update(extra)
    return config


async def test_advanced_crawler_full_run_with_stats_and_exports(site, tmp_path):
    config = base_config(site, storage={"type": "jsonl", "path": str(tmp_path / "r.jsonl")})
    config["start_urls"].append(site.url + "/status/404")
    async with AdvancedCrawler(config, configure_logging=False) as crawler:
        results = await crawler.crawl()
        stats = crawler.get_stats()
        json_path = crawler.export_to_json(tmp_path / "out" / "stats.json")
        html_path = crawler.export_to_html_report(tmp_path / "out" / "report.html")
    assert stats["total_pages"] == stats["successful"] + stats["failed"] == len(results) + 1
    assert stats["failed"] == 1 and stats["status_codes"]["200"] == len(results)
    assert stats["status_codes"]["404"] == 1 and stats["errors_by_type"] == {"PermanentError": 1}
    assert stats["top_domains"][0]["pages"] == len(results) + 1
    assert stats["storage"]["crawler_saved"] == len(results)
    assert len(read_json_records(tmp_path / "r.jsonl")) == len(results)
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert {"generated_at", "config", "summary", "errors"} <= set(payload)
    assert payload["summary"]["successful"] == len(results)
    html = html_path.read_text(encoding="utf-8")
    assert "Отчёт краулера" in html and site.url in html


async def test_advanced_crawler_from_yaml_and_auto_export(site, tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"start_urls: ['{site.url}/']\nmax_depth: 1\nrespect_robots: false\nprogress: false\n"
        f"storage: {{type: csv, path: '{tmp_path / 'r.csv'}'}}\n"
        f"output: {{stats_json: '{tmp_path / 's.json'}', html_report: '{tmp_path / 'r.html'}'}}\n",
        encoding="utf-8")
    async with AdvancedCrawler.from_config(config_path, configure_logging=False) as crawler:
        await crawler.crawl()
    assert (tmp_path / "r.csv").exists() and (tmp_path / "s.json").exists()
    assert "<svg" in (tmp_path / "r.html").read_text(encoding="utf-8")


async def test_sitemap_is_used_as_source_of_urls(site):
    config = base_config(site, start_urls=[], max_depth=0,
                         sitemap_urls=[site.url + "/sitemap_index.xml"])
    async with AdvancedCrawler(config, configure_logging=False) as crawler:
        results = await crawler.crawl()
    assert set(results) == {site.url + p for p in ("/", "/page/1", "/page/2", "/page/3", "/about")}


async def test_sitemap_from_robots_txt(site):
    config = base_config(site, max_depth=0, sitemap_from_robots=True)
    async with AdvancedCrawler(config, configure_logging=False) as crawler:
        results = await crawler.crawl()
    assert len(results) == 5  # стартовая страница уже входит в sitemap, дубля нет
    assert site.paths().count("/") == 1


async def test_sitemap_urls_obey_filters(site):
    config = base_config(site, start_urls=[], max_depth=0, include_patterns=["/page/"],
                         sitemap_urls=[site.url + "/sitemap_index.xml"])
    async with AdvancedCrawler(config, configure_logging=False) as crawler:
        results = await crawler.crawl()
    assert set(results) == {site.url + f"/page/{i}" for i in (1, 2, 3)}


async def test_no_start_urls_is_a_config_error(site):
    async with AdvancedCrawler({"progress": False}, configure_logging=False) as crawler:
        with pytest.raises(ConfigError):
            await crawler.crawl()


async def test_advanced_crawler_respects_robots(site):
    config = base_config(site, max_depth=1, respect_robots=True)
    async with AdvancedCrawler(config, configure_logging=False) as crawler:
        await crawler.crawl()
        stats = crawler.get_stats()
    assert stats["skipped"] == 1 and "/private/secret" not in site.paths()


async def test_advanced_crawler_circuit_breaker_and_user_agents(site):
    config = base_config(site, max_depth=0, user_agents=["A/1", "B/1"],
                         circuit_breaker={"enabled": True, "failure_threshold": 2})
    async with AdvancedCrawler(config, configure_logging=False) as crawler:
        await crawler.crawl()
    assert site.agents[0] in ("A/1", "B/1")
    assert crawler.crawler.circuit_breaker is not None


def test_advanced_crawler_validates_config():
    with pytest.raises(ConfigError):
        AdvancedCrawler({"max_pages": 0}, configure_logging=False)
    with pytest.raises(ConfigError):
        AdvancedCrawler({"nonsense": 1}, configure_logging=False)


# ---------- командная строка ----------

async def test_cli_runs_and_writes_everything(site, tmp_path, capsys):
    out = tmp_path / "res.json"
    code = await amain(["--urls", site.url + "/", "--max-depth", "1", "--no-respect-robots",
                        "--no-progress", "--output", str(out),
                        "--stats-json", str(tmp_path / "s.json"),
                        "--report", str(tmp_path / "r.html"),
                        "--log-file", str(tmp_path / "c.log"), "--log-level", "INFO"])
    assert code == 0
    assert len(json.loads(out.read_text(encoding="utf-8"))) == 5
    assert (tmp_path / "s.json").exists() and (tmp_path / "r.html").exists()
    assert "start url=" in (tmp_path / "c.log").read_text(encoding="utf-8")
    assert "Обработано: 5 страниц" in capsys.readouterr().out
    assert not logging.getLogger("webcrawler").handlers


@pytest.mark.parametrize("suffix", [".jsonl", ".csv", ".db"])
async def test_cli_output_formats(site, tmp_path, suffix):
    out = tmp_path / f"res{suffix}"
    code = await amain(["--urls", site.url + "/about", "--max-depth", "0", "--no-respect-robots",
                        "--no-progress", "--output", str(out)])
    assert code == 0 and out.exists() and out.stat().st_size > 0


async def test_cli_exit_codes(site, tmp_path, capsys):
    assert await amain(["--no-progress"]) == 2
    assert "Ошибка конфигурации" in capsys.readouterr().err
    assert await amain(["--urls", "http://a.test", "--output", str(tmp_path / "x.xlsx")]) == 2
    assert await amain(["--urls", "http://a.test", "--log-level", "LOUD"]) == 2
    assert await amain(["--config", str(tmp_path / "nope.yaml")]) == 2
    assert await amain(["--urls", site.url + "/status/404", "--max-depth", "0",
                        "--no-respect-robots", "--no-progress"]) == 1


def test_cli_priority_command_line_over_config_file(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text("start_urls: [http://file.test/]\nmax_pages: 7\nmax_depth: 4\n"
                    "respect_robots: false\nstorage: {type: csv, path: a.csv}\n", encoding="utf-8")
    args = build_parser().parse_args(["--config", str(path), "--max-pages", "9",
                                      "--respect-robots", "--output", "b.jsonl",
                                      "--include", "x", "--include", "y", "--rate-limit", "2.5"])
    config = build_config(args)
    assert config.max_pages == 9 and config.max_depth == 4 and config.respect_robots is True
    assert config.start_urls == ["http://file.test/"]
    assert config.storage.type == "jsonl" and config.storage.path == "b.jsonl"
    assert config.include_patterns == ["x", "y"] and config.requests_per_second == 2.5


async def test_cli_as_real_process(site, tmp_path):
    out = tmp_path / "proc.jsonl"
    process = await asyncio.create_subprocess_exec(
        sys.executable, "crawler.py", "--urls", site.url + "/", "--max-depth", "1",
        "--no-respect-robots", "--no-progress", "--output", str(out), cwd=ROOT,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, _ = await asyncio.wait_for(process.communicate(), 30)
    assert process.returncode == 0
    assert "Обработано: 5 страниц" in stdout.decode()
    assert len(read_json_records(out)) == 5


async def test_cli_help_works():
    process = await asyncio.create_subprocess_exec(
        sys.executable, "crawler.py", "--help", cwd=ROOT,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, _ = await asyncio.wait_for(process.communicate(), 30)
    assert process.returncode == 0
    for option in ("--urls", "--max-pages", "--max-depth", "--output", "--config",
                   "--respect-robots", "--rate-limit"):
        assert option in stdout.decode()
