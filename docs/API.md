# API

Все классы лежат в пакете `webcrawler`. `AdvancedCrawler` и командная строка: `crawler.py` в корне проекта.

## AsyncCrawler (`webcrawler.client`)

Асинхронный краулер: загрузка страниц, разбор, обход сайта.

```python
AsyncCrawler(
    max_concurrent=10, max_depth=3, max_per_domain=None,
    connect_timeout=5.0, read_timeout=15.0, total_timeout=60.0,
    user_agent="AsyncCrawler/1.0", user_agents=None,
    requests_per_second=None, per_domain_rate=True, min_delay=0.0, jitter=0.0,
    respect_robots=False,
    retry_strategy=None, circuit_breaker=None, timeout_growth=0.5,
    parse_mode="thread", parse_workers=None, keep_results=True,
    storage=None, storage_retries=2, storage_retry_delay=0.05,
    progress_interval=1.0, on_progress=None,
)
```

Используется как асинхронный контекстный менеджер (`async with`) или с явным `await crawler.close()`.

| Метод | Описание |
|---|---|
| `await fetch_url(url) -> str` | текст страницы. Без `retry_strategy` ошибки `aiohttp` пробрасываются как есть; с ней приходят наши типы (`TransientError`, `PermanentError`, ...) |
| `await fetch_page(url) -> FetchResult` | `(status, text, content_type)`; двоичные типы (картинки, PDF) не читаются как текст |
| `await fetch_urls(urls) -> dict[str, str]` | параллельная загрузка. Возвращает успешные страницы, ошибки попадают в `failed_urls` |
| `await fetch_and_parse(url) -> dict` | загрузка и разбор: `url`, `title`, `text`, `links`, `metadata`, `images`, `headings`, `tables`, `lists`, `errors`, `status_code`, `content_type`, `crawled_at` |
| `await crawl(start_urls, max_pages=100, same_domain_only=True, exclude_patterns=None, include_patterns=None) -> dict[str, dict]` | обход сайта по ссылкам |
| `await fetch_raw(url) -> (status, text)` | загрузка без исключений на 4xx и 5xx |
| `get_progress() -> dict` | обработано, в очереди, ошибки, скорость, активные запросы |
| `get_rate_stats() -> dict` | запросов, текущая скорость, средняя пауза, заблокировано `robots.txt` |
| `error_report() -> dict`, `export_error_report(path)` | ошибки по типам, адреса, статистика повторов |
| `get_storage_stats() -> dict` | сколько сохранено, сколько ошибок записи |

Атрибуты после `crawl`: `visited_urls` (set), `failed_urls` (dict url → ошибка), `processed_urls` (dict url → результат), `robots_blocked`, `storage_errors`, `queue`.

`parse_mode`: `thread`, `inline` или `process`. `keep_results=False`: в `processed_urls` остаётся краткая сводка (`url`, `title`, `status_code`, `content_type`, `links_count`, `text_length`), полные страницы идут только в хранилище.

## HTMLParser (`webcrawler.parser`)

```python
HTMLParser(same_domain_only=False, parse_mode="thread", workers=None)
```

| Метод | Описание |
|---|---|
| `await parse_html(html, url) -> dict` | разбор страницы. Битый HTML не роняет: ошибки в списке `errors`, остальное возвращается |
| `extract_links(soup, base_url) -> list[str]` | абсолютные уникальные ссылки (учитывает `<base>`) |
| `extract_text(soup, selector=None) -> str` | текст страницы или элементов по CSS-селектору (без `script`, `style`) |
| `extract_metadata(soup) -> dict` | `title`, `description`, `keywords`, `canonical`, `lang` |
| `extract_images`, `extract_headings`, `extract_tables`, `extract_lists` | картинки (`src`, `alt`), заголовки h1–h3, таблицы, списки |

`normalize_url(href, base_url) -> str | None`: абсолютный чистый адрес или `None`.

## CrawlerQueue (`webcrawler.crawl_queue`)

| Метод | Описание |
|---|---|
| `add_url(url, priority=0, depth=0) -> bool` | добавить; `False`, если адрес уже был или исчерпан `max_urls` |
| `await get_next() -> str \| None` | следующий адрес; `None`, когда работа закончилась |
| `mark_processed(url)`, `mark_failed(url, error)`, `mark_skipped(url, reason)` | пометки |
| `depth(url)`, `get_stats()` | глубина адреса; счётчики очереди |

## Ограничения (`webcrawler.limits`)

- `SemaphoreManager(max_concurrent=10, max_per_domain=None)`: `async with manager.limit(domain)`; `stats()`.
- `RateLimiter(requests_per_second=1.0, per_domain=True, min_delay=0.0, jitter=0.0, max_backoff=30.0)`: `await acquire(domain)`, `set_crawl_delay(domain, delay)`, `report_error(domain)`, `report_success(domain)`, `current_interval(domain)`, `stats()`.

## RobotsParser (`webcrawler.robots`)

```python
RobotsParser(fetcher, user_agent="*")   # fetcher: async (url) -> (status, text)
```

`await fetch_robots(base_url) -> dict`, `can_fetch(url, user_agent=None) -> bool`, `get_crawl_delay(user_agent=None, origin=None) -> float`. Правила кэшируются по сайтам.

## Ошибки (`webcrawler.errors`)

`CrawlerError` → `TransientError`, `PermanentError` (→ `RobotsBlockedError`), `NetworkError`, `ParseError`, `CircuitOpenError`. У ошибки есть `url`, `status`, `retry_after`. `classify_exception(error, url) -> CrawlerError | None`.

## RetryStrategy и CircuitBreaker (`webcrawler.retry`)

```python
RetryStrategy(max_retries=3, backoff_factor=2.0, retry_on=None, base_delay=0.5, max_delay=30.0,
              jitter=0.0, per_type_retries=None, backoff_by_type=None, rate_limit_multiplier=2.0)
await strategy.execute_with_retry(coro_fn, *args, with_attempt=False, **kwargs)
strategy.stats.as_dict()
```

```python
CircuitBreaker(failure_threshold=5, window=30.0, recovery_time=10.0)
breaker.check(domain)            # CircuitOpenError, если домен заблокирован
breaker.record_failure(domain); breaker.record_success(domain); breaker.state(domain)
```

## Хранилища (`webcrawler.storage`)

Общий интерфейс `DataStorage`: `await save(record)`, `await flush()`, `await close()`, `stats()`.

| Класс | Параметры |
|---|---|
| `JSONStorage(path, mode="lines", indent=None, buffer_size=20, encoding="utf-8", append=False)` | `mode="lines"` (JSON Lines) или `"array"` (с `indent`) |
| `CSVStorage(path, fieldnames=None, delimiter=",", encoding="utf-8", buffer_size=20, sanitize_formulas=True, append=False)` | заголовки из первой записи |
| `SQLiteStorage(path, batch_size=20, table="pages")` | `await init_db()`, `count()`, `fetch_all()`, `search_title(text)` |
| `MultiStorage(*storages)` | пишет сразу в несколько |

Вспомогательные: `make_record(parsed)`, `read_json_records(path)`.

## SitemapParser (`webcrawler.sitemap`)

```python
SitemapParser(fetcher, max_depth=3, max_urls=50_000)
await parser.fetch_sitemap(sitemap_url) -> list[str]
```

Обычные и индексные sitemap, рекурсия, защита от циклов и внешних сущностей XML. Файлы `.gz` не поддерживаются.

## CrawlerStats и отчёты

`CrawlerStats.from_crawler(crawler, elapsed)`; свойства `total_pages`, `successful`, `failed`, `skipped`, `success_rate`, `avg_speed`; `top_domains(n)`; `summary() -> dict`. `render_html_report(summary, failed_urls, pages)` возвращает HTML-строку (внешних библиотек и скриптов нет, графики в SVG, весь текст экранируется).

## CrawlerConfig (`webcrawler.config`)

`CrawlerConfig.from_dict(data)`, `load_config(path)`, `config.to_dict()`, `config.override(**values)`, `config.validate()`. Ошибки: `ConfigError`. Все ключи: [CONFIG.md](CONFIG.md).

## AdvancedCrawler (`crawler.py`)

```python
AdvancedCrawler(config=None, configure_logging=True, progress_stream=None)
AdvancedCrawler.from_config(path_or_dict_or_config, **kwargs)
```

| Метод | Описание |
|---|---|
| `await crawl() -> dict` | обход по настройкам (стартовые адреса, sitemap, фильтры, хранилище, отчёты) |
| `get_stats() -> dict` | `total_pages`, `successful`, `failed`, `skipped`, `success_rate`, `elapsed_seconds`, `avg_speed`, `status_codes`, `top_domains`, `errors_by_type`, `rate`, `storage`, `retry` |
| `export_to_json(filename)` | статистика, конфигурация, ошибки в JSON |
| `export_to_html_report(filename)` | HTML-отчёт с таблицами и диаграммами |
| `await close()` | закрыть сессию, хранилище и логи |

## Логи и прогресс

`setup_logging(level="INFO", log_file=None, max_bytes=1_000_000, backup_count=3, json_format=False, console=True)` настраивает логгер `webcrawler`; `teardown_logging()` его отключает. `ProgressMonitor(max_pages, stream=None)` передаётся в `on_progress`; `render_progress(progress, max_pages)` возвращает строку вида `[#####-----] 50% 12/24 | 8.3 стр/сек | ...`.

## Тестовый сайт (`webcrawler.mocksite`)

`MockSite(latency=0.0)`: `async with MockSite() as site: site.url`. Страницы: `/`, `/page/1..30`, `/about`, `/private/*`, `/delay/{сек}`, `/status/{код}`, `/flaky/{ключ}`, `/broken`, `/plain`, `/binary`, `/big/{n}`, `/bulk/{n}`, `/robots.txt`, `/sitemap.xml`, `/sitemap2.xml`, `/sitemap_index.xml`. Поля `requests`, `agents`, `max_active` для проверок. Отдельный процесс: `python -m webcrawler.mocksite`.
