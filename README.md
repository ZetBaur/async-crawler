# async-crawler

Асинхронный веб-краулер на Python (`asyncio` + `aiohttp`): грузит сотни страниц параллельно, разбирает HTML, соблюдает правила вежливости (`robots.txt`, задержки), повторяет неудачные запросы, сохраняет данные в JSON, CSV и SQLite и строит отчёты.

Учебный проект: 7 этапов (Day 1 — Day 7), каждый усложняет предыдущий.

## Что умеет

| Этап | Возможность | Модуль |
|---|---|---|
| Day 1 | загрузка страниц через `aiohttp`, пул соединений, таймауты, логи | `webcrawler/client.py` |
| Day 2 | разбор HTML: ссылки, текст, метаданные, картинки, заголовки, таблицы, списки | `webcrawler/parser.py` |
| Day 3 | очередь с приоритетами, глубина обхода, фильтры, семафоры, прогресс | `crawl_queue.py`, `filters.py`, `limits.py` |
| Day 4 | rate limiting, `robots.txt`, `Crawl-delay`, jitter, backoff, ротация User-Agent | `limits.py`, `robots.py` |
| Day 5 | классификация ошибок, повторы с backoff, circuit breaker, отчёт об ошибках | `errors.py`, `retry.py` |
| Day 6 | сохранение в JSON / JSON Lines / CSV / SQLite, батчи, буферы | `storage.py` |
| Day 7 | sitemap.xml, статистика, HTML-отчёт, YAML/JSON-конфиг, CLI, логи с ротацией, полоса прогресса | `advanced.py`, `crawler.py` и другие |

## Установка

Нужен Python 3.10 или новее.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Быстрый старт

### Из командной строки

```bash
.venv/bin/python crawler.py --urls https://books.toscrape.com/ --max-pages 50 --max-depth 2 \
    --rate-limit 2 --output output/books.json --report output/report.html
```

`books.toscrape.com` — учебный сайт, на котором обход разрешён. Свои адреса обходите только с разрешения владельцев сайтов и с разумной скоростью.

Часто используемые параметры (полный список: `python crawler.py --help`):

| Параметр | Что делает |
|---|---|
| `--urls URL [URL ...]` | стартовые адреса |
| `--max-pages N`, `--max-depth N` | сколько страниц и насколько глубоко |
| `--output FILE` | результаты: `.json`, `.jsonl`, `.csv`, `.db` (SQLite) |
| `--config FILE` | настройки из YAML или JSON ([руководство](docs/CONFIG.md)) |
| `--respect-robots` / `--no-respect-robots` | соблюдать `robots.txt` (по умолчанию да) |
| `--rate-limit RPS` | не чаще N запросов в секунду на домен |
| `--sitemap URL [URL ...]` | брать адреса из sitemap.xml |
| `--report FILE`, `--stats-json FILE` | HTML-отчёт и статистика |

Приоритет настроек: командная строка, затем файл `--config`, затем значения по умолчанию. Коды выхода: `0` успех, `1` ни одной страницы не загружено, `2` ошибка настроек.

### Как библиотека

```python
import asyncio
from webcrawler import AsyncCrawler

async def main():
    async with AsyncCrawler(max_concurrent=5) as crawler:
        pages = await crawler.fetch_urls(["https://example.com", "https://example.org"])
        print(len(pages), "страниц, ошибки:", crawler.failed_urls)

asyncio.run(main())
```

Обход сайта с хранением результатов, повторами и вежливостью:

```python
from webcrawler import AsyncCrawler
from webcrawler.retry import RetryStrategy
from webcrawler.storage import SQLiteStorage

async def main():
    async with AsyncCrawler(
        max_concurrent=10, max_depth=2,
        requests_per_second=2.0, respect_robots=True, user_agent="MyBot/1.0",
        retry_strategy=RetryStrategy(max_retries=3),
        storage=SQLiteStorage("output/crawler.db"),
    ) as crawler:
        results = await crawler.crawl(["https://books.toscrape.com/"], max_pages=50)
        print(f"Обработано {len(results)} страниц")
```

Всё вместе через конфиг:

```python
from crawler import AdvancedCrawler

async def main():
    async with AdvancedCrawler.from_config("config.yaml") as crawler:
        await crawler.crawl()
        stats = crawler.get_stats()
        print(stats["total_pages"], stats["successful"], stats["failed"])
        crawler.export_to_html_report("report.html")
```

Пример конфигурации: [`config.example.yaml`](config.example.yaml).

## Демонстрации (без интернета)

В проекте есть локальный тестовый сайт (`webcrawler/mocksite.py`): быстрый, предсказуемый и никого не нагружает. Демо запускаются на нём:

```bash
.venv/bin/python demos/demo_day1.py   # параллельная загрузка и ошибки
.venv/bin/python demos/demo_day2.py   # разбор HTML
.venv/bin/python demos/demo_day3.py   # обход с очередью и глубиной
.venv/bin/python demos/demo_day4.py   # rate limiting и robots.txt
.venv/bin/python demos/demo_day5.py   # повторы, backoff, circuit breaker
.venv/bin/python demos/demo_day6.py   # JSON, CSV, SQLite
.venv/bin/python demos/demo_day7.py   # конфиг, статистика, отчёты, sitemap
```

Для дней 1–4 и 6 есть флаг `--external`: те же демо на настоящих сайтах (`example.com`, `httpbin.org`).

Тестовый сайт можно запустить отдельно и обойти его командной строкой:

```bash
.venv/bin/python -m webcrawler.mocksite --latency 0.05        # напечатает PORT=...
.venv/bin/python crawler.py --urls http://127.0.0.1:PORT/ --max-depth 3 --no-respect-robots
```

## Тесты

```bash
.venv/bin/python run_tests.py          # все тесты (226)
.venv/bin/python run_tests.py -q tests/test_day5.py
```

Тесты используют тот же локальный сайт и не ходят в интернет. Производительность: `python benchmarks/compare.py` (результаты и выводы в [docs/BENCHMARKS.md](docs/BENCHMARKS.md)).

## Структура

```
crawler.py             командная строка и AdvancedCrawler
config.example.yaml    пример конфигурации
webcrawler/
  client.py            AsyncCrawler: загрузка, обход, сохранение
  parser.py            HTMLParser, normalize_url
  crawl_queue.py       CrawlerQueue
  filters.py           UrlFilter
  limits.py            SemaphoreManager, RateLimiter
  robots.py            RobotsParser
  errors.py            типы ошибок и классификация
  retry.py             RetryStrategy, CircuitBreaker
  storage.py           JSONStorage, CSVStorage, SQLiteStorage, MultiStorage
  sitemap.py           SitemapParser
  stats.py, report.py  CrawlerStats, HTML-отчёт
  config.py            CrawlerConfig, load_config
  logging_setup.py     логи в файл с ротацией, JSON-формат
  monitor.py           полоса прогресса
  advanced.py          AdvancedCrawler
  mocksite.py          локальный тестовый сайт
demos/                 демонстрации по дням
benchmarks/            тест производительности
tests/                 тесты
docs/                  документация
```

## Документация

- [docs/API.md](docs/API.md): описание классов и методов
- [docs/CONFIG.md](docs/CONFIG.md): руководство по конфигурации
- [docs/algorithm.md](docs/algorithm.md): как устроен обход
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md): скорость, память, выводы

## Этика и вежливость

- По умолчанию краулер читает и соблюдает `robots.txt`, учитывает `Crawl-delay`, замедляется после ошибок сервера.
- Задавайте `requests_per_second` и честный `user_agent` с контактом.
- Не обходите сайты, где это запрещено правилами или законом, и не собирайте личные данные.
