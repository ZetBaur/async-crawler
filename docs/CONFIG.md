# Руководство по конфигурации

Настройки можно задать в файле YAML (`.yaml`, `.yml`) или JSON (`.json`) и передать так:

```bash
python crawler.py --config config.yaml
```

или в коде: `AdvancedCrawler.from_config("config.yaml")`.

Все ключи необязательны. Неизвестный ключ, неверный тип или недопустимое значение даёт понятную ошибку со списком всех проблем сразу (код выхода `2`). Рабочий пример со всеми настройками: [`config.example.yaml`](../config.example.yaml).

## Что обходить

| Ключ | По умолчанию | Описание |
|---|---|---|
| `start_urls` | `[]` | стартовые адреса (строка или список) |
| `sitemap_urls` | `[]` | адреса `sitemap.xml` (обычных и индексных) как источник страниц |
| `sitemap_from_robots` | `false` | искать `Sitemap:` в `robots.txt` стартовых сайтов |
| `max_pages` | `100` | максимум страниц за запуск |
| `max_depth` | `3` | глубина обхода (`0` = только стартовые адреса) |
| `same_domain_only` | `true` | ходить только по ссылкам того же домена |
| `include_patterns` | `[]` | если задано, обходятся только адреса, подходящие под одно из регулярных выражений |
| `exclude_patterns` | `[]` | адреса, подходящие под регулярку, пропускаются |

Нужен хотя бы один адрес в `start_urls` или `sitemap_urls`. Стартовые адреса обходятся всегда, фильтры применяются к найденным ссылкам и к адресам из sitemap.

## Скорость и вежливость

| Ключ | По умолчанию | Описание |
|---|---|---|
| `max_concurrent` | `10` | одновременных запросов всего |
| `max_per_domain` | `null` | одновременных запросов на один домен (`null` = как `max_concurrent`) |
| `requests_per_second` | `null` | лимит запросов в секунду на домен (`null` = без лимита) |
| `min_delay` | `0.0` | минимальная пауза между запросами, сек |
| `jitter` | `0.0` | случайная добавка к паузе, сек |
| `respect_robots` | `true` | читать и соблюдать `robots.txt`, учитывать `Crawl-delay` |
| `user_agent` | `AdvancedCrawler/1.0` | заголовок `User-Agent` |
| `user_agents` | `[]` | список для ротации (пусто = только `user_agent`) |
| `connect_timeout`, `read_timeout`, `total_timeout` | `5`, `15`, `60` | таймауты, сек (при повторах растут) |

Пауза между запросами к домену = максимум из `1 / requests_per_second`, `min_delay` и `Crawl-delay` из `robots.txt`, плюс случайная добавка `jitter`. После ошибок сервера (5xx, 429, таймаут, обрыв) пауза удваивается с каждой ошибкой подряд и сбрасывается после успеха.

## Повторы и защита

```yaml
retry:
  enabled: true
  max_retries: 3        # повторов после первой попытки
  backoff_factor: 2.0   # пауза перед n-м повтором: base_delay * backoff_factor ** n
  base_delay: 0.5
  max_delay: 30.0

circuit_breaker:
  enabled: false
  failure_threshold: 5  # сбоев за окно, после которых домен блокируется
  window: 30.0          # длина окна, сек
  recovery_time: 10.0   # через сколько сек пробуем снова
```

Повторяются только временные ошибки (таймауты, 5xx, 429, сетевые). Постоянные (404, 403, 401) не повторяются. На `429` ждём дольше и уважаем заголовок `Retry-After`.

## Хранение

```yaml
storage:
  type: jsonl          # none, json, jsonl, csv или sqlite
  path: output/results.jsonl
  buffer_size: 20      # записей в буфере до сброса (json, jsonl, csv)
  batch_size: 20       # записей в одной вставке (sqlite)
```

| Тип | Формат |
|---|---|
| `jsonl` | одна запись JSON на строку; удобно для больших объёмов |
| `json` | один массив JSON с отступами (файл валиден только после закрытия) |
| `csv` | заголовки из первой записи; списки и словари как JSON-строки |
| `sqlite` | таблица `pages`, индексы по `title` и `crawled_at`; повтор того же `url` заменяет запись |

Формат записи: `url`, `title`, `text`, `links`, `metadata`, `crawled_at`, `status_code`, `content_type`.

## Память и разбор HTML

| Ключ | По умолчанию | Описание |
|---|---|---|
| `keep_results` | `true` | `false`: не держать страницы в памяти (в результатах остаётся краткая сводка). Требует `storage`. Нужен для больших обходов |
| `parse_mode` | `thread` | `thread`, `inline` или `process`: где разбирать HTML |
| `parse_workers` | `null` | число процессов для `parse_mode: process` (`null` = по числу ядер) |

Выбор `parse_mode` по измерениям — в [BENCHMARKS.md](BENCHMARKS.md): для больших страниц лучше `process`, для маленьких разницы почти нет.

## Логи, прогресс, отчёты

```yaml
progress: true         # полоса прогресса в консоли

logging:
  level: INFO          # DEBUG, INFO, WARNING, ERROR
  file: output/crawler.log
  max_bytes: 1000000   # размер файла до ротации
  backup_count: 3      # сколько старых файлов хранить
  json: false          # true: одна строка JSON на сообщение

output:
  stats_json: output/stats.json
  html_report: output/report.html
  error_report: output/errors.json
```

Файлы `output.*` создаются автоматически в конце обхода. Те же отчёты можно сделать в коде: `export_to_json()`, `export_to_html_report()`.

## Командная строка и конфиг

Параметры командной строки перекрывают конфиг. Например, `--max-pages 10` заменит `max_pages` из файла, а `--output x.csv` заменит весь блок `storage`. Булевы параметры имеют пары: `--respect-robots` / `--no-respect-robots`, `--same-domain` / `--no-same-domain`.
