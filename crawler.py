"""Командная строка краулера.

Примеры:
    python crawler.py --urls https://example.com --max-pages 100 --output results.json
    python crawler.py --config config.yaml --report report.html
"""
import argparse
import asyncio
import sys
from pathlib import Path

from webcrawler.advanced import AdvancedCrawler
from webcrawler.config import ConfigError, CrawlerConfig, load_config

__all__ = ["AdvancedCrawler", "build_parser", "amain", "main"]

STORAGE_BY_SUFFIX = {
    ".json": "json", ".jsonl": "jsonl", ".csv": "csv",
    ".db": "sqlite", ".sqlite": "sqlite", ".sqlite3": "sqlite",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crawler.py", description="Асинхронный веб-краулер",
        epilog="Приоритет настроек: командная строка > файл --config > значения по умолчанию.")
    parser.add_argument("--urls", nargs="+", metavar="URL", help="стартовые URL")
    parser.add_argument("--max-pages", type=int, help="максимум страниц")
    parser.add_argument("--max-depth", type=int, help="максимальная глубина обхода")
    parser.add_argument("--output", metavar="FILE",
                        help="файл результатов: .json, .jsonl, .csv, .db/.sqlite")
    parser.add_argument("--config", metavar="FILE", help="конфигурационный файл (.yaml/.json)")
    parser.add_argument("--respect-robots", action=argparse.BooleanOptionalAction,
                        default=None, help="соблюдать robots.txt (по умолчанию да)")
    parser.add_argument("--rate-limit", type=float, metavar="RPS",
                        help="лимит запросов в секунду на домен")
    parser.add_argument("--concurrency", type=int, help="число одновременных запросов")
    parser.add_argument("--sitemap", nargs="+", metavar="URL", help="адреса sitemap.xml")
    parser.add_argument("--same-domain", action=argparse.BooleanOptionalAction,
                        default=None, help="ходить только по ссылкам того же домена")
    parser.add_argument("--include", action="append", metavar="REGEX",
                        help="обходить только адреса, подходящие под шаблон (можно несколько раз)")
    parser.add_argument("--exclude", action="append", metavar="REGEX",
                        help="не обходить адреса, подходящие под шаблон (можно несколько раз)")
    parser.add_argument("--user-agent", help="значение заголовка User-Agent")
    parser.add_argument("--stats-json", metavar="FILE", help="сохранить статистику в JSON")
    parser.add_argument("--report", metavar="FILE", help="сохранить HTML-отчёт")
    parser.add_argument("--log-level", help="DEBUG, INFO, WARNING или ERROR")
    parser.add_argument("--log-file", metavar="FILE", help="писать логи в файл (с ротацией)")
    parser.add_argument("--no-progress", action="store_true", help="не показывать прогресс")
    return parser


def build_config(args: argparse.Namespace) -> CrawlerConfig:
    data = load_config(args.config).to_dict() if args.config else CrawlerConfig().to_dict()
    top = {
        "start_urls": args.urls, "max_pages": args.max_pages, "max_depth": args.max_depth,
        "respect_robots": args.respect_robots, "requests_per_second": args.rate_limit,
        "max_concurrent": args.concurrency, "sitemap_urls": args.sitemap,
        "same_domain_only": args.same_domain, "include_patterns": args.include,
        "exclude_patterns": args.exclude, "user_agent": args.user_agent,
    }
    data.update({key: value for key, value in top.items() if value is not None})
    if args.no_progress:
        data["progress"] = False
    if args.output:
        kind = STORAGE_BY_SUFFIX.get(Path(args.output).suffix.lower())
        if kind is None:
            raise ConfigError("--output: поддерживаются файлы "
                              + ", ".join(sorted(STORAGE_BY_SUFFIX)))
        data["storage"].update(type=kind, path=args.output)
    if args.stats_json:
        data["output"]["stats_json"] = args.stats_json
    if args.report:
        data["output"]["html_report"] = args.report
    if args.log_level:
        data["logging"]["level"] = args.log_level
    if args.log_file:
        data["logging"]["file"] = args.log_file
    return CrawlerConfig.from_dict(data)


async def amain(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = build_config(args)
        crawler = AdvancedCrawler(config)
    except ConfigError as error:
        print(f"Ошибка конфигурации: {error}", file=sys.stderr)
        return 2
    try:
        async with crawler:
            await crawler.crawl()
            stats = crawler.get_stats()
    except ConfigError as error:
        print(f"Ошибка конфигурации: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Остановлено пользователем", file=sys.stderr)
        return 130

    print(f"Обработано: {stats['total_pages']} страниц "
          f"(успешно {stats['successful']}, ошибок {stats['failed']}, "
          f"пропущено robots.txt {stats['skipped']})")
    print(f"Время: {stats['elapsed_seconds']} сек, скорость {stats['avg_speed']} стр/сек")
    if config.storage.type != "none":
        print(f"Результаты: {config.storage.path}")
    for label, path in (("Статистика", config.output.stats_json),
                        ("HTML-отчёт", config.output.html_report)):
        if path:
            print(f"{label}: {path}")
    return 0 if stats["successful"] > 0 else 1


def main(argv: list[str] | None = None) -> int:
    try:
        return asyncio.run(amain(argv))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
