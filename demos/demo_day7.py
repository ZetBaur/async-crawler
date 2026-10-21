"""Демонстрация Day7: AdvancedCrawler из конфига, статистика, отчёты, sitemap, CLI."""
import _common  # noqa: F401

import argparse
import asyncio
import json

from crawler import AdvancedCrawler
from webcrawler.mocksite import MockSite

CONFIG_TEMPLATE = """\
# Конфигурация для демо (создана автоматически)
start_urls:
  - {site}/
sitemap_urls:
  - {site}/sitemap_index.xml
max_pages: 40
max_depth: 3
max_concurrent: 8
requests_per_second: 200
respect_robots: false
progress: true
retry:
  base_delay: 0.05
storage:
  type: sqlite
  path: {out}/crawler.db
logging:
  level: INFO
  file: {out}/crawler.log
  json: true
output:
  stats_json: {out}/stats.json
  html_report: {out}/report.html
  error_report: {out}/errors.json
"""


async def run(site_url: str) -> None:
    out = _common.OUTPUT_DIR / "day7"
    out.mkdir(parents=True, exist_ok=True)
    config_path = out / "config.yaml"
    config_path.write_text(CONFIG_TEMPLATE.format(site=site_url, out=out), encoding="utf-8")
    (out / "crawler.db").unlink(missing_ok=True)
    print(f"Конфигурация: {config_path}\n")

    crawler = AdvancedCrawler.from_config(config_path)
    await crawler.crawl()

    stats = crawler.get_stats()
    print(f"\nОбработано: {stats['total_pages']} страниц")
    print(f"Успешно:   {stats['successful']}")
    print(f"Ошибок:    {stats['failed']}")
    print(f"Скорость:  {stats['avg_speed']} стр/сек за {stats['elapsed_seconds']} сек")
    print("Статус-коды:", stats["status_codes"])
    print("Топ доменов:", stats["top_domains"])

    json_path = crawler.export_to_json(out / "stats_manual.json")
    html_path = crawler.export_to_html_report(out / "report_manual.html")
    await crawler.close()

    print("\nФайлы:")
    for path in sorted(out.iterdir()):
        print(f"  {path.name:20} {path.stat().st_size:>8} байт")
    print("\nПервые строки JSON-лога:")
    for line in (out / "crawler.log").read_text(encoding="utf-8").splitlines()[:2]:
        print("  ", json.loads(line))
    print(f"\nОткрой отчёт в браузере: {html_path}")
    _ = json_path


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.parse_args()
    async with MockSite(latency=0.03) as site:
        await run(site.url)


if __name__ == "__main__":
    asyncio.run(main())
