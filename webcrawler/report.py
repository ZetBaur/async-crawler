"""HTML-отчёт о работе краулера (Day 7). Без внешних библиотек: графики рисуются в SVG."""
from datetime import datetime
from html import escape

CSS = """
:root { --bg:#fff; --fg:#1b1f24; --muted:#59636e; --card:#f6f8fa; --line:#d0d7de;
        --bar:#2f6feb; --bad:#cf222e; --ok:#1a7f37; }
@media (prefers-color-scheme: dark) {
  :root { --bg:#0d1117; --fg:#e6edf3; --muted:#8b949e; --card:#161b22; --line:#30363d;
          --bar:#4493f8; --bad:#f85149; --ok:#3fb950; }
}
body { font:15px/1.5 system-ui,sans-serif; background:var(--bg); color:var(--fg);
       max-width:960px; margin:0 auto; padding:24px 16px; }
h1 { margin:0 0 4px; } h2 { margin:32px 0 12px; font-size:18px; }
.muted { color:var(--muted); }
.cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:12px; }
.card { background:var(--card); border:1px solid var(--line); border-radius:8px; padding:12px; }
.card b { display:block; font-size:24px; }
table { width:100%; border-collapse:collapse; }
th, td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--line);
         word-break:break-all; font-size:14px; }
.ok { color:var(--ok); } .bad { color:var(--bad); }
svg text { fill:var(--fg); font:12px system-ui,sans-serif; }
svg .bar { fill:var(--bar); }
"""


def bar_chart_svg(items: list[tuple[str, float]], width: int = 640,
                  bar_height: int = 22, label_width: int = 170) -> str:
    """Горизонтальная столбчатая диаграмма в виде SVG-строки."""
    if not items:
        return '<p class="muted">Нет данных.</p>'
    maximum = max(value for _, value in items) or 1
    gap = 6
    height = len(items) * (bar_height + gap) + gap
    chart_width = width - label_width - 60
    rows = []
    for index, (label, value) in enumerate(items):
        y = gap + index * (bar_height + gap)
        length = max(2.0, chart_width * value / maximum)
        text = escape(label if len(label) <= 26 else label[:25] + "…")
        rows.append(
            f'<text x="0" y="{y + bar_height - 7}">{text}</text>'
            f'<rect class="bar" x="{label_width}" y="{y}" width="{length:.1f}" '
            f'height="{bar_height}" rx="3"/>'
            f'<text x="{label_width + length + 6:.1f}" y="{y + bar_height - 7}">'
            f"{escape(f'{value:g}')}</text>")
    return (f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" '
            f'xmlns="http://www.w3.org/2000/svg">{"".join(rows)}</svg>')


def _table(headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        return '<p class="muted">Нет данных.</p>'
    head = "".join(f"<th>{escape(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{escape(str(c))}</td>" for c in row) + "</tr>"
                   for row in rows)
    return f"<table><tr>{head}</tr>{body}</table>"


def render_html_report(summary: dict, failed_urls: dict[str, str],
                       pages: list[dict], title: str = "Отчёт краулера",
                       generated_at: datetime | None = None) -> str:
    """Собирает HTML-страницу из сводки, списка ошибок и списка страниц."""
    generated_at = generated_at or datetime.now()
    cards = [
        ("Страниц обработано", summary["total_pages"], ""),
        ("Успешно", summary["successful"], "ok"),
        ("Ошибок", summary["failed"], "bad" if summary["failed"] else ""),
        ("Пропущено (robots.txt)", summary["skipped"], ""),
        ("Доля успеха", f"{summary['success_rate']:.0%}", ""),
        ("Скорость, стр/сек", summary["avg_speed"], ""),
        ("Время работы, сек", summary["elapsed_seconds"], ""),
    ]
    cards_html = "".join(
        f'<div class="card"><span class="muted">{escape(name)}</span>'
        f'<b class="{css}">{escape(str(value))}</b></div>' for name, value, css in cards)
    status_items = [(code, count) for code, count in summary["status_codes"].items()]
    domain_items = [(d["domain"], d["pages"]) for d in summary["top_domains"]]
    error_items = list(summary["errors_by_type"].items())
    failed_rows = [[url, message] for url, message in failed_urls.items()]
    page_rows = [[p["url"], p.get("title", ""),
                  p.get("links_count", len(p.get("links", []))),
                  p.get("text_length", len(p.get("text", "")))] for p in pages[:25]]
    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)}</title><style>{CSS}</style></head><body>
<h1>{escape(title)}</h1>
<p class="muted">Сформирован {generated_at:%Y-%m-%d %H:%M:%S}</p>
<div class="cards">{cards_html}</div>
<h2>Статус-коды и ошибки</h2>{bar_chart_svg(status_items)}
<h2>Топ доменов по числу страниц</h2>{bar_chart_svg(domain_items)}
<h2>Ошибки по типам</h2>{bar_chart_svg(error_items)}
<h2>Страницы с ошибками</h2>{_table(["Адрес", "Ошибка"], failed_rows)}
<h2>Обработанные страницы (первые 25)</h2>
{_table(["Адрес", "Заголовок", "Ссылок", "Длина текста"], page_rows)}
</body></html>"""
