"""Парсинг HTML и извлечение данных (Day 2)."""
import asyncio
import logging
import re
from concurrent.futures import ProcessPoolExecutor
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, NavigableString

logger = logging.getLogger("webcrawler")

ALLOWED_SCHEMES = {"http", "https"}
SKIP_TEXT_PARENTS = {"script", "style", "noscript", "template", "head", "title"}


def normalize_url(href: str | None, base_url: str) -> str | None:
    """Делает ссылку абсолютной и «чистой» или возвращает None, если она не годится.

    Убирает якорь (#...), понижает регистр схемы и домена, пропускает только
    http и https (mailto:, javascript: и подобное отбрасываются).
    """
    if not href or not isinstance(href, str):
        return None
    href = href.strip()
    if not href or href.startswith("#"):
        return None
    try:
        parts = urlparse(urljoin(base_url, href))
    except ValueError:
        return None
    if parts.scheme.lower() not in ALLOWED_SCHEMES or not parts.netloc:
        return None
    return urlunparse(parts._replace(
        scheme=parts.scheme.lower(),
        netloc=parts.netloc.lower(),
        path=parts.path or "/",
        fragment="",
    ))


def make_soup(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        logger.warning("lxml не справился, использую html.parser")
        return BeautifulSoup(html, "html.parser")


PARSE_MODES = ("thread", "inline", "process")


def _parse_in_worker(same_domain_only: bool, html: str, url: str) -> dict:
    """Разбор в дочернем процессе (функция на уровне модуля, чтобы её можно было передать)."""
    return HTMLParser(same_domain_only, "inline")._parse(html, url)


class HTMLParser:
    """Достаёт из HTML ссылки, текст, метаданные, картинки, заголовки, таблицы, списки.

    parse_mode:
      "thread"  — разбор в потоке (по умолчанию): event loop не блокируется, но из-за GIL
                  потоки не ускоряют сам разбор;
      "inline"  — прямо в event loop: чуть быстрее на крошечных страницах, но на больших
                  loop встаёт на время разбора;
      "process" — в отдельных процессах: настоящая параллельность для тяжёлого разбора
                  (стоит дороже при старте, окупается на больших страницах).
    """

    def __init__(self, same_domain_only: bool = False, parse_mode: str = "thread",
                 workers: int | None = None) -> None:
        if parse_mode not in PARSE_MODES:
            raise ValueError(f"parse_mode должен быть одним из {PARSE_MODES}")
        self.same_domain_only = same_domain_only
        self.parse_mode = parse_mode
        self.workers = workers
        self._pool: ProcessPoolExecutor | None = None

    async def parse_html(self, html: str, url: str) -> dict:
        """Разбор страницы в выбранном режиме (см. parse_mode)."""
        if self.parse_mode == "inline":
            return self._parse(html, url)
        if self.parse_mode == "process":
            try:
                if self._pool is None:
                    self._pool = ProcessPoolExecutor(max_workers=self.workers)
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(
                    self._pool, _parse_in_worker, self.same_domain_only, html, url)
            except Exception as error:
                logger.warning("разбор в процессе не удался (%s: %s), разбираю в потоке",
                               type(error).__name__, error)
        return await asyncio.to_thread(self._parse, html, url)

    def close(self) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=False, cancel_futures=True)
            self._pool = None

    def _parse(self, html: str, url: str) -> dict:
        errors: list[str] = []
        result = {
            "url": url, "title": "", "text": "", "links": [], "metadata": {},
            "images": [], "headings": {}, "tables": [], "lists": [], "errors": errors,
        }
        if not isinstance(html, str):
            errors.append(f"html должен быть строкой, получено {type(html).__name__}")
            logger.warning("parse url=%s: %s", url, errors[-1])
            return result
        try:
            soup = make_soup(html)
        except Exception as error:
            errors.append(f"soup: {error}")
            logger.warning("parse url=%s: не удалось разобрать HTML: %s", url, error)
            return result
        extractors = (
            ("metadata", lambda: self.extract_metadata(soup)),
            ("links", lambda: self.extract_links(soup, url)),
            ("text", lambda: self.extract_text(soup)),
            ("images", lambda: self.extract_images(soup, url)),
            ("headings", lambda: self.extract_headings(soup)),
            ("tables", lambda: self.extract_tables(soup)),
            ("lists", lambda: self.extract_lists(soup)),
        )
        for key, extract in extractors:
            try:
                result[key] = extract()
            except Exception as error:
                errors.append(f"{key}: {error}")
                logger.warning("parse url=%s: ошибка в %s: %s", url, key, error)
        result["title"] = result["metadata"].get("title", "")
        return result

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        base_tag = soup.find("base", href=True)
        if base_tag:
            base_url = urljoin(base_url, base_tag["href"])
        base_host = urlparse(base_url).netloc.lower()
        links: dict[str, None] = {}
        for anchor in soup.find_all("a", href=True):
            link = normalize_url(anchor["href"], base_url)
            if link is None:
                continue
            if self.same_domain_only and urlparse(link).netloc != base_host:
                continue
            links[link] = None
        return list(links)

    def extract_text(self, soup: BeautifulSoup, selector: str | None = None) -> str:
        if selector:
            try:
                nodes = soup.select(selector)
            except Exception as error:
                raise ValueError(f"Неверный CSS-селектор {selector!r}: {error}") from error
        else:
            nodes = [soup.body or soup]
        pieces: list[str] = []
        for node in nodes:
            for string in node.find_all(string=True):
                if type(string) is not NavigableString:
                    continue
                if string.parent is not None and string.parent.name in SKIP_TEXT_PARENTS:
                    continue
                pieces.append(string)
        return re.sub(r"\s+", " ", " ".join(pieces)).strip()

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        metadata: dict = {}
        title_tag = soup.find("title")
        metadata["title"] = title_tag.get_text(" ", strip=True) if title_tag else ""
        description = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
        metadata["description"] = (description.get("content", "").strip()
                                   if description else "")
        keywords = soup.find("meta", attrs={"name": re.compile("^keywords$", re.I)})
        raw_keywords = keywords.get("content", "") if keywords else ""
        metadata["keywords"] = [k.strip() for k in raw_keywords.split(",") if k.strip()]
        canonical = soup.find("link", rel="canonical", href=True)
        metadata["canonical"] = canonical["href"].strip() if canonical else ""
        html_tag = soup.find("html")
        metadata["lang"] = html_tag.get("lang", "") if html_tag else ""
        return metadata

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        images: dict[str, dict] = {}
        for img in soup.find_all("img"):
            src = normalize_url(img.get("src"), base_url)
            if src is None:
                continue
            images.setdefault(src, {"src": src, "alt": (img.get("alt") or "").strip()})
        return list(images.values())

    def extract_headings(self, soup: BeautifulSoup) -> dict[str, list[str]]:
        return {
            tag: [h.get_text(" ", strip=True) for h in soup.find_all(tag)]
            for tag in ("h1", "h2", "h3")
        }

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        tables = []
        for table in soup.find_all("table"):
            rows = []
            for tr in table.find_all("tr"):
                cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
                if cells:
                    rows.append(cells)
            tables.append(rows)
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[dict]:
        return [
            {"type": lst.name,
             "items": [li.get_text(" ", strip=True)
                       for li in lst.find_all("li", recursive=False)]}
            for lst in soup.find_all(["ul", "ol"])
        ]
