"""Разбор sitemap.xml (Day 7)."""
import logging
import re
from typing import Awaitable, Callable

from lxml import etree

from webcrawler.parser import normalize_url

logger = logging.getLogger("webcrawler")

Fetcher = Callable[[str], Awaitable[tuple[int, str]]]

_XML_DECLARATION = re.compile(r"^\s*<\?xml[^>]*\?>")


class SitemapParser:
    """Загружает sitemap.xml и вытаскивает из него адреса страниц.

    Понимает обычные sitemap (<urlset>) и индексные (<sitemapindex>, которые
    ссылаются на другие sitemap; они обходятся рекурсивно). Защита: повторно
    один и тот же sitemap не читается (циклы), глубина и число адресов ограничены,
    внешние сущности XML отключены. Файлы .xml.gz не поддерживаются.
    """

    def __init__(self, fetcher: Fetcher, max_depth: int = 3, max_urls: int = 50_000) -> None:
        self._fetcher = fetcher
        self.max_depth = max_depth
        self.max_urls = max_urls

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        urls: dict[str, None] = {}
        await self._walk(sitemap_url, 0, set(), urls)
        return list(urls)

    async def _walk(self, sitemap_url: str, depth: int, seen: set[str],
                    urls: dict[str, None]) -> None:
        if sitemap_url in seen or len(urls) >= self.max_urls:
            return
        seen.add(sitemap_url)
        try:
            status, text = await self._fetcher(sitemap_url)
        except Exception as error:
            logger.warning("sitemap недоступен url=%s %s: %s", sitemap_url,
                           type(error).__name__, error)
            return
        if status != 200:
            logger.warning("sitemap ответил %s url=%s", status, sitemap_url)
            return
        root = self._parse_xml(text, sitemap_url)
        if root is None:
            return
        kind = etree.QName(root).localname
        locations = [
            (child.text or "").strip()
            for element in root
            if isinstance(element.tag, str)
            for child in element
            if isinstance(child.tag, str) and etree.QName(child).localname == "loc"
        ]
        if kind == "sitemapindex":
            if depth >= self.max_depth:
                logger.warning("sitemap слишком глубоко вложен url=%s", sitemap_url)
                return
            for location in locations:
                child_url = normalize_url(location, sitemap_url)
                if child_url:
                    await self._walk(child_url, depth + 1, seen, urls)
        elif kind == "urlset":
            for location in locations:
                url = normalize_url(location, sitemap_url)
                if url and len(urls) < self.max_urls:
                    urls[url] = None
        else:
            logger.warning("неизвестный корневой тег sitemap: %s url=%s", kind, sitemap_url)

    @staticmethod
    def _parse_xml(text: str, sitemap_url: str):
        body = _XML_DECLARATION.sub("", text or "").strip()
        if not body:
            logger.warning("sitemap пустой url=%s", sitemap_url)
            return None
        parser = etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=False)
        try:
            return etree.fromstring(body.encode("utf-8"), parser=parser)
        except etree.XMLSyntaxError as error:
            logger.warning("sitemap не разобран url=%s: %s", sitemap_url, error)
            return None
