import pytest
from bs4 import BeautifulSoup

from webcrawler import AsyncCrawler
from webcrawler.mocksite import BROKEN_HTML, HOME_HTML
from webcrawler.parser import HTMLParser, normalize_url

BASE = "http://example.test/"


@pytest.fixture
def parser():
    return HTMLParser()


async def test_parse_valid_html(parser):
    result = await parser.parse_html(HOME_HTML, BASE)
    assert result["url"] == BASE
    assert result["title"] == "Mock Home"
    assert result["errors"] == []
    assert "Текст главной страницы." in result["text"]
    assert "не должно попасть в текст" not in result["text"]
    assert result["metadata"]["description"] == "Тестовый сайт для краулера"
    assert result["metadata"]["keywords"] == ["crawler", "test", "asyncio"]
    assert result["metadata"]["lang"] == "ru"


async def test_links_are_absolute_unique_and_valid(parser):
    result = await parser.parse_html(HOME_HTML, BASE)
    assert result["links"] == [
        "http://example.test/page/1",
        "http://example.test/page/2",
        "http://example.test/about",
        "https://example.org/external",
        "http://example.test/private/secret",
    ]


async def test_same_domain_only_filters_external():
    parser = HTMLParser(same_domain_only=True)
    result = await parser.parse_html(HOME_HTML, BASE)
    assert "https://example.org/external" not in result["links"]
    assert "http://example.test/about" in result["links"]


async def test_images_headings_tables_lists(parser):
    result = await parser.parse_html(HOME_HTML, BASE)
    assert result["images"] == [
        {"src": "http://example.test/static/logo.png", "alt": "Логотип"},
        {"src": "http://example.test/img/photo.jpg", "alt": ""},
    ]
    assert result["headings"] == {"h1": ["Mock Home"], "h2": ["Разделы"],
                                  "h3": ["Подраздел"]}
    assert result["tables"] == [[["Имя", "Значение"], ["a", "1"], ["b", "2"]]]
    assert result["lists"] == [{"type": "ul", "items": ["Первый", "Второй"]},
                               {"type": "ol", "items": ["Раз", "Два"]}]


async def test_broken_html_gives_partial_result(parser):
    result = await parser.parse_html(BROKEN_HTML, BASE)
    assert result["errors"] == []
    assert "Заголовок без закрытия" in result["text"]
    assert "http://example.test/page/1" in result["links"]


async def test_unclosed_title_does_not_crash(parser):
    result = await parser.parse_html("<html><head><title>Без закрытия<body><p>Текст", BASE)
    assert isinstance(result["title"], str)
    assert result["errors"] == []


@pytest.mark.parametrize("bad", ["", "   ", "<<<>>>", "\x00\x01", "<html"])
async def test_garbage_does_not_crash(parser, bad):
    result = await parser.parse_html(bad, BASE)
    assert result["url"] == BASE
    assert result["links"] == []


async def test_non_string_input_is_reported_not_raised(parser):
    result = await parser.parse_html(None, BASE)
    assert result["errors"]
    assert result["text"] == "" and result["links"] == []


def test_relative_urls_are_converted():
    assert normalize_url("page", "http://a.test/dir/index.html") == "http://a.test/dir/page"
    assert normalize_url("../up", "http://a.test/dir/sub/") == "http://a.test/dir/up"
    assert normalize_url("/root", "http://a.test/dir/") == "http://a.test/root"
    assert normalize_url("//cdn.test/x", "https://a.test/") == "https://cdn.test/x"
    assert normalize_url("HTTP://A.TEST/p#frag", "http://x.test/") == "http://a.test/p"
    assert normalize_url("http://a.test", "http://x.test/") == "http://a.test/"


@pytest.mark.parametrize("href", [None, "", "  ", "#top", "mailto:a@b.c",
                                  "javascript:void(0)", "tel:+123", "ftp://a.test/f"])
def test_invalid_urls_are_rejected(href):
    assert normalize_url(href, BASE) is None


def test_base_tag_is_respected(parser):
    html = '<html><head><base href="http://other.test/dir/"></head><body><a href="x">x</a></body></html>'
    soup = BeautifulSoup(html, "lxml")
    assert parser.extract_links(soup, BASE) == ["http://other.test/dir/x"]


def test_extract_text_with_selector(parser):
    soup = BeautifulSoup(HOME_HTML, "lxml")
    assert parser.extract_text(soup, "h2") == "Разделы"
    assert parser.extract_text(soup, ".nothing") == ""
    with pytest.raises(ValueError):
        parser.extract_text(soup, "a[")


async def test_fetch_and_parse(site):
    async with AsyncCrawler() as crawler:
        result = await crawler.fetch_and_parse(site.url + "/")
    assert result["title"] == "Mock Home"
    assert f"{site.url}/page/1" in result["links"]
    assert f"{site.url}/page/2" in result["links"]
    assert {"url", "title", "text", "links", "metadata"} <= set(result)
