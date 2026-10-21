import pytest_asyncio

from webcrawler.mocksite import MockSite


@pytest_asyncio.fixture
async def site():
    async with MockSite() as running_site:
        yield running_site
