from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
from crawl4ai import AsyncWebCrawler, CrawlResult, CrawlerRunConfig, DefaultMarkdownGenerator, PruningContentFilter
from typing import List
import asyncio
import json
from crawl4ai.deep_crawling.filters import ContentRelevanceFilter, FilterChain
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy

'''
this script takes the sources found by sourceFinder.py and finds the people pages for each source
'''

async def getURLs():
    urls = []
    with open("sources.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    for d in data:
        print(d["url"])
        urls.append(d["url"])
    
    return urls

async def deepCrawl(urls: list[str]):
    scorer = KeywordRelevanceScorer(
        keywords=["team", "staff", "board of directors", "board", "members"],
        weight=0.3
    )

    relevance_filter = ContentRelevanceFilter(
        query="List of staff, members, board of directors, volunteers, etc.",
        threshold=0.2
    )

    config = CrawlerRunConfig(
        deep_crawl_strategy=BestFirstCrawlingStrategy(
            max_depth=2,
            include_external=False,
            max_pages=30,
            url_scorer=scorer,
            filter_chain=FilterChain([relevance_filter]),
        ),
        stream=True,
        verbose=True,
        scraping_strategy=LXMLWebScrapingStrategy()
    )

    results = []
    resultURLs = []

    async with AsyncWebCrawler() as crawler:
        async for result in await crawler.arun_many(
            urls=urls,
            config=config
        ):
            results.append(result)

            if result.success:
                print(result.url)
                print(f"score: {scorer.score(result.markdown.fit_markdown)}") 
                resultURLs.append(result.url)
            else:
                print(result.url)
                print("failed")
                print(result.url, "->", result.error_message)
                
    print("done")
    return resultURLs

async def main():
    urls = await getURLs()
    await deepCrawl(urls)

if __name__ == "__main__":
    asyncio.run(main())