from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
from crawl4ai import AsyncWebCrawler, CrawlResult, CrawlerRunConfig, DefaultMarkdownGenerator, PruningContentFilter, SEOFilter
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
    with open("./sources.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    for d in data:
        print(d["url"])
        urls.append(d["url"])
    
    return urls

async def deepCrawl(urls: list[str]):
    scorer = KeywordRelevanceScorer(
        keywords=["about", "people", "team", "information"],
        weight=0.7
    )

    relevance_filter = ContentRelevanceFilter(
        query="about people information team board values mission",
        k1=1,
        threshold=1
    )

    config = CrawlerRunConfig(
        deep_crawl_strategy=BestFirstCrawlingStrategy(
            max_depth=2,
            include_external=False,
            max_pages=3,
            url_scorer=scorer,
            filter_chain=FilterChain([relevance_filter]),
        ),
        stream=False,  # Changed from True to False
        verbose=True,
        scraping_strategy=LXMLWebScrapingStrategy()
    )

    

    results = []
    resultURLs = []
    scored_results = []  # List to store tuples of (score, url)

    async with AsyncWebCrawler() as crawler:
        # Get results as a list instead of async generator
        for url in urls:
            crawl_results = await crawler.arun(
                url=url,
                config=config
            )
        
        # Now iterate over the list of results
            for result in crawl_results:
                results.append(result)

                if result.success:
                    # Get input score from metadata, default to 0 if not found
                    input_score = result.metadata.get("score")
                    print(f"Input score: {input_score}")
                    scored_results.append((input_score, result.url))
                else:
                    print(result.url)
                    print("failed") 
                    print(result.url, "->", result.error_message)
    
    # Sort results by input score in descending order
    scored_results.sort(key=lambda x: x[0], reverse=True)  # Higher scores first
    resultURLs = [url for score, url in scored_results if score>0]
    return resultURLs

async def main():
    urls = await getURLs()
    result_urls = await deepCrawl(urls)
    with open("people_pages.json", "w", encoding="utf-8") as f:
        json.dump(result_urls, f, indent=4)

if __name__ == "__main__":
    asyncio.run(main())