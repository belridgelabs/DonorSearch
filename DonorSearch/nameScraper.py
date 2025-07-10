from crawl4ai import *
import asyncio
import json

'''
given a list of home pages for organizations, 
scrape the names of members, directors, etc.
'''

print("basic web crawler")

async def crawl_names():
    async with AsyncWebCrawler() as crawler:
        urls = [
            "https://sikhnationalbar.org/board-of-directors/",
            "https://www.sikhcoalition.org/about-us/"
        ]

        results: List[CrawlResult] = await crawler.arun_many(
            urls=urls,
            config = CrawlerRunConfig(
                markdown_generator=DefaultMarkdownGenerator(
                    content_source="cleaned_html",
                    content_filter=PruningContentFilter(),
                    options={"ignore_links": True}
                ),
            )
        )

        '''
        for result in results:
            print('success: ', result.success)
            if result.success:
                print(result.markdown.fit_markdown)
                print(len(result.markdown.fit_markdown))

            else:
                print('failed')
        '''

        data = [(result.url, result.markdown.fit_markdown) for result in results]

    return data

'''          
async def save_to_json(data: list[tuple[str, str]], filename="output.json"):
    out = [{"url": url, "markdown": markdown} for url, markdown in data]
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
'''

async def main():
    results = await crawl_names()
    # await save_to_json(results)

if __name__ == "__main__":
    asyncio.run(main())
