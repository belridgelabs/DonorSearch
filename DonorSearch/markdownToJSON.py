from langchain.chat_models import init_chat_model
from langchain_core.prompts import PromptTemplate
import os
from dotenv import load_dotenv
from nameScraper import crawl_names
import asyncio
import json

'''
this file takes the output of nameScraper.py and converts it to a JSON file
'''

load_dotenv()

model = init_chat_model(model="gpt-4o-mini", api_key=os.getenv("OPENAI_API_KEY"))

prompt = PromptTemplate.from_template(
    """
    Extract names and positions from the following Markdown: {markdown}.
    Return only the JSON output with no code fences, no explanation, and no markdown formatting.
    Format:
    [
        {{
            "organization": "Example Org",
            "members": [
                {{ "name": "John Doe", "position": "CEO" }}
            ]
        }}
    ]
    """
    )

chain = prompt | model

async def main():
    data = await crawl_names()

    results = []

    for url, markdown in data:
        ai_output = chain.invoke({"markdown": markdown}).content
        print(ai_output)

        try:
            parsed = json.loads(ai_output)

            # Handle if single org returned as dict
            if isinstance(parsed, dict):
                parsed = [parsed]

            if isinstance(parsed, str):
                parsed = json.loads(parsed)

            for org in parsed:
                if isinstance(org, dict):
                    org["url"] = url
                    
                    # Check if organization already exists in results
                    existing_org = None
                    for existing in results:
                        if existing.get("organization") == org.get("organization"):
                            existing_org = existing
                            break
                    
                    if existing_org:
                        # Add members to existing organization
                        if "members" in org and isinstance(org["members"], list):
                            if "members" not in existing_org:
                                existing_org["members"] = []
                            existing_org["members"].extend(org["members"])
                    else:
                        # Add new organization
                        results.append(org)

        except Exception as e:
            print(f"[ERROR] Could not parse output from {url}: {e}")
            print("AI Output:", ai_output)

    # Save once at the end
    with open("output.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    asyncio.run(main())