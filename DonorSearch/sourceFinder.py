from langchain_perplexity import ChatPerplexity
from langchain_core.prompts import ChatPromptTemplate
import os
from dotenv import load_dotenv
import asyncio
import json

'''
this script uses the Perplexity API to find sources for a given set of keywords
'''

load_dotenv()

# model = init_chat_model(model="gpt-4o-mini", api_key=os.getenv("OPENAI_API_KEY"))
chat = ChatPerplexity(temperature=0, pplx_api_key=os.getenv("PPLX_API_KEY"), model="sonar")

prompt = ChatPromptTemplate.from_messages([("human",
    """
    You are helping identify online sources that can be used to find the names of potential donors, supporters, or influential individuals associated with a specific cause or community in the United States.

    Given the following keywords:

    {keywords}

    Return a list of EXACTLY 10 U.S.-based organizations that are relevant to the given keywords and whose websites likely include directories of board members, executive teams, advisory councils, or staff leadership.

    You must return only the **homepage URLs** of these organizations. Do not return links to subpages or specific sections of the site. The goal is to collect the homepages of organizations that are promising starting points for researching individual names.

    The keywords may be diverse and not strongly related (e.g., "abortion", "Sikh"). Prioritize organizations that align with **multiple** keywords, and are located in regions specified (if the keywords include any). Then include those that match **niche or identity-based keywords** (e.g., "Latino", "Veteran"). Finally, include any organization that aligns with at least one keyword.

    Exclude:
    - PDFs
    - News articles
    - Login-only portals
    - Dead or broken links

    Format the output as a JSON list of objects. Each object must include:
    - "label": the name of the organization (no summaries or extra description)
    - "url": the homepage URL of the organization

    Example format:
    [
        {{
            "label": "Sikh Coalition",
            "url": "https://www.sikhcoalition.org"
        }}
    ]

    DO NOT include any explanation or extra text other than the label and URL, or any other text outside the JSON.

    """
    )])

chain = prompt | chat

async def main():
    keywords = input("Enter keywords separated by commas: ")
    ai_output = chain.invoke({"keywords": keywords}).content
    print(ai_output)

    # add to json file called sources.json
    with open("sources.json", "a", encoding="utf-8") as f:
        f.write(ai_output)

if __name__ == "__main__":
    asyncio.run(main())