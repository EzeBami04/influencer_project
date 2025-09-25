from __future__ import annotations
import asyncio


import requests
import logging
import os
import json
import urllib.parse

import asyncio
import logging
import random
import urllib.parse
from typing import Optional, List

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from langchain.agents import create_react_agent, AgentExecutor
from langchain_core.tools import Tool 
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_groq import ChatGroq

# ====================== Config =============================
load_dotenv()
logging.basicConfig(level=logging.INFO, format='[%(asctime)s], [%(levelname)s], [%(message)s]', datefmt='%Y-%m-%d %H:%M:%S')

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
CSE_API_KEY = os.getenv("cse_api")
CSE_ID = os.getenv("cse_id")

keywords = ["comedian", "artist", "actor", "music", "influencer", "content", "creator", "blogger",
            "fashion",  "style", "beauty", "makeup", "travel", "digital creator"]
MIN_IG_FOLLOWERS = 50_000
SLEEP_BETWEEN_REQUESTS = (1.5, 4.5)
INSTAGRAM_NON_PROFILE_PATHS = {"explore", "p", "reel", "stories", "tv", "accounts"}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/114.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/113.0.0.0 Safari/537.36",
]

# Proxy configuration
def get_proxy_info():
    username = "brd-customer-hl_8d90267d-zone-quantum"
    password = "v4vzyr0ixuob"
    server = "brd.superproxy.io:33335"
    return f"http://{username}:{password}@{server}"

proxy_url = get_proxy_info()


# ====================== Helpers =============================



# ============================ Config =============================
logging.basicConfig(level=logging.INFO)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) "
    "Gecko/20100101 Firefox/117.0",
]

INSTAGRAM_NON_PROFILE_PATHS = {"p", "explore", "reel", "tv"}


# ============================ Utils =============================
def extract_username_from_url(url: str) -> Optional[str]:
    """Extract username from Instagram profile URL."""
    try:
        path = urllib.parse.urlparse(url).path.strip("/")
        if path and path.split("/")[0] not in INSTAGRAM_NON_PROFILE_PATHS:
            return path.split("/")[0]
    except Exception:
        return None
    return None


def parse_count(s: str) -> int:
    """Convert Instagram counts like '2.4M' → 2400000."""
    s = s.strip().upper().replace(",", "")
    if s.endswith("K"):
        return int(float(s[:-1]) * 1_000)
    elif s.endswith("M"):
        return int(float(s[:-1]) * 1_000_000)
    elif s.endswith("B"):
        return int(float(s[:-1]) * 1_000_000_000)
    return int(float(s))


# ============================ Scraper =============================
# ============================ Scraper =============================
async def get_usernames(keyword):
    """Scrape Instagram profile URLs and extract usernames + followers."""
    results = []

    async with httpx.AsyncClient() as client:
        query = f"site:instagram.com {keyword} Nigeria"
        encoded_query = urllib.parse.quote_plus(query)
        base_url = f"https://www.google.com/search?q={encoded_query}"
        logging.info(f" Starting search for keyword: '{keyword}'")

        for page in range(0, 4):
            paginated_url = f"{base_url}&start={page * 10}"
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            logging.info(f"  Fetching page {page + 1} for keyword '{keyword}'")

            try:
                response = await client.get(
                    paginated_url,
                    headers=headers,
                    follow_redirects=True,
                    timeout=30
                )
                await asyncio.sleep(random.uniform(*SLEEP_BETWEEN_REQUESTS))
                response.raise_for_status()
            except Exception as e:
                logging.error(f" Request failed on page {page + 1} for '{keyword}': {type(e).__name__}: {e}")
                continue

            soup = BeautifulSoup(response.content, "lxml")
            for a in soup.select("a[href^='https://www.instagram.com/']"):
                href = a.get("href", "")
                username = extract_username_from_url(href)
                if not username:
                    continue
                if href not in [l["url"] for l in results]:
                    followers_text = None
                    followers_el = a.find_next("cite", class_="qLRx3b")
                    if followers_el and "followers" in followers_el.get_text():
                        followers_text = followers_el.get_text(strip=True)
                    results.append({
                        "username": username,
                        "url": href,
                        "followers": followers_text,
                        "keyword": keyword
                    })

            await asyncio.sleep(random.uniform(*SLEEP_BETWEEN_REQUESTS))

    logging.info(f"Found {len(results)} unique Instagram profiles for '{keyword}'.")
    return results

def instagram_tool(keyword: str):
    """Synchronous wrapper for async get_usernames"""
    return asyncio.run(get_usernames(keyword))

# # ====================== CSE Agent =============================
def query_cse(query: str, num_results: int = 1) -> List[str]:
    """Query Google Custom Search Engine."""
    url = "https://www.googleapis.com/customsearch/v1"
    params = {"q": query, "key": CSE_API_KEY, "cx": CSE_ID, "num": num_results} 
    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return [item["link"] for item in resp.json().get("items", [])]
    except Exception as e:
        logging.warning(f"CSE query failed: {e}")
        return []


# ====================== LLM Orchestration =============================
def llm_filter_and_format(agent_input: dict):
    """Filter influencers using Groq LLM."""
    llm = ChatGroq(model="llama-3.3-70b-versatile", temperature=0.3)
    parser = JsonOutputParser()
    
    custom_prompt = PromptTemplate(
    input_variables=["input", "tools", "tool_names", "keywords", "agent_scratchpad"],
    template="""
        You are a web research Assistant. You have access to these tools:
        {tools}

        Tool names you can call: {tool_names}

        Instructions:
        1. Search Instagram and return usernames whose account have more than 50k followers and
        the given {keywords} in their bio.
        2. Use the username from Instagram to make a search on TikTok, X, and YouTube.
        3. For X.com create a search for influencers based on the {keywords}.
        4. Do not return any explanation. Return only a JSON array of valid influencer objects.

        Format:
        [{{"instagram":"username"}}, {{"x": "username"}}]

        {agent_scratchpad}
        """
        )
   
    tools = [
        Tool(
        name = 'instagram crawler',
        func = instagram_tool,
        description = 'Extract usernames of influencers on instagram'
        ),
        Tool(
            name = 'social medial crawler',
            func = lambda user:  [query_cse(user) for kw in keywords for user in instagram_tool(kw)],
            description = " get usernames across social platforms"

        )]
    agent = create_react_agent(llm=llm,
                               tools=tools,
                               prompt=custom_prompt)
    executor = AgentExecutor.from_agent_and_tools(agent=agent,
                                                  tools=tools,
                                                  verbose=False,
                                                  handle_parsing_errors=True)
    try:
        response = executor.invoke({"input": "Find social medial influencers  in Nigeria and West Africa"})
        output_text = response.get("output", "")
        return parser.parse(output_text)

    except Exception as e:
        logging.error(f"Failed to parse LLM output: {e}")
        return []



# # ====================== Main =============================
async def main():
    logging.info("===== Starting Unified Influencer Discovery ========")

    results = {
        "instagram": [],
        "tiktok": [],
        "youtube": [],
        "x": []
    }

    # === Step 1: Scrape Instagram usernames ===
    for keyword in keywords:
        influencers = await get_usernames(keyword)
        logging.info(f"Processed {len(influencers)} influencers for keyword '{keyword}'")

        for inf in influencers:
            followers_num = None
            if inf.get("followers"):
                try:
                    followers_num = parse_count(
                        inf["followers"].replace("followers", "").strip("+ ").strip()
                    )
                except Exception:
                    followers_num = None

            results["instagram"].append({
                "username": inf["username"],
                "platform": "instagram",
                "url": inf["url"],
                "followers_raw": inf["followers"],
                "followers": followers_num,
                "keyword": keyword
            })

    # === Step 2: Pass results into the agent ===
    agent_input = {
        "input": f"Here are Instagram results: {json.dumps(results['instagram'][:400], indent=2)}. "
                 "Filter those with more than 50k followers and check if they exist on TikTok, X, and YouTube."
    }

    agent_output = llm_filter_and_format(agent_input)
    results["agent_output"] = agent_output

    # === Step 3: Save to JSON ===
    with open("influencers.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    logging.info(" Results saved to influencers.json")

if __name__ == "__main__":
    asyncio.run(main())