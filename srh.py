
from httpx import AsyncClient
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import psycopg2
from playwright._impl._api_structures import ProxySettings  

import json
import random
import asyncio
import re
import logging
import os

from dotenv import load_dotenv

load_dotenv()

# ============================== Config =====================================
logging.getLogger().setLevel(logging.INFO)
non_prof_path = {"p", "explore", "reel", "tv"}

def get_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Missing environment variable: {name}")
    return value

server1 = get_env_var("PROXY_SERVER")
username1 = get_env_var("PROXY_USERNAME")
password1 = get_env_var("PROXY_PASSWORD")



proxy_str1 = f"http://{username1}:{password1}@{server1}"



def get_proxy(username, password, server):
    proxy: ProxySettings | None = None
    if server and username and password:
        proxy = ProxySettings(
            server=server,
            username=username,
            password=password)
    return proxy

proxy = get_proxy(username1, password1, server1)

user_agents = [
    # Windows Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Mac Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Safari/605.1.15",
    # Linux Chrome
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Android Chrome
    "Mozilla/5.0 (Linux; Android 11; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.230 Mobile Safari/537.36",
    # iPhone Safari
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    # Windows Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
]

keywords = ["comedian", "influencer", "actor", "blogger", "artist", "creator"]

# keywords = ["comedian", "influencer", "actor", "blogger", "artist", "creator", "analyst", "fashion", "public figure",
#             "beauty", "fitness", "digital creator"]


# ======================================= Helpers ================================
def extract_follower(text: str) -> int:
    """Convert follower text like '2.5M+ followers' into an integer."""
    if not text:
        return 0
    text = text.strip().lower().replace(",", "").replace("+", "")
    match = re.search(r"([\d\.]+)\s*([kmb]?)", text)
    if not match:
        return 0
    num, suffix = match.groups()
    num = float(num)
    if suffix == "k":
        return int(num * 1_000)
    elif suffix == "m":
        return int(num * 1_000_000)
    elif suffix == "b":
        return int(num * 1_000_000_000)
    return int(num)


def is_profile_link(url: str) -> bool:
    if not url or "instagram.com" not in url:
        return False
    parts = url.split("/")
    if len(parts) < 4:
        return False
    path = parts[3]
    return bool(path) and path not in non_prof_path


async def usernames(keyword: str):
    usernames = []
    async with AsyncClient(proxy=proxy_str1, verify=False) as client:
        for pages in range(0, 40, 10):
            try:
                response = await client.get(
                    f"https://www.google.com/search?q=site:instagram.com+{keyword}+Nigeria&start={pages}",
                    headers={"User-Agent": random.choice(user_agents)},
                )
                await asyncio.sleep(random.uniform(8, 15))
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                results = soup.select("div.MjjYud")
                for res in results:
                    link_tag = res.select_one("a[href]")
                    link = link_tag["href"] if link_tag else None
                    if link and is_profile_link(link):
                       
                        username = link.split("/")[3]
                        follower_tag = res.select_one("div.byrV5b")
                        follower_text = follower_tag.get_text(strip=True) if follower_tag else "0"
                        followers = extract_follower(follower_text)

                        if followers >= 50_000 and username not in usernames:
                            usernames.append(username)
                            logging.info(f"Found {username} ({followers})")
            except Exception as e:
                logging.error(f"Error fetching page {pages//10 + 1}: {e}")
                continue
            await asyncio.sleep(random.randint(3, 6))
    return usernames




# ------------------- YOUTUBE -------------------

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)"
]

async def youtube_fallback(username: str):
    """
    Searches YouTube for the given username and returns the first @channel handle found
    """
    search_url = f"https://www.youtube.com/results?search_query={username}"

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=random.choice(user_agents),
                ignore_https_errors=True,
                viewport={"width": 1280, "height": 800}
            )
            page = await context.new_page()

            await page.goto(search_url, timeout=15000)
            await page.wait_for_timeout(random.randint(1500, 3000))

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # Find all channel links that contain /@
            channel_links = soup.select('a.channel-link[href^="/@"]')

            if not channel_links:
                # Backup selector (some YouTube layouts differ)
                channel_links = soup.select('a[href^="/@"]')

            if channel_links:
                href = channel_links[0].get("href")
                if href.startswith("/@"):
                    handle = href.split("/@")[-1]
                    return handle.strip()
            return None

    except Exception as e:
        print(f"youtube_fallback error for {username}: {e}")
        return None

    finally:
        try:
            await context.close()
            await browser.close()
        except Exception:
            pass
            
async def youtube_search(username: str):
    browser = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, proxy=proxy)
            context = await browser.new_context(
                user_agent=random.choice(user_agents),
                ignore_https_errors=True
            )
            page = await context.new_page()

            query = f"site:youtube.com/@{username}"
            search_url = f"https://www.google.com/search?q={query}"
            await page.goto(search_url, timeout=30000)
            await page.wait_for_timeout(random.randint(1200, 2500))

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            for a_tag in soup.select("a[href^='https://www.youtube.com/@']"):
                href = a_tag.get("href")
                if not href:
                    continue

                handle = href.split("@")[-1].split("/")[0].strip()

                # Exact match
                if handle.lower() == username.lower():
                    logging.info(f" YouTube exact match: {handle}")
                    return handle

                # Partial match
                if username.lower() in handle.lower():
                    logging.info(f" YouTube possible match: {handle}")
                    return handle

            # Fallback if Google search failed
            logging.warning(f" No YouTube match via Google for {username}. Trying fallback...")
            fb_yt = await youtube_fallback(username)
            if fb_yt:
                logging.info(f" Found via fallback: {fb_yt}")
                return fb_yt

            logging.warning(f" No match found for {username}")
            return None

    except Exception as e:
        logging.error(f" YouTube search error for {username}: {e}")
        # fallback if Google fails entirely
        fb_yt = await youtube_fallback(username)
        return fb_yt

    finally:
        if browser:
            await browser.close()

# ============================== TIKTOK ============================

def extract_username(link: str) -> str | None:
    """Extract TikTok username from profile link."""
    match = re.search(r"tiktok\.com/@([\w\._-]+)", link)
    return match.group(1) if match else None


async def tiktok_search(username):
    """Check if a TikTok profile exists via Google search."""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True,
                                             proxy=proxy)
            context = await browser.new_context(user_agent=random.choice(user_agents),
                                                ignore_https_errors=True)
            page = await context.new_page()

            query = f"site:tiktok.com/@{username}"
            search_url = f"https://www.google.com/search?q={query}"
            await page.goto(search_url, timeout=30000)
            await page.wait_for_timeout(1500)

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # Find first TikTok result
            for a_tag in soup.select("a[href^='https://www.tiktok.com/@']"):
                link = a_tag.get("href")
                handle = extract_username(link)
                if handle.lower() == username.lower():
                    logging.info(f" TikTok match found: {handle}")
                    return handle
            return None

    except Exception as e:
        logging.error(f"TikTok error for {username}: {e}")
        return None

    finally:
        if 'browser' in locals():
            await browser.close()

   

# ------------------- X / TWITTER -------------------

def extract_x_username(link: str) -> str | None:
    """Extract X username from profile link."""
    match = re.search(r"x\.com/([A-Za-z0-9_\.]+)$", link)
    return match.group(1) if match else None


async def x_search(username: str):
    """Scrape the handle (@username) from an X (Twitter) profile."""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True,
                                              proxy=proxy)
            context = await browser.new_context(
                user_agent=random.choice(user_agents),
                ignore_https_errors=True
            )
            page = await context.new_page()

            search_url = f"https://x.com/{username}"
            await page.goto(search_url, timeout=60000)
            await page.wait_for_timeout(random.randint(2500, 4000))

            # Dismiss or bypass the login popup if visible
            try:
                popup_selector = 'div[role="dialog"]'
                if await page.query_selector(popup_selector):
                    await page.keyboard.press("Escape")
                    await page.wait_for_timeout(1000)
            except Exception:
                pass

            # Ensure the @handle element is visible
            await page.wait_for_selector('span:has-text("@")', timeout=10000)

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # Find the @handle by class pattern
            handle_span = soup.find("span", string=re.compile(r"^@"))
            if handle_span:
                handle_text = handle_span.get_text(strip=True)
                logging.info(f"Found handle: {handle_text}")
                handle = handle_text.lstrip("@")
                if handle.lower() == username.lower():
                    return handle
            else:
                logging.warning(f" No handle found for {username}")
                return None

    except Exception as e:
        logging.error(f" X lookup error for {username}: {e}")
        return None

    finally:
        if "browser" in locals():
            await browser.close()
# ------------------- MAIN -------------------
# ------------------- MAIN -------------------
def connect_to_db():
    """Connect to PostgreSQL database."""
    host = get_env_var("DB_HOST")
    database = get_env_var("DB_NAME")
    user = get_env_var("DB_USERNAME")
    password = get_env_var("DB_PASS")
    port = get_env_var("DB_PORT")

    try:
        conn = psycopg2.connect(
            host=host,
            dbname=database,
            user=user,
            password=password,
            port=port,
            sslmode="require"
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return None


def create_table():
    """Create table if it does not exist."""
    conn = connect_to_db()
    if not conn:
        return

    cursor = conn.cursor()
    try:
        cursor.execute("""
            DROP TABLE IF EXISTS username_search;
            CREATE TABLE IF NOT EXISTS username_search(
                id SERIAL PRIMARY KEY,
                instagram_username VARCHAR(150),
                youtube_username VARCHAR(150),
                tiktok_username VARCHAR(150),
                x_username VARCHAR(150),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logging.info(" username_search table ready.")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
    finally:
        cursor.close()
        conn.close()


def insert_username(instagram, youtube, tiktok, x):
    """Insert username record into PostgreSQL."""
    conn = connect_to_db()
    if not conn:
        return

    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO username_search (instagram_username, youtube_username, tiktok_username, x_username)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (instagram, youtube, tiktok, x))
        logging.info(f" Inserted: {instagram}")
    except Exception as e:
        logging.error(f" Error inserting {instagram}: {e}")
    finally:
        cursor.close()
        conn.close()


async def process_username(username: str) -> dict:
    """Run platform searches for a single username in parallel."""
    results = {"instagram": username, "youtube": None, "tiktok": None, "x": None}

    yt_task = asyncio.create_task(youtube_search(username))
    tt_task = asyncio.create_task(tiktok_search(username))
    tw_task = asyncio.create_task(x_search(username))

    yt, tt, tw = await asyncio.gather(yt_task, tt_task, tw_task)

    results["youtube"] = yt
    results["tiktok"] = tt
    results["x"] = tw

    # Save each user result directly into DB
    insert_username(username, yt, tt, tw)

    return results


async def run_search(parallel_limit: int = 3):
    """Main search pipeline."""
    logging.info("Starting influencer discovery...")
    create_table()

    all_usernames = []
    for kw in keywords:
        users = await usernames(kw)
        all_usernames.extend(users)

    ig_usernames = list(set(all_usernames))
    logging.info(f"Total Instagram usernames found: {len(ig_usernames)}")

    sem = asyncio.Semaphore(parallel_limit)

    async def sem_task(username):
        async with sem:
            return await process_username(username)

    tasks = [asyncio.create_task(sem_task(u)) for u in ig_usernames]
    await asyncio.gather(*tasks)

    logging.info(" All usernames saved to database.")


if __name__ == "__main__":
    asyncio.run(run_search())
