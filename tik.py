from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import psycopg2
from sqlalchemy import create_engine


import emoji
import asyncio
import random
import logging
import re


import time
import json


from dotenv import load_dotenv
import os
import pandas as pd

# ============================================ Config ===========================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

def remove_emojis(text: str) -> str:
    return emoji.replace_emoji(text, replace="")

def get_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Missing environment variable: {name}")
    return value



def connect_to_database():
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_NAME")
    user = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASS")
    port = os.getenv("DB_PORT")
    if not all([host, database, user, password, port]):
        logging.error("Missing required environment variables")
        return None
    try:
  
        engine = psycopg2.connect(
            host=host,
            dbname=database,
            user=user,
            password=password,
            port=port,
            sslmode="require"
            )
        logging.info("Connection to PostgreSQL database successful")
        return engine

    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return None

# ============ User Agents ============
user_agents = [
    # Chrome - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.182 Safari/537.36",
    
    # Chrome - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.182 Safari/537.36",
    
    # Chrome - Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.113 Safari/537.36",
    
    # Edge - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.182 Safari/537.36 Edg/126.0.2592.87",
    
    # Edge - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.113 Safari/537.36 Edg/125.0.2535.67",
    
    # Firefox - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    
    # Firefox - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.0; rv:127.0) Gecko/20100101 Firefox/127.0",
    
    # Safari - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    
    # Opera - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.113 Safari/537.36 OPR/110.0.5130.80",
    
    # Chromium - Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/124.0.6367.91 Chrome/124.0.6367.91 Safari/537.36"
]


# ================== Helper ==============================
def extract_number(text):
    text = text.lower().replace(',', '').strip()
    match = re.findall(r"([\d\.]+)([kmb]?)", text)
    if not match:
        return 0
    val, suf = match[0]
    val = float(val)
    return int(val * {'': 1, 'k': 1_000, 'm': 1_000_000, 'b': 1_000_000_000}[suf])

# ======================== Scraper ===============================
async def get_tiktok_profile(username):
    logging.info(f"Fetching TikTok profile for @{username} ...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True,
                                          args=[
                                              '--disable-gpu',
                                              '--disable-dev-shm-usage',
                                              '--disable-setuid-sandbox',
                                              '--no-sandbox'
                                              
                                          ])
        context = await browser.new_context(
            user_agent=random.choice(user_agents),
            viewport={"width": random.randint(1280, 1920), "height": random.randint(720, 1080)},
            locale="en-US",)
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        page = await context.new_page()

        try:
            url = f"https://www.tiktok.com/@{username}"
            await page.goto(url, timeout=60000)

            for attempt in range(2):
                try:
                    await page.wait_for_selector('a[href*="/video/"]', timeout=30000)
                    break
                except:
                    if attempt == 0:
                        logging.info("Retrying once...")
                        await asyncio.sleep(10)
                    else:
                        logging.warning(f"No videos found for @{username}")


            try:
                await page.locator("button:has-text('Allow all')").click(timeout=5000)
                logging.info("Clicked 'Allow all' cookie popup.")
            except:
                logging.info("No cookie popup found.")

            try:
                await page.wait_for_selector('[data-e2e="followers-count"]', timeout=20000)
            except Exception as e:
                logging.warning(f"Followers count selector not found for @{username}: {e}")
                followers = 0

            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(3)

            html = await page.content()
            # with open(f"tiktok_{username}.html", "w", encoding="utf-8") as f:
            #     f.write(html)
            soup = BeautifulSoup(html, "html.parser")

            followers_tag = soup.select_one('span[data-e2e*="followers"]') or soup.find("span", string=re.compile("Followers"))

            followers = extract_number(followers_tag.text) if followers_tag else 0

            likes_tag = soup.find("strong", {"data-e2e": "likes-count"})
            total_likes = extract_number(likes_tag.text) if likes_tag else 0

            bio_tag = soup.find("h2", {"data-e2e": "user-bio"})
            bio = bio_tag.text.strip() if bio_tag else ""

            videos_data = []
            for block in soup.find_all("div", {"data-e2e": "user-post-item"})[:10]:
                a_tag = block.find("a", href=True)
                video_url = a_tag["href"] if a_tag else None
                view_tag = block.find("strong", {"data-e2e": "video-views"})
                views = extract_number(view_tag.text) if view_tag else 0

                if video_url and f"/@{username}/video" in video_url:
                    videos_data.append({
                        "username": username,
                        "profile_url": f"https://www.tiktok.com/@{username}",
                        "followers": followers,
                        "total_likes": total_likes,
                        "bio": bio,
                        "video_id": re.search(r"/video/(\d+)", video_url).group(1) if re.search(r"/video/(\d+)", video_url) else None, 
                        "video_url": video_url,
                        "video_views": views
                    })
            await asyncio.sleep(random.randint(5, 10))

            await browser.close()

            if videos_data:
                logging.info(f"Found {len(videos_data)} videos for @{username}")
                return videos_data
            else:
                logging.warning(f"No videos found or profile inaccessible for @{username}")


        except Exception as e:
            logging.error(f"Error fetching @{username}: {e}")
            return []

def process_load(username):
    data = asyncio.run(get_tiktok_profile(username))
    if not data:
        logging.warning(f"No videos found or profile inaccessible for @{username}")
        return

    df = pd.DataFrame(data)
    


    if df.empty:
        logging.warning(f"No valid data for @{username}, skipping.")
        return
   
    #==================== Type Cast and transformation ==============================
    df = df.astype({
    "followers": int,
    "total_likes": int,
    "video_views": int
        })
    df["username"] = df["username"].astype(str).fillna("no screen name").apply(lambda x: x.lower())
    df["profile_url"] = df["profile_url"].astype(str).fillna("")
    df['followers'] = (df['followers'].fillna(0).astype(int).mask(df["followers"].duplicated(),0))
    df["total_likes"] = df["total_likes"].astype(int).mask(df["total_likes"].duplicated(), 0)
    df["bio"] = df["bio"].apply(remove_emojis).astype(str).fillna(" ")
    df["video_url"] = df["video_url"].astype(str)
    df["video_views"] = df["video_views"].astype(int).fillna(0)
    df["video_id"] = df["video_id"].astype(str).fillna("none")
    df = df[df['followers'] >= 50000]

    logging.info(f"{df.dtypes}")

    records = [
            (
                str(row["username"]),
                str(row["profile_url"]),
                int(row["followers"]),
                int(row["total_likes"]),
                str(row["bio"]),
                str(row["video_id"]),
                str(row["video_url"]),
                int(row["video_views"])
            )
            for _, row in df.iterrows()
]


    engine = connect_to_database()
    if engine:
        try:
            with engine.cursor() as cursor:
                cursor.execute(
                    """CREATE TABLE IF NOT EXISTS influencer_tiktok(
                    username VARCHAR(100) NOT NULL,
                    profile_url TEXT,
                    followers INT,
                    total_likes INT,
                    bio TEXT,
                    video_id VARCHAR(100) NOT NULL,
                    video_url TEXT,
                    video_views INT,
                    PRIMARY KEY (username, video_id)
                    );
                    """
                )
                engine.commit()
                
                #=== Upsert Logic=======
                upsert_stmt = """
                    INSERT INTO influencer_tiktok (username, profile_url, followers, total_likes, bio, video_id, video_url, video_views)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (username, video_id) DO UPDATE SET
                        profile_url = EXCLUDED.profile_url,
                        followers = EXCLUDED.followers,
                        total_likes = EXCLUDED.total_likes,
                        bio = EXCLUDED.bio,
                        video_url = EXCLUDED.video_url,
                        video_views = EXCLUDED.video_views;
                """
                cursor.executemany(upsert_stmt, records)

                engine.commit()
                logging.info(f"Upserted {len(records)} rows to influencer_tiktok.")
        except psycopg2.Error as e:
            engine.rollback()
            logging.error(f"Database error: {e.pgerror or e}")
        finally:
            cursor.close()
            engine.close()


  
# ============ Test Run ============
if __name__ == "__main__":
    
    try:
        # Create SQLAlchemy engine from environment variables
        engine = create_engine(
            f"postgresql+psycopg2://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode=require"
        )

        query = "SELECT tiktok_username FROM username_search WHERE tiktok_username IS NOT NULL;"
        names = pd.read_sql(query, engine)

        usernames = (
            names["tiktok"].astype(str).str.strip().str.lower().dropna().unique().tolist())

        logging.info(f"Loaded {len(usernames)} usernames from database.")
        for users in usernames:
            process_load(users)
            time.sleep(random.randint(5, 10))
    except Exception as e:
        logging.info(f"error reading username: {e}")
        
