import requests
import pandas as pd
import duckdb
import psycopg2

from sqlalchemy import create_engine
import pytz
import os
import time
import random
import logging
from datetime import datetime, timedelta
from functools import cache
from typing import List, Dict, Any, Optional


import emoji
from dotenv import load_dotenv

load_dotenv()

# --- config & logging ---
logging.getLogger().setLevel(level=logging.INFO)

GRAPH_API = "v23.0"
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
ACCESS_TOKEN = os.getenv("FB_TOKEN")
ig_id = os.getenv("IG_BUSINESS_ID")


def remove_emojis(text: str) -> str:
    """Helper to strip emojis."""
    return emoji.replace_emoji(text, replace="")


def connect_to_database():
    """Connect to PostgreSQL."""
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_NAME")
    user = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    port = os.getenv("DB_PORT")

    if not all([host, database, user, password, port]):
        logging.error("Missing required environment variables for DB connection.")
        return None

    try:
        con_str = f"host={host} dbname={database} user={user} password={password} port={port} sslmode=require"
        conn = psycopg2.connect(con_str)
        logging.info("Connected to PostgreSQL database successfully.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        return None
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

def request_get(
    url: str,
    params: dict,
    timeout: float = 30.0,
    max_retries: int = 2,
    backoff_factor: float = 1.5,) -> Optional[dict]:
    """Simple GET with retry/backoff using requests."""
    headers = {
        "User-Agent": f"{random.choice(user_agents)}"
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)

            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict) and "error" in data:
                    err = data["error"]
                    code = err.get("code")
                    msg = err.get("message", "").lower()
                    if code in (4, 17, 613) or "rate limit" in msg or "too many" in msg:
                        logging.warning(f"Rate limited: {err}")
                        return {"_status": "RATE_LIMIT", "error": err}
                    return {"_status": "ERROR", "error": err}
                return {"_status": "OK", "data": data}

            elif resp.status_code in (429, 500, 502, 503, 504):
                logging.warning(f"Retrying ({attempt}/{max_retries}) after status {resp.status_code}")
                time.sleep(random.uniform(600, 900))
                continue

            elif resp.status_code == 403:
                logging.warning("403 Forbidden sleeping for 30s to 1 minute")
                time.sleep(random.uniform(300, 600))
                return None

            elif resp.status_code == 443:
                logging.warning("443 Name not found")
                return None

        except requests.Timeout:
            logging.warning(f"Timeout on GET {url} attempt {attempt}")
            time.sleep(backoff_factor ** attempt)
            continue
        except requests.RequestException as e:
            logging.warning(f"Network error: {e} attempt {attempt}")
            time.sleep(backoff_factor ** attempt)
            continue

    logging.error(f"Failed after {max_retries} retries: {url}")
    return None


@cache
def get_instagram_business_id_cached(page_id: str) -> Optional[str]:
    """Retrieve Instagram business ID from Facebook Page ID."""
    if not page_id or not ACCESS_TOKEN:
        logging.error("Missing FB_PAGE_ID or ACCESS_TOKEN.")
        return None

    url = f"https://graph.facebook.com/{GRAPH_API}/{page_id}"
    params = {"fields": "instagram_business_account", "access_token": ACCESS_TOKEN}

    resp = request_get(url, params)
    if not resp or resp.get("_status") != "OK":
        logging.error("Failed to retrieve Instagram business account info.")
        return None

    data = resp["data"]
    ig = data.get("instagram_business_account", {})
    ig_id = ig.get("id")
    if ig_id:
        logging.info(f"Instagram Business ID found: {ig_id}")
        return ig_id
    logging.error(f"No IG business account linked to page {page_id}.")
    return None


def fetch_user_and_media(ig_business_id: str, username: str) -> Dict[str, Any]:
    """Fetch user data and media from Instagram Graph API."""
    url = f"https://graph.facebook.com/{GRAPH_API}/{ig_business_id}"
    fields = (
        f"business_discovery.username({username})"
        "{id,username,profile_picture_url,name,biography,followers_count,media_count,"
        "media.limit(10){id,caption,like_count,comments_count,timestamp,media_url,permalink}}"
    )
    params = {"fields": fields, "access_token": ACCESS_TOKEN}

    resp = request_get(url, params, timeout=60)
    if not resp:
        return {"status": "NETWORK_FAIL", "username": username}

    if resp.get("_status") == "RATE_LIMIT":
        return {"status": "RATE_LIMIT", "username": username, "error": resp.get("error")}

    if resp.get("_status") == "ERROR":
        return {"status": "API_ERROR", "username": username, "error": resp.get("error")}

    data = resp.get("data", {})
    if "business_discovery" not in data:
        return {"status": "NOT_BUSINESS", "username": username}

    return {"status": "OK", "username": username, "user": data["business_discovery"]}


def process_user(ig_business_id: str, username: str, cutoff_days: int = 90) -> List[Dict[str, Any]]:
    logging.info(f"Fetching @{username} ...")
    result = fetch_user_and_media(ig_business_id, username)

    if result["status"] != "OK":
        logging.info(f"Skipping @{username}: {result['status']}")
        return []

    user = result["user"]
    follower_count = user.get("followers_count", 0)
    media_items = user.get("media", {}).get("data", []) or []
    cutoff_date = datetime.now(pytz.UTC) - timedelta(days=cutoff_days)

    rows = []
    for m in media_items[:10]:  # Limit to 10 posts explicitly
        ts = m.get("timestamp")
        if not ts:
            continue
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt < cutoff_date:  # Skip posts older than cutoff
            continue
        rows.append({
            "user_id": user.get("id"),
            "username": user.get("username"),
            "profile_url": f"https://www.instagram.com/{user.get('username')}/",
            "name": user.get("name"),
            "profile_picture_url": user.get("profile_picture_url"),
            "bio": user.get("biography"),
            "follower_count": follower_count,
            "media_count": user.get("media_count"),
            "post_id": m.get("id"),
            "post_caption": m.get("caption"),
            "like_count": m.get("like_count"),
            "comments_count": m.get("comments_count"),
            "timestamp": m.get("timestamp"),
            "post_media_url": m.get("media_url"),
            "post_permalink": m.get("permalink"),
        })
        
    return rows

def run_pipeline(usernames: List[str]):
    """Main ETL pipeline using requests."""
    
    ig_business_id = get_instagram_business_id_cached(FB_PAGE_ID)
    if not ig_business_id:
        logging.error("Cannot proceed without Instagram Business ID.")
        return

    all_rows = []
    for u in usernames:
        all_rows.extend(process_user(ig_business_id, u))
        time.sleep(random.uniform(2, 4))

    if not all_rows:
        logging.info("No influencer rows to write.")
        return

    df = pd.DataFrame(all_rows)
    if df.empty:
        return

    # Clean data
    df['bio'] = df['bio'].astype(str).str.replace("/", "", regex=True)
    df['follower_count'] = df['follower_count'].fillna(0).astype(int)
    df['like_count'] = df['like_count'].fillna(0).astype(int)
    df['post_id'] = df['post_id'].astype(str)
    df['comments_count'] = df['comments_count'].fillna(0).astype(int)
    df['media_count'] = df['media_count'].fillna(0).astype(int)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['profile_picture_url'] = df['profile_picture_url'].astype(str).str.rstrip("/")
    df['post_media_url'] = df['post_media_url'].astype(str).str.rstrip("/")
    df['bio'] = df['bio'].astype(str).replace(r'@\w+', ' ', regex=True).apply(remove_emojis)
    df['post_caption'] = df['post_caption'].astype(str).replace(r'@\w+', '', regex=True)
    df['post_caption'] = df['post_caption'].astype(str).replace(r'http\S+|www\S+|https\S+', '', regex=True).apply(remove_emojis)
    df['name'] = df['name'].fillna('').astype(str).apply(remove_emojis)
    # DuckDB transform

    duck = duckdb.connect()
    duck.register("df", df)
    df_cleaned = duck.execute("""
        SELECT 
            user_id, username, name, profile_url, follower_count,
            REPLACE(REPLACE(REPLACE(bio, '|', ' '), '#', ' '), '&', ' ') AS bio,
            media_count, profile_picture_url, timestamp, post_id,
            post_caption, like_count, comments_count, post_media_url, post_permalink
        FROM df
    """).fetchdf()
    # Breaking up the table into user data table and post table 
    df_user = df_cleaned[["user_id", "username", "name", "bio", "profile_url", "follower_count", "media_count"]]
    df_posts = df_cleaned[["user_id", "post_id", "post_caption", 
                           "like_count", "comments_count", "timestamp", "post_media_url", "post_permalink"]]
    # Write to Postgres
    conn = connect_to_database()
    if not conn:
        return
    try:
        cur = conn.cursor()
        # user table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS insta_user_data(
                user_id TEXT NOT NULL,
                username VARCHAR(100) NOT NULL, 
                name VARCHAR(100),
                name VARCHAR(100), profile_url TEXT,
                follower_count BIGINT, bio TEXT, media_count INT,
                profile_picture_url TEXT,
                PRIMARY KEY(user_id)
            );
        """)

        # Post table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS insta_post_data(
                    post_id TEXT NOT NULL,
                    post_caption TEXT,
                    like_count BIGINT,
                    comments_count BIGINT,
                    timestamp TIMESTAMP,
                    post_media_url TEXT,
                    post_permalink TEXT,
                    user_id TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES insta_user_data(user_id),
                    PRIMARY KEY(post_id)
            );
        """)
        # Upsert user data
        upsert_user_sql = """
        INSERT INTO insta_user_data(
            user_id, username, name, 
            profile_url, follower_count, bio, media_count, profile_picture_url
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                name = EXCLUDED.name,
                profile_url = EXCLUDED.profile_url,
                follower_count = EXCLUDED.follower_count,
                bio = EXCLUDED.bio,
                media_count = EXCLUDED.media_count,
                profile_picture_url = EXCLUDED.profile_picture_url,     
        """
        # Post_upsert query

        upsert_post_sql = """
        INSERT INTO insta_post_data(
            post_id, post_caption, like_count, comments_count, timestamp, post_media_url, post_permalink, user_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (post_id) DO UPDATE SET
            post_caption = EXCLUDED.post_caption,
            like_count = EXCLUDED.like_count,
            comments_count = EXCLUDED.comments_count,
            timestamp = EXCLUDED.timestamp,
            post_media_url = EXCLUDED.post_media_url,
            post_permalink = EXCLUDED.post_permalink,
            user_id = EXCLUDED.user_id
        """

        user_records = [(
            str(row["user_id"]),
            str(row["username"]),
            str(row["name"]),
            str(row["profile_url"]),
            int(row["follower_count"]),
            str(row["bio"]),
            int(row["media_count"]),
            str(row["profile_picture_url"]),
            
        )
        for _, row in df_cleaned.iterrows()
        ]
        post_records = [(
            str(row["post_id"]),
            str(row["timestamp"]),
            str(row["post_caption"]),
            int(row["like_count"]),
            int(row["comments_count"]),
            str(row["post_media_url"]),
            str(row["post_permalink"]))
            for _, row in df_cleaned.iterrows()]
        
        cur.executemany(upsert_user_sql, user_records)
        cur.executemany(upsert_post_sql, post_records)
        conn.commit()
        logging.info(f"Upserted {len(df_cleaned)} rows into influencer_instagram.")
    except Exception as e:
        conn.rollback()
        logging.exception(f"Database error during upsert.{e}")
    finally:
        cur.close()
        conn.close()


def main(usernames: List[str]):
    """Entrypoint for pipeline run."""
    uniq_usernames = list(dict.fromkeys(usernames))
    run_pipeline(uniq_usernames)


if __name__ == "__main__":
    try:
        # Create SQLAlchemy engine from environment variables
        conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        sslmode="require")

        query = "SELECT instagram_username FROM username_search WHERE instagram_username IS NOT NULL;"
        names = pd.read_sql(query, conn)

        usernames = (
            names["instagram_username"].astype(str).str.strip().str.lower().dropna().unique().tolist())

        logging.info(f"Loaded {len(usernames)} usernames from database.")
        main(usernames)

    except Exception as e:
        logging.error(f"Error reading usernames: {e}")
