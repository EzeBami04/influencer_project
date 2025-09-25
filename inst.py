# from .search import run_engine_and_save
import requests
import duckdb
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import psycopg2
from sqlalchemy import MetaData

import pandas as pd

import emoji


import os
import random
from datetime import datetime, timedelta
import pytz
import time

from dotenv import load_dotenv
from functools import wraps, cache
from .database import connect_to_database
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logging.basicConfig(level=logging.INFO)

metadata = MetaData()

# ============== STEP 1: Remove emojis/emoticons ==============
def remove_emojis(text: str) -> str:
    return emoji.replace_emoji(text, replace="")



#============================================================================================================#


load_dotenv()
#============================================= Config ====================================================
logging.getLogger().setLevel(logging.INFO)

metadata = MetaData()
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
access_token = os.getenv("fb_token")
graph_api = "v22.0"
min_followers = 50000
ig_id= os.getenv("ig_business_id")

session = requests.Session()
retry = Retry(
    total=3,
    backoff_factor=100,
    status_forcelist=[400, 443, 429, 403, 500, 502, 503, 504]
)
session.mount("https://", HTTPAdapter(max_retries=retry))



INSTAGRAM_NON_PROFILE_PATHS = {"explore", "p", "reel", "stories", "tv", "accounts"}
#=======================================================================================



def random_sleep(a=1, b=3):
    """ Sleeps for a random duration to mimic human behavior. """
    time.sleep(random.uniform(a, b))

def retry_on_rate_limit(max_retries=3, backoff_base=60): 
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                result = func(*args, **kwargs)
                if result == "RATE_LIMIT":
                    sleep_time = backoff_base * (2 ** attempt)
                    logging.warning(f"Rate limit hit. Sleeping for {sleep_time} seconds before retrying {func.__name__} (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(sleep_time)
                    continue
                return result
            logging.error(f"Exceeded max retries for {func.__name__}. Giving up.")
            return None
        return wrapper
    return decorator

@cache
def get_instagram_business_id(page_id):
    """ Fetches the Instagram Business Account ID linked to a Facebook Page. """
    url = f"https://graph.facebook.com/{graph_api}/{page_id}"
    params = {
        'fields': 'instagram_business_account',
        'access_token': access_token,
    }
    logging.info(f"Attempting to fetch Instagram Business ID for FB Page ID: {page_id}")
    try:
        res = session.get(url, params=params, timeout=30)
        res.raise_for_status()
        data = res.json()
        ig_account_info = data.get('instagram_business_account')
        if ig_account_info and 'id' in ig_account_info:
            ig_id = ig_account_info['id']
            logging.info(f"Successfully retrieved Instagram Business ID: {ig_id}")
            return ig_id
        else:
            logging.error(f"Error: 'instagram_business_account' field not found or missing 'id'. Response: {data}")
            logging.error("Ensure the Facebook Page is connected to an Instagram Business/Creator account.")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Instagram Business ID: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response Status Code: {e.response.status_code}")
            logging.error(f"Response Text: {e.response.text}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred in get_instagram_business_id: {e}")
        return None

@cache
@retry_on_rate_limit(max_retries=4, backoff_base=60)
def validate_and_fetch_user(username, ig_business_id):
    """Fetches Instagram Business/Creator data from Graph API (no follower re-validation)."""
    
    fields = (
        f'business_discovery.username({username})'
        '{id,username,profile_picture_url,name,biography,followers_count,media_count,media.limit(10){id,caption,like_count,comments_count,timestamp,media_url,permalink}}'
    )
    url = f'https://graph.facebook.com/{graph_api}/{ig_business_id}'
    params = {
        'fields': fields, 
        'access_token': access_token
    }

    logging.info(f"Fetching data for @{username} via Graph API...")
    try:
        res = session.get(url, params=params, timeout=60)
        res.raise_for_status()
        try:
            data = res.json()
        except ValueError as e:
            logging.error(f"Failed to parse JSON for @{username}: {e}. Response: {res.text[:500]}")
            return None

        if "error" in data:
            error_code = data["error"].get("code")
            if error_code in [4, 17, 613] or "rate limit" in data["error"].get("message", "").lower():
                logging.warning(f"API Error (rate limit) for @{username}: {data['error'].get('message', '')}")
                return "RATE_LIMIT"
            logging.info(f"API Error for @{len(username)}: {data['error'].get('message', '')}. Skipping.")
            return None

        user_data = data.get("business_discovery")
        if not user_data:
            logging.info(f"'business_discovery' missing for @{len(username)}, likely not a Business/Creator account.")
            return None

        
        logging.info(f"  @{len(username)} found. Followers: {user_data.get('followers_count', 0)}")
        return user_data

    except requests.exceptions.Timeout:
        logging.warning(f"Timeout fetching @{len(username)}. Skipping.")
        return None
    except requests.exceptions.RequestException as e:
        logging.warning(f"Network error fetching @{len(username)}: {e}")
        return None
    except Exception as e:
        logging.warning(f"Unexpected error fetching @{len(username)}: {e}")
        return None




def insta_data(usernames):
    get_instagram_business_id(FB_PAGE_ID)
    validated_influencer_data = []
    validated_usernames = set()

    logging.info("====== Starting parallel validation with Facebook Graph API ======")
    
    with ThreadPoolExecutor(max_workers=2) as executor: 
        future_to_username = {
            executor.submit(validate_and_fetch_user, username, ig_id): username
            for username in list(usernames) 
            }

        for future in as_completed(future_to_username):
            username = future_to_username[future]
            try:
                user_details = future.result()
                if user_details:
                    # Define media_items first
                    media_items = user_details.get("media", {}).get("data", [])
                    # Use offset-aware cutoff_date
                    cutoff_date = datetime.now(pytz.UTC) - timedelta(days=30*5)
                    # Filter media items
                    filtered_media = [
                        m for m in media_items
                        if "timestamp" in m and datetime.fromisoformat(m["timestamp"].replace("Z", "+00:00")) >= cutoff_date
                    ]

                    validated_usernames.add(user_details.get("username"))
                    posts = user_details.get("media", {}).get("data", [])
                    for post in posts:
                        post_id = post.get("id")

                        # Default values
                        impressions, reach = 0, 0

                        # Fetch impressions & reach for each post
                        try:
                            insight_url = f"https://graph.facebook.com/{graph_api}/{post_id}/insights"
                            insight_params = {
                                "metric": "impressions,reach",
                                "access_token": access_token
                            }
                            insight_res = session.get(insight_url, params=insight_params, timeout=30).json()
                            if "data" in insight_res:
                                for metric in insight_res["data"]:
                                    if metric["name"] == "impressions":
                                        impressions = metric["values"][0]["value"]
                                    elif metric["name"] == "reach":
                                        reach = metric["values"][0]["value"]
                        except Exception as e:
                            logging.warning(f"Could not fetch insights for media {post_id}: {e}")

                        validated_influencer_data.append({
                            "user_id": user_details.get("id"),
                            "username": user_details.get("username"),
                            "profile_url": f"https://www.instagram.com/{user_details.get('username')}/",
                            "name": user_details.get("name"),
                            "profile_picture_url": user_details.get("profile_picture_url"),
                            "bio": user_details.get("biography"),
                            "follower_count": user_details.get("followers_count"),
                            "media_count": user_details.get("media_count"),
                            "post_caption": post.get("caption"),
                            "like_count": post.get("like_count"),
                            "comments_count": post.get("comments_count"),
                            "timestamp": post.get("timestamp"),
                            "post_media_url": post.get("media_url"),
                            "post_permalink": post.get("permalink"),
                            "impressions": impressions,      
                            "reach": reach                   
                        })

            except Exception as e:
                logging.error(f"Error processing @{username}: {e}")

    logging.info(f"===Finished validation. Valid influencers: {len(validated_usernames)}. Total rows (posts): {len(validated_influencer_data)}")


    if validated_influencer_data:
            df = pd.DataFrame(validated_influencer_data)
            columns_order = [
                "user_id","username", "name", "profile_url", "follower_count", "bio", "media_count",
                "profile_picture_url", "timestamp", "post_caption", "like_count", "impression", "reach",
                "comments_count", "post_media_url", "post_permalink"
                ]
            df_columns = [col for col in columns_order if col in df.columns]
            df = df[df_columns]
            
            # ========================= Data Cleaning and Type Casting================================== 
            df['bio'] = df['bio'].astype(str).str.replace("/", "", regex=True)
            df['follower_count'] = df['follower_count'].fillna(0).astype(int)
            df['like_count'] = df['like_count'].fillna(0).astype(int)
            df['comments_count'] = df['comments_count'].fillna(0).astype(int)
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df['profile_picture_url'] = df['profile_picture_url'].str.rstrip("/")
            df['post_media_url'] = df['post_media_url'].str.rstrip("/")
            df['bio'] = df['bio'].astype(str).str.replace(r'@\w+', ' ', regex=True)
            df["bio"] = df['bio'].apply(remove_emojis)
            df['impression'] = df['impression'].fillna(0).astype(int)
            df['reach'] = df['reach'].fillna(0).astype(int)
            df['post_caption'] = df['post_caption'].astype(str).replace(r'@\w+', '', regex=True)
            df['post_caption'] = df['post_caption'].astype(str).replace(r'http\S+|www\S+|https\S+', '', regex=True)
            df["post_caption"] = df['post_caption'].apply(remove_emojis)
            df['name'] = df['name'].fillna('').astype(str)
            df['name'] = df['name'].apply(remove_emojis)
            df['media_count'] = (
                df.groupby("user_id").cumcount().apply(lambda x: 0 if x > 0 else None).fillna(df["media_count"]).astype(int))
    
            df['follower_count'] = (
                df.groupby("user_id").cumcount().apply(lambda x: 0 if x > 0 else None).fillna(df["follower_count"]).astype(int))
            
            duck = duckdb.connect() 
            duck_df = duck.register("df", df)
            df_cleaned = duck_df.execute("""
                SELECT 
                    user_id, username, name, profile_url, follower_count,
                    REPLACE(REPLACE(REPLACE(bio, '|', ' '), '#', ' '), '&', ' ') AS bio,
                    media_count, impression, reach, profile_picture_url, timestamp, 
                    post_caption, like_count, comments_count, post_media_url, post_permalink
                FROM df
            """).fetchdf()
            #=========================Load to Postgres ==================================
            upsert_columns = """user_id,username, name, profile_url, follower_count, bio, media_count,
                                       profile_picture_url, timestamp, post_caption, like_count, impression, reach,
                                       comments_count, post_media_url, post_permalink"""
            upsert_stmt = f"""INSERT INTO TABLE influencer_instagram({upsert_columns})
                                VALUES (%s, %s, %s)
                                ON CONFLICT (user_id, user_name)
                                DO UPDATE SET 
                                    user_id = EXCLUDED.user_id,
                                    username = EXCLUDED.username, 
                                    name = EXCLUDED.name, 
                                    profile_url = EXCLUDED.profile_url, 
                                    follower_count = EXCLUDED.follow_count, 
                                    bio = EXCLUDED.bio, 
                                    media_count = EXCLUDED.media_count,
                                    profile_picture_url = EXCLUDED.profile_picture_url, 
                                    timestamp = EXCLUDED.timestamp, 
                                    post_caption = EXCLUDED.post_caption, 
                                    like_count = EXCLUDED.like_count, 
                                    impression = EXCLUDED.impression, 
                                    reach = EXCLUDED.reach,
                                    comments_count = EXCLUDED.comments_count, 
                                    post_media_url = EXCLUDED.post_media_url, 
                                    post_permalink = EXCLUDED.post_permalink"""
            engine = connect_to_database()
            if engine:
                try:
                    with engine.cursor() as cursor:
                    # =================================== Creating table if not exists===================================
                        cursor.execute("SET SCHEMA PATH TO PUBLIC")
                        cursor.execute("""
                                    Create table if not exists influencer_instagram(
                                                    user_id Text,username Varchar(100), name Varchar(100), profile_url Text,
                                                    follower_count Bigint, bio Text, media_count Int,
                                                    impression Int, reach Int, profile_picture_url Text, timestamp Timestamp, post_caption Text, like_count Int,
                                                    comments_count Int, post_media_url Text, post_permalink Text);""")
                        # ================= Incremental Load to Postgres===========
                        records = df_cleaned.to_records(index=False)
                        for row in records:
                            cursor.execute(upsert_stmt, tuple(row))
                        engine.commit()
                except psycopg2.DatabaseError as e:
                    logging.error("Database Error on this Transaction")
                    engine.rollback()
                finally:
                    cursor.close()
                    engine.close()

import csv
from pathlib import Path

if __name__ == "__main__":
    path = "C:/Users/Bamidele/Desktop/webscraping/usernames.csv"


    usernames = []
    with open(path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            username = row[0]
            if username:
                
                username = username.strip()
                if len(username) > 2: 
                    usernames.append(username)  

    insta_data(usernames)