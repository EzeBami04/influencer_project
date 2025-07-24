from concurrent.futures import ThreadPoolExecutor, as_completed
import undetected_chromedriver as uc
from fake_useragent import UserAgent
from requests_random_user_agent import USER_AGENTS


import pandas as pd
from sqlalchemy import Table, Column, Integer, BigInteger, Text, TIMESTAMP, MetaData
from sel import keywords, search_ig_usernames_sequential, get_instagram_business_id, validate_and_fetch_user, create_db_connection
from contextlib import closing
from mysql.connector import connection
import logging

import os 
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='[%(asctime)s], [%(levelname)s], [%(message)s]', datefmt='%Y-%m-%d %H:%M:%S')
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
access_token = os.getenv("fb_token")
graph_api = "v22.0"
min_followers = 50000
metadata = MetaData()
ig_id = os.getenv("ig_business_id")

INSTAGRAM_NON_PROFILE_PATHS = {"explore", "p", "reel", "stories", "tv", "accounts"}


def main():
    logging.info("Starting Instagram Influencer Scraping Process...")

    if not access_token or not FB_PAGE_ID:
        logging.error("Missing 'fb_token' or 'fb_page_id' in .env.")
        return

    ig_business_id = ig_id
    if not ig_business_id:
        logging.error("Unable to get Instagram business ID. Exiting.")
        return

    logging.info(f"Using IG Business ID: {ig_business_id}")

    all_scraped_usernames = set()
    logging.info("--- Starting sequential scraping of Google for usernames ---")
    
    ua = UserAgent()
    
    # Optimization: Reuse a single browser instance for all keywords or batch keywords per browser
    options = uc.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

    options.add_argument("--disable-gpu")
    # options.add_argument("--window-size=1920,1080") # Set a fixed window size for headless

    chrome_main_version = 138 
    # -----------------------------------------------------------------

    try:
        # Single browser instance for all keyword searches (sequential operation)
        with closing(uc.Chrome(options=options, version_main=chrome_main_version)) as driver:
            
            all_scraped_usernames = search_ig_usernames_sequential(keywords, driver)
            logging.info(f"Total unique usernames found after scraping: {len(all_scraped_usernames)}")
    except Exception as e:
        logging.error(f"Failed to start Chrome or perform Google searches: {e}")
        return

    if not all_scraped_usernames:
        logging.warning("No usernames found. Exiting.")
        return

    # === Parallel API validation ===
    validated_influencer_data = []
    validated_usernames = set()

    logging.info("--- Starting parallel validation with Facebook Graph API ---")
    # Max workers for API calls can be higher as it's I/O bound
    with ThreadPoolExecutor(max_workers=20) as executor: # Increased max_workers for API calls
        future_to_username = {
            executor.submit(validate_and_fetch_user, username, ig_business_id): username
            for username in list(all_scraped_usernames) # Convert set to list for iteration
        }

        for future in as_completed(future_to_username):
            username = future_to_username[future]
            try:
                user_details = future.result()
                if user_details:
                    validated_usernames.add(user_details.get("username"))
                    posts = user_details.get("media", {}).get("data", [])
                    if not posts:
                        validated_influencer_data.append({
                            "user_id": user_details.get("id"),
                            "username": user_details.get("username"),
                            "profile_url": f"https://www.instagram.com/{user_details.get('username')}/",
                            "name": user_details.get("name"),
                            "profile_picture_url": user_details.get("profile_picture_url"),
                            "bio": user_details.get("biography"),
                            "follower_count": user_details.get("followers_count"),
                            "media_count": user_details.get("media_count"),
                            "post_caption": None,
                            "like_count": None,
                            "comments_count": None,
                            "timestamp": None,
                            "post_media_url": None,
                            "post_permalink": None,
                        })
                    else:
                        for post in posts:
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
                            })
            except Exception as e:
                logging.error(f"Error processing @{username}: {e}")

    logging.info(f"--- Finished validation. Valid influencers: {len(validated_usernames)}. Total rows (posts): {len(validated_influencer_data)}")

    if validated_influencer_data:
        df = pd.DataFrame(validated_influencer_data)
        columns_order = [
            "user_id","username", "name", "profile_url", "follower_count", "bio", "media_count",
            "profile_picture_url", "timestamp", "post_caption", "like_count",
            "comments_count", "post_media_url", "post_permalink"
        ]
        df_columns = [col for col in columns_order if col in df.columns]
        df = df[df_columns]

        # Data Cleaning and Type Conversion
        df['bio'] = df['bio'].astype(str).replace("/", "", regex=True)
        df['follower_count'] = df['follower_count'].fillna(0).astype(int)
        df['like_count'] = df['like_count'].fillna(0).astype(int)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['profile_picture_url'] = df['profile_picture_url'].str.rstrip("/")
        df['post_media_url'] = df['post_media_url'].str.rstrip("/")
        df['comments_count'] = df['comments_count'].fillna(0).astype(int)
        df['bio'] = df['bio'].astype(str).str.replace(r'@\w+', '', regex=True)
        df['post_caption'] = df['post_caption'].fillna("").astype(str).str.replace(r'http\S+|www\S+|https\S+', '', regex=True)
        df['post_caption'] = df['post_caption'].astype(str).str.replace(r'@\w+', '', regex=True)

        # Define table schema using SQLAlchemy
        influencer_table = Table(
            "influencer_instagram",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("user_id", BigInteger),
            Column("username", Text),
            Column("name", Text),
            Column("profile_url", Text),
            Column("follower_count", BigInteger),
            Column("bio", Text),
            Column("media_count", BigInteger),
            Column("profile_picture_url", Text),
            Column("timestamp", TIMESTAMP),
            Column("post_caption", Text),
            Column("like_count", BigInteger),
            Column("comments_count", BigInteger),
            Column("post_media_url", Text),
            Column("post_permalink", Text)
        )

        engine = create_db_connection()
        if engine:
            # Create table if it doesn't exist (already handled by metadata.create_all(engine))
            # Perform bulk insertion
            try:
                # Use method='multi' for faster insertion for MySQL
                df.to_sql("influencer_instagram", con=engine,
                          if_exists="append", index=False, method='multi')
                logging.info(f"Data successfully saved to database table 'influencer_instagram'.")
            except Exception as e:
                logging.error(f"Error saving data to database: {e}")
        else:
            logging.error("Failed to establish database connection. Data not saved.")
    else:
        logging.warning("No influencers met the criteria after validation, or no data could be fetched.")

if __name__ == "__main__":
    main()
