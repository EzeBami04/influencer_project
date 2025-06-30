import random
import re
import time
import os
import requests
import pandas as pd
import re
from mysql.connector import connection
from sqlalchemy import create_engine
from dotenv import load_dotenv
import undetected_chromedriver as uc
from contextlib import closing
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import Table, Column, Integer, BigInteger, Text, String, TIMESTAMP, MetaData
import time
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
load_dotenv()

# ================= Configuration =============#
KEYWORDS = [
    "artist", "actor", "influencer", "creator", "comedian", "branding", "analyst", "lifestyle",
    "blogger", "fitness", "coach", "model", "educator", "entrepreneur", "public figure", "content", "fashon", "photography", "style", "beauty",
    "makeup", "travel", "motivation", "digital", "branding", "music", "wellness", "inspiration", "social media", "marketing", "fashion influencer", "foodie", "tech reviewer", "gaming", "pets", "family"
    ]
# Consider using a more specific keyword list if needed

FB_TOKEN = os.getenv("fb_token")
FB_PAGE_ID = os.getenv("fb_page_id")
ig_business_id = os.getenv("ig_business_id")
MIN_FOLLOWERS = 50000  
MAX_GOOGLE_PAGES_PER_KEYWORD = 13
GRAPH_API_VERSION = "v22.0" 
metadata = MetaData()

# =============== Common non-profile paths on Instagram ===================
INSTAGRAM_NON_PROFILE_PATHS = {"explore", "p", "reel", "stories", "tv", "accounts", "emailsignup", "directory"}

# ======================= Functions =======================#

def random_sleep(a=2.5, b=5.5):
    """ Sleeps for a random duration to mimic human behavior. """
    time.sleep(random.uniform(a, b))

def get_instagram_business_id(page_id):
    """ Fetches the Instagram Business Account ID linked to a Facebook Page. """
    url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{page_id}"
    params = {
        'fields': 'instagram_business_account',
        'access_token': FB_TOKEN,
    }
    print(f"Attempting to fetch Instagram Business ID for FB Page ID: {page_id}")
    try:
        res = requests.get(url, params=params, timeout=15)
        res.raise_for_status() 
        data = res.json()
        ig_account_info = data.get('instagram_business_account')
        if ig_account_info and 'id' in ig_account_info:
            ig_id = ig_account_info['id']
            print(f"Successfully retrieved Instagram Business ID: {ig_id}")
            return ig_id
        else:
            print("Error: 'instagram_business_account' field not found or missing 'id'. Response:")
            print(data)
            print("Ensure the Facebook Page is connected to an Instagram Business/Creator account.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Instagram Business ID: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response Status Code: {e.response.status_code}")
            print(f"Response Text: {e.response.text}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred in get_instagram_business_id: {e}")
        return None


def search_instagram_usernames(keyword):
    query = f"site:instagram.com+{keyword}"
    options = uc.ChromeOptions()
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    options.add_argument("--headless") 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    usernames = set()

    with closing(uc.Chrome(options=options, version_main=136)) as driver:
        driver.implicitly_wait(10)

        for page in range(0, 130, 10): 
            query = f"site:instagram.com+{keyword}"
            search_url = f"https://www.google.com/search?q={query}&start={page}"
            logging.info(f"Scraping Google page {page//10 + 1} for keyword '{keyword}'")
            driver.get(search_url)

            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'yuRUbf')]"))
                )
                elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'yuRUbf')]")
                for el in elements:
                    link = el.find_element(By.XPATH, ".//a").get_attribute("href")
                    if "instagram.com" in link:
                        parts = link.split("/")
                        if len(parts) > 3:
                            username = parts[3]
                            if username and username not in ["p", "explore", "reel", "tv"] and len(username) < 40:
                                if username not in usernames:
                                    usernames.add(username)
                                    logging.info(f"Found username: {username}")
                time.sleep(2)  
            except Exception as e:
                logging.warning(f"Failed on page {page//10 + 1}: {e}")
                break  

    logging.info(f"Total usernames found: {len(usernames)}")
    return list(usernames)

from functools import wraps

def retry_on_rate_limit(max_retries=4, backoff_base=120):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                result = func(*args, **kwargs)
                if result == "RATE_LIMIT":
                    sleep_time = backoff_base * (attempt + 1)
                    logging.warning(f"Rate limit hit. Sleeping for {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    continue
                return result
            logging.error(f"Exceeded max retries for {func.__name__}")
            return None
        return wrapper
    return decorator


@retry_on_rate_limit(max_retries=4, backoff_base=60)
def validate_and_fetch_user(username, ig_business_id):
    """ Validates a username via Facebook Graph API and fetches data if criteria are met. """
    fields = (
        f'business_discovery.username({username})'
        '{id,username,profile_picture_url,name,biography,followers_count,media_count,media.limit(10){caption,like_count,comments_count,timestamp,media_url,permalink}}'
        
    )
    url = f'https://graph.facebook.com/{GRAPH_API_VERSION}/{ig_business_id}'
    params = {
        'fields': fields,
        'access_token': ACCESS_TOKEN
    }

    print(f"Validating @{username} via Graph API...")
    try:
        res = requests.get(url, params=params, timeout=20) # Increased timeout

        if res.status_code in [429, 80004]:
            logging.warning(f"Rate limit hit for @{username}. Response: {res.text[:200]}")
            return "RATE_LIMIT"

        data = res.json()
        if "error" in data:
            error_code = data["error"].get("code")
            if error_code in [4, 17] or "rate limit" in data["error"].get("message", "").lower():
                return "RATE_LIMIT"
            time.sleep(60)
                     # Optional: could implement retry logic here
            return None # Failed validation

        user_data = data.get("business_discovery")
        if not user_data:
            logging.info(f"  'business_discovery' field missing for @{username}, likely not a discoverable Business/Creator account. Response: {data}")
            return None # Not discoverable

        followers = user_data.get("followers_count", 0)
        logging.info(f"  @{username} found. Followers: {followers}")
        if followers >= MIN_FOLLOWERS:
            logging.info(f"  @{username} meets follower threshold ({MIN_FOLLOWERS}).")
            return user_data # Valid user!
        else:
                # print(f"  @{username} does not meet follower threshold ({followers} < {MIN_FOLLOWERS}). Skipping.")
            return None # Doesn't meet follower count

    except requests.exceptions.TooManyRedirects:
        logging.warning(f"  Too many redirects for @{username}. Skipping.")
        return None

    except requests.exceptions.Timeout:
         logging.warning(f"  Timeout error validating @{username}. Skipping.")
         return None
    except requests.exceptions.RequestException as e:
        logging.warning(f"  Network error validating @{username}: {e}")
        return None # Failed validation
    except Exception as e:
         logging.warning(f" An unexpected error occurred validating @{username}: {e}")
         return None # Failed validation

def extract_name(text):
    match = re.match(r'^(.*?)\s*\|{1,2}', text)
    return match.group(1).strip() if match else text.strip()



def create_db_connection():
    """ Create a database connection. """
    username = os.getenv("username")
    pwd = os.getenv("pwd")
    port = os.getenv("port")
    database_name = "influencer"
    host = os.getnenv("host')
    try:
        logging.info("Attempting to connect to the database...")
        logging.info("Database connection established.")
        engine = create_engine(f"mysql+pymysql://{username}:{pwd}@{host}:{port}/{database_name}")
        # ========Create the table on the database ============
        metadata.create_all(engine)

        return engine
    except connection.errors as e:
        logging.error(f"Error connecting to database: {e}")
        return None

def commit():
    """ Main execution function. """
    logging.info("Starting Instagram Influencer Scraping Process...")

    if not ACCESS_TOKEN or not FB_PAGE_ID:
        logging.error("Error: 'fb_token' or 'fb_page_id' not found in environment variables/.env file.")
        logging.error("Please ensure your .env file is correctly set up.")
        return

    ig_business_id = get_instagram_business_id(FB_PAGE_ID)
    if not ig_business_id:
        logging.critical("Critical Error: Could not retrieve Instagram business account ID.")
        logging.critical("Validation using Facebook API will not work. Please check your Page ID, Token, and permissions.")
        return

    logging.info(f"Using Instagram Business ID: {ig_business_id}")
    logging.info(f"Minimum Follower Threshold: {MIN_FOLLOWERS}")
    logging.info(f"Keywords: {KEYWORDS}")

    all_scraped_usernames = set()
    for keyword in KEYWORDS:
        random_sleep(5, 10)
        usernames_for_keyword = search_instagram_usernames(keyword)
        count_before = len(all_scraped_usernames)
        all_scraped_usernames.update(usernames_for_keyword)
        count_after = len(all_scraped_usernames)
        logging.info(f"Added {count_after - count_before} new unique usernames from keyword '{keyword}'. Total unique: {count_after}")

    total_unique_scraped = len(all_scraped_usernames)
    logging.info(f">>> Google scraping finished. Found a total of {total_unique_scraped} unique potential usernames across all keywords.")

    if total_unique_scraped == 0:
        logging.warning("No potential usernames were scraped from Google. Exiting.")
        return

    logging.info("--- Starting Facebook API Validation Phase ---")
    validated_influencer_data = []
    validated_usernames = set()
    usernames_to_validate = sorted(list(all_scraped_usernames))
    processed_count = 0

    for username in usernames_to_validate:
        processed_count += 1
        logging.info(f"Processing {processed_count}/{total_unique_scraped} - @{username}")
        user_details = validate_and_fetch_user(username, ig_business_id)

        if user_details:
            validated_usernames.add(user_details.get("username"))
            posts = user_details.get("media", {}).get("data", [])

            if not posts:
                logging.info(f"@{username} validated, but no recent media found via API.")
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
                logging.info(f"Adding {len(posts)} posts for validated user @{username}")
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
            random_sleep(1, 3)
        else:
            random_sleep(0.5, 1.5)

    logging.info("--- Validation Finished ---")
    logging.info(f"Successfully validated and met criteria for {len(validated_usernames)} unique influencers.")
    logging.info(f"Total rows (including posts) collected: {len(validated_influencer_data)}")

    if validated_influencer_data:
        df = pd.DataFrame(validated_influencer_data)
        columns_order = [
            "user_id","username", "name", "profile_url", "follower_count", "bio", "media_count",
            "profile_picture_url", "timestamp", "post_caption", "like_count",
            "comments_count", "post_media_url", "post_permalink"
        ]
        df_columns = [col for col in columns_order if col in df.columns]
        df = df[df_columns]
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
        metadata.create_all(engine)
        if engine:
            df.to_sql("influencer_instagram", con=engine,
                      if_exists="append", index=False)
            logging.info(f"Data successfully saved to database table 'influencer_instagram'.")
        csv_output_path = "instagram_influencers.csv"
        df.to_csv(csv_output_path, index=False)
        logging.info(f"Data saved to CSV file: {csv_output_path}")

    else:
        logging.warning("No influencers met the criteria after validation, or no data could be fetched. No CSV file generated.")

