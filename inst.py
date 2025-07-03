import os
import random
import time
import pickle
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine,  MetaData
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from contextlib import closing

# ================ CONFIGURATION ================
load_dotenv()
ACCESS_TOKEN = os.getenv("FB_TOKEN")
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
GRAPH_API_VERSION = "v22.0"
MIN_FOLLOWERS = 50000
KEYWORDS = [
    "artist", "actor", "influencer", "creator", "comedian", "branding", "analyst", 
    "lifestyle", "blogger", "fitness", "coach", "model", "educator", "entrepreneur",
    "public figure", "content", "fashion", "photography", "style", "beauty", "makeup",
    "travel", "motivation", "digital creator", "branding", "music", "wellness", 
    "inspiration", "social media", "marketing", "fashion influencer", "foodie", 
    "tech reviewer", "gaming", "pets", "family"
]
INSTAGRAM_NON_PROFILE_PATHS = {"explore", "p", "reel", "stories", "tv", "accounts"}

# ================ LOGGING SETUP ================
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

metadata = MetaData()

# ================ FUNCTIONS ================
def random_sleep(a=1, b=2):
    time.sleep(random.uniform(a, b))

def get_instagram_business_id(page_id):
    url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{page_id}"
    params = {'fields': 'instagram_business_account', 'access_token': ACCESS_TOKEN}
    try:
        res = requests.get(url, params=params, timeout=15)
        res.raise_for_status()
        data = res.json()
        ig_id = data.get('instagram_business_account', {}).get('id')
        return ig_id
    except Exception as e:
        logging.error(f"Error getting IG business ID: {e}")
        return None

def search_instagram_usernames_multi_keywords(driver):
    all_usernames = set()
    for keyword in KEYWORDS:
        query = f"site:instagram.com+{keyword}"
        logging.info(f"Searching Google for keyword: {keyword}")
        for page in range(0, 130, 10):
            search_url = f"https://www.google.com/search?q={query}&start={page}"
            try:
                driver.get(search_url)
                WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'yuRUbf')]")))
                elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'yuRUbf')]")
                for el in elements:
                    link = el.find_element(By.XPATH, ".//a").get_attribute("href")
                    if "instagram.com" in link:
                        parts = link.split("/")
                        if len(parts) > 3:
                            username = parts[3]
                            if username and username not in INSTAGRAM_NON_PROFILE_PATHS and len(username) < 40:
                                all_usernames.add(username)
                time.sleep(0.5)
            except Exception as e:
                logging.warning(f"Failed on keyword {keyword} page {page//10+1}: {e}")
                break
        # Save checkpoint after each keyword
        with open("usernames_checkpoint.pkl", "wb") as f:
            pickle.dump(all_usernames, f)
    return list(all_usernames)

def validate_and_fetch_user(username, ig_business_id):
    url = f'https://graph.facebook.com/{GRAPH_API_VERSION}/{ig_business_id}'
    fields = (
        f'business_discovery.username({username})'
        '{id,username,profile_picture_url,name,biography,followers_count,media_count,'
        'media.limit(10){caption,like_count,comments_count,timestamp,media_url,permalink}}'
    )
    try:
        res = requests.get(url, params={'fields': fields, 'access_token': ACCESS_TOKEN}, timeout=20)
        if res.status_code == 429 or "rate limit" in res.text.lower():
            logging.warning(f"Rate limit on @{username}")
            time.sleep(60)
            return None
        data = res.json()
        user = data.get("business_discovery")
        if user and user.get("followers_count", 0) >= MIN_FOLLOWERS:
            return user
    except Exception as e:
        logging.warning(f"Error fetching @{username}: {e}")
    return None

def process_users_concurrently(usernames, ig_business_id):
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(validate_and_fetch_user, u, ig_business_id): u for u in usernames}
        for future in as_completed(futures):
            user = future.result()
            if user:
                posts = user.get("media", {}).get("data", [])
                if posts:
                    for p in posts:
                        results.append({**user, **p})
                else:
                    results.append({**user})
    return results

def create_db_connection():
    username = "avnadmin"
    pwd = "AVNS_iJU3jgYQOVJFlrnC96d"
    host = "influencer-db-eomobamidele-84f0.j.aivencloud.com"
    port = 10780
    db = "influencer"
    return create_engine(f"mysql+pymysql://{username}:{pwd}@{host}:{port}/{db}")

def save_to_db(df, engine):
    df.to_sql("influencer_instagram", con=engine, if_exists="append", index=False, method="multi", chunksize=500)
    logging.info(f"Saved {len(df)} rows to DB.")

# ================ MAIN =================
def load():
    ig_business_id = get_instagram_business_id(FB_PAGE_ID)
    if not ig_business_id:
        logging.critical("Could not get IG business ID. Exiting.")
        return

    options = uc.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")

    with closing(uc.Chrome(options=options, version_main=136)) as driver:
        driver.implicitly_wait(10)
        usernames = search_instagram_usernames_multi_keywords(driver)

    if not usernames:
        logging.warning("No usernames found.")
        return

    logging.info(f"Total unique usernames scraped: {len(usernames)}")
    validated_data = process_users_concurrently(usernames, ig_business_id)
    logging.info(f"Validated {len(validated_data)} influencer records (posts included).")

    if validated_data:
        df = pd.json_normalize(validated_data)
        df['profile_url'] = "https://www.instagram.com/" + df['username']
        engine = create_db_connection()
        save_to_db(df, engine)
    else:
        logging.warning("No influencers met the validation criteria.")


