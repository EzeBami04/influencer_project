import undetected_chromedriver as uc
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
import random
import requests
import time
from sqlalchemy import create_engine, MetaData
from functools import wraps

from contextlib import closing
import logging
import os
from dotenv import load_dotenv

import mysql.connector.errors as MySQLConnectorError 

# --- Configuration and Initialization ---
load_dotenv()

logging.basicConfig(level=logging.INFO, format='[%(asctime)s], [%(levelname)s], [%(message)s]', datefmt='%Y-%m-%d %H:%M:%S')

FB_PAGE_ID = os.getenv("FB_PAGE_ID")
access_token = os.getenv("fb_token")
graph_api = "v22.0"
min_followers = 50000
metadata = MetaData()
INSTAGRAM_NON_PROFILE_PATHS = {"explore", "p", "reel", "stories", "tv", "accounts"}
keywords = ["artist", "actor", "influencer", "creator", "comedian", "blogger"]

# --- Utility Functions ---

def random_sleep(a=1, b=3): # Reduced default sleep times slightly
    """ Sleeps for a random duration to mimic human behavior. """
    time.sleep(random.uniform(a, b))

def extract_username_from_url(url):
    try:
        path_parts = url.split("instagram.com/")
        if len(path_parts) > 1:
            path = path_parts[1].split("/")[0]
            if path and path not in INSTAGRAM_NON_PROFILE_PATHS:
                return path
    except IndexError:
        return None
    return None

def retry_on_rate_limit(max_retries=4, backoff_base=60): # Reduced backoff_base to 60 as per original
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                result = func(*args, **kwargs)
                if result == "RATE_LIMIT":
                    sleep_time = backoff_base * (2 ** attempt) # Exponential backoff is better
                    logging.warning(f"Rate limit hit. Sleeping for {sleep_time} seconds before retrying {func.__name__} (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(sleep_time)
                    continue
                return result
            logging.error(f"Exceeded max retries for {func.__name__}. Giving up.")
            return None
        return wrapper
    return decorator

def get_instagram_business_id(page_id):
    """ Fetches the Instagram Business Account ID linked to a Facebook Page. """
    url = f"https://graph.facebook.com/{graph_api}/{page_id}"
    params = {
        'fields': 'instagram_business_account',
        'access_token': access_token,
    }
    logging.info(f"Attempting to fetch Instagram Business ID for FB Page ID: {page_id}")
    try:
        res = requests.get(url, params=params, timeout=15)
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

@retry_on_rate_limit(max_retries=4, backoff_base=60)
def validate_and_fetch_user(username, ig_business_id):
    """ Validates a username via Facebook Graph API and fetches data if criteria are met. """
    fields = (
        f'business_discovery.username({username})'
        '{id,username,profile_picture_url,name,biography,followers_count,media_count,media.limit(10){caption,like_count,comments_count,timestamp,media_url,permalink}}'
    )
    url = f'https://graph.facebook.com/{graph_api}/{ig_business_id}'
    params = {
        'fields': fields,
        'access_token': access_token
    }

    logging.info(f"Validating @{username} via Graph API...")
    try:
        res = requests.get(url, params=params, timeout=20)

        if res.status_code in [429] or (res.status_code == 400 and "rate limit" in res.text.lower()): # Check for 400 with rate limit message
            logging.warning(f"Rate limit hit for @{username}. Response: {res.text[:200]}")
            return "RATE_LIMIT"

        data = res.json()
        if "error" in data:
            error_code = data["error"].get("code")
            # More specific rate limit error codes or messages
            if error_code in [4, 17, 613] or "rate limit" in data["error"].get("message", "").lower():
                logging.warning(f"API Error (potential rate limit) for @{username}: {data['error'].get('message', 'No message')}")
                return "RATE_LIMIT"
            logging.info(f"API Error for @{username}: {data['error'].get('message', 'No message')}. Skipping.")
            return None

        user_data = data.get("business_discovery")
        if not user_data:
            logging.info(f"  'business_discovery' field missing for @{username}, likely not a discoverable Business/Creator account. Response: {data}")
            return None

        followers = user_data.get("followers_count", 0)
        logging.info(f"  @{username} found. Followers: {followers}")
        if followers >= min_followers:
            logging.info(f"  @{username} meets follower threshold ({min_followers}).")
            return user_data
        else:
            logging.info(f"  @{username} does not meet follower threshold ({followers} < {min_followers}). Skipping.")
            return None

    except requests.exceptions.Timeout:
        logging.warning(f"  Timeout error validating @{username}. Skipping.")
        return None
    except requests.exceptions.RequestException as e:
        logging.warning(f"  Network error validating @{username}: {e}")
        return None
    except Exception as e:
        logging.warning(f" An unexpected error occurred validating @{username}: {e}")
        return None

def create_db_connection():
    """ Create a database connection engine. """
    username = os.getenv("USERNAME")
    pwd = os.getenv("PWD")
    host = os.getenv("HOST")
    port = 10780
    db = "influencer"
    try:
        logging.info("Attempting to connect to the database...")
        # Use a connection pool for better performance with multiple connections
        engine = create_engine(f"mysql+pymysql://{username}:{pwd}@{host}:{port}/{database_name}",
                               pool_size=10, max_overflow=20) 
        metadata.create_all(engine) 
        logging.info("Database connection established.")
        return engine
    except MySQLConnectorError as e:
        logging.error(f"MySQL connection error: {e}")
        return None
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return None

# --- Main Logic ---

def search_ig_usernames_sequential(keywords_list, driver):
    """
    Searches Instagram usernames for a list of keywords using a single
    Selenium driver instance sequentially.
    Includes robust Google block detection and debug HTML dumps.
    """
    found_users = set()

    for kw in keywords_list:
        query = f"site:instagram.com+{kw}"
        base_url = f"https://www.google.com/search?q={query}"
        logging.info(f"Searching Google for keyword: '{kw}'")

        for page in range(0, 13):
            start = page *10
            try:
                driver.get(f"{base_url}&start={start}")
                
                # Wait for any links to load
                WebDriverWait(driver, 10).until(
                    ec.presence_of_all_elements_located((By.TAG_NAME, "a"))
                )
                
                # Detect if Google is showing bot block page
                page_text = driver.page_source.lower()
                if "unusual traffic" in page_text or "/sorry/" in page_text or "our systems have detected" in page_text:
                    logging.warning(f"Blocked by Google on page {page} for keyword '{kw}'. Sleeping for 2 minutes.")
                    time.sleep(120)
                    continue

                #
                elements = driver.find_elements(By.TAG_NAME, "a")
                for link in elements:
                    href = link.get_attribute("href")
                    if href and "instagram.com" in href:
                        username = extract_username_from_url(href)
                        if username:
                            found_users.add(username)
                
                random_sleep(3, 6)  
                

            except Exception as e:
                logging.warning(f"Error on Google search page {page} for keyword '{kw}': {e}")
                # Save the page HTML to debug
                debug_file = f"debug_google_{kw}_{page}.html"
                try:
                    with open(debug_file, "w", encoding="utf-8") as f:
                        f.write(driver.page_source)
                    logging.warning(f"Saved debug HTML to '{debug_file}'")
                except Exception as file_error:
                    logging.error(f"Failed to write debug file: {file_error}")

    return found_users
    
