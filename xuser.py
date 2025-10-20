import tweepy


import duckdb
import pandas as pd
import psycopg2
import emoji

import os
import time
from dotenv import load_dotenv
import logging

load_dotenv()

#============================== Config =====================================
logging.getLogger().setLevel(logging.INFO)
bearer_token = os.getenv("x_bearer_token")
client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
#=============================================================================
def connect_to_database():
    try:
        engine = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            password=os.getenv("DB_PASS"),
            user=os.getenv("DB_USERNAME"),
            sslmode="require"
        )
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return None
    return engine

def remove_emojis(text: str) -> str:
    """Helper to strip emojis."""
    return emoji.replace_emoji(text, replace="")
def user_data(username):
    """
    Fetch user profile (and optionally tweets) from Twitter API.

    Args:
        client: Tweepy client
        username (str): Twitter username
        fetch_tweets (bool): If True, also fetch recent tweets

    Returns:
        dict: user data
    """
    while True:
        try:
            # Get profile info
            users_response = client.get_users(
                usernames=[username],
                user_fields=[
                    "id", "name", "username", "description", 
                    "public_metrics", "created_at", "verified", "location"
                ],
                expansions=["pinned_tweet_id"]
            )
            if not users_response.data:
                logging.warning(f"No user data found for {username}")
                return None

            user = users_response.data[0]
            user_data = {
                "id": user.id,
                "name": user.name,
                "username": user.username,
                "description": user.description,
                "followers_count": user.public_metrics["followers_count"],
                "following_count": user.public_metrics["following_count"],
                "tweet_count": user.public_metrics["tweet_count"],
                "listed_count": user.public_metrics["listed_count"],
                "created_at": str(user.created_at),
                "verified": user.verified,
                "location": user.location,
                "tweets": []  # always included for consistency
            }

            # Only fetch tweets if requested
            if user_data:
                tweets_response = client.get_users_tweets(
                    user.id,
                    max_results=5,
                    tweet_fields=["created_at", "public_metrics", "text"]
                )
                if tweets_response.data:
                    user_data["tweets"] = [
                        {
                            "id": tweet.id,
                            "text": tweet.text,
                            "created_at": str(tweet.created_at),
                            "retweet_count": tweet.public_metrics["retweet_count"],
                            "reply_count": tweet.public_metrics["reply_count"],
                            "like_count": tweet.public_metrics["like_count"],
                            "quote_count": tweet.public_metrics["quote_count"],
                        }
                        for tweet in tweets_response.data
                    ]

            return user_data

        except tweepy.TooManyRequests as e:
            # backoff
            reset_time = int(e.response.headers.get("x-rate-limit-reset", time.time() + 900))
            sleep_for = max(reset_time - int(time.time()), 60)
            logging.warning(f"Rate limit hit. Sleeping {sleep_for}s...")
            time.sleep(sleep_for)
            continue
        except Exception as e:
            logging.error(f"Unexpected error fetching data for {username}: {e}")
            return None


def x_data(username):
    logging.info(f"getting data for @{username}")
    data = user_data(username)

    if not data:
        logging.warning(f"No data for {username}")
        return

    columns = ["created_at",
               "username", "id", "bio", "location", "profile_image_url", 
               "followers", "is_verified", "published_at", "text", 
               "likes", "retweets", "comments_count"]             
    df = pd.DataFrame(data)
    column = [col for col in columns if col in df.columns]
    df = df[column]
    duck = duckdb.connect()
    duck.register("df", df)
    #====================== Type Casting and Data cleansing ===========================
    df['username'] = df["username"].astype(str).apply(remove_emojis)
    df['id'] = df["id"].astype(str)
    df['bio'] = df["bio"].astype(str).apply(remove_emojis).replace(r'[@#"/]', ' ', regex=True)
    df['location'] = df["location"].astype(str)
    df['profile_image_url'] = df["profile_image_url"].astype(str)
    df['followers'] = df["followers"].fillna(0).astype(int)
    df['is_verified'] = df['is_verified'].astype(bool)
    df['created_at'] = pd.to_datetime(df['created_at'], errors="coerce")
    df['published_at'] = pd.to_datetime(df['published_at'], errors="coerce")
    df['text'] = df["text"].astype(str).apply(remove_emojis).replace(r'[@#"/]', ' ', regex=True)
    df['likes'] = df['likes'].astype(int)
    df['retweets'] = df['retweets'].astype(int)
    df['comments_count'] = df['comments_count'].astype(int)
    df_clean = duck.execute("""
                    SELECT created_at, username, id, 
                        REPLACE(bio, '|', ' ') AS bio, 
                        location, profile_image_url, 
                        followers, is_verified, 
                        published_at, 
                        REPLACE(text, '|', ' ') AS text, 
                        likes, retweets, comments_count 
                    FROM df
                """).fetchdf()

    records = df_clean.to_records(index=False)
    query = f"""
                INSERT INTO influencer_x({', '.join(columns)})
                VALUES ({', '.join(['%s'] * len(columns))})
                
                ON CONFLICT (id)  
                DO UPDATE SET
                    created_at = EXCLUDED.created_at,
                    username = EXCLUDED.username,
                    bio = EXCLUDED.bio,
                    location = EXCLUDED.location,
                    profile_image_url = EXCLUDED.profile_image_url,
                    followers = EXCLUDED.followers,
                    is_verified = EXCLUDED.is_verified,
                    published_at = EXCLUDED.published_at,
                    text = EXCLUDED.text,
                    likes = EXCLUDED.likes,
                    retweets = EXCLUDED.retweets,
                    comments_count = EXCLUDED.comments_count"""

    engine = connect_to_database()
    if engine:
        try:
            with engine.cursor() as cursor:
                cursor.execute("SET sceham_path TO PUBLIC") 
                engine.commit() 

                cursor.execute("""CREATE TABLE IF NOT EXISTS influencer_x(created_at TIMESTAMP,
                            username Text, id Varchar(50) PRIMARY KEY, bio Text, location Text, profile_image_url Text, 
                            followers INT, is_verified Boolean, published_at TIMESTAMP, text Text, likes INT, retweets INT, comments_count INT)"""
                            )  
                engine.commit()

                cursor.executemany(query, records)
                engine.commit()
        except psycopg2.DatabaseError as e:
            engine.rollback()
        finally:
            if cursor:
                cursor.close()
            if engine:
                engine.close()


#Test 
from pathlib import Path
if __name__ == "__main__":
    try:
        # Create SQLAlchemy engine from environment variables
        conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASS"),
        sslmode="require")

        query = "SELECT x_username FROM username_search WHERE x_username IS NOT NULL;"
        names = pd.read_sql(query, conn)

        username = (
            names["x_username"].astype(str).str.strip().str.lower().dropna().unique().tolist())
        
        usernames = username.lstrip("@")

        logging.info(f"Loaded {len(usernames)} usernames from database.")
    
        x_data(usernames)
    except Exception as e:
        logging.info(f"error reading usernames: {e}")

