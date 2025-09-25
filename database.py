from sqlalchemy import create_engine
import psycopg2
import os
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

def connect_to_database():
    host = os.getenv("db_host")
    database = os.getenv("db_name")
    user = os.getenv("db_username")
    password = os.getenv("db_pass")
    port = os.getenv("db_port", "10780")
    if not all([host, database, user, password, port]):
        logging.error("Missing required environment variables")
        return None
    try:
        con_str = con_str = f"host={host} dbname={database} user={user} password={password} port={port} sslmode=require"
  
        with  psycopg2.connect(con_str) as engine:
            logging.info("Connection to PostgreSQL database successful")
        return engine
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return None