import psycopg2
import requests
import json
import time
import sys
import os
import yaml
import logging

from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

# set up paths
ROOT_DIR = Path(__file__).parents[1]

load_dotenv(ROOT_DIR / ".env")

# logs
LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "opensky_flight_ex.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# config yaml
with open(ROOT_DIR / "config.yaml") as c:
    config = yaml.safe_load(c)

# url for the api data
URL = config["api_get_endpoint"]

# url for refreshing token for authentication
TOKEN_URL = config["api_post_endpoint"]

# credentials for openflight api
CREDENTIALS_FILE = ROOT_DIR / "credentials.json"

# timeouts and cadences for if i exceed api limits
INTERVAL = 120
COOL_DOWN = 300

# open sky credentials file loads
with open(CREDENTIALS_FILE) as f:
    creds = json.load(f)

CLIENT_ID = creds["clientId"]
CLIENT_SECRET = creds["clientSecret"]

# Postgres Database
db_params = {
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT")
}

# setting the token state
token = None
expires_at = None


# token helpers for when i bog down the api
def refresh_token():
    global token, expires_at

    logging.info("Refreshing OAuth token...")

    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        },
        timeout=10
    )

    response.raise_for_status()

    data = response.json()
    token = data["access_token"]
    expires_in = data.get("expires_in", 1800)
    expires_at = datetime.now() + timedelta(seconds=expires_in - 30)

    logging.info(f"Token valid for {expires_in}s")


def get_headers():
    global token, expires_at

    if token is None or datetime.now() >= expires_at:
        refresh_token()

    return {"Authorization": f"Bearer {token}"}


# Database Connection
def get_connection():
    for attempt in range(3):
        try:
            conn = psycopg2.connect(**db_params)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:
            logging.warning(f"Connection attempt {attempt + 1} failed: {e}")
            time.sleep(5)
    raise Exception("Could not connect to database after 3 attempts")


def insert_flight_data(conn, raw_json):
    try:
        with conn.cursor() as cur:
            # postgres loading into DB
            cur.execute(
                "INSERT INTO dev_env.stg_flight_data (data) VALUES (%s)",
                (json.dumps(raw_json),)
            )
        return True

    except psycopg2.OperationalError:
        logging.warning("Lost DB connection. Reconnecting...")
        return False

    except psycopg2.Error as e:
        logging.error(f"DATABASE ERROR - {e}")
        return False


def fetch_flights(url):
    try:
        response = requests.get(
            url,
            headers=get_headers(),
            timeout=20
        )

        # error handling for too many tries w/ a cool down
        if response.status_code == 429:
            logging.warning(f"Rate limited. Cooling down {COOL_DOWN}s...")
            time.sleep(COOL_DOWN)
            return None

        # error handling if token expires
        if response.status_code == 401:
            logging.warning("Token rejected. Refreshing...")
            refresh_token()
            return None

        response.raise_for_status()

        # pulling data
        return response.json()

    except requests.exceptions.RequestException as e:
        logging.error(f"NETWORK ERROR - {e}")
        return None


def run_stream():
    conn = get_connection()

    # logging statements for while loop
    logging.info("--- Flight Stream Started ---")
    logging.info(f"Loaded credentials for client: {CLIENT_ID}")
    logging.info(f"Polling every {INTERVAL}s...")

    while True:
        loop_start = time.time()

        # pulling data
        raw_json = fetch_flights(URL)

        if raw_json:
            success = insert_flight_data(conn, raw_json)

            if not success:
                conn = get_connection()
            else:
                # statement for record counts
                flight_count = len(raw_json.get("states", []))
                elapsed = time.time() - loop_start
                logging.info(f"SUCCESS - Loaded {flight_count} flights | {elapsed:.2f}s")

        # fine tuning the wait between hitting the api again
        used_time = time.time() - loop_start
        wait_time = max(0, INTERVAL - used_time)
        time.sleep(wait_time)


if __name__ == "__main__":
    try:
        run_stream()
    except KeyboardInterrupt:
        logging.info("Stream stopped by user.")
        conn.close()
        sys.exit(0)