import clickhouse_connect
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
from logging.handlers import TimedRotatingFileHandler

from logging_config import ClickHouseLogHandler, build_email_handler, RunIdFilter, new_run_id



# set up paths
ROOT_DIR = Path(__file__).parents[1]
load_dotenv(ROOT_DIR / ".env")

# config yaml (loaded early, logging setup needs it)ALERT_EMAIL=
with open(ROOT_DIR / "config.yaml") as c:
    config = yaml.safe_load(c)

# ClickHouse Database
clickhouse_params = {
    "host": os.getenv("CLICKHOUSE_HOST"),
    "port": int(os.getenv("CLICKHOUSE_PORT")),
    "database": os.getenv("CLICKHOUSE_DB"),
    "username": os.getenv("CLICKHOUSE_USER"),
    "password": os.getenv("CLICKHOUSE_PASSWORD")
}

# logs
LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

RUN_ID = new_run_id()

file_handler = TimedRotatingFileHandler(
    LOG_DIR / "opensky_flight_ex.log", when="midnight", backupCount=30
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(filename)s:%(funcName)s:%(lineno)d | run %(run_id)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[file_handler]
)

logging.getLogger().addFilter(RunIdFilter(RUN_ID))

logging.getLogger().addHandler(
    ClickHouseLogHandler(clickhouse_params, table=config["log_table"])
)
logging.getLogger().addHandler(build_email_handler())

logging.info(f"Run ID: {RUN_ID}")

# url for the api data
URL = config["api_get_endpoint"]

# url for refreshing token for authentication
TOKEN_URL = config["api_post_endpoint"]

# credentials for openflight api
CREDENTIALS_FILE = ROOT_DIR / "credentials.json"

# poll API every 5 minutes
INTERVAL = 300

# cooldown if API rate limits us
COOL_DOWN = 300

# open sky credentials file loads
with open(CREDENTIALS_FILE) as f:
    creds = json.load(f)

CLIENT_ID = creds["clientId"]
CLIENT_SECRET = creds["clientSecret"]


# setting the token state
token = None
expires_at = None


# token helpers
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
            conn = clickhouse_connect.get_client(**clickhouse_params)

            # test connection
            conn.command("SELECT 1")

            logging.info("Connected to ClickHouse.")

            return conn

        except Exception as e:
            logging.warning(
                f"Connection attempt {attempt + 1} failed: {e}"
            )
            time.sleep(5)

    raise Exception("Could not connect to ClickHouse after 3 attempts")


# Insert one API response as one row
def insert_flight_data(conn, raw_json):
    try:
        conn.insert(
            "dev_world_flight_tracker.stg_flight_data",
            [(json.dumps(raw_json),)],
            column_names=["data"]
        )

        return True

    except Exception as e:
        logging.error(f"DATABASE ERROR - {e}")
        return False


def fetch_flights(url):
    try:
        response = requests.get(
            url,
            headers=get_headers(),
            timeout=20
        )

        # error handling for too many requests
        if response.status_code == 429:
            logging.warning(
                f"Rate limited. Cooling down {COOL_DOWN}s..."
            )
            time.sleep(COOL_DOWN)
            return None

        # error handling if token expires
        if response.status_code == 401:
            logging.warning(
                "Token rejected. Refreshing..."
            )
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

    try:
        logging.info("--- Flight Stream Started ---")
        logging.info(
            f"Loaded credentials for client: {CLIENT_ID}"
        )
        logging.info(
            f"Polling every {INTERVAL}s..."
        )

        while True:
            loop_start = time.time()

            # pulling data
            raw_json = fetch_flights(URL)

            if raw_json:
                success = insert_flight_data(
                    conn,
                    raw_json
                )

                if not success:
                    logging.warning(
                        "Lost DB connection. Reconnecting..."
                    )

                    try:
                        conn.close()
                    except Exception:
                        pass

                    conn = get_connection()

                else:
                    # statement for record counts
                    flight_count = len(
                        raw_json.get("states", [])
                    )

                    elapsed = time.time() - loop_start

                    logging.info(
                        f"SUCCESS - Loaded {flight_count} flights "
                        f"| 1 row | {elapsed:.2f}s"
                    )

            # wait until 5 minutes from start of this cycle
            used_time = time.time() - loop_start
            wait_time = max(
                0,
                INTERVAL - used_time
            )

            time.sleep(wait_time)

    finally:
        conn.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    try:
        run_stream()
    except KeyboardInterrupt:
        logging.info("Stream stopped by user.")
        sys.exit(0)
    except Exception:
        logging.critical("Pipeline crashed", exc_info=True)
        sys.exit(1)