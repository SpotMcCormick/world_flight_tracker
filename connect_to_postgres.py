import psycopg2
import requests
import json
import time
import sys
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

# Setup paths and environment
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

# --- Config ---
URL = "https://opensky-network.org/api/states/all"
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
CREDENTIALS_FILE = ROOT_DIR / "credentials.json"
INTERVAL = 120   # 2 minutes
COOL_DOWN = 300  # 5 min on rate limit

db_params = {
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT")
}

# --- Load credentials.json ---
with open(CREDENTIALS_FILE) as f:
    creds = json.load(f)

CLIENT_ID = creds["clientId"]
CLIENT_SECRET = creds["clientSecret"]

# --- Token Manager ---
class TokenManager:
    def __init__(self):
        self.token = None
        self.expires_at = None

    def get_token(self):
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self):
        print(f"INFO: {datetime.now().strftime('%H:%M:%S')} - Refreshing OAuth token...")
        r = requests.post(TOKEN_URL, data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }, timeout=10)
        r.raise_for_status()
        data = r.json()
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.now() + timedelta(seconds=expires_in - 30)
        print(f"INFO: Token valid for {expires_in}s")
        return self.token

    def headers(self):
        return {"Authorization": f"Bearer {self.get_token()}"}

    def invalidate(self):
        self.token = None

tokens = TokenManager()

# --- Main Loop ---
print(f"--- Flight Stream Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
print(f"Loaded credentials for client: {CLIENT_ID}")
print(f"Polling every {INTERVAL}s...")

try:
    while True:
        loop_start = time.time()

        try:
            response = requests.get(URL, headers=tokens.headers(), timeout=20)

            if response.status_code == 429:
                print(f"WARN: {datetime.now().strftime('%H:%M:%S')} - Rate limited (429). Cooling down {COOL_DOWN}s...")
                time.sleep(COOL_DOWN)
                continue

            if response.status_code == 401:
                print(f"WARN: {datetime.now().strftime('%H:%M:%S')} - Token rejected (401). Forcing refresh...")
                tokens.invalidate()
                continue

            response.raise_for_status()
            raw_json = response.json()
            flight_count = len(raw_json.get("states", []) or [])

            with psycopg2.connect(**db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO dev_env.stg_flight_data (data) VALUES (%s)",
                        (json.dumps(raw_json),)
                    )

            elapsed = time.time() - loop_start
            print(f"SUCCESS: {datetime.now().strftime('%H:%M:%S')} - Loaded {flight_count} flights | {elapsed:.2f}s")

        except requests.exceptions.RequestException as e:
            print(f"NETWORK ERROR: {datetime.now().strftime('%H:%M:%S')} - {e}")
        except psycopg2.Error as e:
            print(f"DATABASE ERROR: {datetime.now().strftime('%H:%M:%S')} - {e}")
        except Exception as e:
            print(f"UNEXPECTED ERROR: {datetime.now().strftime('%H:%M:%S')} - {e}")

        used_time = time.time() - loop_start
        wait_time = max(0, INTERVAL - used_time)
        sys.stdout.flush()
        time.sleep(wait_time)

except KeyboardInterrupt:
    print("\nStream stopped by user.")
    sys.exit(0)