import psycopg2
import requests
import json
import time
import sys
import os
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Setup paths and environment
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

# Configuration
URL = "https://opensky-network.org/api/states/all"
INTERVAL = 120  # 2 minutes
COOL_DOWN = 300 # 5 minutes if rate limited

db_params = {
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT")
}

print(f"--- Flight Stream Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
print(f"Interval set to {INTERVAL}s. Monitoring OpenSky API...")

try:
    while True:
        loop_start = time.time()
        
        try:
            # 1. Extraction
            response = requests.get(URL, timeout=20)
            
            # Specifically handle the 429 Rate Limit
            if response.status_code == 429:
                print(f"ERROR: {datetime.now().strftime('%H:%M:%S')} - Rate Limit (429). Waiting 5 mins...")
                time.sleep(COOL_DOWN)
                continue # Skip the rest of this loop and try again
                
            response.raise_for_status()
            raw_json = response.json()
            
            # Count flights
            flight_count = len(raw_json.get("states", [])) if raw_json.get("states") else 0
            
            # 2. Load to Postgres
            with psycopg2.connect(**db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO dev_env.stg_flight_data (data) VALUES (%s)", 
                        (json.dumps(raw_json),)
                    )
            
            elapsed = time.time() - loop_start
            print(f"SUCCESS: Loaded {flight_count} flights | Process Time: {elapsed:.2f}s")

        except requests.exceptions.RequestException as e:
            print(f"NETWORK ERROR: {datetime.now().strftime('%H:%M:%S')} - {e}")
        except psycopg2.Error as e:
            print(f"DATABASE ERROR: {datetime.now().strftime('%H:%M:%S')} - {e}")
        except Exception as e:
            print(f"UNEXPECTED ERROR: {datetime.now().strftime('%H:%M:%S')} - {e}")

        # 3. Dynamic sleep logic
        # Forces the loop to restart exactly at the INTERVAL mark
        used_time = time.time() - loop_start
        wait_time = max(0, INTERVAL - used_time)
        
        # Ensure output is written to the log file immediately
        sys.stdout.flush()
        
        time.sleep(wait_time)

except KeyboardInterrupt:
    print("\nStream stopped by user.")
    sys.exit(0)