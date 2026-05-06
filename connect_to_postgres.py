import psycopg2
import requests
import json
import time
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")


URL = "https://opensky-network.org/api/states/all"
db_params = {
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT")
}

# 10 seconds is aggressive for global data; 15-20s is safer for a 2010 CPU
INTERVAL = 120

print(f"--- Flight Stream Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

try:
    while True:
        loop_start = time.time()
        
        try:
            # Extraction
            response = requests.get(URL, timeout=15)
            response.raise_for_status()
            raw_json = response.json()
            
            # Count flights for the print statement
            flight_count = len(raw_json.get("states", [])) if raw_json.get("states") else 0
            
            # Load
            with psycopg2.connect(**db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO dev_env.stg_flight_data (data) VALUES (%s)", (json.dumps(raw_json),))
            
            elapsed = time.time() - loop_start
            print(f"SUCCESS: Loaded {flight_count} flights into dev_env.stg_flight_data | Process Time: {elapsed:.2f}s")

        except Exception as e:
            print(f"ERROR: {datetime.now().strftime('%H:%M:%S')} - {e}")

        # Dynamic sleep: ensures we start a new request exactly every INTERVAL seconds
        # regardless of how long the previous request took.
        wait_time = max(0, INTERVAL - (time.time() - loop_start))
        time.sleep(wait_time)

except KeyboardInterrupt:
    print("\nStream stopped by user.")