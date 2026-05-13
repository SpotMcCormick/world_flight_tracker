import psycopg2
import yaml
import logging
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path
import polars as pl

# set up paths
ROOT_DIR = Path(__file__).parents[1]
load_dotenv(ROOT_DIR / ".env")

# logs
LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "s3_hourly_upload.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# config yaml
with open(ROOT_DIR / "config.yaml") as c:
    config = yaml.safe_load(c)

# Postgres Database
db_params = {
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT")
}

conn_uri = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"

# query for previous complete hour
QUERY = """
    SELECT DISTINCT 
        origin_country, 
        time_position::DATE AS fly_date, 
        EXTRACT(HOUR FROM time_position) AS fly_hour, 
        COUNT(DISTINCT icao24) AS fly_count,
        ROUND(AVG(velocity), 2) AS hourly_avg_velocity,
        ROUND(AVG(baro_altitude), 2) AS hourly_avg_altitude
    FROM dev_env.dm_flight_data
    WHERE 1=1
        AND time_position >= date_trunc('hour', NOW()) - INTERVAL '1 hour'
        AND time_position < date_trunc('hour', NOW())
        AND on_ground = FALSE
    GROUP BY origin_country, DATE(time_position), EXTRACT(HOUR FROM time_position)
    ORDER BY fly_hour, origin_country
"""

# aws config 
BUCKET = config["s3_bucket"]
S3_KEY = config["s3_key"]


def query_postgres():
    try:
        df = pl.read_database_uri(query=QUERY, uri=conn_uri)
        logging.info(f"Query returned {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"QUERY ERROR - {e}")
        return None


def upload_to_s3(df):
    try:
        s3_path = f"s3://{BUCKET}/{S3_KEY}"
        df.write_parquet(
            s3_path,
            compression="snappy",
             storage_options={
                "region": "us-east-1",
                "endpoint_url": "https://s3.amazonaws.com"
            },
            use_pyarrow=True,
            partition_by=["fly_date", "fly_hour"],
        )
        logging.info(f"SUCCESS - Uploaded to s3://world-flight-tracker/flights/")
    except Exception as e:
        logging.error(f"S3 UPLOAD ERROR - {e}")


if __name__ == "__main__":
    df = query_postgres()
    if df is not None:
        upload_to_s3(df)