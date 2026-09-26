import yaml
import logging
import os
import boto3
import tempfile
import clickhouse_connect
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
import polars as pl

from logging_config import ClickHouseLogHandler, build_email_handler, RunIdFilter, new_run_id

# set up paths
ROOT_DIR = Path(__file__).parents[1]
load_dotenv(ROOT_DIR / ".env")

# config yaml (loaded early, logging setup needs it)
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
    LOG_DIR / "s3_hourly_upload.log", when="midnight", backupCount=30
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

# quiet noisy third-party loggers (boto3 credential lookups, etc.)
for noisy_logger in ("boto3", "botocore", "urllib3", "s3transfer"):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)

logging.info(f"Run ID: {RUN_ID}")

# query for previous complete hour
QUERY = """
SELECT
    origin_country,
    toDate(time_position) AS fly_date,
    toHour(time_position) AS fly_hour,
    COUNT(DISTINCT icao24) AS fly_count,
    ROUND(AVG(velocity), 2) AS hourly_avg_velocity,
    ROUND(AVG(baro_altitude), 2) AS hourly_avg_altitude
FROM dev_world_flight_tracker.mart_flight_data
WHERE time_position >= toStartOfHour(now()) - INTERVAL 1 HOUR
    AND time_position < toStartOfHour(now())
    AND on_ground = FALSE
GROUP BY origin_country, fly_date, fly_hour
ORDER BY fly_hour, origin_country
"""

# aws config
BUCKET = config["s3_bucket"]
S3_KEY = config["s3_key"]


def query_clickhouse():
    try:
        client = clickhouse_connect.get_client(**clickhouse_params)
        result = client.query(QUERY)
        df = pl.DataFrame(result.result_rows, schema=result.column_names, orient="row")
        client.close()

        logging.info(f"Query returned {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"QUERY ERROR - {e}")
        return None


def upload_to_s3(df):
    try:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name

        df.write_parquet(tmp_path, compression="snappy")

        data_hour = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        s3_key = f"{S3_KEY}{data_hour.year}/{data_hour.month:02d}/{data_hour.day:02d}/{data_hour.hour:02d}/flights.parquet"

        s3 = boto3.client("s3")
        s3.upload_file(tmp_path, BUCKET, s3_key)

        logging.info(f"SUCCESS - Uploaded to s3://{BUCKET}/{s3_key}")

    except Exception as e:
        logging.error(f"S3 UPLOAD ERROR - {e}")


if __name__ == "__main__":
    try:
        df = query_clickhouse()
        if df is not None:
            upload_to_s3(df)
    except Exception:
        logging.critical("Hourly upload pipeline crashed", exc_info=True)