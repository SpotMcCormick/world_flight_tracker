import streamlit as st
import polars as pl
import pandas as pd
import boto3
import io
import datetime
import os
from databricks import sql
from dotenv import load_dotenv
from pathlib import Path

# set up paths
ROOT_DIR = Path(__file__).parents[1]
load_dotenv(ROOT_DIR / ".env")

# aws config
BUCKET = "world-flight-tracker"
S3_KEY = "flights/lastest_gold/gold.parquet"
REFRESH_INTERVAL = 300

#databricks config
db_params = {
    "server_hostname": os.getenv("DATABRICKS_HOST"),
    "http_path": os.getenv("DATABRICKS_HTTP"),
    "access_token": os.getenv("DATABRICKS_TOKEN"),
}

st.set_page_config(page_title="World Flight Tracker", layout="wide")


@st.cache_data(ttl=REFRESH_INTERVAL, show_spinner=False)
def fetch_data_polars():
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=BUCKET, Key=S3_KEY)
        df = pl.read_parquet(io.BytesIO(obj["Body"].read()))
        return df.drop_nulls(subset=["latitude", "longitude", "origin_country"])
    except Exception as e:
        st.error(f"S3 Connection Error: {e}")
        return pl.DataFrame()


@st.cache_data(ttl=REFRESH_INTERVAL, show_spinner=False)
def fetch_analytics(country):
    try:
        connection = sql.connect(**db_params)
        cursor = connection.cursor()
        cursor.execute("""
            SELECT
                fly_date,
                SUM(fly_count) as flights,
                ROUND(AVG(hourly_avg_altitude), 2) as avg_altitude,
                ROUND(AVG(hourly_avg_velocity), 2) as avg_velocity
            FROM workspace.default.flight_analytics
            WHERE origin_country = ?
            GROUP BY fly_date
            ORDER BY fly_date DESC
        """, [country])
        result = cursor.fetchall()
        columns = [d[0] for d in cursor.description]
        cursor.close()
        connection.close()
        return pd.DataFrame(result, columns=columns)
    except Exception as e:
        st.error(f"Databricks Connection Error: {e}")
        return pd.DataFrame()


def main():
    if "selected_country" not in st.session_state:
        st.session_state.selected_country = None

    df = fetch_data_polars()

    if df.is_empty():
        st.warning("No valid data found in S3 bucket.")
        return

    # landing page
    if st.session_state.selected_country is None:
        _, center_col, _ = st.columns([1, 2, 1])

        with center_col:
            st.title("World Flight Tracker")
            countries = df["origin_country"].unique().sort().to_list()
            choice = st.selectbox("Search countries:", [""] + countries)
            if st.button("View Map", use_container_width=True) and choice != "":
                st.session_state.selected_country = choice
                st.rerun()

    #analytics and table
    else:
        target = st.session_state.selected_country

        filtered_df = (
            df.filter(pl.col("origin_country") == target)
            .with_columns([
                pl.col("latitude").cast(pl.Float64),
                pl.col("longitude").cast(pl.Float64)
            ])
        )

        if st.button("Back"):
            st.session_state.selected_country = None
            st.rerun()

        st.markdown(f"### {target}")

        # analytics from databricks
        analytics_df = fetch_analytics(target)

        if not analytics_df.empty:
            st.dataframe(
                analytics_df,
                column_config={
                    "fly_date": "Daily Date",
                    "flights": "Daily Flights",
                    "avg_altitude": "Daily Avg Altitude (m)",
                    "avg_velocity": "Daily Avg Velocity (m/s)"
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info("No historical data available yet.")

        # map of live flights
        st.markdown("### Current Flight Locations")
        if not filtered_df.is_empty():
            st.map(
                data=filtered_df.to_pandas(),
                latitude="latitude",
                longitude="longitude",
                color='#0080ff',
                size=15
            )
        else:
            st.info(f"No active flights for {target}.")

        st.caption(f"Sync: {datetime.datetime.now().strftime('%H:%M:%S')}")

        @st.fragment(run_every=REFRESH_INTERVAL)
        def auto_sync():
            fetch_data_polars()
            st.rerun()

        auto_sync()


if __name__ == "__main__":
    main()