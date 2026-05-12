import streamlit as st
import polars as pl
import boto3
import io
import datetime

# --- Configuration ---
BUCKET = "world-flight-tracker"
S3_KEY = "flights/lastest_gold/gold.parquet"
REFRESH_INTERVAL = 300 

st.set_page_config(page_title="World Flight Tracker", layout="wide")

@st.cache_data(ttl=REFRESH_INTERVAL, show_spinner=False)
def fetch_data_polars():
    """Fetches flight data and cleans nulls immediately."""
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=BUCKET, Key=S3_KEY)
        df = pl.read_parquet(io.BytesIO(obj["Body"].read()))
        # Clean data for stability
        return df.drop_nulls(subset=["latitude", "longitude", "origin_country"])
    except Exception as e:
        st.error(f"S3 Connection Error: {e}")
        return pl.DataFrame()

def main():
    if "selected_country" not in st.session_state:
        st.session_state.selected_country = None

    df = fetch_data_polars()

    if df.is_empty():
        st.warning("No valid data found in S3 bucket.")
        return

    # --- VIEW 1: Country Selection (Centered) ---
    if st.session_state.selected_country is None:
        # 1:2:1 ratio centers the middle column
        _, center_col, _ = st.columns([1, 2, 1])
        
        with center_col:
            st.title("World Flight Tracker")
            countries = df["origin_country"].unique().sort().to_list()
            choice = st.selectbox("Search countries:", [""] + countries)
            if st.button("View Map", use_container_width=True) and choice != "":
                st.session_state.selected_country = choice
                st.rerun()

    # --- VIEW 2: The Map ---
    else:
        target = st.session_state.selected_country
        
        filtered_df = (
            df.filter(pl.col("origin_country") == target)
            .with_columns([
                pl.col("latitude").cast(pl.Float64),
                pl.col("longitude").cast(pl.Float64)
            ])
        )

        # Basic Nav
        if st.button("← Back"):
            st.session_state.selected_country = None
            st.rerun()

        if not filtered_df.is_empty():
            # Centering logic for the map engine
            avg_lat = filtered_df["latitude"].mean()
            avg_lon = filtered_df["longitude"].mean()
            
            # st.map uses the data's center by default, 
            # but providing clean data ensures it snaps correctly.
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