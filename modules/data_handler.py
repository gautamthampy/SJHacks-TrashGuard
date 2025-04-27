import streamlit as st
import requests
import pandas as pd
import datetime
import json
import numpy as np
import logging

logger = logging.getLogger(__name__)

CKAN_URL_BASE = "https://data.sanjoseca.gov"
SQL_ENDPOINT = "/api/3/action/datastore_search_sql"
CACHE_TTL = 6 * 60 * 60

@st.cache_data(ttl=CACHE_TTL)
def load_and_process_data(resource_id: str) -> tuple[pd.DataFrame | None, list | None]:
    """
    Fetches illegal dumping data using CKAN SQL endpoint (filtering only by Service Type)
    and then filters by date using Pandas for robustness.
    Uses correct column names like "Date Created", "Service Type", "Latitude", "Longitude".
    Returns the processed DataFrame and a list of hotspot data points.
    """
    logger.info(f"Fetching data via SQL (Service Type filter only) from CKAN Resource ID: {resource_id}")
    if not resource_id:
        st.error("CKAN Resource ID not found in secrets.")
        return None, None

    sql_query = f"""
    SELECT
        "Latitude",
        "Longitude",
        "Status",
        "Date Created"
    FROM
        "{resource_id}"
    WHERE
        "Service Type" = 'Illegal Dumping'
    LIMIT 100000
    """
    logger.info(f"Executing CKAN SQL Query:\n{sql_query}")

    try:
        response = requests.get(
            f"{CKAN_URL_BASE}{SQL_ENDPOINT}",
            params={'sql': sql_query},
            timeout=60
        )
        response.raise_for_status()
        data = response.json()

        if not data.get('success'):
            error_details = data.get('error', {})
            error_message = error_details.get('message', 'Unknown CKAN error')
            st.error(f"CKAN API returned an error: {error_message}")
            logger.error(f"CKAN Error: {error_message} | Details: {error_details}")
            return None, None

        records = data.get("result", {}).get("records", [])
        if not records:
            st.warning("No 'Illegal Dumping' records found via SQL based on 'Service Type'.")
            return pd.DataFrame(), [] # Return empty, not None

    except Exception as e:
        st.error(f"CKAN Fetch Error: {e}")
        logger.error(f"CKAN Fetch Error: {e}", exc_info=True)
        return None, None

    df = pd.DataFrame(records)
    initial_record_count = len(df)
    logger.info(f"Fetched {initial_record_count} raw 'Illegal Dumping' records via SQL.")

    # Date Conversion and Filtering
    date_col = "Date Created"
    if date_col not in df.columns:
        st.error(f"Critical '{date_col}' column missing. Cannot filter by date.")
        return None, None
    try:
        df['created_datetime'] = pd.to_datetime(df[date_col], errors='coerce')
        original_rows = len(df)
        df.dropna(subset=['created_datetime'], inplace=True)
        valid_date_rows = len(df)
        if original_rows > valid_date_rows:
            st.warning(f"Could not parse '{date_col}' for {original_rows - valid_date_rows} rows.")
        if df.empty:
            st.warning(f"No records remaining after parsing '{date_col}'.")
            return pd.DataFrame(), []

        cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=90)
        if df['created_datetime'].dt.tz is None:
            cutoff_date = cutoff_date.replace(tzinfo=None)
        df = df[df['created_datetime'] >= cutoff_date].copy()
        logger.info(f"Filtered down to {len(df)} records within the last 90 days using Pandas.")
        if df.empty:
            st.warning("No 'Illegal Dumping' records found within the last 90 days.")
            return pd.DataFrame(), []

    except Exception as e:
        st.error(f"Error processing dates: {e}")
        logger.error(f"Date processing error: {e}", exc_info=True)
        return None, None

    # Lat/Lon Cleaning
    lat_col = "Latitude"; lon_col = "Longitude"
    if not all(col in df.columns for col in [lat_col, lon_col]):
         st.error(f"Critical '{lat_col}' or '{lon_col}' column missing.")
         return None, None
    df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce')
    df[lon_col] = pd.to_numeric(df[lon_col], errors='coerce')
    original_rows_before_latlon_drop = len(df)
    df.dropna(subset=[lat_col, lon_col], inplace=True)
    if len(df) < original_rows_before_latlon_drop:
         logger.info(f"Dropped {original_rows_before_latlon_drop - len(df)} rows missing valid lat/lon.")
    if df.empty:
        st.warning("No records with valid latitude/longitude found.")
        return pd.DataFrame(), []

    # Grid & Weight for Heatmap
    try:
        df = df.assign(
            tile=list(zip(np.round(df[lat_col] * 500), np.round(df[lon_col] * 500)))
        )
        heat_df = df.groupby("tile").size().reset_index(name="cnt")
        heat_df["lat"] = heat_df["tile"].apply(lambda t: t[0] / 500)
        heat_df["lng"] = heat_df["tile"].apply(lambda t: t[1] / 500)
        max_weight = 15
        hotspots_data = [
            {"location": {"lat": row.lat, "lng": row.lng}, "weight": min(row.cnt, max_weight)}
            for row in heat_df.itertuples()
        ]
        logger.info(f"Data processed: {len(df)} final records, {len(hotspots_data)} hotspots.")
        return df, hotspots_data

    except Exception as e:
        st.error(f"Error creating heatmap data: {e}")
        logger.error(f"Heatmap processing error: {e}", exc_info=True)
        return None, None