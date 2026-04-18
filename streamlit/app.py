import streamlit as st
import requests
import pandas as pd
import os
 
st.set_page_config(
    page_title="Davidson County Traffic Dashboards",
    page_icon="🚦",
    layout="wide",
)
 
API_BASE = os.environ.get("API_BASE", "http://api:8000")
 
@st.cache_data(ttl=30)
def fetch_live_segments() -> pd.DataFrame:
    """Fetch live segment data from the FastAPI backend.
    Defined here so the cache is shared with the Live Map page,
    which must import and call this same function."""
    resp = requests.get(f"{API_BASE}/live/segments", timeout=15)
    if resp.status_code == 503:
        return pd.DataFrame()
    resp.raise_for_status()
    return pd.DataFrame(resp.json())
 
FOLDERS = ["by_hour", "by_day_of_week", "by_road_type", "by_direction", "top_segments"]

@st.cache_data(ttl=600)
def fetch_folder_year(folder: str, year: int) -> pd.DataFrame:
    try:
        res = requests.get(
            f"{API_BASE}/historical/{folder}",
            params={"year": year},
            timeout=5,
        )
        if res.status_code == 200:
            data = res.json()
            if data:
                return pd.DataFrame(data)
    except requests.exceptions.RequestException:
        pass
    return pd.DataFrame()


@st.cache_data(ttl=600)
def preload_year(year: int):
    return {f: fetch_folder_year(f, year) for f in FOLDERS}

# Warm the cache silently while the user reads the welcome page.
with st.spinner("Loading live traffic data…"):
    fetch_live_segments()
with st.spinner("Preparing historical analytics…"):
    preload_year(2023)  # <-- warms ALL historical datasets

 
st.title("🚦 Davidson County Traffic Dashboards")
 
st.markdown("""
Welcome! Use the sidebar to navigate between:
- **Live Map**: Real-time congestion and segment data
- **Batch Analysis**: Historical trends and aggregations by month
""")