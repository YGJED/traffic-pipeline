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
 
# Warm the cache silently while the user reads the welcome page.
with st.spinner("Loading live traffic data…"):
    fetch_live_segments()
 
st.title("🚦 Davidson County Traffic Dashboards")
 
st.markdown("""
Welcome! Use the sidebar to navigate between:
- **Live Map**: Real-time congestion and segment data
- **Batch Analysis**: Historical trends and aggregations by month
""")