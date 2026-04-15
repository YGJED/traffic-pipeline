import streamlit as st
import pandas as pd
import requests
import os

API_BASE = os.environ.get("API_BASE", "http://api:8000")

@st.cache_data(ttl=600)
def fetch_historical(folder: str, year: int, month: int) -> pd.DataFrame:
    try:
        resp = requests.get(
            f"{API_BASE}/historical/{folder}",
            params={"year": year, "month": month},
            timeout=30,
        )
        if resp.status_code == 404:
            return pd.DataFrame()
        resp.raise_for_status()
        return pd.DataFrame(resp.json())
    except Exception as e:
        st.error(f"Failed to load '{folder}' for {year}-{month:02d}: {e}")
        return pd.DataFrame()

import requests
import pandas as pd
import streamlit as st

@st.cache_data(ttl=600)
def load_year(year: int) -> dict[str, pd.DataFrame]:
    folders = ["by_hour", "by_day_of_week", "by_road_type", "by_direction", "top_segments"]
    combined = {f: [] for f in folders}

    for month in range(1, 13):
        for folder in folders:
            try:
                res = requests.get(
                    f"{API_BASE}/historical/{folder}",
                    params={"year": year, "month": month},
                    timeout=5
                )

                if res.status_code == 200:
                    data = res.json()
                    if data:  # avoid empty responses
                        df = pd.DataFrame(data)
                        df["month"] = month
                        combined[folder].append(df)

            except requests.exceptions.RequestException:
                print("EXCEPTION")
                continue  # skip failures (optional: log)

    return {
        folder: pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        for folder, frames in combined.items()
    }
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MONTH_LABELS = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

def month_over_month(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    """
    Pivot a concatenated df so each row is a month and each column is a
    group value, with avg_speed as the cell value. Adds a delta column
    showing change vs the previous month.
    """
    if df.empty or group_col not in df.columns:
        return pd.DataFrame()
    pivot = (
        df.groupby(["month", group_col])["avg_speed"]
        .mean()
        .unstack(group_col)
        .sort_index()
    )
    pivot.index = pivot.index.map(MONTH_LABELS)
    return pivot

def delta_table(pivot: pd.DataFrame) -> pd.DataFrame:
    """Return month-over-month difference for a pivot table."""
    return pivot.diff()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

years = [2023]

with st.sidebar:
    st.markdown("### Settings")
    sel_year = st.selectbox("Year", years, index=0)

    st.divider()
    st.markdown("### Drill Into Month")
    drill_month = st.selectbox(
        "Month (optional)",
        [None] + list(range(1, 13)),
        format_func=lambda m: "— Year overview —" if m is None else f"{MONTH_LABELS[m]} {sel_year}",
    )

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

with st.spinner("Loading full year…"):
    year_data = load_year(sel_year)

# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

if drill_month is None:
    st.title(f"📊 {sel_year} — Year Overview")
    st.caption("Showing month-over-month trends. Pick a month in the sidebar to drill in.")

    # --- Speed by Hour (heatmap-style pivot) ---
    st.subheader("Avg Speed by Hour — Monthly Trend")
    df = year_data["by_hour"]
    if not df.empty:
        pivot = month_over_month(df, "hour")
        if not pivot.empty:
            st.line_chart(pivot)
            with st.expander("Month-over-month Δ speed (by hour)"):
                deltas = delta_table(pivot).dropna()
                st.dataframe(deltas.style.background_gradient(cmap="RdYlGn", axis=None), use_container_width=True)
    else:
        st.info("No data available.")

    st.divider()
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Avg Speed by Day of Week")
        df = year_data["by_day_of_week"]
        if not df.empty:
            pivot = month_over_month(df, "day_of_week")
            if not pivot.empty:
                st.line_chart(pivot)
                with st.expander("Month-over-month Δ"):
                    st.dataframe(delta_table(pivot).dropna().style.background_gradient(cmap="RdYlGn", axis=None), use_container_width=True)
        else:
            st.info("No data available.")

    with c2:
        st.subheader("Avg Speed by Direction")
        df = year_data["by_direction"]
        if not df.empty:
            pivot = month_over_month(df, "direction")
            if not pivot.empty:
                st.line_chart(pivot)
                with st.expander("Month-over-month Δ"):
                    st.dataframe(delta_table(pivot).dropna().style.background_gradient(cmap="RdYlGn", axis=None), use_container_width=True)
        else:
            st.info("No data available.")

    st.divider()

    st.subheader("Avg Speed by Road Type — Monthly")
    df = year_data["by_road_type"]
    if not df.empty:
        pivot = month_over_month(df, "road_type")
        if not pivot.empty:
            st.bar_chart(pivot)
            with st.expander("Month-over-month Δ"):
                st.dataframe(delta_table(pivot).dropna().style.background_gradient(cmap="RdYlGn", axis=None), use_container_width=True)
    else:
        st.info("No data available.")

    st.divider()

    st.subheader("Most Congested Segments — Avg Across Year")
    df = year_data["top_segments"]
    if not df.empty:
        summary = (
            df.groupby(["xd_id", "road-name", "frc"])
            .agg(avg_congestion=("avg_congestion", "mean"), avg_speed=("avg_speed", "mean"))
            .reset_index()
            .sort_values("avg_congestion", ascending=False)
            .head(20)
        )
        st.dataframe(summary, use_container_width=True)
    else:
        st.info("No data available.")

# ---------------------------------------------------------------------------
# Drill-down: single month
# ---------------------------------------------------------------------------
else:
    month_label = MONTH_LABELS[drill_month]
    st.title(f"📊 {month_label} {sel_year} — Detail")
    st.caption("Showing single-month breakdown. Change the sidebar to return to the year overview.")

    def single_month(folder):
        df = year_data[folder]
        return df[df["month"] == drill_month].drop(columns=["month"]) if not df.empty else pd.DataFrame()

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Avg Speed by Hour")
        df = single_month("by_hour")
        if not df.empty:
            st.line_chart(df.set_index("hour")["avg_speed"])
        else:
            st.info("No data.")

    with c2:
        st.subheader("Avg Speed by Day of Week")
        df = single_month("by_day_of_week")
        if not df.empty:
            st.bar_chart(df.set_index("day_of_week")["avg_speed"])
        else:
            st.info("No data.")

    st.divider()
    c3, c4 = st.columns(2)

    with c3:
        st.subheader("Avg Speed by Road Type (FRC)")
        df = single_month("by_road_type")
        if not df.empty:
            st.bar_chart(df.set_index("road_type")["avg_speed"])
        else:
            st.info("No data.")

    with c4:
        st.subheader("Avg Speed by Direction")
        df = single_month("by_direction")
        if not df.empty:
            st.bar_chart(df.set_index("direction")["avg_speed"])
        else:
            st.info("No data.")

    st.divider()

    st.subheader("Top 20 Most Congested Segments")
    df = single_month("top_segments")
    if not df.empty:
        st.dataframe(df[["xd_id", "road-name", "bearing", "frc", "avg_congestion", "avg_speed"]], use_container_width=True)
    else:
        st.info("No data.")