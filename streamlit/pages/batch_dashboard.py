import streamlit as st
import pandas as pd
import requests
import os
import plotly.express as px

API_BASE = os.environ.get("API_BASE", "http://api:8000")

MONTH_LABELS = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

# Slideshow pages — top_segments is always rendered separately below
# We added a "type" key to distinguish between standard line/bars and our custom visual layouts
SLIDESHOW_PAGES = [
    # {"key": "by_hour",        "title": "Speed by Hour of Day",  "group_col": "hour", "type": "standard"},
    # {"key": "by_day_of_week", "title": "Speed by Day of Week",  "group_col": "day_of_week", "type": "standard"},
    {"key": "by_hour",        "title": "Rush Hour Shift Heatmap", "type": "heatmap"},
    {"key": "by_day_of_week", "title": "Weekend vs Weekday Seasonality", "type": "seasonality"},
    {"key": "by_road_type",   "title": "Speed by Road Type",    "group_col": "road_type", "type": "standard"},
    {"key": "by_direction",   "title": "Speed by Direction",    "group_col": "direction", "type": "standard"},
]

# ---------------------------------------------------------------------------
# Lazy per-folder, per-month loader
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600, show_spinner=False)
def fetch_folder_month(folder: str, year: int, month: int) -> pd.DataFrame:
    try:
        res = requests.get(
            f"{API_BASE}/historical/{folder}",
            params={"year": year, "month": month},
            timeout=5,
        )
        if res.status_code == 200:
            data = res.json()
            if data:
                df = pd.DataFrame(data)
                df["month"] = month
                return df
    except requests.exceptions.RequestException:
        pass
    return pd.DataFrame()


def load_folder(folder: str, year: int, months: list[int]) -> pd.DataFrame:
    """Fetch and concatenate a single folder across the requested months."""
    frames = [fetch_folder_month(folder, year, m) for m in months]
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def month_over_month(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
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
    return pivot.diff()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### Settings")
    sel_year = st.selectbox("Year", [2023], index=0)

    st.divider()
    st.markdown("### Filter by Month")
    drill_month = st.selectbox(
        "Month",
        [None] + list(range(1, 13)),
        format_func=lambda m: "Full year overview" if m is None else f"{MONTH_LABELS[m]} {sel_year}",
    )
    if drill_month is None:
        st.caption("Use ← → arrows to cycle through charts.")

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

if "page_idx" not in st.session_state:
    st.session_state.page_idx = 0

# Clamp in case SLIDESHOW_PAGES changes length
st.session_state.page_idx = min(st.session_state.page_idx, len(SLIDESHOW_PAGES) - 1)

months_to_load = [drill_month] if drill_month else list(range(1, 13))

# ===========================================================================
# MONTH DRILL-DOWN: all charts on one page, no slideshow
# ===========================================================================

if drill_month is not None:
    month_label = MONTH_LABELS[drill_month]
    st.title(f"📊 {month_label} {sel_year} — Detail")
    st.caption("Single-month breakdown across all metrics.")

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Speed by Hour of Day")
        with st.spinner("Loading…"):
            df = load_folder("by_hour", sel_year, months_to_load)
        if not df.empty:
            st.line_chart(df.groupby("hour")["avg_speed"].mean())
        else:
            st.info("No data.")

    with c2:
        st.subheader("Speed by Day of Week")
        with st.spinner("Loading…"):
            df = load_folder("by_day_of_week", sel_year, months_to_load)
        if not df.empty:
            st.bar_chart(df.groupby("day_of_week")["avg_speed"].mean())
        else:
            st.info("No data.")

    st.divider()
    c3, c4 = st.columns(2)

    with c3:
        st.subheader("Speed by Road Type (FRC)")
        with st.spinner("Loading…"):
            df = load_folder("by_road_type", sel_year, months_to_load)
        if not df.empty:
            st.bar_chart(df.groupby("road_type")["avg_speed"].mean())
        else:
            st.info("No data.")

    with c4:
        st.subheader("Speed by Direction")
        with st.spinner("Loading…"):
            df = load_folder("by_direction", sel_year, months_to_load)
        if not df.empty:
            st.bar_chart(df.groupby("direction")["avg_speed"].mean())
        else:
            st.info("No data.")

    st.divider()
    st.subheader("Top 20 Most Congested Segments")
    with st.spinner("Loading…"):
        df = load_folder("top_segments", sel_year, months_to_load)
    if not df.empty:
        display_df = (
            df[["xd_id", "road-name", "bearing", "frc", "avg_congestion", "avg_speed"]]
            .sort_values("avg_congestion", ascending=False)
            .head(20)
        )
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No data.")

# ===========================================================================
# YEAR OVERVIEW: slideshow for the 4 metric charts + custom insights + segments
# ===========================================================================

else:
    page_idx = st.session_state.page_idx
    current_page = SLIDESHOW_PAGES[page_idx]

    # --- Slideshow header with nav arrows ---
    left, center, right = st.columns([1, 6, 1])

    with left:
        st.write("")  # vertical nudge
        if page_idx > 0:
            if st.button("←", use_container_width=True):
                st.session_state.page_idx -= 1
                st.rerun()

    with center:
        st.markdown(
            f"<h2 style='text-align:center; margin:0'>{current_page['title']}</h2>"
            f"<p style='text-align:center; color:gray; margin:4px 0 0 0'>"
            f"{sel_year} Overview &nbsp;·&nbsp; {page_idx + 1} / {len(SLIDESHOW_PAGES)}</p>",
            unsafe_allow_html=True,
        )

    with right:
        st.write("")
        if page_idx < len(SLIDESHOW_PAGES) - 1:
            if st.button("→", use_container_width=True):
                st.session_state.page_idx += 1
                st.rerun()

    st.divider()

    # --- Lazy-load ONLY the visible slide's folder ---
    folder = current_page["key"]
    page_type = current_page.get("type", "standard")
    
    with st.spinner(f"Loading {current_page['title']}…"):
        df = load_folder(folder, sel_year, months_to_load)

    if df.empty:
        st.info("No data available for this view.")
    else:
        if page_type == "standard":
            group_col = current_page["group_col"]
            pivot = month_over_month(df, group_col)
            
            if not pivot.empty:
                chart_fn = st.line_chart if group_col in ("hour", "direction") else st.bar_chart
                chart_fn(pivot)
                with st.expander("Month-over-month Δ speed"):
                    deltas = delta_table(pivot).dropna()
                    st.dataframe(
                        deltas.style.background_gradient(cmap="RdYlGn", axis=None),
                        use_container_width=True,
                    )
            else:
                st.info("Not enough data to calculate month-over-month.")

        elif page_type == "heatmap":
            # --- Rush Hour Shift Heatmap ---
            df["Month Name"] = df["month"].map(MONTH_LABELS)
            
            # Aggregate to get a clean mean per month/hour just in case
            hm_df = df.groupby(["month", "Month Name", "hour"])["avg_congestion_score"].mean().reset_index()
            
            fig = px.density_heatmap(
                hm_df, 
                x="hour", 
                y="Month Name", 
                z="avg_congestion_score",
                histfunc="avg", 
                nbinsx=24,
                color_continuous_scale="OrRd", 
                labels={"hour": "Hour of Day", "Month Name": "Month", "avg_congestion_score": "Avg Congestion"}
            )
            
            # Lock the Y-axis so months stay chronological top-to-bottom
            ordered_months = [MONTH_LABELS[m] for m in range(1, 13)]
            fig.update_yaxes(categoryorder="array", categoryarray=list(reversed(ordered_months)))
            
            # Lock X-axis to standard 24 hours
            fig.update_xaxes(dtick=1)
            
            st.plotly_chart(fig, use_container_width=True)

        elif page_type == "seasonality":
            # --- Weekend vs Weekday Seasonality ---
            # PySpark dayofweek: 1 = Sunday, 7 = Saturday
            df["Day Type"] = df["day_of_week"].apply(lambda x: "Weekend" if x in [1, 7] else "Weekday")
            df["Month Name"] = df["month"].map(MONTH_LABELS)

            seas_df = df.groupby(["month", "Month Name", "Day Type"])["avg_speed"].mean().reset_index()
            seas_df = seas_df.sort_values("month")

            fig = px.bar(
                seas_df, 
                x="Month Name", 
                y="avg_speed", 
                color="Day Type", 
                barmode="group",
                color_discrete_sequence=["#1f77b4", "#ff7f0e"],
                labels={"avg_speed": "Average Speed", "Month Name": "Month", "Day Type": ""}
            )
            
            # Ensure months display chronologically left to right
            fig.update_xaxes(categoryorder="array", categoryarray=[MONTH_LABELS[m] for m in range(1, 13)])
            fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))

            st.plotly_chart(fig, use_container_width=True)

    # --- Congested segments always pinned below the slideshow ---
    st.divider()
    st.subheader("Most Congested Segments — Year Average")

    with st.spinner("Loading segments…"):
        seg_df = load_folder("top_segments", sel_year, months_to_load)

    if not seg_df.empty:
        summary = (
            seg_df.groupby(["xd_id", "road-name", "frc"])
            .agg(avg_congestion=("avg_congestion", "mean"), avg_speed=("avg_speed", "mean"))
            .reset_index()
            .sort_values("avg_congestion", ascending=False)
            .head(20)
        )
        st.dataframe(summary, use_container_width=True)
        st.caption("Averaged across all months in the selected year.")
    else:
        st.info("No segment data available.")