import streamlit as st
import pandas as pd
import plotly.express as px
from app import preload_year

MONTH_LABELS = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

SLIDESHOW_PAGES = [
    {"key": "by_hour",        "title": "Rush Hour Shift Heatmap", "type": "heatmap"},
    {"key": "by_day_of_week", "title": "Weekend vs Weekday Seasonality", "type": "seasonality"},
    {"key": "by_road_type",   "title": "Speed by Road Type",    "group_col": "road_type", "type": "standard"},
    {"key": "by_direction",   "title": "Speed by Direction",    "group_col": "direction", "type": "standard"},
]

# ---------------------------------------------------------------------------
# Data loading (YEAR-LEVEL ONLY)
# ---------------------------------------------------------------------------

def load_folder(data_cache, folder: str, months: list[int]) -> pd.DataFrame:
    df = data_cache.get(folder, pd.DataFrame())

    if df.empty:
        return df

    # Filter if single month selected
    if months and len(months) == 1:
        return df[df["month"] == months[0]]

    return df


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
# Preload ALL data once
# ---------------------------------------------------------------------------

data_cache = preload_year(sel_year)

months_to_load = [drill_month] if drill_month else list(range(1, 13))

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

if "page_idx" not in st.session_state:
    st.session_state.page_idx = 0

st.session_state.page_idx = min(st.session_state.page_idx, len(SLIDESHOW_PAGES) - 1)

# ===========================================================================
# MONTH DRILL-DOWN
# ===========================================================================

if drill_month is not None:
    month_label = MONTH_LABELS[drill_month]
    st.title(f"📊 {month_label} {sel_year} — Detail")

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Speed by Hour of Day")
        df = load_folder(data_cache, "by_hour", months_to_load)
        if not df.empty:
            st.line_chart(df.groupby("hour")["avg_speed"].mean())
        else:
            st.info("No data.")

    with c2:
        st.subheader("Speed by Day of Week")
        df = load_folder(data_cache, "by_day_of_week", months_to_load)
        if not df.empty:
            st.bar_chart(df.groupby("day_of_week")["avg_speed"].mean())
        else:
            st.info("No data.")

    st.divider()
    c3, c4 = st.columns(2)

    with c3:
        st.subheader("Speed by Road Type (FRC)")
        df = load_folder(data_cache, "by_road_type", months_to_load)
        if not df.empty:
            st.bar_chart(df.groupby("road_type")["avg_speed"].mean())
        else:
            st.info("No data.")

    with c4:
        st.subheader("Speed by Direction")
        df = load_folder(data_cache, "by_direction", months_to_load)
        if not df.empty:
            st.bar_chart(df.groupby("direction")["avg_speed"].mean())
        else:
            st.info("No data.")

    st.divider()
    st.subheader("Top 20 Most Congested Segments")
    df = load_folder(data_cache, "top_segments", months_to_load)

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
# YEAR OVERVIEW (SLIDESHOW)
# ===========================================================================

else:
    page_idx = st.session_state.page_idx
    current_page = SLIDESHOW_PAGES[page_idx]

    left, center, right = st.columns([1, 6, 1])

    with left:
        if page_idx > 0:
            if st.button("←", use_container_width=True):
                st.session_state.page_idx -= 1
                st.rerun()

    with center:
        st.markdown(
            f"<h2 style='text-align:center'>{current_page['title']}</h2>",
            unsafe_allow_html=True,
        )

    with right:
        if page_idx < len(SLIDESHOW_PAGES) - 1:
            if st.button("→", use_container_width=True):
                st.session_state.page_idx += 1
                st.rerun()

    st.divider()

    folder = current_page["key"]
    page_type = current_page.get("type", "standard")
    df = load_folder(data_cache, folder, months_to_load)

    if df.empty:
        st.info("No data available.")
    else:
        if page_type == "standard":
            group_col = current_page["group_col"]
            pivot = month_over_month(df, group_col)

            if not pivot.empty:
                chart_fn = st.line_chart if group_col in ("hour", "direction") else st.bar_chart
                chart_fn(pivot)

        elif page_type == "heatmap":
            df["Month Name"] = df["month"].map(MONTH_LABELS)
            hm_df = df.groupby(["month", "Month Name", "hour"])["avg_congestion_score"].mean().reset_index()

            pivot = (
                hm_df.pivot_table(index="month", columns="hour", values="avg_congestion_score")
                .reindex(columns=range(24))
                .sort_index()
            )
            pivot.index = [MONTH_LABELS[m] for m in pivot.index]

            fig = px.imshow(
                pivot,
                labels={"x": "Hour of Day", "y": "Month", "color": "Avg Congestion"},
                color_continuous_scale="OrRd",
                aspect="auto",
            )
            fig.update_xaxes(tickmode="linear", tick0=0, dtick=1)
            st.plotly_chart(fig, use_container_width=True)

        elif page_type == "seasonality":
            df["Day Type"] = df["day_of_week"].apply(lambda x: "Weekend" if x in [1, 7] else "Weekday")
            df["Month Name"] = df["month"].map(MONTH_LABELS)

            seas_df = df.groupby(["month", "Month Name", "Day Type"])["avg_speed"].mean().reset_index()

            fig = px.bar(seas_df, x="Month Name", y="avg_speed", color="Day Type", barmode="group")
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Most Congested Segments — Year Average")

    seg_df = load_folder(data_cache, "top_segments", months_to_load)

    if not seg_df.empty:
        summary = (
            seg_df.groupby(["xd_id", "road-name", "frc"])
            .agg(avg_congestion=("avg_congestion", "mean"), avg_speed=("avg_speed", "mean"))
            .reset_index()
            .sort_values("avg_congestion", ascending=False)
            .head(20)
        )
        st.dataframe(summary, use_container_width=True)
    else:
        st.info("No segment data available.")