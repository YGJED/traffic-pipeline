import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from streamlit_autorefresh import st_autorefresh
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app import fetch_live_segments


st.set_page_config(
    page_title="Davidson County — Road Network",
    page_icon="🗺️",
    layout="wide",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'IBM Plex Sans', sans-serif;
    }
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }

    .top-bar {
        display: flex; align-items: baseline; gap: 16px;
        border-bottom: 2px solid #1a1a2e; padding-bottom: 10px; margin-bottom: 1rem;
    }
    .top-bar h1 {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 1.4rem; font-weight: 600; color: #1a1a2e; margin: 0;
    }
    .top-bar span {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.75rem; color: #888; letter-spacing: 0.08em;
    }

    .stat-card {
        background: #f7f7f9;
        border-left: 3px solid #1a1a2e;
        border-radius: 4px;
        padding: 10px 14px;
        font-family: 'IBM Plex Mono', monospace;
    }
    .stat-card .val { font-size: 1.5rem; font-weight: 600; color: #1a1a2e; }
    .stat-card .lbl { font-size: 0.7rem; color: #888; letter-spacing: 0.06em; text-transform: uppercase; margin-top: 2px; }

    .bearing-legend {
        display: flex; gap: 10px; flex-wrap: wrap;
        font-family: 'IBM Plex Mono', monospace; font-size: 0.75rem;
        margin-bottom: 0.5rem;
    }
    .bearing-pill {
        padding: 3px 10px; border-radius: 3px;
        font-weight: 600; color: white;
    }

    div[data-testid="stDataFrame"] { border: 1px solid #e9ecef; border-radius: 6px; }
</style>
""", unsafe_allow_html=True)

st_autorefresh(interval=10_000, key="live_autorefresh")

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

df, is_stale = fetch_live_segments()

if df.empty or "road_name" not in df.columns:
    st.warning("No data available. The data source may be unreachable or credentials may be misconfigured.")
    st.stop()

# if is_stale:
#     st.info("Data source temporarily unavailable (snapshot refresh in progress). Showing last known data.")

# Now use df for all filtering, mapping, and table display.
# For example, to create a display_name:
df["display_name"] = df["road_name"].fillna("").str.strip()
df["display_name"] = df.apply(
    lambda r: r["display_name"] if r["display_name"] else f"Segment {r['xd_id']}", axis=1
)
# Remove miles, zip, county, state, mid_lat, mid_long (not present in snapshot)
df["mid_lat"] = (df["start_lat"] + df["end_lat"]) / 2
df["mid_long"] = (df["start_long"] + df["end_long"]) / 2

# ---------------------------------------------------------------------------

# Congestion score → color (green = low, yellow = moderate, red = high)
def congestion_color(score):
    if pd.isna(score):
        return "#cccccc"  # gray for missing
    if score < 0.15:
        return "#16a34a"  # green
    elif score < 0.3:
        return "#facc15"  # yellow
    else:
        return "#dc2626"  # red

# Offset in degrees (~10–15 m) perpendicular to travel direction
OFFSET = 0.00012

BEARING_OFFSET = {
    "N": (-OFFSET, 0),       # northbound → shift west
    "S": ( OFFSET, 0),       # southbound → shift east
    "E": (0,  OFFSET),       # eastbound  → shift north
    "W": (0, -OFFSET),       # westbound  → shift south
    "?": (0,  0),
}

# FRC → line weight  (FRC 1=motorway, 5=local)
def frc_weight(frc):
    return {1: 6, 2: 5, 3: 5, 4: 4, 5: 3}.get(int(frc) if pd.notna(frc) else 4, 4)

def offset_coords(row):
    dlat, dlon = BEARING_OFFSET.get(row["bearing"], (0, 0))
    return [
        [row["start_lat"] + dlat, row["start_long"] + dlon],
        [row["end_lat"]   + dlat, row["end_long"]   + dlon],
    ]

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------


with st.sidebar:
    st.markdown("### Filters")

    # Road name filter
    road_names = sorted(df["display_name"].unique())
    sel_road = st.selectbox(
        "Road name (optional)",
        ["(All roads)"] + road_names,
        index=0,
        help="Show only segments for a specific road name"
    )

    frc_min, frc_max = int(df["frc"].min()), int(df["frc"].max())
    sel_frc = st.slider(
        "Functional Road Class (1=highway, 5=local)",
        frc_min, frc_max, (frc_min, frc_max)
    )

    st.markdown("---")

# Apply filters
mask = (
    df["frc"].between(sel_frc[0], sel_frc[1])
)
if sel_road != "(All roads)":
    mask = mask & (df["display_name"] == sel_road)

filtered = df[mask].copy()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

from datetime import datetime, timezone

data_ts = ""
if "window_end" in df.columns and not df["window_end"].isna().all():
    ts = pd.to_datetime(df["window_end"]).max()
    data_ts = f"DATA AS OF {ts.strftime('%Y-%m-%d %H:%M')} UTC"

refreshed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

st.markdown(f"""
<div class="top-bar">
  <h1>🗺️ Davidson County Road Network</h1>
  <br>
  <span>{len(filtered):,} of {len(df):,} segments</span>
  <span style="margin-left:auto">{data_ts} · REFRESHED {refreshed_at}</span>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Stat cards
# ---------------------------------------------------------------------------


c1, c2, c3, c4, c5 = st.columns(5)
avg_cong = filtered["avg_congestion"].mean() if not filtered.empty else float('nan')
stats = [
    (c1, len(filtered), "Segments shown"),
    (c2, f"{avg_cong:.2f}" if pd.notna(avg_cong) else "–", "Avg. congestion"),
]
for col, val, lbl in stats:
    col.markdown(f'<div class="stat-card"><div class="val">{val}</div><div class="lbl">{lbl}</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

if filtered.empty:
    st.warning("No segments match the current filters.")
    st.stop()

center_lat = filtered["mid_lat"].mean()
center_lon = filtered["mid_long"].mean()

fmap = folium.Map(
    location=[center_lat, center_lon],
    zoom_start=13,
    tiles="CartoDB positron",
    prefer_canvas=True,
)

features = []
for _, row in filtered.iterrows():
    coords = offset_coords(row)
    features.append({
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": [[c[1], c[0]] for c in coords],  # lon, lat
        },
        "properties": {
            "color": congestion_color(row.get("avg_congestion")),
            "weight": frc_weight(row["frc"]),
            "tooltip": f"{row['display_name']} · {row['bearing']}-bound · Cong: {row['avg_congestion']:.2f}",
        }
    })

geojson = {"type": "FeatureCollection", "features": features}

folium.GeoJson(
    geojson,
    style_function=lambda f: {
        "color": f["properties"]["color"],
        "weight": f["properties"]["weight"],
        "opacity": 0.85,
    },
    tooltip=folium.GeoJsonTooltip(fields=["tooltip"]),
).add_to(fmap)

# Map legend
legend_html = """
<div style=\"position:fixed;bottom:24px;left:24px;z-index:1000;
         background:white;padding:12px 16px;border-radius:6px;
         border:1px solid #ddd;font-family:'IBM Plex Mono',monospace;font-size:11px;
         box-shadow:0 2px 8px rgba(0,0,0,0.1)\">
    <b style=\"font-size:12px\">Congestion</b><br>
    <span style=\"color:#16a34a\">━━</span> Low (&lt; 0.15)<br>
    <span style=\"color:#facc15\">━━</span> Moderate (0.15–0.3)<br>
    <span style=\"color:#dc2626\">━━</span> High (&ge; 0.3)<br>
    <br>
    <b style=\"font-size:12px\">Line weight</b><br>
    Thicker = higher road class
</div>
"""
fmap.get_root().html.add_child(folium.Element(legend_html))

st_folium(fmap, height=560, width="stretch", returned_objects=[])
