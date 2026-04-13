import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from pathlib import Path

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

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

CSV_PATH = Path("XD_identification.csv")

@st.cache_data
def load_segments(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()

    # Rename to safe internal names
    df = df.rename(columns={
        "xd":               "xd_id",
        "road-name":        "road_name",
        "road-num":         "road_num",
        "bearing":          "bearing",
        "miles":            "miles",
        "frc":              "frc",
        "county":           "county",
        "state":            "state",
        "zip":              "zip",
        "timezone_name":    "timezone",
        "start_latitude":   "start_lat",
        "start_longitude":  "start_lon",
        "end_latitude":     "end_lat",
        "end_longitude":    "end_lon",
    })

    df["road_name"] = df["road_name"].fillna("").str.strip()
    df["bearing"]   = df["bearing"].fillna("?").str.strip().str.upper()
    df["display_name"] = df.apply(
        lambda r: r["road_name"] if r["road_name"] else f"Segment {r['xd_id']}", axis=1
    )
    df["mid_lat"] = (df["start_lat"] + df["end_lat"]) / 2
    df["mid_lon"] = (df["start_lon"] + df["end_lon"]) / 2
    return df

if not CSV_PATH.exists():
    st.error(f"Could not find `{CSV_PATH}` — place it in the same directory as app.py.")
    st.stop()

df = load_segments(CSV_PATH)

# ---------------------------------------------------------------------------
# Bearing → color + lateral offset so N/S and E/W pairs don't overlap
# ---------------------------------------------------------------------------

BEARING_COLORS = {
    "N": "#2563eb",   # blue
    "S": "#dc2626",   # red
    "E": "#16a34a",   # green
    "W": "#d97706",   # amber
    "?": "#6b7280",   # gray fallback
}

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
        [row["start_lat"] + dlat, row["start_lon"] + dlon],
        [row["end_lat"]   + dlat, row["end_lon"]   + dlon],
    ]

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### Filters")

    all_bearings = sorted(df["bearing"].unique())
    sel_bearings = st.multiselect(
        "Bearing", all_bearings, default=all_bearings,
        help="Filter by travel direction"
    )

    frc_min, frc_max = int(df["frc"].min()), int(df["frc"].max())
    sel_frc = st.slider(
        "Functional Road Class (1=highway, 5=local)",
        frc_min, frc_max, (frc_min, frc_max)
    )

    st.markdown("---")
    show_labels = st.toggle("Show speed labels on map", value=False)
    show_table  = st.toggle("Show segment table", value=True)

# Apply filters
mask = (
    df["bearing"].isin(sel_bearings) &
    df["frc"].between(sel_frc[0], sel_frc[1])
)

filtered = df[mask].copy()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown(f"""
<div class="top-bar">
  <h1>🗺️ Davidson County Road Network</h1>
  <span>XD SEGMENT VIEWER · {len(filtered):,} of {len(df):,} segments</span>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Stat cards
# ---------------------------------------------------------------------------

c1, c2, c3, c4, c5 = st.columns(5)
stats = [
    (c1, len(filtered),                              "Segments shown"),
    (c5, ", ".join(sorted(filtered["bearing"].unique())), "Bearings"),
]
for col, val, lbl in stats:
    col.markdown(f'<div class="stat-card"><div class="val">{val}</div><div class="lbl">{lbl}</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Bearing legend
# ---------------------------------------------------------------------------

pills = "".join(
    f'<span class="bearing-pill" style="background:{BEARING_COLORS[b]}">{b}</span> {b}ound &nbsp;&nbsp;'
    for b in ["N", "S", "E", "W"] if b in df["bearing"].values
)
st.markdown(f'<div class="bearing-legend">{pills}</div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

if filtered.empty:
    st.warning("No segments match the current filters.")
    st.stop()

center_lat = filtered["mid_lat"].mean()
center_lon = filtered["mid_lon"].mean()

fmap = folium.Map(
    location=[center_lat, center_lon],
    zoom_start=13,
    tiles="CartoDB positron",
    prefer_canvas=True,
)

for _, row in filtered.iterrows():
    color  = BEARING_COLORS.get(row["bearing"], "#6b7280")
    weight = frc_weight(row["frc"])
    coords = offset_coords(row)

    road_label = row["display_name"]
    road_num   = str(row["road_num"]).strip() if pd.notna(row["road_num"]) else ""
    full_name  = f"{road_label} ({road_num})" if road_num else road_label

    popup_html = f"""
    <div style="font-family:'IBM Plex Mono',monospace;min-width:200px;font-size:12px;line-height:1.7">
      <b style="font-size:13px;font-family:'IBM Plex Sans',sans-serif">{full_name}</b><br>
      <span style="color:{color};font-weight:600">▶ {row['bearing']}-bound</span><br>
      <hr style="margin:5px 0;border-color:#eee">
      <span style="color:#777">XD ID:</span> {row['xd_id']}<br>
      <span style="color:#777">Length:</span> {row['miles']:.3f} mi<br>
      <span style="color:#777">FRC:</span> {int(row['frc']) if pd.notna(row['frc']) else '?'}<br>
      <span style="color:#777">ZIP:</span> {row['zip']}<br>
      <span style="color:#777">County:</span> {row['county']}, {row['state']}<br>
      <span style="color:#777">Start:</span> {row['start_lat']:.5f}, {row['start_lon']:.5f}<br>
      <span style="color:#777">End:</span> {row['end_lat']:.5f}, {row['end_lon']:.5f}
    </div>
    """

    folium.PolyLine(
        locations=coords,
        color=color,
        weight=weight,
        opacity=0.85,
        popup=folium.Popup(popup_html, max_width=250),
        tooltip=f"{full_name} · {row['bearing']}-bound · {row['miles']:.2f} mi",
    ).add_to(fmap)

    # Optional midpoint label (XD ID)
    if show_labels:
        folium.Marker(
            location=[row["mid_lat"], row["mid_lon"]],
            icon=folium.DivIcon(
                html=f'<div style="font-size:8px;font-family:monospace;color:#1a1a2e;white-space:nowrap;'
                     f'background:rgba(255,255,255,0.75);padding:1px 3px;border-radius:2px">'
                     f'{row["xd_id"]}</div>',
                icon_size=(80, 16),
                icon_anchor=(40, 8),
            ),
        ).add_to(fmap)

# Map legend
legend_html = """
<div style="position:fixed;bottom:24px;left:24px;z-index:1000;
     background:white;padding:12px 16px;border-radius:6px;
     border:1px solid #ddd;font-family:'IBM Plex Mono',monospace;font-size:11px;
     box-shadow:0 2px 8px rgba(0,0,0,0.1)">
  <b style="font-size:12px">Bearing</b><br>
  <span style="color:#2563eb">━━</span> Northbound<br>
  <span style="color:#dc2626">━━</span> Southbound<br>
  <span style="color:#16a34a">━━</span> Eastbound<br>
  <span style="color:#d97706">━━</span> Westbound<br>
  <br>
  <b style="font-size:12px">Line weight</b><br>
  Thicker = higher road class
</div>
"""
fmap.get_root().html.add_child(folium.Element(legend_html))

st_folium(fmap, height=560, width="stretch", returned_objects=[])

# ---------------------------------------------------------------------------
# Segment table
# ---------------------------------------------------------------------------

if show_table:
    st.markdown("---")
    st.markdown("#### Segment detail")

    table_df = filtered[[
        "xd_id", "display_name", "bearing", "miles", "frc",
        "zip", "start_lat", "start_lon", "end_lat", "end_lon"
    ]].copy()

    table_df.columns = [
        "XD ID", "Road", "Bearing", "Miles", "FRC",
        "ZIP", "Start Lat", "Start Lon", "End Lat", "End Lon"
    ]
    table_df["Miles"] = table_df["Miles"].round(4)

    st.dataframe(
        table_df,
        width=True,
        hide_index=True,
        column_config={
            "XD ID":    st.column_config.TextColumn(width="medium"),
            "Road":     st.column_config.TextColumn(width="large"),
            "Bearing":  st.column_config.TextColumn(width="small"),
            "Miles":    st.column_config.NumberColumn(width="small", format="%.4f"),
            "FRC":      st.column_config.NumberColumn(width="small"),
        }
    )

    st.caption(f"{len(filtered):,} segments · {filtered['miles'].sum():.2f} total miles")