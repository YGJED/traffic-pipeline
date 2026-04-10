import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
from datetime import datetime
import time

st.set_page_config(
    page_title="Traffic Intelligence",
    page_icon="🚦",
    layout="wide",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }
    .live-badge {
        display: inline-flex; align-items: center; gap: 6px;
        background: #d4edda; color: #155724;
        padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: 500;
    }
    .live-dot {
        width: 8px; height: 8px; border-radius: 50%; background: #28a745;
        animation: pulse 1.5s ease-in-out infinite;
    }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }
    .section-header { font-size: 15px; font-weight: 600; color: #343a40; margin-bottom: 0.75rem; }
    div[data-testid="stMetric"] { background: #f8f9fa; border-radius: 10px; padding: 0.75rem 1rem; border: 1px solid #e9ecef; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Hardcoded segment data — matches DynamoDB schema from PySpark output
# Replace this block with: requests.get("http://fastapi/congestion").json()
# ---------------------------------------------------------------------------

SEGMENTS = [
    {
        "xd_id": "1140699194",
        "road_name": "I-65 N",
        "lat": 36.1921, "lon": -86.7792,
        "current_speed": 28.4,
        "reference_speed": 65.0,
        "historical_avg_speed": 58.2,
        "speed_vs_historical": -29.8,
        "congestion_ratio": 0.44,
        "severity": "slow",
        "segment_miles": 0.8,
        "confidence_score": 0.91,
        "last_updated": "2024-03-15T08:32:00Z",
    },
    {
        "xd_id": "1140699201",
        "road_name": "I-40 E",
        "lat": 36.1612, "lon": -86.7201,
        "current_speed": 12.1,
        "reference_speed": 70.0,
        "historical_avg_speed": 61.5,
        "speed_vs_historical": -49.4,
        "congestion_ratio": 0.17,
        "severity": "severe",
        "segment_miles": 1.1,
        "confidence_score": 0.95,
        "last_updated": "2024-03-15T08:32:00Z",
    },
    {
        "xd_id": "1140699215",
        "road_name": "I-24 W",
        "lat": 36.1334, "lon": -86.8198,
        "current_speed": 52.3,
        "reference_speed": 60.0,
        "historical_avg_speed": 54.0,
        "speed_vs_historical": -1.7,
        "congestion_ratio": 0.87,
        "severity": "normal",
        "segment_miles": 0.9,
        "confidence_score": 0.88,
        "last_updated": "2024-03-15T08:32:00Z",
    },
    {
        "xd_id": "1140699230",
        "road_name": "Briley Pkwy",
        "lat": 36.2241, "lon": -86.8612,
        "current_speed": 31.0,
        "reference_speed": 55.0,
        "historical_avg_speed": 48.3,
        "speed_vs_historical": -17.3,
        "congestion_ratio": 0.56,
        "severity": "slow",
        "segment_miles": 0.7,
        "confidence_score": 0.84,
        "last_updated": "2024-03-15T08:32:00Z",
    },
    {
        "xd_id": "1140699244",
        "road_name": "US-31 N",
        "lat": 36.2389, "lon": -86.8034,
        "current_speed": 44.7,
        "reference_speed": 50.0,
        "historical_avg_speed": 43.1,
        "speed_vs_historical": 1.6,
        "congestion_ratio": 0.89,
        "severity": "normal",
        "segment_miles": 0.6,
        "confidence_score": 0.79,
        "last_updated": "2024-03-15T08:32:00Z",
    },
    {
        "xd_id": "1140699258",
        "road_name": "Charlotte Ave",
        "lat": 36.1601, "lon": -86.8401,
        "current_speed": 9.8,
        "reference_speed": 40.0,
        "historical_avg_speed": 28.7,
        "speed_vs_historical": -18.9,
        "congestion_ratio": 0.25,
        "severity": "severe",
        "segment_miles": 0.4,
        "confidence_score": 0.93,
        "last_updated": "2024-03-15T08:32:00Z",
    },
    {
        "xd_id": "1140699271",
        "road_name": "Murfreesboro Rd",
        "lat": 36.1198, "lon": -86.7489,
        "current_speed": 33.2,
        "reference_speed": 45.0,
        "historical_avg_speed": 36.4,
        "speed_vs_historical": -3.2,
        "congestion_ratio": 0.74,
        "severity": "normal",
        "segment_miles": 0.5,
        "confidence_score": 0.86,
        "last_updated": "2024-03-15T08:32:00Z",
    },
    {
        "xd_id": "1140699285",
        "road_name": "Gallatin Pike",
        "lat": 36.2012, "lon": -86.7334,
        "current_speed": 18.5,
        "reference_speed": 40.0,
        "historical_avg_speed": 31.2,
        "speed_vs_historical": -12.7,
        "congestion_ratio": 0.46,
        "severity": "slow",
        "segment_miles": 0.5,
        "confidence_score": 0.81,
        "last_updated": "2024-03-15T08:32:00Z",
    },
    {
        "xd_id": "1140699299",
        "road_name": "Nolensville Pk",
        "lat": 36.1023, "lon": -86.7801,
        "current_speed": 29.1,
        "reference_speed": 35.0,
        "historical_avg_speed": 30.5,
        "speed_vs_historical": -1.4,
        "congestion_ratio": 0.83,
        "severity": "normal",
        "segment_miles": 0.4,
        "confidence_score": 0.77,
        "last_updated": "2024-03-15T08:32:00Z",
    },
    {
        "xd_id": "1140699312",
        "road_name": "8th Ave S",
        "lat": 36.1512, "lon": -86.7912,
        "current_speed": 22.4,
        "reference_speed": 35.0,
        "historical_avg_speed": 27.8,
        "speed_vs_historical": -5.4,
        "congestion_ratio": 0.64,
        "severity": "slow",
        "segment_miles": 0.3,
        "confidence_score": 0.90,
        "last_updated": "2024-03-15T08:32:00Z",
    },
]

# ---------------------------------------------------------------------------
# Hardcoded Waze events — matches Waze data schema
# Replace with: requests.get("http://fastapi/waze-alerts").json()
# ---------------------------------------------------------------------------

WAZE_EVENTS = [
    {
        "uuid": "waze-uuid-001",
        "type": "ACCIDENT",
        "subtype": "ACCIDENT_MAJOR",
        "street": "I-40 E",
        "lat": 36.1634, "lon": -86.7178,
        "date": "2024-03-15", "hour": 8, "minute": 18,
        "day_of_week": "Friday",
        "confidence": 0.92,
        "reliability": 8,
        "nthumbsup": 14,
        "reportrating": 4,
    },
    {
        "uuid": "waze-uuid-002",
        "type": "HAZARD",
        "subtype": "HAZARD_ON_ROAD_OBJECT",
        "street": "I-65 N",
        "lat": 36.1945, "lon": -86.7801,
        "date": "2024-03-15", "hour": 8, "minute": 25,
        "day_of_week": "Friday",
        "confidence": 0.78,
        "reliability": 6,
        "nthumbsup": 5,
        "reportrating": 3,
    },
    {
        "uuid": "waze-uuid-003",
        "type": "ROAD_CLOSED",
        "subtype": "ROAD_CLOSED_CONSTRUCTION",
        "street": "I-24 W",
        "lat": 36.1312, "lon": -86.8223,
        "date": "2024-03-15", "hour": 7, "minute": 55,
        "day_of_week": "Friday",
        "confidence": 0.99,
        "reliability": 10,
        "nthumbsup": 22,
        "reportrating": 5,
    },
    {
        "uuid": "waze-uuid-004",
        "type": "JAM",
        "subtype": "JAM_STAND_STILL_TRAFFIC",
        "street": "Charlotte Ave",
        "lat": 36.1589, "lon": -86.8388,
        "date": "2024-03-15", "hour": 8, "minute": 10,
        "day_of_week": "Friday",
        "confidence": 0.95,
        "reliability": 9,
        "nthumbsup": 31,
        "reportrating": 5,
    },
    {
        "uuid": "waze-uuid-005",
        "type": "WEATHERHAZARD",
        "subtype": "HAZARD_WEATHER_FLOOD",
        "street": "Nolensville Pk",
        "lat": 36.1001, "lon": -86.7823,
        "date": "2024-03-15", "hour": 7, "minute": 40,
        "day_of_week": "Friday",
        "confidence": 0.85,
        "reliability": 7,
        "nthumbsup": 9,
        "reportrating": 4,
    },
]

# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

SEVERITY_COLOR = {
    "severe": "#dc3545",
    "slow":   "#fd7e14",
    "normal": "#28a745",
}

WAZE_META = {
    "ACCIDENT":      {"icon": "🚨", "label": "Accident"},
    "HAZARD":        {"icon": "⚠️",  "label": "Hazard"},
    "ROAD_CLOSED":   {"icon": "🚧", "label": "Road closed"},
    "JAM":           {"icon": "🚗", "label": "Traffic jam"},
    "WEATHERHAZARD": {"icon": "🌊", "label": "Weather hazard"},
}

def severity_label(s):
    return s.capitalize()

def waze_timestamp(e):
    return f"{e['day_of_week']} {e['hour']:02d}:{e['minute']:02d}"

# ---------------------------------------------------------------------------
# Derived metrics — pure display math, all inputs pre-computed by PySpark
# ---------------------------------------------------------------------------

df = pd.DataFrame(SEGMENTS)

avg_speed         = round(df["current_speed"].mean(), 1)
avg_vs_historical = round(df["speed_vs_historical"].mean(), 1)
congested_count   = int((df["severity"] != "normal").sum())
worst             = df.loc[df["current_speed"].idxmin()]
last_updated_fmt  = datetime.fromisoformat(
    SEGMENTS[0]["last_updated"].replace("Z", "")
).strftime("%I:%M %p")

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

col_title, col_badge, col_refresh = st.columns([4, 1, 1])
with col_title:
    st.markdown("## 🚦 Live Traffic Intelligence")
with col_badge:
    st.markdown('<div class="live-badge"><div class="live-dot"></div> Live</div>', unsafe_allow_html=True)
with col_refresh:
    if st.button("↻ Refresh", width='stretch'):
        st.rerun()

st.caption(f"Nashville metro · Davidson County · data as of {last_updated_fmt}")
st.divider()

# ---------------------------------------------------------------------------
# Metric cards
# ---------------------------------------------------------------------------

m1, m2, m3, m4 = st.columns(4)
m1.metric(
    "Avg Network Speed",
    f"{avg_speed} mph",
    f"{avg_vs_historical:+.1f} mph vs historical avg",
)
m2.metric(
    "Congested Segments",
    f"{congested_count} / {len(SEGMENTS)}",
    f"{len(SEGMENTS) - congested_count} flowing normally",
)
m3.metric(
    "Worst Bottleneck",
    worst["road_name"],
    f"{worst['current_speed']} mph · {round(worst['congestion_ratio']*100)}% of free-flow",
)
m4.metric(
    "Active Waze Alerts",
    len(WAZE_EVENTS),
    f"avg confidence {round(sum(e['confidence'] for e in WAZE_EVENTS) / len(WAZE_EVENTS) * 100)}%",
)

st.divider()

# ---------------------------------------------------------------------------
# Map + bar chart
# ---------------------------------------------------------------------------

map_col, chart_col = st.columns([1.5, 1])

with map_col:
    st.markdown('<div class="section-header">Live map — congestion &amp; Waze alerts</div>', unsafe_allow_html=True)

    fmap = folium.Map(
        location=[36.165, -86.785],
        zoom_start=12,
        tiles="CartoDB positron",
        prefer_canvas=True,
    )

    for seg in SEGMENTS:
        hex_color = SEVERITY_COLOR[seg["severity"]]
        pct_of_ff = round(seg["congestion_ratio"] * 100)
        vs_hist   = seg["speed_vs_historical"]
        vs_str    = f"+{vs_hist:.1f}" if vs_hist >= 0 else f"{vs_hist:.1f}"

        popup_html = f"""
        <div style="font-family:sans-serif;min-width:190px;font-size:13px">
          <b style="font-size:14px">{seg['road_name']}</b>
          &nbsp;<span style="color:{hex_color};font-weight:600">{severity_label(seg['severity'])}</span><br>
          <hr style="margin:6px 0">
          <span style="color:#555">XD segment:</span> {seg['xd_id']}<br>
          <span style="color:#555">Current speed:</span> <b>{seg['current_speed']} mph</b><br>
          <span style="color:#555">Reference (free-flow):</span> {seg['reference_speed']} mph<br>
          <span style="color:#555">Historical avg:</span> {seg['historical_avg_speed']} mph<br>
          <span style="color:#555">vs historical:</span> <b style="color:{hex_color}">{vs_str} mph</b><br>
          <span style="color:#555">% of free-flow:</span> {pct_of_ff}%<br>
          <span style="color:#555">Segment length:</span> {seg['segment_miles']} mi<br>
          <span style="color:#555">Confidence:</span> {round(seg['confidence_score']*100)}%<br>
          <span style="color:#aaa;font-size:11px">Updated {last_updated_fmt}</span>
        </div>
        """

        folium.CircleMarker(
            location=[seg["lat"], seg["lon"]],
            radius=14,
            color=hex_color,
            fill=True,
            fill_color=hex_color,
            fill_opacity=0.75,
            popup=folium.Popup(popup_html, max_width=240),
            tooltip=f"{seg['road_name']} · {seg['current_speed']} mph ({severity_label(seg['severity'])})",
        ).add_to(fmap)

        folium.Marker(
            location=[seg["lat"], seg["lon"]],
            icon=folium.DivIcon(
                html=f'<div style="font-size:10px;font-weight:700;color:white;text-align:center;line-height:28px">{round(seg["current_speed"])}</div>',
                icon_size=(28, 28),
                icon_anchor=(14, 14),
            ),
        ).add_to(fmap)

    for event in WAZE_EVENTS:
        meta = WAZE_META.get(event["type"], {"icon": "📍", "label": event["type"]})
        ts   = waze_timestamp(event)

        waze_popup = f"""
        <div style="font-family:sans-serif;min-width:190px;font-size:13px">
          <b style="font-size:14px">{meta['icon']} {meta['label']}</b><br>
          <span style="color:#555;font-size:12px">{event['subtype'].replace('_', ' ').title()}</span><br>
          <hr style="margin:6px 0">
          <span style="color:#555">Street:</span> <b>{event['street']}</b><br>
          <span style="color:#555">Reported:</span> {ts}<br>
          <span style="color:#555">Confidence:</span> {round(event['confidence']*100)}%<br>
          <span style="color:#555">Reliability:</span> {event['reliability']}/10<br>
          <span style="color:#555">Thumbs up:</span> {event['nthumbsup']}<br>
          <span style="color:#555">Report rating:</span> {event['reportrating']}/5<br>
          <span style="color:#aaa;font-size:11px">UUID: {event['uuid']}</span>
        </div>
        """

        folium.Marker(
            location=[event["lat"], event["lon"]],
            icon=folium.DivIcon(
                html=f'<div style="font-size:20px;filter:drop-shadow(0 1px 2px rgba(0,0,0,0.35))">{meta["icon"]}</div>',
                icon_size=(28, 28),
                icon_anchor=(14, 14),
            ),
            popup=folium.Popup(waze_popup, max_width=240),
            tooltip=f"{meta['icon']} {meta['label']} · {event['street']}",
        ).add_to(fmap)

    legend_html = """
    <div style="position:fixed;bottom:20px;left:20px;z-index:1000;background:white;
         padding:10px 14px;border-radius:8px;border:1px solid #dee2e6;font-family:sans-serif;font-size:12px">
      <b>Congestion (INRIX)</b><br>
      <span style="color:#dc3545">●</span> Severe (&lt;40% free-flow)<br>
      <span style="color:#fd7e14">●</span> Slow (40–65%)<br>
      <span style="color:#28a745">●</span> Normal (&gt;65%)<br>
      <br><b>Waze alerts</b><br>
      🚨 Accident &nbsp; ⚠️ Hazard<br>
      🚧 Road closed &nbsp; 🚗 Jam &nbsp; 🌊 Weather
    </div>
    """
    fmap.get_root().html.add_child(folium.Element(legend_html))
    st_folium(fmap, height=440, width='stretch')

with chart_col:
    st.markdown('<div class="section-header">Speed by segment</div>', unsafe_allow_html=True)

    chart_df = df.sort_values("current_speed")

    fig_bar = go.Figure()
    fig_bar.add_trace(go.Bar(
        y=chart_df["road_name"],
        x=chart_df["reference_speed"],
        name="Free-flow (reference_speed)",
        orientation="h",
        marker_color="rgba(200,200,200,0.4)",
    ))
    fig_bar.add_trace(go.Bar(
        y=chart_df["road_name"],
        x=chart_df["historical_avg_speed"],
        name="Historical avg",
        orientation="h",
        marker_color="rgba(55,138,221,0.35)",
    ))
    fig_bar.add_trace(go.Bar(
        y=chart_df["road_name"],
        x=chart_df["current_speed"],
        name="Current speed",
        orientation="h",
        marker_color=[SEVERITY_COLOR[s] for s in chart_df["severity"]],
        text=[f"{v} mph" for v in chart_df["current_speed"]],
        textposition="outside",
    ))
    fig_bar.update_layout(
        barmode="overlay",
        height=420,
        margin=dict(l=0, r=50, t=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, x=0, font=dict(size=11)),
        xaxis_title="Speed (mph)",
        yaxis=dict(tickfont=dict(size=11)),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(gridcolor="#f0f0f0"),
    )
    st.plotly_chart(fig_bar, width='stretch')

st.divider()

# ---------------------------------------------------------------------------
# Waze alert list + segment data table
# ---------------------------------------------------------------------------

waze_col, table_col = st.columns([1, 1.5])

with waze_col:
    st.markdown('<div class="section-header">Active Waze alerts</div>', unsafe_allow_html=True)
    for event in sorted(WAZE_EVENTS, key=lambda e: e["confidence"], reverse=True):
        meta = WAZE_META.get(event["type"], {"icon": "📍", "label": event["type"]})
        c1, c2 = st.columns([0.12, 0.88])
        c1.markdown(f"<div style='font-size:22px;padding-top:2px'>{meta['icon']}</div>", unsafe_allow_html=True)
        c2.markdown(
            f"**{meta['label']}** · {event['street']}  \n"
            f"{event['subtype'].replace('_', ' ').title()}  \n"
            f"Confidence {round(event['confidence']*100)}% · {event['nthumbsup']} thumbs up · {waze_timestamp(event)}"
        )
        st.divider()

with table_col:
    st.markdown('<div class="section-header">Segment detail</div>', unsafe_allow_html=True)
    table_df = pd.DataFrame({
        "XD ID":        [s["xd_id"] for s in SEGMENTS],
        "Road":         [s["road_name"] for s in SEGMENTS],
        "Speed (mph)":  [s["current_speed"] for s in SEGMENTS],
        "Ref (mph)":    [s["reference_speed"] for s in SEGMENTS],
        "Hist avg":     [s["historical_avg_speed"] for s in SEGMENTS],
        "vs Hist":      [f"{s['speed_vs_historical']:+.1f}" for s in SEGMENTS],
        "% Free-flow":  [f"{round(s['congestion_ratio']*100)}%" for s in SEGMENTS],
        "Severity":     [severity_label(s["severity"]) for s in SEGMENTS],
        "Confidence":   [f"{round(s['confidence_score']*100)}%" for s in SEGMENTS],
        "Miles":        [s["segment_miles"] for s in SEGMENTS],
    })
    st.dataframe(table_df, width='stretch', hide_index=True)

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------

st.caption("⏱ Auto-refreshes every 30 seconds. Click ↻ Refresh to update manually.")
time.sleep(30)
st.rerun()
