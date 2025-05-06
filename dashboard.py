import streamlit as st
import pandas as pd
import psycopg2
from config import DB_CONFIG

st.set_page_config("🚀 TripSafe Dashboard", layout="wide")
st.title("🚀 TripSafe Analytics")

@st.cache_data
def load_metrics():
    with psycopg2.connect(**DB_CONFIG) as conn:
        return pd.read_sql(
            "SELECT city, event_date, total_trips, avg_trip_duration, cancellations "
            "FROM trip_metrics ORDER BY event_date, city",
            conn, parse_dates=["event_date"]
        )

@st.cache_data
def load_alerts():
    with psycopg2.connect(**DB_CONFIG) as conn:
        return pd.read_sql(
            "SELECT trip_id, driver_id, alert_type, description, timestamp AT TIME ZONE 'UTC' AS ts "
            "FROM trip_alerts ORDER BY timestamp DESC LIMIT 500",
            conn, parse_dates=["ts"]
        )

# clear caches when the user clicks “Refresh”
if st.sidebar.button("🔄 Refresh"):
    st.cache_data.clear()

metrics_df = load_metrics()
alerts_df  = load_alerts()

# sidebar filters
st.sidebar.header("Filters")
dmin, dmax = metrics_df.event_date.min(), metrics_df.event_date.max()
date_sel   = st.sidebar.date_input("Date range", [dmin, dmax])
cities     = ["All"] + sorted(metrics_df.city.unique())
city_sel   = st.sidebar.selectbox("City", cities)
alerts     = ["All"] + sorted(alerts_df.alert_type.unique())
alert_sel  = st.sidebar.selectbox("Alert type", alerts)

tab1, tab2 = st.tabs(["Metrics", "Fraud Alerts"])

with tab1:
    dr0, dr1 = pd.to_datetime(date_sel[0]), pd.to_datetime(date_sel[1])
    m = metrics_df[(metrics_df.event_date >= dr0) & (metrics_df.event_date <= dr1)]
    if city_sel != "All":
        m = m[m.city == city_sel]

    if m.empty:
        st.warning("No data")
    else:
        latest = m[m.event_date == m.event_date.max()]
        tot    = int(latest.total_trips.sum())
        avg_d  = (
            round((latest.avg_trip_duration * latest.total_trips).sum() / tot, 2)
            if tot else 0
        )
        canc   = int(latest.cancellations.sum())

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Trips", tot)
        c2.metric("Avg Duration", f"{avg_d} min")
        c3.metric("Cancellations", canc)

        st.line_chart(
            m.set_index("event_date")[["total_trips", "avg_trip_duration", "cancellations"]]
        )

with tab2:
    a = alerts_df.copy()
    if alert_sel != "All":
        a = a[a.alert_type == alert_sel]

    st.write(f"{len(a)} alerts")
    st.dataframe(a[["ts", "trip_id", "driver_id", "alert_type", "description"]], height=300)
    st.bar_chart(a.alert_type.value_counts())
