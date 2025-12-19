"""Streamlit dashboard sourcing data from MongoDB aggregations."""

from __future__ import annotations

import pandas as pd
import streamlit as st
import altair as alt

from travel_pipeline.core.config import get_settings
from travel_pipeline.db.mongo import get_mongo_client

# Load configuration once so all views share the same MongoDB settings.
settings = get_settings()

# Configure the overall Streamlit page (title + wide layout for charts).
st.set_page_config(page_title="BTS Flight Reliability", layout="wide")


@st.cache_resource(show_spinner=False)
def get_client():
    """Create a single MongoDB client and reuse it across reruns.

    Streamlit caches this so we don't open a new connection for every
    interaction (which would be slow and unnecessary).
    """

    client = get_mongo_client(settings)
    return client


@st.cache_data(ttl=60, show_spinner=False)
def load_collection(collection_name: str) -> pd.DataFrame:
    """Load a MongoDB collection into a pandas DataFrame.

    Results are cached for 60 seconds to avoid hitting the database on
    every widget interaction while still keeping the data reasonably fresh.
    """

    client = get_client()
    database = client[settings.database]
    frame = pd.DataFrame(list(database[collection_name].find({}, {"_id": 0})))
    return frame


def carrier_view():
    """Show daily average departure and arrival delays by carrier."""

    st.subheader("Carrier Delay Trend (Daily)")
    frame = load_collection(settings.agg_carrier_collection)
    if frame.empty:
        st.info("Run the aggregation stage to populate this view.")
        return
    carrier = st.selectbox("Carrier", sorted(frame["carrier"].unique()))
    filtered = frame[frame["carrier"] == carrier].sort_values("flight_date")
    # Convert flight_date to string for better axis formatting if needed
    if pd.api.types.is_datetime64_any_dtype(filtered["flight_date"]):
        filtered["flight_date"] = filtered["flight_date"].dt.strftime("%Y-%m-%d")
    st.line_chart(filtered, x="flight_date", y=["avg_dep_delay", "avg_arr_delay"])


def origin_view():
    """Display cancellation rate by origin airport from gold aggregates."""

    st.subheader("Origin Cancellation Rate")
    frame = load_collection(settings.agg_origin_collection)
    if frame.empty:
        st.info("Aggregation data missing.")
        return
    limit = st.slider("Show top airports", min_value=5, max_value=25, value=10)
    st.bar_chart(frame.head(limit), x="origin", y="cancel_rate")


def route_view():
    """Tabular preview of route-level delay and volume metrics."""

    st.subheader("Route Delay Heatmap")
    frame = load_collection(settings.agg_route_collection)
    if frame.empty:
        st.info("Aggregation data missing.")
        return
    frame["route"] = frame["origin"] + " → " + frame["destination"]
    st.dataframe(frame[["route", "avg_arr_delay", "avg_dep_delay", "flights"]].head(25))


def on_time_scorecard_view():
    """High-level KPIs summarizing fleet on-time performance."""

    st.subheader("On-Time Performance Scorecard")
    frame = load_collection(settings.clean_collection)
    if frame.empty:
        st.info("Clean data missing. Run ingest + clean stages.")
        return
    frame["flight_date"] = pd.to_datetime(frame["flight_date"])
    arr = pd.to_numeric(frame["arr_delay"], errors="coerce")
    total_flights = len(frame)
    on_time_15 = (arr <= 15).mean()
    avg_delay = arr.mean()
    cancel_rate = frame["cancelled"].mean() if "cancelled" in frame else float("nan")
    divert_rate = frame["diverted"].mean() if "diverted" in frame else float("nan")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total flights", f"{total_flights:,}")
    c2.metric("On-time ≤15 min", f"{on_time_15 * 100:0.1f}%")
    c3.metric("Avg arrival delay", f"{avg_delay:0.1f} min")
    c4.metric("Cancellation rate", f"{cancel_rate * 100:0.2f}%")
    c5.metric("Diversion rate", f"{divert_rate * 100:0.2f}%")


def delay_waterfall_view():
    """Break down average delay for one airline into key components."""

    st.subheader("Delay Waterfall (Schedule vs Actual)")
    frame = load_collection(settings.clean_collection)
    if frame.empty:
        st.info("Clean data missing. Run ingest + clean stages.")
        return
    carrier = st.selectbox("Airline", sorted(frame["carrier"].dropna().unique()))
    subset = frame[frame["carrier"] == carrier]
    if subset.empty:
        st.info("No data for selected airline.")
        return

    dep = pd.to_numeric(subset["dep_delay"], errors="coerce").mean()
    taxi_out = pd.to_numeric(subset["taxi_out"], errors="coerce").mean()
    taxi_in = pd.to_numeric(subset["taxi_in"], errors="coerce").mean()
    arr = pd.to_numeric(subset["arr_delay"], errors="coerce").mean()
    airborne = arr - dep - taxi_out - taxi_in

    parts = pd.DataFrame(
        {
            "component": [
                "Departure delay",
                "Taxi-out",
                "Airborne",
                "Taxi-in",
            ],
            "minutes": [dep, taxi_out, airborne, taxi_in],
        }
    )
    st.bar_chart(parts, x="component", y="minutes")


def route_risk_matrix_view():
    """Bubble chart of route-level delay and cancellation risk for one carrier."""

    st.subheader("Route Profitability Risk Matrix")
    frame = load_collection(settings.clean_collection)
    if frame.empty:
        st.info("Clean data missing. Run ingest + clean stages.")
        return
    frame["arr_delay_num"] = pd.to_numeric(frame["arr_delay"], errors="coerce")
    frame["is_delayed15"] = frame["arr_delay_num"] >= 15
    grouped = (
        frame.groupby(["carrier", "origin", "destination"], as_index=False)
        .agg(
            avg_delay=("arr_delay_num", "mean"),
            cancel_rate=("cancelled", "mean"),
            flights=("flight_date", "count"),
            delayed_share=("is_delayed15", "mean"),
        )
    )
    airline = st.selectbox(
        "Airline (risk matrix)",
        sorted(grouped["carrier"].dropna().unique()),
    )
    subset = grouped[grouped["carrier"] == airline]
    if subset.empty:
        st.info("No data for selected airline.")
        return
    chart = (
        alt.Chart(subset)
        .mark_circle(opacity=0.7)
        .encode(
            x=alt.X("avg_delay", title="Avg arrival delay (min)"),
            y=alt.Y("cancel_rate", title="Cancellation rate"),
            size=alt.Size("flights", title="Flights", scale=alt.Scale(range=[10, 800])),
            color=alt.Color("origin", title="Origin"),
            tooltip=["origin", "destination", "flights", "avg_delay", "cancel_rate"],
        )
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)


def airport_congestion_heatmap_view():
    """Heatmap showing average delay by day-of-week and hour for a station."""

    st.subheader("Airport Congestion Heatmap (Hour × Day-of-week)")
    frame = load_collection(settings.clean_collection)
    if frame.empty:
        st.info("Clean data missing. Run ingest + clean stages.")
        return
    frame["flight_date"] = pd.to_datetime(frame["flight_date"])
    frame["dow"] = frame["flight_date"].dt.day_name()
    hours = (pd.to_numeric(frame["crs_dep_time"], errors="coerce") // 100).astype("Int64")
    frame["hour"] = hours
    airport = st.selectbox("Airport (origin)", sorted(frame["origin"].dropna().unique()))
    subset = frame[frame["origin"] == airport]
    if subset.empty:
        st.info("No data for selected airport.")
        return
    subset["arr_delay_num"] = pd.to_numeric(subset["arr_delay"], errors="coerce")
    heat = (
        subset.groupby(["dow", "hour"], as_index=False)
        .agg(avg_delay=("arr_delay_num", "mean"))
    )
    dow_order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    heat["dow"] = pd.Categorical(heat["dow"], categories=dow_order, ordered=True)
    chart = (
        alt.Chart(heat)
        .mark_rect()
        .encode(
            x=alt.X("hour:O", title="Hour of day"),
            y=alt.Y("dow:O", title="Day of week"),
            color=alt.Color("avg_delay:Q", title="Avg delay (min)", scale=alt.Scale(scheme="inferno")),
            tooltip=["dow", "hour", "avg_delay"],
        )
    )
    st.altair_chart(chart, use_container_width=True)


def missed_connection_risk_view():
    """Trend of share of flights arriving 30/45/60+ minutes late into a hub."""

    st.subheader("Missed Connection Risk Proxy")
    frame = load_collection(settings.clean_collection)
    if frame.empty:
        st.info("Clean data missing. Run ingest + clean stages.")
        return
    frame["arr_delay_num"] = pd.to_numeric(frame["arr_delay"], errors="coerce")
    frame["late_30"] = frame["arr_delay_num"] >= 30
    frame["late_45"] = frame["arr_delay_num"] >= 45
    frame["late_60"] = frame["arr_delay_num"] >= 60
    hub = st.selectbox("Destination hub", sorted(frame["destination"].dropna().unique()))
    subset = frame[frame["destination"] == hub]
    if subset.empty:
        st.info("No data for selected hub.")
        return
    summary = (
        subset.groupby("flight_date", as_index=False)
        .agg(
            share_30=("late_30", "mean"),
            share_45=("late_45", "mean"),
            share_60=("late_60", "mean"),
        )
        .sort_values("flight_date")
    )
    st.line_chart(summary, x="flight_date", y=["share_30", "share_45", "share_60"])


def airline_benchmark_view():
    """League table comparing on-time performance across carriers."""

    st.subheader("Airline Benchmarking League Table")
    frame = load_collection(settings.clean_collection)
    if frame.empty:
        st.info("Clean data missing. Run ingest + clean stages.")
        return
    frame["arr_delay_num"] = pd.to_numeric(frame["arr_delay"], errors="coerce")
    frame["on_time"] = frame["arr_delay_num"] <= 15
    summary = (
        frame.groupby("carrier", as_index=False)
        .agg(on_time_rate=("on_time", "mean"), flights=("flight_date", "count"))
    )
    min_flights = st.slider("Minimum flights", min_value=100, max_value=int(summary["flights"].max()), value=1000)
    summary = summary[summary["flights"] >= min_flights]
    summary = summary.sort_values("on_time_rate", ascending=False)
    st.dataframe(summary)


def control_chart_view():
    """Control chart for average delay over time for a route or airport."""

    st.subheader("Control Chart for Route or Airport")
    frame = load_collection(settings.clean_collection)
    if frame.empty:
        st.info("Clean data missing. Run ingest + clean stages.")
        return
    frame["flight_date"] = pd.to_datetime(frame["flight_date"])
    frame["arr_delay_num"] = pd.to_numeric(frame["arr_delay"], errors="coerce")
    mode = st.radio("Control chart for", ["Route", "Airport"], horizontal=True)
    if mode == "Route":
        frame["route"] = frame["origin"] + " → " + frame["destination"]
        choice = st.selectbox("Route", sorted(frame["route"].dropna().unique()))
        subset = frame[frame["route"] == choice]
    else:
        choice = st.selectbox("Airport", sorted(frame["origin"].dropna().unique()))
        subset = frame[frame["origin"] == choice]
    if subset.empty:
        st.info("No data for selection.")
        return
    daily = (
        subset.groupby("flight_date", as_index=False)
        .agg(avg_delay=("arr_delay_num", "mean"))
        .sort_values("flight_date")
    )
    mean = daily["avg_delay"].mean()
    std = daily["avg_delay"].std()
    daily["ucl"] = mean + 3 * std
    daily["lcl"] = mean - 3 * std
    base = alt.Chart(daily).encode(x="flight_date:T")
    line = base.mark_line().encode(y="avg_delay:Q")
    center = base.mark_rule(color="green").encode(y=alt.datum(mean))
    band = base.mark_area(opacity=0.2, color="red").encode(y="lcl:Q", y2="ucl:Q")
    st.altair_chart(band + center + line, use_container_width=True)


def pareto_delay_view():
    """Pareto chart showing which dimensions contribute most delay minutes."""

    st.subheader("Pareto of Delay Contributors")
    frame = load_collection(settings.clean_collection)
    if frame.empty:
        st.info("Clean data missing. Run ingest + clean stages.")
        return
    frame["arr_delay_num"] = pd.to_numeric(frame["arr_delay"], errors="coerce")
    frame = frame[frame["arr_delay_num"] > 0]
    dim = st.selectbox("Dimension", ["origin", "destination", "tail_number"])
    grouped = (
        frame.groupby(dim, as_index=False)
        .agg(delay_minutes=("arr_delay_num", "sum"))
        .sort_values("delay_minutes", ascending=False)
    )
    grouped["cum_share"] = grouped["delay_minutes"].cumsum() / grouped["delay_minutes"].sum()
    top_n = st.slider("Top N", min_value=5, max_value=min(50, len(grouped)), value=20)
    subset = grouped.head(top_n)
    bars = alt.Chart(subset).mark_bar().encode(x=dim, y="delay_minutes")
    line = (
        alt.Chart(subset)
        .mark_line(color="red")
        .encode(x=dim, y="cum_share")
    )
    st.altair_chart(bars + line, use_container_width=True)


def disruption_map_view():
    """Rank origins by average delay and cancellation to highlight hotspots."""

    st.subheader("Disruption Map (Tabular Proxy)")
    frame = load_collection(settings.clean_collection)
    if frame.empty:
        st.info("Clean data missing. Run ingest + clean stages.")
        return
    frame["arr_delay_num"] = pd.to_numeric(frame["arr_delay"], errors="coerce")
    summary = (
        frame.groupby("origin", as_index=False)
        .agg(
            avg_delay=("arr_delay_num", "mean"),
            cancel_rate=("cancelled", "mean"),
            flights=("flight_date", "count"),
        )
        .sort_values("avg_delay", ascending=False)
    )
    st.dataframe(summary.head(50))


def daily_volume_view():
    """Plot total number of flights per day across all carriers."""

    st.subheader("Daily Flight Volume (All Carriers)")
    frame = load_collection(settings.clean_collection)
    if frame.empty:
        st.info("Clean data missing. Run ingest + clean stages.")
        return
    frame["flight_date"] = pd.to_datetime(frame["flight_date"])
    daily = (
        frame.groupby(frame["flight_date"].dt.date)
        .size()
        .reset_index(name="flights")
        .sort_values("flight_date")
    )
    st.line_chart(daily, x="flight_date", y="flights")


def top_delayed_routes_view():
    """Bar chart of the N routes with the highest average arrival delay."""

    st.subheader("Top N Most Delayed Routes")
    frame = load_collection(settings.agg_route_collection)
    if frame.empty:
        st.info("Aggregation data missing.")
        return
    frame["route"] = frame["origin"] + " → " + frame["destination"]
    max_n = min(100, len(frame)) if len(frame) > 0 else 10
    top_n = st.slider("How many routes?", min_value=5, max_value=max_n, value=min(10, max_n))
    top = frame.sort_values("avg_arr_delay", ascending=False).head(top_n)
    st.bar_chart(top, x="route", y="avg_arr_delay")


def dow_cancellation_view():
    """Show how cancellation rate changes across the days of the week."""

    st.subheader("Cancellation Rate by Day of Week")
    frame = load_collection(settings.clean_collection)
    if frame.empty:
        st.info("Clean data missing. Run ingest + clean stages.")
        return
    frame["flight_date"] = pd.to_datetime(frame["flight_date"])
    frame["dow"] = frame["flight_date"].dt.day_name()
    summary = (
        frame.groupby("dow", as_index=False)
        .agg(
            cancel_rate=("cancelled", "mean"),
            flights=("flight_date", "count"),
        )
    )
    order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    summary["dow"] = pd.Categorical(summary["dow"], categories=order, ordered=True)
    summary = summary.sort_values("dow")
    st.bar_chart(summary, x="dow", y="cancel_rate")


def dep_delay_distribution_view():
    """Histogram-style view of the overall departure delay distribution."""

    st.subheader("Distribution of Departure Delays")
    frame = load_collection(settings.clean_collection)
    if frame.empty:
        st.info("Clean data missing. Run ingest + clean stages.")
        return
    delays = pd.to_numeric(frame["dep_delay"], errors="coerce").dropna()
    if delays.empty:
        st.info("No departure delay data available.")
        return
    # Use 20 equal-width bins over the observed delay range
    hist = pd.cut(delays, bins=20).value_counts().sort_index()
    hist_df = hist.reset_index()
    hist_df.columns = ["bin", "flights"]
    hist_df["bin_label"] = hist_df["bin"].astype(str)
    st.bar_chart(hist_df, x="bin_label", y="flights")


def ontime_by_month_view():
    """Daily on-time rate for a selected carrier across the time range."""

    st.subheader("Daily On-Time Performance by Carrier")
    frame = load_collection(settings.clean_collection)
    if frame.empty:
        st.info("Clean data missing. Run ingest + clean stages.")
        return
    frame["flight_date"] = pd.to_datetime(frame["flight_date"])
    frame["flight_day"] = frame["flight_date"].dt.date
    frame["on_time"] = pd.to_numeric(frame["arr_delay"], errors="coerce") <= 0
    carrier = st.selectbox(
        "Carrier (On-Time Rate)", sorted(frame["carrier"].dropna().unique())
    )
    filtered = frame[frame["carrier"] == carrier]
    if filtered.empty:
        st.info("No data for selected carrier.")
        return
    summary = (
        filtered.groupby("flight_day", as_index=False)
        .agg(on_time_rate=("on_time", "mean"), flights=("flight_date", "count"))
        .sort_values("flight_day")
    )
    st.line_chart(summary, x="flight_day", y="on_time_rate")


route_risk_matrix_view()
airport_congestion_heatmap_view()
top_delayed_routes_view()
