"""Aggregation layer for curated analytics tables."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd
from loguru import logger
from pymongo.collection import Collection

from travel_pipeline.core.config import Settings, get_settings
from travel_pipeline.core.logging import configure_logging
from travel_pipeline.db.mongo import get_collection, get_mongo_client


def carrier_month_summary(frame: pd.DataFrame) -> pd.DataFrame:
    # Aggregate by carrier and flight_date (day) to provide daily KPI trends.
    # Ensure 'flight_date' is present and formatted as string for Streamlit compatibility
    frame = frame.copy()
    if pd.api.types.is_datetime64_any_dtype(frame["flight_date"]):
        frame["flight_date"] = frame["flight_date"].dt.strftime("%Y-%m-%d")
    group = (
        frame.groupby(["carrier", "flight_date"], as_index=False)
        .agg(
            avg_arr_delay=("arr_delay", "mean"),
            avg_dep_delay=("dep_delay", "mean"),
            flights=("flight_date", "count"),
            cancel_rate=("cancelled", "mean"),
        )
        .round(2)
    )
    return group


def origin_cancel_summary(frame: pd.DataFrame) -> pd.DataFrame:
    # Highlight stations that struggle with cancellations.
    group = (
        frame.groupby("origin", as_index=False)
        .agg(
            cancel_rate=("cancelled", "mean"),
            flights=("flight_date", "count"),
        )
        .round(3)
        .sort_values("cancel_rate", ascending=False)
    )
    return group


def route_delay_summary(frame: pd.DataFrame) -> pd.DataFrame:
    # Focus on origin/destination pairs to spotlight route-level congestion.
    group = (
        frame.groupby(["origin", "destination"], as_index=False)
        .agg(
            avg_dep_delay=("dep_delay", "mean"),
            avg_arr_delay=("arr_delay", "mean"),
            flights=("flight_date", "count"),
        )
        .round(2)
        .sort_values("avg_arr_delay", ascending=False)
    )
    return group


def run_aggregate(settings: Settings | None = None) -> Dict[str, int]:
    settings = settings or get_settings()
    configure_logging()
    client = get_mongo_client(settings)
    clean_collection = get_collection(client, settings.clean_collection, settings)

    carrier_collection = get_collection(client, settings.agg_carrier_collection, settings)
    origin_collection = get_collection(client, settings.agg_origin_collection, settings)
    route_collection = get_collection(client, settings.agg_route_collection, settings)

    carrier_collection.drop()
    origin_collection.drop()
    route_collection.drop()

    frame = pd.DataFrame(list(clean_collection.find({}, projection={"_id": 0})))
    if frame.empty:
        logger.warning("Clean collection is empty; skipping aggregation stage")
        return {"carrier": 0, "origin": 0, "route": 0}

    summaries = {
        "carrier": carrier_month_summary(frame),
        "origin": origin_cancel_summary(frame),
        "route": route_delay_summary(frame),
    }

    carrier_collection.insert_many(summaries["carrier"].to_dict("records"))
    origin_collection.insert_many(summaries["origin"].to_dict("records"))
    route_collection.insert_many(summaries["route"].to_dict("records"))

    logger.info("Aggregations complete", counts={k: len(v) for k, v in summaries.items()})
    return {key: len(value) for key, value in summaries.items()}
