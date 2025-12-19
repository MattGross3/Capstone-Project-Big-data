"""Clean layer transformations for BTS data."""

from __future__ import annotations

from typing import Dict, Iterable, List

import pandas as pd
from loguru import logger
from pymongo.collection import Collection

from travel_pipeline.core.config import Settings, get_settings
from travel_pipeline.core.logging import configure_logging
from travel_pipeline.db.mongo import get_collection, get_mongo_client
from travel_pipeline.models.flight import FlightRecord

RENAMES = {
    "YEAR": "year",
    "MONTH": "month",
    "FL_DATE": "flight_date",
    "OP_UNIQUE_CARRIER": "carrier",
    "TAIL_NUM": "tail_number",
    "OP_CARRIER_FL_NUM": "flight_number",
    "ORIGIN_AIRPORT_ID": "origin_airport_id",
    "ORIGIN_AIRPORT_SEQ_ID": "origin_airport_seq_id",
    "ORIGIN_CITY_MARKET_ID": "origin_city_market_id",
    "ORIGIN": "origin",
    "DEST_AIRPORT_ID": "dest_airport_id",
    "DEST_AIRPORT_SEQ_ID": "dest_airport_seq_id",
    "DEST_CITY_MARKET_ID": "dest_city_market_id",
    "DEST": "destination",
    "CRS_DEP_TIME": "crs_dep_time",
    "DEP_TIME": "dep_time",
    "DEP_DELAY": "dep_delay",
    "TAXI_OUT": "taxi_out",
    "TAXI_IN": "taxi_in",
    "CRS_ARR_TIME": "crs_arr_time",
    "ARR_TIME": "arr_time",
    "ARR_DELAY": "arr_delay",
    "CANCELLED": "cancelled",
    "DIVERTED": "diverted",
}

TEXT_COLUMNS = ["carrier", "tail_number", "origin", "destination"]
NUMERIC_FILL = [
    "dep_delay",
    "arr_delay",
    "taxi_out",
    "taxi_in",
]
DEDUP_COLUMNS = [
    "flight_date",
    "carrier",
    "flight_number",
    "origin",
    "destination",
]


def clean_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    """Apply deterministic cleaning steps to a dataframe."""


    cleaned = frame.rename(columns=RENAMES).copy()
    logger.info(f"After renaming: {len(cleaned)} records; columns: {list(cleaned.columns)}")
    logger.info(f"Sample flight_date values before parsing: {cleaned['flight_date'].dropna().astype(str).unique()[:5]}")

    # Convert date strings into timezone-aware timestamps with explicit format.
    cleaned["flight_date"] = pd.to_datetime(cleaned["flight_date"], format="%m/%d/%Y %I:%M:%S %p", utc=True, errors="coerce")
    logger.info(f"Sample flight_date values after parsing: {cleaned['flight_date'].dropna().astype(str).unique()[:5]}")
    logger.info(f"After date parsing: {len(cleaned)} records")

    for column in TEXT_COLUMNS:
        if column in cleaned:
            # Normalize casing and whitespace so joins/aggregations stay consistent.
            cleaned[column] = cleaned[column].fillna("").str.strip().str.upper()

    for column in NUMERIC_FILL:
        if column in cleaned:
            # Use the median to limit the impact of outliers on imputed values.
            cleaned[column] = cleaned[column].fillna(cleaned[column].median())

    # Convert all NaN in numeric columns to None for Pydantic compatibility
    numeric_nullable = [
        "dep_time", "arr_time", "dep_delay", "arr_delay", "taxi_out", "taxi_in"
    ]
    for column in numeric_nullable:
        if column in cleaned:
            cleaned[column] = cleaned[column].where(pd.notnull(cleaned[column]), None)

    before_dropna = len(cleaned)
    cleaned = cleaned.dropna(subset=["flight_date", "carrier", "origin", "destination"])
    logger.info(f"After dropna (flight_date, carrier, origin, destination): {len(cleaned)} records (dropped {before_dropna - len(cleaned)})")

    # Deduplicate to enforce one canonical record per carrier/flight/date/route.
    before_dedup = len(cleaned)
    cleaned = cleaned.drop_duplicates(subset=DEDUP_COLUMNS, keep="first")
    logger.info(f"After deduplication: {len(cleaned)} records (dropped {before_dedup - len(cleaned)})")

    if "cancelled" in cleaned:
        cleaned["cancelled"] = cleaned["cancelled"].astype(float).round().astype(bool)
    if "diverted" in cleaned:
        cleaned["diverted"] = cleaned["diverted"].astype(float).round().astype(bool)

    return cleaned


def validate_records(frame: pd.DataFrame) -> List[Dict]:
    """Validate records with Pydantic and return serializable dicts."""

    documents: List[Dict] = []
    for record in frame.to_dict("records"):
        # Pydantic provides both validation and type coercion.
        flight = FlightRecord(**record)
        documents.append(flight.model_dump())
    return documents


def run_clean(settings: Settings | None = None) -> int:
    settings = settings or get_settings()
    configure_logging()
    client = get_mongo_client(settings)
    raw_collection = get_collection(client, settings.raw_collection, settings)
    clean_collection = get_collection(client, settings.clean_collection, settings)

    clean_collection.drop()

    # Diagnostic: count records in raw collection before cleaning
    raw_count = raw_collection.count_documents({})
    logger.info(f"Raw collection contains {raw_count} records before cleaning.")

    total_inserted = 0
    cursor = raw_collection.find({}, projection={"_id": 0})
    batch: List[Dict] = []
    for record in cursor:
        batch.append(record)
        if len(batch) >= settings.batch_size:
            total_inserted += _flush_batch(batch, clean_collection)
            batch = []
    if batch:
        total_inserted += _flush_batch(batch, clean_collection)

    logger.info("Finished cleaning stage", rows=total_inserted)
    return total_inserted


def _flush_batch(batch: List[Dict], collection: Collection) -> int:
    frame = pd.DataFrame(batch)
    cleaned = clean_dataframe(frame)
    documents = validate_records(cleaned)
    if documents:
        collection.insert_many(documents)
    return len(documents)
