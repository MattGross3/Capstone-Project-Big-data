"""Pydantic models that define the canonical flight schema."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

import pandas as pd
from pydantic import BaseModel, Field


class FlightRecord(BaseModel):
    """Validated representation of a cleaned BTS record."""

    year: int
    month: int
    flight_date: datetime
    carrier: str = Field(min_length=2)
    tail_number: str | None = None
    flight_number: int = Field(ge=0)
    origin_airport_id: int
    origin_airport_seq_id: int
    origin_city_market_id: int
    origin: str
    dest_airport_id: int
    dest_airport_seq_id: int
    dest_city_market_id: int
    destination: str
    crs_dep_time: int
    dep_time: float | None = None
    dep_delay: float | None = None
    taxi_out: float | None = None
    taxi_in: float | None = None
    crs_arr_time: int
    arr_time: float | None = None
    arr_delay: float | None = None
    cancelled: bool = False
    diverted: bool = False

    class Config:
        json_encoders = {datetime: lambda value: value.isoformat()}

    @classmethod
    def from_raw_record(cls, record: Dict[str, Any]) -> "FlightRecord":
        """Convert a raw Mongo document or CSV row into the canonical schema."""

        normalized = record.copy()
        normalized["FL_DATE"] = _normalize_date(normalized.get("FL_DATE"))
        normalized["OP_UNIQUE_CARRIER"] = _safe_upper(normalized.get("OP_UNIQUE_CARRIER"))
        normalized["TAIL_NUM"] = _safe_upper(normalized.get("TAIL_NUM"))
        normalized["ORIGIN"] = _safe_upper(normalized.get("ORIGIN"))
        normalized["DEST"] = _safe_upper(normalized.get("DEST"))
        normalized["CANCELLED"] = bool(normalized.get("CANCELLED", 0))
        normalized["DIVERTED"] = bool(normalized.get("DIVERTED", 0))

        field_map = {
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

        payload: Dict[str, Any] = {alias: normalized.get(column) for column, alias in field_map.items()}
        return cls(**payload)


def _normalize_date(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if value is None:
        raise ValueError("FL_DATE cannot be null")
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"Unable to parse FL_DATE value {value}")
    return ts.to_pydatetime()


def _safe_upper(value: Any) -> str | None:
    return str(value).strip().upper() if value not in (None, "") else None
