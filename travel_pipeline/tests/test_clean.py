"""Unit tests for cleaning logic."""

from __future__ import annotations

import pandas as pd

from travel_pipeline.clean.pipeline import clean_dataframe, validate_records


def _sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "YEAR": [2025, 2025],
            "MONTH": [1, 1],
            # Match the real BTS format used in cleaning: "%m/%d/%Y %I:%M:%S %p"
            "FL_DATE": ["1/1/2025 12:00:00 AM", "1/1/2025 12:00:00 AM"],
            "OP_UNIQUE_CARRIER": ["aa", "aa"],
            "TAIL_NUM": ["n123", "n123"],
            "OP_CARRIER_FL_NUM": [10, 10],
            "ORIGIN_AIRPORT_ID": [12478, 12478],
            "ORIGIN_AIRPORT_SEQ_ID": [1247805, 1247805],
            "ORIGIN_CITY_MARKET_ID": [31703, 31703],
            "ORIGIN": ["jfk ", "jfk "],
            "DEST_AIRPORT_ID": [12892, 12892],
            "DEST_AIRPORT_SEQ_ID": [1289208, 1289208],
            "DEST_CITY_MARKET_ID": [32575, 32575],
            "DEST": [" lax", " lax"],
            "CRS_DEP_TIME": [800, 800],
            "DEP_TIME": [754, 754],
            "DEP_DELAY": [-6.0, -6.0],
            "TAXI_OUT": [35.0, 35.0],
            "TAXI_IN": [4.0, 4.0],
            "CRS_ARR_TIME": [1129, 1129],
            "ARR_TIME": [1107, 1107],
            "ARR_DELAY": [-22.0, -22.0],
            "CANCELLED": [0.0, 0.0],
            "DIVERTED": [0.0, 0.0],
        }
    )


def test_clean_dataframe_normalizes_text_and_dedupes():
    frame = _sample_frame()
    cleaned = clean_dataframe(frame)
    assert len(cleaned) == 1
    assert cleaned.iloc[0]["origin"] == "JFK"
    assert cleaned.iloc[0]["destination"] == "LAX"


def test_validate_records_returns_serializable_dicts():
    cleaned = clean_dataframe(_sample_frame())
    documents = validate_records(cleaned)
    assert documents[0]["carrier"] == "AA"
    assert documents[0]["cancelled"] is False
