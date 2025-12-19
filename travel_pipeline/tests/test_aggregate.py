"""Tests for aggregation helper functions."""

from __future__ import annotations

import pandas as pd

from travel_pipeline.aggregate.pipeline import (
    carrier_month_summary,
    origin_cancel_summary,
    route_delay_summary,
)


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "carrier": ["AA", "AA", "DL"],
            "month": [1, 2, 1],
            "arr_delay": [10.0, -5.0, 3.0],
            "dep_delay": [8.0, -3.0, 2.0],
            "flight_date": pd.to_datetime(["2025-01-01", "2025-02-01", "2025-01-05"], utc=True),
            "cancelled": [False, True, False],
            "origin": ["JFK", "JFK", "ATL"],
            "destination": ["LAX", "LAX", "LAX"],
        }
    )


def test_carrier_daily_summary_returns_expected_columns():
    summary = carrier_month_summary(_frame())
    # Daily carrier summary should include carrier, flight_date and key KPI columns.
    expected = {"carrier", "flight_date", "avg_arr_delay", "avg_dep_delay", "flights", "cancel_rate"}
    assert expected.issubset(set(summary.columns))


def test_origin_cancel_summary_orders_by_cancel_rate():
    summary = origin_cancel_summary(_frame())
    assert summary.iloc[0]["origin"] == "JFK"


def test_route_delay_summary_calculates_route_key_counts():
    summary = route_delay_summary(_frame())
    assert summary.iloc[0]["flights"] >= 1
