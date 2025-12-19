"""Unit tests for ingestion helpers."""

from __future__ import annotations

import pandas as pd

from travel_pipeline.ingest.pipeline import infer_schema


def test_infer_schema_returns_dtype_mapping():
    frame = pd.DataFrame({"YEAR": [2025, 2025], "MONTH": [1, 2], "OP_UNIQUE_CARRIER": ["AA", "DL"]})
    schema = infer_schema(frame)
    assert schema["YEAR"].startswith("int")
    assert schema["OP_UNIQUE_CARRIER"] == "object"
