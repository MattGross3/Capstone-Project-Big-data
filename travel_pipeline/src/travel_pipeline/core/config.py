"""Application configuration and central settings management.

This module centralizes every configurable value so stage-specific modules
can remain testable and environment agnostic. Settings are validated through
Pydantic to guard against missing or malformed environment variables.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import List

from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(ENV_PATH)


def _path_env(key: str, default: Path) -> Path:
    value = os.getenv(key)
    return Path(value) if value else default


def _int_env(key: str, default: int) -> int:
    value = os.getenv(key)
    return int(value) if value else default


class Settings(BaseModel):
    """Typed representation of environment configuration."""

    mongodb_uri: str = Field(
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true"),
        description="Full connection string used by pymongo",
    )
    database: str = Field(default=os.getenv("MONGODB_DATABASE", "travel_ops"))
    raw_collection: str = Field(default=os.getenv("RAW_COLLECTION", "flights_raw"))
    clean_collection: str = Field(default=os.getenv("CLEAN_COLLECTION", "flights_clean"))
    agg_carrier_collection: str = Field(default=os.getenv("AGG_CARRIER_COLLECTION", "agg_carrier_month"))
    agg_origin_collection: str = Field(default=os.getenv("AGG_ORIGIN_COLLECTION", "agg_origin_cancel"))
    agg_route_collection: str = Field(default=os.getenv("AGG_ROUTE_COLLECTION", "agg_route_delay"))
    metadata_collection: str = Field(default=os.getenv("METADATA_COLLECTION", "metadata"))
    chunk_size: int = Field(default_factory=lambda: _int_env("CHUNK_SIZE", 100_000), ge=10_000)
    batch_size: int = Field(default_factory=lambda: _int_env("BATCH_SIZE", 50_000), ge=5_000)
    jan_file: Path = Field(default_factory=lambda: _path_env("JAN_FILE", PROJECT_ROOT / "data" / "JAN_DATA.csv"))
    feb_file: Path = Field(default_factory=lambda: _path_env("FEB_FILE", PROJECT_ROOT / "data" / "FEB_DATA.csv"))

    @property
    def raw_files(self) -> List[Path]:
        """Return every file that should be ingested into the raw layer."""

        return [path for path in (self.jan_file, self.feb_file) if path.exists()]

    @field_validator("mongodb_uri")
    @classmethod
    def ensure_uri(cls, value: str) -> str:
        if not value.startswith("mongodb://") and not value.startswith("mongodb+srv://"):
            msg = "MONGODB_URI must start with mongodb:// or mongodb+srv://"
            raise ValueError(msg)
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load settings once per interpreter for reuse across modules."""

    return Settings()
