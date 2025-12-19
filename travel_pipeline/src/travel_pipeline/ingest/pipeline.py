"""Raw layer ingestion utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd
from loguru import logger
from pymongo.collection import Collection
from pymongo.mongo_client import MongoClient

from travel_pipeline.core.config import Settings, get_settings
from travel_pipeline.core.logging import configure_logging
from travel_pipeline.db.mongo import get_collection, get_mongo_client


def infer_schema(frame: pd.DataFrame) -> Dict[str, str]:
    """Return a pandas dtype mapping used for metadata collection."""

    return {column: str(dtype) for column, dtype in frame.dtypes.items()}


def chunk_csv(path: Path, chunk_size: int) -> Iterable[pd.DataFrame]:
    """Yield pandas chunks while logging helpful progress info."""

    logger.info("Reading chunked CSV", file=str(path), chunk_size=chunk_size)
    for chunk in pd.read_csv(path, chunksize=chunk_size, low_memory=False):
        yield chunk


def insert_chunk(chunk: pd.DataFrame, collection: Collection) -> int:
    """Insert a pandas chunk into MongoDB after filling NaNs with None."""

    # Pandas stores missing values as NaN which PyMongo cannot serialize, so convert to None.
    documents = chunk.where(pd.notnull(chunk), None).to_dict("records")
    if not documents:
        return 0
    collection.insert_many(documents)
    return len(documents)


def persist_metadata(
    metadata_collection: Collection,
    file_path: Path,
    rows_inserted: int,
    schema: Dict[str, str],
) -> None:
    payload = {
        "source_file": file_path.name,
        "rows_inserted": rows_inserted,
        "schema": schema,
    }
    metadata_collection.insert_one(payload)


def ingest_raw(settings: Settings | None = None) -> Dict[str, int]:
    """Ingest every configured CSV into the raw Mongo collection."""

    settings = settings or get_settings()
    configure_logging()
    client = get_mongo_client(settings)
    raw_collection = get_collection(client, settings.raw_collection, settings)
    metadata_collection = get_collection(client, settings.metadata_collection, settings)

    raw_collection.drop()
    metadata_collection.drop()

    summary: Dict[str, int] = {}
    for csv_path in settings.raw_files:
        # Track rows per file so we can log and store them in the metadata collection.
        inserted_total = 0
        last_schema: Dict[str, str] = {}
        for chunk in chunk_csv(csv_path, settings.chunk_size):
            # Persist each chunk and remember the last schema snapshot for metadata.
            inserted_total += insert_chunk(chunk, raw_collection)
            last_schema = infer_schema(chunk)
        persist_metadata(metadata_collection, csv_path, inserted_total, last_schema)
        summary[csv_path.name] = inserted_total
        logger.info(
            "Finished raw ingestion",
            file=csv_path.name,
            rows=inserted_total,
            collection=settings.raw_collection,
        )
    return summary


def attach_indexes(client: MongoClient, settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    raw_collection = get_collection(client, settings.raw_collection, settings)
    raw_collection.create_index(
        [
            ("FL_DATE", 1),
            ("OP_UNIQUE_CARRIER", 1),
            ("ORIGIN", 1),
            ("DEST", 1),
        ]
    )
