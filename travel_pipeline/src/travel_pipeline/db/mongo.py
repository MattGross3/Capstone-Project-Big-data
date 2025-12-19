"""Utility helpers for MongoDB connectivity."""

from __future__ import annotations

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ServerSelectionTimeoutError

from travel_pipeline.core.config import Settings, get_settings
from travel_pipeline.core.logging import get_logger

logger = get_logger(module="mongo")


def get_mongo_client(settings: Settings | None = None) -> MongoClient:
    """Instantiate a Mongo client with a conservative timeout."""

    settings = settings or get_settings()
    client = MongoClient(settings.mongodb_uri, serverSelectionTimeoutMS=5_000)
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError as exc:
        logger.error("Unable to reach MongoDB: {exc}", exc=exc)
        raise
    return client


def get_database(client: MongoClient, settings: Settings | None = None) -> Database:
    settings = settings or get_settings()
    return client[settings.database]


def get_collection(client: MongoClient, name: str, settings: Settings | None = None) -> Collection:
    database = get_database(client, settings)
    return database[name]
