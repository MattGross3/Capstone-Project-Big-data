"""Command line interface for orchestrating pipeline stages."""

from __future__ import annotations

import argparse

from travel_pipeline.aggregate.pipeline import run_aggregate as run_aggregate_stage
from travel_pipeline.clean.pipeline import run_clean as run_clean_stage
from travel_pipeline.core.logging import configure_logging
from travel_pipeline.ingest.pipeline import ingest_raw


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Travel pipeline orchestrator")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    subparsers.add_parser("ingest", help="Load CSV files into MongoDB raw layer")
    subparsers.add_parser("clean", help="Transform raw documents into curated layer")
    subparsers.add_parser("aggregate", help="Compute gold tables for dashboards")
    return parser


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    args = build_parser().parse_args(argv)
    if args.command == "ingest":
        summary = ingest_raw()
        print(summary)
    elif args.command == "clean":
        inserted = run_clean_stage()
        print({"clean_rows": inserted})
    elif args.command == "aggregate":
        summary = run_aggregate_stage()
        print(summary)


def run_ingest() -> None:
    main(["ingest"])


def run_clean() -> None:  # type: ignore[override]
    main(["clean"])


def run_aggregate() -> None:
    main(["aggregate"])
