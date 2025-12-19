# Travel Pipeline (BTS On-Time Performance)

Distributed data engineering project that ingests BTS Reporting Carrier On-Time Performance data into a sharded MongoDB cluster, cleans and validates the schema with Pydantic, builds aggregated gold tables, and powers a Streamlit dashboard for visual analysis.

## Project Map

- Docker Compose launches a sharded MongoDB cluster (config server, two shards, mongos router, bootstrapper).
- UV-managed Python project with pandas processing, Pydantic validation, loguru logging, mypy typing, and pytest tests.
- Pipelines follow a bronze (raw) → silver (clean) → gold (aggregated) flow. Each stage persists back to MongoDB.
- Streamlit reads directly from the gold collections to render interactive visuals.
- Architecture diagram: see [docs/architecture.mmd](docs/architecture.mmd).

## Getting Started

1. **Copy data**
	- Place `JAN_DATA.csv` and `FEB_DATA.csv` into `data/` (create the folder) or update `.env` paths.
2. **Environment setup**
	- `cp .env.example .env` and edit values if needed (credentials, file paths, chunk sizes).
	- `uv sync` to install project and dev dependencies.
3. **MongoDB cluster**
	- `docker compose up -d configsvr shard1 shard2 mongos` then `docker compose up bootstrap`.
	- Verify routing: `mongosh --host localhost:27017 --eval "sh.status()"`.
4. **Pipeline execution**
	- Raw ingestion: `uv run travel-pipeline-ingest`.
	- Cleaning: `uv run travel-pipeline-clean`.
	- Aggregations: `uv run travel-pipeline-aggregate`.
5. **Visuals**
	- `uv run streamlit run src/travel_pipeline/visuals/app.py` and explore the three dashboards.

## Testing & Quality Gates

- `uv run pytest` executes three unit test modules covering ingestion helpers, cleaning logic, and aggregations.
- `uv run mypy .` enforces typing discipline across the `src/` tree.
- Logging is configured through `travel_pipeline.core.logging.configure_logging` and emits structured events per stage.


## Repository Contents

- `docker-compose.yml` + `docker/mongo/bootstrap.sh`: MongoDB sharded cluster automation.
- `src/travel_pipeline/core|db|ingest|clean|aggregate|visuals`: modular Python code with thorough comments.
- `tests/`: pytest suites.
- `.env.example`, `mypy.ini`, `pytest.ini`, and UV-managed `pyproject.toml` for reproducibility.


