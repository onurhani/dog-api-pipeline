# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

A data ingestion pipeline that pulls data from [The Dog API](https://thedogapi.com) using **dlt** (data load tool) and lands it in **DuckDB**. The pipeline is orchestrated by **Apache Airflow**.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Set your API key in `.dlt/secrets.toml` (never commit this file):
```toml
[sources.dog_api]
api_key = "your_key_here"
```

Or via environment variable: `SOURCES__DOG_API__API_KEY=your_key`

## Running the pipeline

```bash
# Incremental / merge run
python pipelines/dog_api_pipeline.py

# Full refresh (drops state and reloads from scratch)
python pipelines/dog_api_pipeline.py --full-refresh
```

## Running Airflow locally

```bash
DOG_API_KEY=your_key docker-compose up
```

Airflow UI at http://localhost:8080 (admin/admin). The DAG `dog_api_pipeline` runs daily at 06:00 UTC.

## Tests

```bash
pytest tests/
```

## Architecture

**`pipelines/dog_api_pipeline.py`** — The core dlt source (`dog_api_source`) and `run_pipeline()` entrypoint. Defines 5 resources against `https://api.thedogapi.com/v1/`:

| Resource    | Strategy  | Auth     | Notes |
|-------------|-----------|----------|-------|
| `breeds`    | replace   | x-api-key | ~200 static rows |
| `images`    | merge     | x-api-key | merge on `id` |
| `votes`     | merge     | bearer   | API spec requires Bearer for GET /votes |
| `favourites`| merge     | x-api-key | no cursor param in API spec |
| `breed_facts` | replace | x-api-key | child of breeds: `/breeds/{id}/facts` |

**`dags/dag_dog_api.py`** — Airflow DAG that wraps the pipeline using dlt's `PipelineTasksGroup` with `decompose="serialize"`, creating one Airflow task per resource. The `pipelines/` directory is added to `sys.path` at import time.

**Key dlt config** (`.dlt/config.toml`): `page_limit = 25` (Dog API max is 100).

**Dog API pagination quirk**: The API is zero-based (`?page=0` is the first page). All paginators use `initial_page: 0` — this is critical.

**Data destination**: DuckDB file `dog_api_pipeline.duckdb` in the project root (gitignored). Dataset name: `dog_api`.

**`rest_api_pipeline.py`** — Reference/example file for GitHub and Pokémon REST API pipelines using dlt. Not part of the Dog API pipeline.

## Inspecting loaded data

```bash
dlt pipeline dog_api_pipeline show
duckdb dog_api_pipeline.duckdb
```
