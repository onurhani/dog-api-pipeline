# Dog API Pipeline

A production-style data ingestion pipeline built with [dlt](https://dlthub.com) and orchestrated with Apache Airflow. Pulls data from [The Dog API](https://thedogapi.com) incrementally into DuckDB.

Built as part of a hands-on exploration of the modern data stack — specifically dlt as an alternative to heavier ingestion tools like Airbyte or Fivetran for API-based sources.

---

## What it does

Ingests five resources from The Dog API on a daily schedule, using merge and replace strategies depending on the nature of each resource:

| Resource | Strategy | Notes |
|---|---|---|
| `breeds` | replace | ~200 static rows, full refresh daily |
| `images` | merge | deduped on `id` |
| `votes` | merge | requires Bearer auth (API quirk) |
| `favourites` | merge | no cursor available in API spec |

All resources land in a local DuckDB file under the `dog_api` dataset.

---

## Stack

- **[dlt](https://dlthub.com)** — ingestion and schema management
- **DuckDB** — local analytical destination
- **Apache Airflow** — orchestration (Dockerized)
- **Python 3.11+**

---

## Architecture

```
The Dog API
    │
    ▼
dlt source (pipelines/dog_api_pipeline.py)
    │   ├── breeds          (replace)
    │   ├── images          (merge)
    │   ├── votes           (merge)
    │   ├── favourites      (merge)
    │   └── breed_facts     (replace, child resource)
    │
    ▼
DuckDB (dog_api_pipeline.duckdb)
    │
    ▼
Airflow DAG (dags/dag_dog_api.py)
    └── PipelineTasksGroup, decompose="serialize"
        └── one Airflow task per resource
```

The DAG runs daily at 06:00 UTC. Each resource is a separate Airflow task via dlt's `PipelineTasksGroup`, which makes it easy to rerun individual resources without triggering a full pipeline reload.

---

## Running locally

**Prerequisites:** Python 3.11+, Docker (for Airflow)

```bash
# Clone and set up environment
git clone https://github.com/yourusername/dog-api-pipeline.git
cd dog-api-pipeline

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Add your API key to `.dlt/secrets.toml` (never committed):
```toml
[sources.dog_api]
api_key = "your_key_here"
```

Get a free API key at [thedogapi.com](https://thedogapi.com).

```bash
# Run the pipeline once
python pipelines/dog_api_pipeline.py

# Full refresh (drops incremental state)
python pipelines/dog_api_pipeline.py --full-refresh

# Inspect loaded data
dlt pipeline dog_api_pipeline show
duckdb dog_api_pipeline.duckdb
```

---

## Running with Airflow

```bash
# Add your API key to .env (for Docker)
echo "DOG_API_KEY=your_key_here" > .env

docker-compose up
```

Airflow UI at [http://localhost:8080](http://localhost:8080) — credentials: `admin / admin`

The DAG `dog_api_pipeline` will appear automatically. Trigger it manually on first run or wait for the 06:00 UTC schedule.

---

## Tests

```bash
pytest tests/
```

---

## Design notes

**Why dlt over Airbyte?**
For API-based ingestion at this scale, dlt is significantly lighter. No infrastructure to run, schema inference is automatic, and incremental state is managed natively. The tradeoff is that you write Python instead of clicking through a UI — which is a feature, not a bug, if you want version-controlled, testable pipelines.

**Pagination quirk**
The Dog API uses zero-based pagination (`?page=0` is the first page, not `?page=1`). All paginators are configured with `initial_page: 0`. Worth noting because most APIs are one-based and dlt's REST source defaults accordingly.

**Auth inconsistency**
The `/votes` endpoint requires Bearer token auth while all other endpoints use `x-api-key` header. Both are handled in the source definition.

---

## Project structure

```
├── pipelines/
│   └── dog_api_pipeline.py   # dlt source and pipeline entrypoint
├── dags/
│   └── dag_dog_api.py        # Airflow DAG definition
├── tests/
├── .dlt/
│   ├── config.toml           # non-secret config (page_limit, etc.)
│   └── secrets.toml          # gitignored — API keys go here
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## What I learned

- dlt's incremental loading model using cursor fields and pipeline state
- How `PipelineTasksGroup` maps dlt resources to individual Airflow tasks
- Handling API authentication inconsistencies within a single dlt source
- The zero-based pagination edge case and how to configure dlt's REST source for it
- Running dlt pipelines inside Dockerized Airflow without leaking secrets
