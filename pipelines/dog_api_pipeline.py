"""

Usage:
    python dog_api_pipeline.py
    python dog_api_pipeline.py --full-refresh
"""

import argparse
import logging

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dlt.source(
    name="dog_api",
    section="dog_api",          # maps to [sources.dog_api] in .dlt/config.toml and secrets.toml
    max_table_nesting=2,        # images contain a nested `breeds` array → creates images__breeds table
    schema_contract="evolve",   # allow new fields from the API without breaking the pipeline
    parallelized=True,          # extract breeds, images, votes, favourites concurrently
)
def dog_api_source(
    api_key: str = dlt.secrets.value,
    page_limit: int = dlt.config.value,
):
    """
    Dog API source — corrected against the actual OpenAPI spec.

    RESOURCE STRATEGIES:
    ┌──────────────┬───────────────────┬───────────┬──────────────────────────────────┐
    │ Resource     │ write_disposition │ auth      │ Notes                            │
    ├──────────────┼───────────────────┼───────────┼──────────────────────────────────┤
    │ breeds       │ replace           │ x-api-key │ ~200 static rows                 │
    │ images       │ merge             │ x-api-key │ Public images, merge on id       │
    │ votes        │ merge             │ x-api-key │ x-api-key works; Bearer returns 401 │
    │ favourites   │ merge             │ x-api-key │ No cursor param in API spec      │
    └──────────────┴───────────────────┴───────────┴──────────────────────────────────┘
    NOTE: /facts requires a paid plan (403 on free tier). /breeds/{id}/facts returns 404.

    KEY FIXES vs v1:
    - base_page=0: Dog API is ZERO-BASED (?page=0 is first page)
    - votes uses x-api-key auth (Bearer returns 401 despite spec; use same header as all endpoints)
    - removed created_after from favourites: param doesn't exist in spec
    - facts removed: /facts requires a paid plan (403), /breeds/{id}/facts returns 404
    """

    config: RESTAPIConfig = {

        "client": {
            "base_url": "https://api.thedogapi.com/v1/",
            "auth": {
                "type": "api_key",
                "name": "x-api-key",
                "api_key": api_key,
                "location": "header",
            },
            # FIX 1: Dog API is zero-based. base_page=0 is critical.
            # Without it, page 0 (the first page!) is skipped entirely.
            "paginator": {
                "type": "page_number",
                "page_param": "page",
                "base_page": 0,
                "total_path": None,
            },
        },

        "resource_defaults": {
            "endpoint": {
                "params": {
                    "limit": page_limit,
                    "order": "ASC",
                },
            },
        },

        "resources": [

            # ── BREEDS ────────────────────────────────────────────────────
            # Static reference data. Full replace every run.
            {
                "name": "breeds",
                "primary_key": "id",
                "write_disposition": "replace",
                "endpoint": {
                    "path": "breeds",
                    "params": {"limit": page_limit},
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 0,
                        "total_path": None,
                        "maximum_page": 10,
                    },
                },
            },

            # ── IMAGES ────────────────────────────────────────────────────
            # Public images with breed metadata. Merge on id.
            {
                "name": "images",
                "primary_key": "id",
                "write_disposition": "merge",
                "endpoint": {
                    "path": "images/search",
                    "params": {
                        "limit": page_limit,
                        "has_breeds": 1,
                        "order": "ASC",
                    },
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 0,
                        "total_path": None,
                        "maximum_page": 10,
                    },
                },
            },

            # ── VOTES ─────────────────────────────────────────────────────
            # Uses the same x-api-key header auth as all other endpoints.
            {
                "name": "votes",
                "primary_key": "id",
                "write_disposition": "merge",
                "endpoint": {
                    "path": "votes",
                    "params": {
                        "limit": page_limit,
                        "order": "ASC",
                    },
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 0,
                        "total_path": None,
                        "maximum_page": 50,
                    },
                },
            },

            # ── FAVOURITES ────────────────────────────────────────────────
            # FIX 3: removed "incremental" block — created_after param
            # does NOT exist in the spec. Merge on id is the correct strategy.
            {
                "name": "favourites",
                "primary_key": "id",
                "write_disposition": "merge",
                "endpoint": {
                    "path": "favourites",
                    "params": {
                        "limit": page_limit,
                        "order": "ASC",
                    },
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 0,
                        "total_path": None,
                        "maximum_page": 50,
                    },
                },
            },

        ],
    }

    yield from rest_api_resources(config)


def run_pipeline(full_refresh: bool = False) -> None:
    """Run the Dog API pipeline."""

    pipeline = dlt.pipeline(
        pipeline_name="dog_api_pipeline",
        destination="duckdb",
        dataset_name="dog_api",
        progress="log",
    )

    source = dog_api_source()

    if full_refresh:
        logger.info("Full refresh — dropping state and reloading from scratch")
        pipeline.drop()
        load_info = pipeline.run(source, write_disposition="replace")
    else:
        logger.info("Incremental / merge run")
        load_info = pipeline.run(source)

    print("\n" + "=" * 60)
    print("LOAD RESULT")
    print("=" * 60)
    print(load_info)

    row_counts = pipeline.last_trace.last_normalize_info
    if row_counts:
        print("\nRow counts this run:")
        print(row_counts)

    if load_info.has_failed_jobs:
        for job in load_info.failed_jobs:
            logger.error(f"FAILED: {job.job_file_info}: {job.failed_message}")
        raise RuntimeError("Pipeline finished with failed jobs — see errors above")

    print("\nInspect: dlt pipeline dog_api_pipeline show")
    print("Query:   duckdb dog_api_pipeline.duckdb")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dog API dlt pipeline")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Drop state and reload everything from scratch",
    )
    args = parser.parse_args()
    run_pipeline(full_refresh=args.full_refresh)