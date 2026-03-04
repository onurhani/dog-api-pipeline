"""
Airflow DAG: Dog API Pipeline
==============================
Uses dlt's PipelineTasksGroup to turn each dlt resource into an Airflow task.
This gives per-resource retry, visibility, and logs.

"""

import sys, os
from datetime import datetime, timedelta

from airflow import DAG
from dlt.helpers.airflow_helper import PipelineTasksGroup
import dlt

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from pipelines.dog_api_pipeline import dog_api_source


default_args = {
    "owner": "onur",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="dog_api_pipeline",
    description="Incremental Dog API ingestion via dlt → DuckDB",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dlt", "dog-api", "duckdb"],
) as dag:

    tasks = PipelineTasksGroup(
        pipeline_name="dog_api_pipeline",
        use_data_folder=False,
        wipe_local_data=True,
        save_load_info=True,
        save_trace_info=True,
        abort_task_if_any_job_failed=True,
    )

    pipeline = dlt.pipeline(
        pipeline_name="dog_api_pipeline",
        destination="duckdb",
        dataset_name="dog_api",
    )

    # decompose="serialize" → one Airflow task per resource, sequential
    # Each resource (breeds, images, votes, favourites) is its own task node.
    tasks.add_run(
        pipeline=pipeline,
        data=dog_api_source(),
        decompose="serialize",
        trigger_rule="all_done",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )