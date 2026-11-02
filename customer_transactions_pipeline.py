"""Run setup/metadata, GCS-to-Bronze, Bronze-to-Silver, then Silver-to-Gold."""
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = "project-437b3d91-7f41-4318-ad5"
REGION = "us-central1"
LOCATION = "US"
CLUSTER_NAME = "bronze-notebook-cluster"  # Must already be running.
BUCKET = "hackathon-hcl"
PYSPARK_URI = f"gs://{BUCKET}/pipeline/gcs_to_bronze.py"
METADATA_OBJECT = "pipeline/metadata_entity_config.jsonl"
SQL_DIR = Path(__file__).resolve().parent


def sql_text(filename):
    return (SQL_DIR / filename).read_text(encoding="utf-8")


with DAG(
    dag_id="customer_transactions_gcs_bronze_silver_gold",
    description="Metadata setup -> GCS/Dataproc Bronze -> BigQuery Silver -> Gold",
    start_date=pendulum.datetime(2026, 10, 28, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2},
    tags=["banking", "gcs", "bigquery", "dataproc"],
) as dag:
    setup_bigquery = BigQueryInsertJobOperator(
        task_id="create_datasets_and_bronze_metadata_table",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {"query": sql_text("00_setup_bigquery.sql"), "useLegacySql": False}
        },
    )

    load_metadata = GCSToBigQueryOperator(
        task_id="load_entity_metadata_before_ingestion",
        project_id=PROJECT_ID,
        bucket=BUCKET,
        source_objects=[METADATA_OBJECT],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.bronze_dataset.metadata_entity_config"
        ),
        source_format="NEWLINE_DELIMITED_JSON",
        schema_fields=[
            {"name": "entity_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "target_table", "type": "STRING", "mode": "REQUIRED"},
            {"name": "business_columns", "type": "STRING", "mode": "REPEATED"},
            {"name": "primary_key", "type": "STRING", "mode": "REQUIRED"},
            {"name": "required_columns", "type": "STRING", "mode": "REPEATED"},
            {"name": "expected_batches", "type": "STRING", "mode": "REPEATED"},
            {"name": "load_behavior", "type": "STRING", "mode": "REQUIRED"},
        ],
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        location=LOCATION,
    )

    bronze_load = DataprocSubmitJobOperator(
        task_id="load_gcs_to_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_URI,
                "args": [],
            },
        },
        deferrable=False,
    )

    silver_build = BigQueryInsertJobOperator(
        task_id="build_silver_and_run_dq",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {"query": sql_text("01_bronze_to_silver.sql"), "useLegacySql": False}
        },
    )

    gold_build = BigQueryInsertJobOperator(
        task_id="build_gold_marts_and_assertions",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {"query": sql_text("02_silver_to_gold.sql"), "useLegacySql": False}
        },
    )

    setup_bigquery >> load_metadata >> bronze_load >> silver_build >> gold_build