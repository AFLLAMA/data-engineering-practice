from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator
)
from airflow.utils.dates import days_ago
from airflow.models import Variable


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    "process_user_profiles",
    default_args=default_args,
    description="From jsonl file create Bronze dataset, rename and save to Silver",
    start_date=days_ago(1),
    tags=["bronze_to_silver", "raw_to_bronze", "bigquery", "R_D"],
) as dag:
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_bronze_external_table",
        bucket=Variable.get("BUCKET_NAME"),
        source_objects=["user_profiles/user_profiles.json"],
        destination_project_dataset_table=f"{Variable.get("PROJECT_ID")}.bronze.user_profiles",
        schema_fields=[
            {"name": "state", "type": "STRING", "mode": "REQUIRED"},
            {"name": "birth_date", "type": "STRING", "mode": "REQUIRED"},
            {"name": "full_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "phone_number", "type": "STRING", "mode": "REQUIRED"},
            {"name": "email", "type": "STRING", "mode": "REQUIRED"},
        ],
        source_format="NEWLINE_DELIMITED_JSON",
        skip_leading_rows=1,
        gcp_conn_id="my_gcs_connection_id"
    )

    query_transform = f"""
    CREATE OR REPLACE TABLE `{Variable.get("PROJECT_ID")}.silver.user_profiles` AS
    SELECT 
        state, 
        CAST(birth_date AS DATE) AS birth_date, 
        full_name, 
        phone_number, 
        email
    FROM `{Variable.get("PROJECT_ID")}.bronze.user_profiles`
    """

    transform_to_silver = BigQueryInsertJobOperator(
        task_id="transform_bronze_to_silver",
        configuration={
            "query": {
                "query": query_transform,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="my_gcs_connection_id"
    )

    create_external_table >> transform_to_silver
