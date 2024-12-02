from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateExternalTableOperator
)
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    "process_sales",
    default_args=default_args,
    description="Create and transform partitioned Bronze data to Silver",
    start_date=days_ago(1),
    tags=["bronze_to_silver", "raw_to_bronze", "bigquery", "R_D"],
) as dag:
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_bronze_external_table",
        bucket=Variable.get("BUCKET_NAME"),
        source_objects=[f"sales/*.csv"],
        destination_project_dataset_table=f"{Variable.get("PROJECT_ID")}.bronze.sales",
        schema_fields=[
            {"name": "CustomerId", "type": "STRING", "mode": "REQUIRED"},
            {"name": "PurchaseDate", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Product", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Price", "type": "STRING", "mode": "REQUIRED"},
        ],
        source_format="CSV",
        skip_leading_rows=1,
        gcp_conn_id="my_gcs_connection_id"
    )

    query_transform = f"""
    CREATE OR REPLACE TABLE `{Variable.get("PROJECT_ID")}.silver.sales` AS
    SELECT
        CAST(CustomerId AS INT64) AS client_id,
        CASE
            WHEN SAFE.PARSE_DATE("%Y/%m/%d", PurchaseDate) IS NOT NULL THEN SAFE.PARSE_DATE("%Y/%m/%d", PurchaseDate) 
            WHEN SAFE.PARSE_DATE("%Y-%m-%d", PurchaseDate) IS NOT NULL THEN SAFE.PARSE_DATE("%Y-%m-%d", PurchaseDate) 
            WHEN SAFE.PARSE_DATE("%Y-%b-%d", PurchaseDate) IS NOT NULL THEN SAFE.PARSE_DATE("%Y-%b-%d", PurchaseDate) 
            ELSE NULL
        END AS purchase_date,
        Product AS product_name,
        CAST(REGEXP_REPLACE(price,"[^0-9 ]","") AS INT64) AS price
    FROM `{Variable.get("PROJECT_ID")}.bronze.sales`
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
