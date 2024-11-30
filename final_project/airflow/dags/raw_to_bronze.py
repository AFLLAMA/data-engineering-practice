import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from datetime import datetime
from airflow.models import Variable
from airflow.decorators import task_group
from airflow.utils.dates import days_ago


with DAG(
    "process_sales",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["R_D", "raw_to_bronze", "bigquery"],
) as dag:
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id='create_bronze_external_table',
        # source_uris=[f"gs://raw_deep_atlas_400/sales/*/*.csv"], 
        bucket=Variable.get("BUCKET_NAME"),
        source_objects=['sales/*.csv'],
        destination_project_dataset_table="de-07-andrii-feshchenko.bronze.sales",
        schema_fields=[
            {'name': 'CustomerId', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'Product', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'Price', 'type': 'STRING', 'mode': 'REQUIRED'},
        ],
        source_format='CSV',
        skip_leading_rows=1,
        # write_disposition='WRITE_TRUNCATE',
        gcp_conn_id="my_gcs_connection_id"
    )
