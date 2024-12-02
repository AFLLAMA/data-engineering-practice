from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator
)
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

CUSTOMER_FOLDER = "customers/"

SCHEMA = [
    {"name": "Id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
    {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
    {"name": "RegistrationDate", "type": "STRING", "mode": "NULLABLE"},
    {"name": "State", "type": "STRING", "mode": "NULLABLE"},
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


def get_latest_date(objects):
    """Helper function to extract the latest date folder from GCS objects."""
    dates = set(obj.split("/")[1] for obj in objects if obj.startswith(CUSTOMER_FOLDER))
    return max(dates)


with DAG(
    "process_customers",
    default_args=default_args,
    description="Load latest customer data into BigQuery",
    start_date=days_ago(1),
    tags=["bronze_to_silver", "raw_to_bronze", "bigquery", "R_D"],
) as dag:

    list_objects = GCSListObjectsOperator(
        task_id="list_objects",
        bucket=Variable.get("BUCKET_NAME"),
        prefix=CUSTOMER_FOLDER,
        gcp_conn_id="my_gcs_connection_id"
    )

    determine_latest_date = PythonOperator(
        task_id="determine_latest_date",
        python_callable=lambda **kwargs: get_latest_date(kwargs["ti"].xcom_pull(task_ids="list_objects")),
        provide_context=True,
    )

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_bronze_external_table",
        bucket=Variable.get("BUCKET_NAME"),
        source_objects=[f"{CUSTOMER_FOLDER}{{ ti.xcom_pull(task_ids='determine_latest_date') }}/*.csv"],
        destination_project_dataset_table="de-07-andrii-feshchenko.bronze.customers",
        schema_fields=SCHEMA,
        source_format="CSV",
        skip_leading_rows=1,
        gcp_conn_id="my_gcs_connection_id"
    )
    query_transform = f"""
    CREATE OR REPLACE TABLE `{Variable.get("PROJECT_ID")}.silver.customers` AS
    SELECT DISTINCT
        CAST(Id AS INT64) AS client_id,
        FirstName AS first_name,
        LastName AS last_name,
        Email AS email,
        CASE
            WHEN SAFE.PARSE_DATE("%Y/%m/%d", RegistrationDate) IS NOT NULL THEN SAFE.PARSE_DATE("%Y/%m/%d", RegistrationDate) 
            WHEN SAFE.PARSE_DATE("%Y-%m-%d", RegistrationDate) IS NOT NULL THEN SAFE.PARSE_DATE("%Y-%m-%d", RegistrationDate) 
            WHEN SAFE.PARSE_DATE("%Y-%b-%d", RegistrationDate) IS NOT NULL THEN SAFE.PARSE_DATE("%Y-%b-%d", RegistrationDate) 
            ELSE NULL
        END AS registration_date,
        State AS state,
        
    FROM `{Variable.get("PROJECT_ID")}.bronze.customers`
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

    list_objects >> determine_latest_date >> create_external_table >> transform_to_silver
