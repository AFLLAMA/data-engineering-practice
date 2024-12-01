from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateExternalTableOperator
)
from airflow.utils.dates import days_ago
from airflow.models import Variable

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "retries": 1,
# }

with DAG(
    "process_customers",
    # default_args=default_args,
    description="Create and transform partitioned Bronze data to Silver",
    schedule="0 1 * * *",
    start_date=datetime(2022, 9, 1),
    # end_date=datetime(2022, 9, 30),
    tags=["bronze_to_silver", "raw_to_bronze", "bigquery", "R_D"],
    catchup=True,
    max_active_runs=1,
) as dag:
    date = "{{ ds }}"
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_bronze_external_table",
        bucket=Variable.get("BUCKET_NAME"),
        source_objects=[f"customers/{date}/*.csv"], 
        destination_project_dataset_table="de-07-andrii-feshchenko.bronze.customers",
        schema_fields=[
            {"name": "Id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "FirstName", "type": "STRING", "mode": "REQUIRED"},
            {"name": "LastName", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Email", "type": "STRING", "mode": "REQUIRED"},
            {"name": "RegistrationDate", "type": "STRING", "mode": "REQUIRED"},
            {"name": "State", "type": "STRING", "mode": "REQUIRED"},
        ],
        source_format="CSV",
        skip_leading_rows=1,
        gcp_conn_id="my_gcs_connection_id"
    )

    query_create_table = """
    CREATE TABLE IF NOT EXISTS `de-07-andrii-feshchenko.silver.customers` (
        client_id INT64,
        first_name STRING,
        last_name STRING,
        email STRING,
        registration_date DATE,
        state STRING
    )"""

    query_transform = """
    MERGE INTO `de-07-andrii-feshchenko.silver.customers` AS target
    USING (
        SELECT
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
            State AS state
        FROM `de-07-andrii-feshchenko.bronze.customers`
        WHERE registration_date = CAST('{{ ds }}' AS DATE)
    ) AS source
    ON target.client_id = source.client_id
    WHEN MATCHED THEN
        UPDATE SET 
            target.first_name = source.first_name,
            target.last_name = source.last_name,
            target.email = source.email,
            target.registration_date = source.registration_date,
            target.state = source.state
    WHEN NOT MATCHED THEN
        INSERT (client_id, first_name, last_name, email, registration_date, state)
        VALUES (source.client_id, source.first_name, source.last_name, source.email, source.registration_date, source.state)
    """

    # Transform and merge data from bronze to silver
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