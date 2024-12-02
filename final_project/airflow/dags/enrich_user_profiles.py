from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
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
    "enrich_user_profile",
    default_args=default_args,
    description="Create gold.user_profiles_enriched using all data from silver.customers enriched from silver.user_profiles",
    start_date=days_ago(1),
    tags=["silver_gold", "bigquery", "R_D"],
) as dag:
    
    query = f"""
    CREATE OR REPLACE TABLE `{Variable.get("PROJECT_ID")}.gold.user_profiles_enriched` AS
    SELECT
        c.client_id,
        c.email,
        c.registration_date,
        p.state,
        p.birth_date,
        p.phone_number,
        CASE 
            WHEN p.full_name IS NULL THEN SPLIT(p.full_name, ' ')[OFFSET(0)]
            ELSE c.first_name
        END AS first_name,
        CASE 
            WHEN p.full_name IS NULL THEN SPLIT(p.full_name, ' ')[OFFSET(1)] 
            ELSE c.last_name
        END AS last_name
    FROM
        `{Variable.get("PROJECT_ID")}.silver.customers` c
    LEFT JOIN
        `{Variable.get("PROJECT_ID")}.silver.user_profiles` p
    ON
        c.email = p.email;
    """

    enrich_user_profile = BigQueryInsertJobOperator(
        task_id="enrich_user_profiles",
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="my_gcs_connection_id",
    )
