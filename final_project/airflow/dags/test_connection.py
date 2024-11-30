from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable


with DAG(
    'test_gcs_connection',
    description='Test Google Cloud Storage connection',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['test', 'R_D'],
) as dag:

    list_files = GCSListObjectsOperator(
        task_id='list_files_in_gcs',
        bucket= "deep_atlas_400",#Variable.get("BUCKET_NAME"),
        gcp_conn_id='my_gcs_connection_id',
    )
    list_files
    