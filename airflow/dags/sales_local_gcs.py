import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from datetime import datetime
from airflow.models import Variable
from airflow.models.param import Param


with DAG(
    "save_json_to_gcs",
    start_date=datetime(2024, 10, 31),
    schedule_interval=None,
    catchup=False,
    params={
        "date": Param(default="2022-08-09", description="Date to upload"),
    },
    tags=["R_D"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    date = dag.params["date"]
    folder_path = os.path.join("/mnt", "data", "raw", "sales", date)

    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)

            upload_task = LocalFilesystemToGCSOperator(
                task_id=f"upload_{filename}_to_gcs",
                src=file_path,
                dst=f"{'/'.join(date.split('-'))}/{filename}",
                bucket=Variable.get("BUCKET_NAME"),
                gcp_conn_id="my_gcs_connection_id",
            )

            # Define tasks for each local file in selected date
            start >> upload_task >> end
    else:
        raise FileNotFoundError(f"The folder {folder_path} does not exist.")
