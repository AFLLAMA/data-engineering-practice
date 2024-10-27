import json
import os
from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Variable

with DAG(
    dag_id=Path(__file__).stem,
    schedule="0 1 * * *",
    start_date=datetime(2022, 8, 9),
    catchup=True,
    max_active_runs=1,
    tags=["R_D"],
) as dag:
    date = "{{ ds }}"
    job1 = HttpOperator(
        task_id="extract_data_from_api",
        method="POST",
        # requires env variable AIRFLOW_CONN_LOCAL_JOB1='{"conn_type": "HTTP", "host": "host.docker.internal", "port": 8081}'
        http_conn_id="local_job1",
        # requires env variable AIRFLOW_VAR_BASE_DIR="/full/local/path"
        data=json.dumps(
            {
                "date": date,
                "raw_dir": os.path.join(Variable.get("BASE_DIR"), "lesson_02", "data", "raw", "sales", date),
            }
        ),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 201,
    )

    job2 = HttpOperator(
        task_id="convert_to_avro",
        method="POST",
        # requires env variable AIRFLOW_CONN_LOCAL_JOB2='{"conn_type": "HTTP", "host": "host.docker.internal", "port": 8082}'
        http_conn_id="local_job2",
        # requires env variable AIRFLOW_VAR_BASE_DIR="/full/local/path"
        data=json.dumps(
            {
                "raw_dir": os.path.join(Variable.get("BASE_DIR"), "lesson_02", "data", "raw", "sales", date),
                "stg_dir": os.path.join(Variable.get("BASE_DIR"), "lesson_02", "data", "stg", "sales", date),
            }
        ),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 201,
    )

    job1 >> job2
