# 9. Рекомендовано протестити весь workflow. Для цього потрібно запустити першу джобу в одному терміналі, другу джобу в другому терміналі. Airflow треба запустити в окремому терміналі. 
#   Стежте, щоб у кожному середовищі було активоване відповідне віртуальне середовище Python. Якщо ви все зробили правильно, Airflow запустить dag-рани за 3 дні послідовно, бо всі 3 дати вже в минулому. 
#   У вашій папці storage мають зʼявитися папки з датами та даними.

import json
from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator

with DAG(
        dag_id=Path(__file__).stem,
        schedule="0 1 * * *",
        start_date=datetime(2022, 8, 9),
        catchup=True,
        max_active_runs=1,
        tags=["R_D"]
) as dag:
    date = "{{ ds }}"
    job1 = HttpOperator(
        task_id="extract_data_from_api",
        method="POST",
        http_conn_id="local_job1",
        data=json.dumps({
            "date": date,
            "raw_dir": f"/home/llama/Workspace/data-engineering-practice/lesson_02/data/raw/sales/{date}"
            }),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.json()["json"]["status_code"] == 201,
    )

    job2 = HttpOperator(
        task_id="convert_to_avro",
        method="POST",
        http_conn_id="local_job2",
        data=json.dumps({
            "raw_dir": f"/home/llama/Workspace/data-engineering-practice/lesson_02/data/raw/sales/{date}",
            "stg_dir": f"/home/llama/Workspace/data-engineering-practice/lesson_02/data/stg/sales/{date}"
            }),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.json()["json"]["status_code"] == 201,
    )

    job1 >> job2
