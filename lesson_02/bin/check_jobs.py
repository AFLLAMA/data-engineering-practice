import os
import time
import requests


BASE_DIR = os.environ.get("BASE_DIR")

if not BASE_DIR:
    print("BASE_DIR environment variable must be set")
    exit(1)


def run_job1(job1_port: int, raw_dir: str):
    print("Starting job1:")
    resp = requests.post(
        url=f"http://localhost:{job1_port}/",
        json={"date": "2022-08-09", "raw_dir": raw_dir},
    )
    assert resp.status_code == 201
    print("job1 completed!")


def run_job2(job2_port: int, raw_dir: str, stg_dir: str):
    print("Starting job2:")
    resp = requests.post(
        url=f"http://localhost:{job2_port}/",
        json={"raw_dir": raw_dir, "stg_dir": stg_dir},
    )
    assert resp.status_code == 201
    print("job2 completed!")


if __name__ == "__main__":
    raw_dir = os.path.join(BASE_DIR, "lesson_02", "data", "raw", "sales", "2022-08-09")
    stg_dir = os.path.join(BASE_DIR, "lesson_02", "data", "stg", "sales", "2022-08-09")
    run_job1(job1_port=8081, raw_dir=raw_dir)
    time.sleep(3)
    run_job2(job2_port=8082, raw_dir=raw_dir, stg_dir=stg_dir)
