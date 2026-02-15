from __future__ import annotations

from datetime import datetime
import time
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

import boto3


AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
GLUE_JOB_NAME = os.getenv("GLUE_JOB_SALES", "process_sales")


def run_glue_job(**context):
    glue = boto3.client("glue", region_name=AWS_REGION)

    resp = glue.start_job_run(JobName=GLUE_JOB_NAME)
    run_id = resp["JobRunId"]

    # poll
    while True:
        jr = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id, PredecessorsIncluded=False)
        state = jr["JobRun"]["JobRunState"]
        if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            if state != "SUCCEEDED":
                raise RuntimeError(f"Glue job {GLUE_JOB_NAME} failed: state={state}")
            return
        time.sleep(20)


with DAG(
    dag_id="process_sales",
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=["project"],
) as dag:
    PythonOperator(
        task_id="run_glue_process_sales",
        python_callable=run_glue_job,
    )
