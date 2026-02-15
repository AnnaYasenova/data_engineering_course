from __future__ import annotations

from datetime import datetime
import time
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

import boto3


AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

REDSHIFT_WORKGROUP = os.getenv("REDSHIFT_WORKGROUP", "anna-dp-workgroup")
REDSHIFT_DB = os.getenv("REDSHIFT_DB", "dev")
REDSHIFT_SECRET_ARN = os.getenv("REDSHIFT_SECRET_ARN", "arn:aws:iam::827992710155:role/anna-dp-redshift-service-role")  # краще задати
REDSHIFT_USER = os.getenv("REDSHIFT_USER", "admin")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD", "RedshiftPass123!!")


MERGE_SQL = r"""
MERGE INTO gold.user_profiles_enriched
USING (
    SELECT
        c.id as client_id,
        COALESCE(c.firstname, SPLIT_PART(u.full_name, ' ', 1)) AS first_name,
        COALESCE(c.lastname, SPLIT_PART(u.full_name, ' ', 2)) AS last_name,
        COALESCE(c.state, u.state) AS state,
        c.email,
        CAST(c.registrationdate AS DATE) as registration_date,
        CAST(u.birth_date AS DATE) as birth_date,
        u.phone_number
    FROM spectrum_schema.customers c
    LEFT JOIN spectrum_schema.user_profiles u
        ON c.email = u.email
) source
ON gold.user_profiles_enriched.client_id = source.client_id

WHEN MATCHED THEN
UPDATE SET
    first_name = source.first_name,
    last_name = source.last_name,
    state = source.state,
    birth_date = source.birth_date,
    phone_number = source.phone_number

WHEN NOT MATCHED THEN
INSERT (
    client_id, first_name, last_name, email,
    registration_date, state, birth_date, phone_number
)
VALUES (
    source.client_id, source.first_name, source.last_name,
    source.email, source.registration_date,
    source.state, source.birth_date, source.phone_number
);
"""


def run_redshift_sql(**context):
    rsd = boto3.client("redshift-data", region_name=AWS_REGION)

    kwargs = dict(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DB,
        Sql=MERGE_SQL,
    )

    if REDSHIFT_SECRET_ARN:
        kwargs["SecretArn"] = REDSHIFT_SECRET_ARN
    else:
        # fallback (не рекомендую, але працює)
        if not REDSHIFT_PASSWORD:
            raise RuntimeError("Set REDSHIFT_SECRET_ARN or REDSHIFT_PASSWORD env var")
        kwargs["DbUser"] = REDSHIFT_USER
        kwargs["Password"] = REDSHIFT_PASSWORD

    resp = rsd.execute_statement(**kwargs)
    stmt_id = resp["Id"]

    while True:
        desc = rsd.describe_statement(Id=stmt_id)
        status = desc["Status"]
        if status in ("FINISHED", "FAILED", "ABORTED"):
            if status != "FINISHED":
                raise RuntimeError(f"Redshift statement failed: {status} - {desc.get('Error')}")
            return
        time.sleep(10)


with DAG(
    dag_id="enrich_user_profiles",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["project", "gold"],
) as dag:
    PythonOperator(
        task_id="run_redshift_merge_enrichment",
        python_callable=run_redshift_sql,
    )
