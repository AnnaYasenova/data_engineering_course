from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from pendulum import timezone

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}


with (DAG(
    dag_id="process_iris",
    default_args=default_args,
    description="ETL and ML pipeline for Iris dataset processing",
    schedule_interval='0 1 * * *',
    start_date=datetime(2025, 4, 22),
    end_date=datetime(2025, 4, 25),
    catchup=True,
    tags=["ml", "dbt", "iris"],
) as dag):

    dbt_run = BashOperator(
        task_id="dbt_run_iris_processed",
        bash_command=(
            "cd /opt/airflow/dags/dbt/homework && "
            "dbt run --select "
            + "+mart.iris_processed"
            + " --vars '{process_date: "
            + "{{ ds }}"
            + "}' "
            "--project-dir /opt/airflow/dags/dbt/homework "
            "--profiles-dir /opt/airflow/dags/dbt; "
            "EXIT_CODE=$?; "
            "if [ $EXIT_CODE -eq 0 ]; then echo 'DBT SUCCESS (simulating HTTP 201)'; exit 0; "
            "else echo 'DBT FAILED'; exit $EXIT_CODE; fi"
        ),
    )

    train_model = BashOperator(
        task_id="train_model",
        bash_command="python /opt/airflow/dags/python_scripts/train_model.py {{ ds }}",
    )

    send_email = EmailOperator(
        task_id="notify_email",
        to="yasenova2013@gmail.com",
        subject="Airflow: Iris pipeline finished successfully",
        html_content="DAG process_iris finished successfully for date {{ ds }}.",
    )

    dbt_run >> train_model >> send_email