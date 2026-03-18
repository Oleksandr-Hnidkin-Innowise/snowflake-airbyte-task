from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='dbt_run',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:
    run_dbt = BashOperator(
        task_id='dbt_run_task',
        bash_command='cd /opt/airflow/dbt/pagila_analytics && dbt run --profiles-dir .'
    )