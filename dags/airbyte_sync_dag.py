from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from datetime import datetime
from airflow.providers.http.operators.http import HttpOperator
import json
import os
from dotenv import load_dotenv

load_dotenv()

with DAG(
    dag_id='airbyte_sync',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:
    sync_pagila = HttpOperator(
        task_id='airbyte_sync_pagila',
        http_conn_id='airbyte_http',
        endpoint='/api/v1/connections/sync',
        method='POST',
        data=json.dumps({"connectionId": os.getenv("AIRFLOW_AIRBYTE_PAGILA_CONN_ID")}),
        headers={"Content-Type": "application/json"},
    )

    sync_sakila = HttpOperator(
        task_id='airbyte_sync_sakila',
        http_conn_id='airbyte_http',
        endpoint='/api/v1/connections/sync',
        method='POST',
        data=json.dumps({"connectionId": os.getenv("AIRFLOW_AIRBYTE_SAKILA_CONN_ID")}),
        headers={"Content-Type": "application/json"},
    )

    sync_pagila >> sync_sakila