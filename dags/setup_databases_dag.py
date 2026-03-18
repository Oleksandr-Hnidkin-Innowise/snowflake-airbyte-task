from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

with DAG(
    dag_id='setup_databases',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=['/opt/airflow/scripts/'],
) as dag:
    drop_pagila = BashOperator(
        task_id='drop_pagila',
        bash_command=(
            'PGPASSWORD=airflow psql -h postgres -U airflow -d postgres -c "DROP DATABASE IF EXISTS pagila WITH (FORCE);" && '
            'PGPASSWORD=airflow psql -h postgres -U airflow -d postgres -c "CREATE DATABASE pagila;"'
        )
    )

    drop_sakila = BashOperator(
        task_id='drop_sakila',
        bash_command=(
            'PGPASSWORD=airflow psql -h postgres -U airflow -d postgres -c "DROP DATABASE IF EXISTS sakila WITH (FORCE);" && '
            'PGPASSWORD=airflow psql -h postgres -U airflow -d postgres -c "CREATE DATABASE sakila;"'
        )
    )

    #PAGILA
    setup_pagila_schema = SQLExecuteQueryOperator(
        task_id='setup_pagila_schema',
        conn_id=os.getenv("PAGILA_CONN"),
        sql='pagila-schema.sql',
    )

    setup_pagila_data = BashOperator(
        task_id='setup_pagila_data',
        bash_command='''
        PGPASSWORD=airflow psql -h postgres -U airflow -d pagila -f /opt/airflow/scripts/pagila-data.sql
        '''
    )

    #SAKILA
    setup_sakila_schema = SQLExecuteQueryOperator(
        task_id='setup_sakila_schema',
        conn_id=os.getenv("SAKILA_CONN"),
        sql='sakila-schema.sql',
    )

    setup_sakila_data = SQLExecuteQueryOperator(
        task_id='setup_sakila_data',
        conn_id=os.getenv("SAKILA_CONN"),
        sql='sakila-data.sql',
    )

    drop_pagila >> setup_pagila_schema >> setup_pagila_data
    drop_sakila >> setup_sakila_schema >> setup_sakila_data