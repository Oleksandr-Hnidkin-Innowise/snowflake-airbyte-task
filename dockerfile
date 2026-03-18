FROM apache/airflow:3.1.6

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    apache-airflow-providers-snowflake \
    apache-airflow-providers-airbyte \
    dbt-snowflake

