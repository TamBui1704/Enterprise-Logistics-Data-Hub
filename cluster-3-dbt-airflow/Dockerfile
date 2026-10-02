FROM apache/airflow:2.8.1-python3.10

USER root
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

USER airflow

# Cài đặt dbt-core và các dbt adapters: dbt-spark, dbt-databricks, dbt-clickhouse
RUN pip install --no-cache-dir \
    dbt-core==1.7.9 \
    dbt-spark==1.7.1 \
    dbt-databricks==1.7.3 \
    dbt-clickhouse==1.7.3 \
    apache-airflow-providers-http
