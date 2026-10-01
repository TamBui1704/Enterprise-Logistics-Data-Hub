"""
Airflow DAG: End-to-End Logistics Data Pipeline
Orchestrates:
1. PySpark Initial Bulk Load / Medallion Batch (Cluster 2)
2. dbt-clickhouse transformation models (Cluster 3 -> ClickHouse DWH)
3. dbt Data Quality tests
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "e2e_logistics_data_pipeline",
    default_args=default_args,
    description="Full E2E Data Pipeline: Spark Medallion -> ClickHouse dbt Transformation",
    schedule_interval="@daily",
    catchup=False,
    tags=["logistics", "spark", "dbt", "clickhouse"],
) as dag:

    # 1. Trigger Spark Bulk Initial Load via Docker exec on spark_runner
    spark_bulk_load = BashOperator(
        task_id="spark_oracle_bulk_initial_load",
        bash_command="docker exec spark_runner spark-submit --master spark://spark_runner:7077 /opt/bitnami/spark/spark_jobs/oracle_bulk_initial_load.py",
    )

    # 2. Trigger Spark Medallion Transformation (Bronze -> Silver)
    spark_medallion_transform = BashOperator(
        task_id="spark_bronze_to_silver_medallion",
        bash_command="docker exec spark_runner spark-submit --master spark://spark_runner:7077 /opt/bitnami/spark/spark_jobs/bronze_to_silver_medallion.py",
    )

    # 3. Trigger dbt run to build ClickHouse Staging & Fact Tables
    dbt_run = BashOperator(
        task_id="dbt_run_clickhouse_models",
        bash_command="dbt run --project-dir /opt/airflow/dbt_logistics --profiles-dir /opt/airflow/dbt_logistics",
    )

    # 4. Trigger dbt test to validate Data Quality
    dbt_test = BashOperator(
        task_id="dbt_test_clickhouse_models",
        bash_command="dbt test --project-dir /opt/airflow/dbt_logistics --profiles-dir /opt/airflow/dbt_logistics",
    )

    # Define Task Dependencies
    spark_bulk_load >> spark_medallion_transform >> dbt_run >> dbt_test
