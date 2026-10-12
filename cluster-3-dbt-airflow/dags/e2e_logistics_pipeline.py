"""
Airflow DAG: End-to-End Multi-Project Logistics Data Pipeline
Orchestrates:
1. PySpark Initial Bulk Load & Streaming Medallion Batch (Cluster 2) via SSHOperator
2. dbt_lakehouse_spark Project: Clean CDC data from S3 Bronze -> Silver & Gold Delta Lake (Spark Cluster)
3. dbt_warehouse_clickhouse Project: Load S3 Gold Delta OBT data into ClickHouse OLAP Warehouse & Run Data Quality Tests
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

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
    description="Multi-Project E2E Pipeline: Spark Lakehouse S3 -> ClickHouse OLAP Warehouse",
    schedule_interval="@daily",
    catchup=False,
    tags=["logistics", "spark", "lakehouse", "clickhouse", "dbt_mesh", "ssh_operator"],
) as dag:

    # 1. Trigger Spark Bulk Initial Load via SSH on spark-runner
    spark_bulk_load = SSHOperator(
        task_id="spark_oracle_bulk_initial_load",
        ssh_conn_id="spark_ssh_conn",
        command="/opt/bitnami/spark/bin/spark-submit --master spark://spark-runner:7077 /opt/bitnami/spark/spark_jobs/oracle_bulk_initial_load.py",
        cmd_timeout=3600,
    )

    # 2. Trigger Spark Medallion Transformation (Bronze -> Silver) via SSH on spark-runner
    spark_medallion_transform = SSHOperator(
        task_id="spark_bronze_to_silver_medallion",
        ssh_conn_id="spark_ssh_conn",
        command="/opt/bitnami/spark/bin/spark-submit --master spark://spark-runner:7077 /opt/bitnami/spark/spark_jobs/bronze_to_silver_medallion.py --table ALL",
        cmd_timeout=3600,
    )

    # 3. Project 1: dbt_lakehouse_spark (DE Team: Clean S3 Bronze -> Silver & Gold Delta Lake)
    dbt_spark_lakehouse_run = BashOperator(
        task_id="dbt_spark_lakehouse_run",
        bash_command="dbt run --project-dir /opt/airflow/dbt_lakehouse_spark --profiles-dir /opt/airflow/dbt_lakehouse_spark",
    )

    # 4. Project 2: dbt_warehouse_clickhouse (DA/AE Team: Load S3 Gold OBT -> ClickHouse OLAP Warehouse)
    dbt_clickhouse_warehouse_run = BashOperator(
        task_id="dbt_clickhouse_warehouse_run",
        bash_command="dbt run --project-dir /opt/airflow/dbt_warehouse_clickhouse --profiles-dir /opt/airflow/dbt_warehouse_clickhouse",
    )

    # 5. Data Quality Tests on ClickHouse OLAP Warehouse
    dbt_clickhouse_test = BashOperator(
        task_id="dbt_clickhouse_test",
        bash_command="dbt test --project-dir /opt/airflow/dbt_warehouse_clickhouse --profiles-dir /opt/airflow/dbt_warehouse_clickhouse",
    )

    # Task Dependencies Flow
    spark_bulk_load >> spark_medallion_transform >> dbt_spark_lakehouse_run >> dbt_clickhouse_warehouse_run >> dbt_clickhouse_test
