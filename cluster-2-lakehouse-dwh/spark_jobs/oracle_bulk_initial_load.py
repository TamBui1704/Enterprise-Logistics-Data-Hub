"""
PySpark Batch Initial Bulk Load Job
Đọc trực tiếp dữ liệu lớn (Initial Bulk Load) từ Oracle Database qua JDBC -> Ghi thẳng vào tầng SILVER
(Tạo đồng thời 2 folder: silver/value_<table_name> và silver/history_<table_name>)
Sử dụng khi khởi tạo dữ liệu ban đầu trước khi bật Debezium CDC Incremental Stream.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import time

def build_spark_session():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio-lakehouse:9000")
    return SparkSession.builder \
        .appName("EMS-Logistics-Oracle-Initial-Bulk-Load-to-Silver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadminpassword") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def main():
    print("⚡ [INITIAL BULK LOAD TO SILVER] Khởi tạo PySpark Engine nạp dữ liệu ban đầu từ Oracle DB...")
    start_time = time.time()
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Cấu hình JDBC Oracle DB
    jdbc_url = os.getenv("ORACLE_JDBC_URL", "jdbc:oracle:thin:@source_oracle_db:1521/FREEPDB1")
    connection_properties = {
        "user": "debezium",
        "password": "dbz",
        "driver": "oracle.jdbc.OracleDriver",
        "fetchsize": "20000"
    }

    tables_to_load = [
        "shipment_bookings",
        "delivery_remunerations",
        "return_remunerations",
        "shipment_event_trackings"
    ]

    current_ts = current_timestamp()

    for table in tables_to_load:
        print(f"📥 Đang nạp Initial Bulk Load cho bảng 'DEBEZIUM.{table.upper()}' từ Oracle DB...")
        t_start = time.time()
        
        oracle_df = spark.read.jdbc(
            url=jdbc_url,
            table=f"DEBEZIUM.{table.upper()}",
            properties=connection_properties
        )

        record_count = oracle_df.count()

        # 1. Ghi vào Silver Value Folder (Current State 1:1 với Ingested_at)
        silver_value_path = f"s3a://logistics-lakehouse/silver/value_{table}"
        print(f"🚀 [SILVER VALUE] Ghi {record_count:,} bản ghi vào ({silver_value_path})...")
        value_df = oracle_df.withColumn("ingested_at", current_ts)
        value_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(silver_value_path)

        # 2. Ghi vào Silver History Folder (SCD Type 2 Initial State)
        silver_history_path = f"s3a://logistics-lakehouse/silver/history_{table}"
        print(f"📜 [SILVER HISTORY SCD2] Ghi {record_count:,} bản ghi vào ({silver_history_path})...")
        history_df = oracle_df \
            .withColumn("valid_from", current_ts) \
            .withColumn("valid_to", lit("9999-12-31 23:59:59").cast("timestamp")) \
            .withColumn("is_current", lit(1)) \
            .withColumn("ingested_at", current_ts)
        
        history_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(silver_history_path)

        t_elapsed = time.time() - t_start
        print(f"✅ Hoàn tất nạp Initial Load Silver cho '{table}': {record_count:,} bản ghi trong {t_elapsed:.2f} giây!\n")

    total_time = time.time() - start_time
    print(f"🎉 HOÀN THÀNH INITIAL BULK LOAD VÀO SILVER LAYER IN {total_time:.2f} GIÂY!")

if __name__ == "__main__":
    main()
