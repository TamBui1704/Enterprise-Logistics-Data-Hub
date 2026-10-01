"""
PySpark Medallion Architecture Job: Bronze -> Silver Value (Current State 1:1) & Silver History (SCD Type 2)
Xử lý chính xác mã op Debezium CDC: 'r' (Read Snapshot), 'c' (Create), 'u' (Update), 'd' (Delete).
Tự động MERGE INTO Delta Lake, quản lý Cutoff Time và Data Lineage Metadata.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import time

def build_spark_session():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    return SparkSession.builder \
        .appName("EMS-Logistics-Medallion-Bronze-to-Silver") \
        .config("spark.jars.packages", 
                "io.delta:delta-spark_2.12:3.1.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadminpassword") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def get_latest_cdc_dedup(bronze_df):
    """
    Lấy bản ghi biến động mới nhất của từng BOOKING_ID dựa trên kafka_offset hoặc ts_ms.
    Xử lý mã op Debezium: 'r' (read/snapshot), 'c' (create), 'u' (update) -> Upsert, 'd' -> Delete.
    """
    window_spec = Window.partitionBy("BOOKING_ID").orderBy(col("kafka_offset").desc())
    dedup_df = bronze_df.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    return dedup_df

def process_silver_value(spark, dedup_df):
    """
    Tầng Silver Value (Current Snapshot 1:1 với Source)
    Hỗ trợ op IN ('r', 'c', 'u') -> WHEN MATCHED THEN UPDATE / INSERT
    Hỗ trợ op = 'd' -> WHEN MATCHED THEN DELETE
    """
    silver_value_path = "s3a://logistics-lakehouse/silver/value_shipment_bookings"
    print(f"🔄 [SILVER VALUE] MERGE INTO dữ liệu hiện tại (Current Snapshot 1:1) tại {silver_value_path}...")

    upsert_df = dedup_df.filter(col("op").isin(["r", "c", "u"])).withColumn("ingested_at", current_timestamp())
    delete_df = dedup_df.filter(col("op") == "d")

    if not DeltaTable.isDeltaTable(spark, silver_value_path):
        print("🌱 Khởi tạo bảng Silver Value Delta Lake lần đầu...")
        upsert_df.write.format("delta").mode("overwrite").save(silver_value_path)
    else:
        silver_table = DeltaTable.forPath(spark, silver_value_path)
        
        # 1. Upsert (Read 'r', Create 'c', Update 'u')
        if upsert_df.count() > 0:
            silver_table.alias("target").merge(
                upsert_df.alias("source"),
                "target.BOOKING_ID = source.BOOKING_ID"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()

        # 2. Delete ('d')
        if delete_df.count() > 0:
            silver_table.alias("target").merge(
                delete_df.alias("source"),
                "target.BOOKING_ID = source.BOOKING_ID"
            ).whenMatchedDelete().execute()

    print("✅ Hoàn tất MERGE INTO Silver Value!")

def process_silver_history_scd2(spark, dedup_df):
    """
    Tầng Silver History (SCD Type 2: Lưu vết lịch sử biến đổi dữ liệu)
    Hỗ trợ op IN ('r', 'c', 'u') -> Đóng phiên bản cũ, mở phiên bản mới với valid_from, valid_to, is_current
    """
    silver_history_path = "s3a://logistics-lakehouse/silver/history_shipment_bookings"
    print(f"📜 [SILVER HISTORY - SCD TYPE 2] Cập nhật lịch sử thay đổi tại {silver_history_path}...")

    current_ts = current_timestamp()
    
    upsert_df = dedup_df.filter(col("op").isin(["r", "c", "u"])) \
        .withColumn("valid_from", current_ts) \
        .withColumn("valid_to", lit("9999-12-31 23:59:59").cast("timestamp")) \
        .withColumn("is_current", lit(1)) \
        .withColumn("ingested_at", current_ts)

    if not DeltaTable.isDeltaTable(spark, silver_history_path):
        print("🌱 Khởi tạo bảng Silver History SCD Type 2 Delta Lake lần đầu...")
        upsert_df.write.format("delta").mode("overwrite").save(silver_history_path)
    else:
        history_table = DeltaTable.forPath(spark, silver_history_path)
        
        # 1. Đóng phiên bản cũ (Close old version: is_current = 0, valid_to = current_ts)
        history_table.alias("target").merge(
            upsert_df.alias("source"),
            "target.BOOKING_ID = source.BOOKING_ID AND target.is_current = 1"
        ).whenMatchedUpdate(set={
            "is_current": "0",
            "valid_to": "source.valid_from"
        }).execute()

        # 2. Chèn phiên bản mới (Insert new version: is_current = 1, valid_to = 9999-12-31)
        upsert_df.write.format("delta").mode("append").save(silver_history_path)

    print("✅ Hoàn tất Cập nhật Silver History SCD Type 2!")

def log_control_metadata(spark, batch_id, status, records_count, start_cutoff, end_cutoff):
    control_path = "s3a://logistics-lakehouse/control/etl_batch_control"
    print(f"📊 [CONTROL METADATA] Ghi nhận Batch Run ID '{batch_id}' [Status: {status}]...")

    control_data = [(
        batch_id,
        "bronze_to_silver_shipment_bookings",
        "bronze.shipment_bookings",
        "silver.value_shipment_bookings & history_shipment_bookings",
        start_cutoff,
        end_cutoff,
        records_count,
        status,
        str(datetime.now())
    )]
    
    control_df = spark.createDataFrame(control_data, [
        "batch_id", "pipeline_name", "source_table", "target_table",
        "start_cutoff_time", "end_cutoff_time", "records_processed", "status", "created_at"
    ])

    control_df.write.format("delta").mode("append").save(control_path)

def main():
    print("⚡ Khởi tạo Medallion Engine (Bronze -> Silver Value & History SCD Type 2)...")
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_path = "s3a://logistics-lakehouse/bronze/shipment_bookings"
    
    if not DeltaTable.isDeltaTable(spark, bronze_path):
        print("⚠️ Chưa tìm thấy dữ liệu Bronze Layer. Vui lòng chạy nạp Bronze trước.")
        return

    bronze_df = spark.read.format("delta").load(bronze_path)
    records_count = bronze_df.count()

    # Deduplicate Lấy bản ghi CDC mới nhất của từng BOOKING_ID trong đợt Batch này
    dedup_df = get_latest_cdc_dedup(bronze_df)

    start_cutoff = str(datetime.now() - timedelta(days=1))
    end_cutoff = str(datetime.now())
    batch_id = f"BATCH-{int(time.time())}"

    # 1. Transform & Update Silver Value (Current State 1:1)
    process_silver_value(spark, dedup_df)

    # 2. Transform & Update Silver History (SCD Type 2)
    process_silver_history_scd2(spark, dedup_df)

    # 3. Log Lineage & Cutoff Control Table
    log_control_metadata(spark, batch_id, "SUCCESS", records_count, start_cutoff, end_cutoff)

    print("\n🎉 HOÀN THÀNH TIẾN TRÌNH BIẾN ĐỔI BRONZE -> SILVER VALUE & HISTORY SCD TYPE 2!")

if __name__ == "__main__":
    from datetime import datetime, timedelta
    main()
