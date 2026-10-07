import os
import time
import json
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, row_number, from_json, get_json_object
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from delta.tables import DeltaTable

def build_spark_session(table_name):
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio-lakehouse:9000")
    return SparkSession.builder \
        .appName(f"EMS-Logistics-Medallion-Bronze-to-Silver-{table_name}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadminpassword") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def get_table_schema(table_name):
    # TODO: Khai báo Data Types cho tất cả 11 bảng ở đây
    if table_name == "SHIPMENT_BOOKINGS":
        return StructType([
            StructField("BOOKING_ID", StringType(), True),
            StructField("ITEM_CODE", StringType(), True),
            StructField("CUSTOMER_ID", StringType(), True),
            StructField("SERVICE_ID", StringType(), True),
            StructField("SENDING_POS_CODE", StringType(), True),
            StructField("RECEIVING_POS_CODE", StringType(), True),
            StructField("WEIGHT_GRAM", LongType(), True),
            StructField("WEIGHT_TIER_ID", StringType(), True),
            StructField("ROUTING_TYPE_ID", StringType(), True),
            StructField("TOTAL_REVENUE", DoubleType(), True),
            StructField("COST_AMOUNT", DoubleType(), True),
            StructField("STATUS_ID", StringType(), True)
        ])
    return StructType([]) # Fallback tạm thời để script không bị crash nếu bảng chưa khai báo schema

def get_latest_cdc_dedup(bronze_df, primary_keys):
    window_spec = Window.partitionBy(*primary_keys).orderBy(col("kafka_offset").desc())
    return bronze_df.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

def process_silver_value(spark, dedup_df, silver_value_path, primary_keys):
    print(f"🔄 [SILVER VALUE] MERGE INTO dữ liệu hiện tại tại {silver_value_path}...")
    upsert_df = dedup_df.filter(col("op").isin(["r", "c", "u"])).withColumn("ingested_at", current_timestamp())
    delete_df = dedup_df.filter(col("op") == "d")

    merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])

    if not DeltaTable.isDeltaTable(spark, silver_value_path):
        upsert_df.write.format("delta").mode("overwrite").save(silver_value_path)
    else:
        silver_table = DeltaTable.forPath(spark, silver_value_path)
        if upsert_df.count() > 0:
            silver_table.alias("target").merge(upsert_df.alias("source"), merge_condition) \
             .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        if delete_df.count() > 0:
            silver_table.alias("target").merge(delete_df.alias("source"), merge_condition) \
             .whenMatchedDelete().execute()
    print("✅ Hoàn tất MERGE INTO Silver Value!")

def process_silver_history_scd2(spark, dedup_df, silver_history_path, primary_keys):
    print(f"📜 [SILVER HISTORY - SCD TYPE 2] Cập nhật lịch sử thay đổi tại {silver_history_path}...")
    current_ts = current_timestamp()
    
    upsert_df = dedup_df.filter(col("op").isin(["r", "c", "u"])) \
        .withColumn("valid_from", current_ts) \
        .withColumn("valid_to", lit("9999-12-31 23:59:59").cast("timestamp")) \
        .withColumn("is_current", lit(1)) \
        .withColumn("ingested_at", current_ts)

    if not DeltaTable.isDeltaTable(spark, silver_history_path):
        upsert_df.write.format("delta").mode("overwrite").save(silver_history_path)
    else:
        history_table = DeltaTable.forPath(spark, silver_history_path)
        
        pk_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
        close_condition = f"{pk_condition} AND target.is_current = 1"
        
        history_table.alias("target").merge(
            upsert_df.alias("source"),
            close_condition
        ).whenMatchedUpdate(set={
            "is_current": "0",
            "valid_to": "source.valid_from"
        }).execute()

        upsert_df.write.format("delta").mode("append").save(silver_history_path)
    print("✅ Hoàn tất Cập nhật Silver History SCD Type 2!")

def log_control_metadata(spark, batch_id, status, records_count, start_cutoff, end_cutoff, table_name, config):
    chk_base = config["checkpoint_base_path"]
    control_path = f"{chk_base}/etl_batch_control"
    print(f"📊 [CONTROL METADATA] Ghi nhận Batch Run ID \{batch_id}\ [Status: {status}]...")

    control_data = [(
        batch_id,
        f"bronze_to_silver_{table_name.lower()}",
        f"bronze.{table_name.lower()}",
        f"silver.value_{table_name.lower()} & history_{table_name.lower()}",
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, help="Tên bảng cần xử lý (VD: SHIPMENT_BOOKINGS)")
    args = parser.parse_args()
    table_name = args.table

    with open("tables_config.json", "r") as f:
        config = json.load(f)

    if table_name not in config["tables"]:
        raise ValueError(f"Không tìm thấy bảng {table_name} trong tables_config.json")

    table_conf = config["tables"][table_name]
    
    # [TỐI ƯU] Nối động topic_prefix và table_name thay vì hardcode
    prefix = config["topic_prefix"]
    target_topic = f"{prefix}.{table_name}"
    primary_keys = table_conf["primary_keys"]
    
    spark = build_spark_session(table_name)
    spark.sparkContext.setLogLevel("WARN")

    bronze_path = config["bronze_base_path"]
    if not DeltaTable.isDeltaTable(spark, bronze_path):
        print("⚠️ Chưa tìm thấy Delta Table ở tầng Bronze.")
        return

    print(f"⚡ Đang xử lý Bronze -> Silver cho bảng: {table_name} (Topic: {target_topic})")

    bronze_df = spark.read.format("delta").load(bronze_path) \
        .filter(col("kafka_topic") == target_topic)
    
    schema = get_table_schema(table_name)
    parsed_bronze_df = bronze_df.withColumn(
        "after_payload", from_json(get_json_object(col("raw_value"), "$.after"), schema)
    ).select(
        "op", "ts_ms", "kafka_offset", "ingested_at", "after_payload.*"
    )

    records_count = parsed_bronze_df.count()
    if records_count == 0:
        print("Trống - Không có biến động CDC mới cho bảng này.")
        return

    dedup_df = get_latest_cdc_dedup(parsed_bronze_df, primary_keys)

    start_cutoff = str(datetime.now() - timedelta(days=1))
    end_cutoff = str(datetime.now())
    batch_id = f"BATCH-{int(time.time())}"

    svb = config["silver_value_base_path"]
    silver_val_path = f"{svb}/{table_name.lower()}"
    process_silver_value(spark, dedup_df, silver_val_path, primary_keys)
    
    shb = config["silver_history_base_path"]
    silver_hist_path = f"{shb}/{table_name.lower()}"
    process_silver_history_scd2(spark, dedup_df, silver_hist_path, primary_keys)

    log_control_metadata(spark, batch_id, "SUCCESS", records_count, start_cutoff, end_cutoff, table_name, config)

    print(f"\n🎉 HOÀN THÀNH TIẾN TRÌNH BIẾN ĐỔI: {table_name}")

if __name__ == "__main__":
    main()
