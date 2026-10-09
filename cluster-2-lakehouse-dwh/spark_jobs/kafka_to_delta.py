import os
import json
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, get_json_object, coalesce, from_unixtime

def build_spark_session():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio-lakehouse:9000")
    return SparkSession.builder \
        .appName("EMS-Logistics-Kafka-CDC-to-Bronze-Delta-Wildcard") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadminpassword") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def main():
    parser = argparse.ArgumentParser(description="Kafka CDC to Bronze Delta Stream")
    parser.add_argument("--once", action="store_true", help="Kéo toàn bộ message mới rồi tự ngắt (AvailableNow)")
    args, _ = parser.parse_known_args()

    print("⚡ Khởi tạo Spark Streaming Engine nạp Raw CDC sang Bronze Delta Layer (Multi-tables)...")
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.getenv("TABLES_CONFIG_PATH", os.path.join(script_dir, "tables_config.json"))
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # Lấy prefix từ cấu hình và build Regex Pattern động
    prefix = config["topic_prefix"]
    topic_pattern = f"{prefix}.*"
    print(f"📥 Đang kết nối tới Apache Kafka với Regex Pattern \"{topic_pattern}\"...")
    
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribePattern", topic_pattern) \
        .option("startingOffsets", "earliest") \
        .load()

    # Bóc tách metadata Debezium chuẩn xác: Giữ ts_ms nguyên bản + bổ sung event_time (Human-readable)
    base_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        coalesce(
            get_json_object(col("value").cast("string"), "$.payload.op"),
            get_json_object(col("value").cast("string"), "$.op")
        ).alias("op"),
        coalesce(
            get_json_object(col("value").cast("string"), "$.payload.ts_ms"),
            get_json_object(col("value").cast("string"), "$.ts_ms")
        ).cast("long").alias("ts_ms"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
    )

    parsed_df = base_df.withColumn(
        "event_time", from_unixtime(col("ts_ms") / 1000).cast("timestamp")
    ).withColumn(
        "ingested_at", current_timestamp()
    )

    delta_path = config["bronze_base_path"]
    chk_base = config["checkpoint_base_path"]
    checkpoint_path = f"{chk_base}/bronze_all_tables"

    writer = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .partitionBy("kafka_topic") \
        .option("mergeSchema", "true") \
        .option("checkpointLocation", checkpoint_path)

    if args.once or os.getenv("TRIGGER_AVAILABLE_NOW", "false").lower() == "true":
        print("🚀 Chạy chế độ: Trigger.AvailableNow (Xử lý toàn bộ message tồn đọng rồi tự tắt để giải phóng RAM)...")
        writer = writer.trigger(availableNow=True)
    else:
        print("🚀 Chạy chế độ: Realtime Continuous Streaming (Lắng nghe liên tục)...")

    query = writer.start(delta_path)
    query.awaitTermination()
    print("✅ Hoàn tất tiến trình Streaming!")

if __name__ == "__main__":
    main()
