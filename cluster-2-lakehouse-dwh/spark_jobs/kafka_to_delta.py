import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, get_json_object

def build_spark_session():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio-lakehouse:9000")
    return SparkSession.builder \
        .appName("EMS-Logistics-Kafka-CDC-to-Bronze-Delta-Wildcard") \
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
    print("⚡ Khởi tạo Spark Streaming Engine nạp Raw CDC sang Bronze Delta Layer (Multi-tables)...")
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    with open("tables_config.json", "r") as f:
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

    # KHÔNG ép schema ở tầng Bronze. Chỉ bóc tách metadata.
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        get_json_object(col("value").cast("string"), "$.op").alias("op"),
        get_json_object(col("value").cast("string"), "$.ts_ms").cast("long").alias("ts_ms"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
    ).withColumn("ingested_at", current_timestamp())

    delta_path = config["bronze_base_path"]
    chk_base = config["checkpoint_base_path"]
    checkpoint_path = f"{chk_base}/bronze_all_tables"

    print(f"🚀 Ghi luồng Streaming Append-Only vào MinIO, tự động chia Partition theo Topic...")
    query = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .partitionBy("kafka_topic") \
        .option("checkpointLocation", checkpoint_path) \
        .start(delta_path)

    query.awaitTermination()

if __name__ == "__main__":
    main()
