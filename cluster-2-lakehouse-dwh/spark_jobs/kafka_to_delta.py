"""
PySpark Structured Streaming Job
Đọc dữ liệu CDC thô từ Apache Kafka (Debezium Oracle) -> Parse Debezium Envelope (gồm op: r/c/u/d)
-> Ghi Append-Only vào MinIO Delta Lake Bronze Layer (Immutability Data Lakehouse)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

def build_spark_session():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    return SparkSession.builder \
        .appName("EMS-Logistics-Kafka-CDC-to-Bronze-Delta") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
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

def main():
    print("⚡ Khởi tạo Spark Streaming Engine nạp Raw CDC sang Bronze Delta Layer...")
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Debezium Record Payload Schema (Bao gồm op: r = read/snapshot, c = create, u = update, d = delete)
    booking_after_schema = StructType([
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

    debezium_envelope_schema = StructType([
        StructField("before", booking_after_schema, True),
        StructField("after", booking_after_schema, True),
        StructField("op", StringType(), True),      # 'r' = read (snapshot), 'c' = create, 'u' = update, 'd' = delete
        StructField("ts_ms", LongType(), True)      # Timestamp miligiây từ Debezium CDC
    ])

    topic_name = "cdc_logistics_oracle.DEBEZIUM.SHIPMENT_BOOKINGS"
    print(f"📥 Đang kết nối tới Apache Kafka Topic '{topic_name}'...")
    
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON Debezium Payload & bổ sung Metadata Kafka + Ingested_at
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        from_json(col("value").cast("string"), debezium_envelope_schema).alias("cdc_payload"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        "kafka_key",
        "cdc_payload.op",
        "cdc_payload.ts_ms",
        "cdc_payload.after.*",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp"
    ).withColumn("ingested_at", current_timestamp())

    delta_path = "s3a://logistics-lakehouse/bronze/shipment_bookings"
    checkpoint_path = "s3a://logistics-lakehouse/checkpoints/shipment_bookings"

    print(f"🚀 Ghi luồng Streaming Append-Only vào MinIO Delta Lake Bronze ({delta_path})...")
    query = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .start(delta_path)

    query.awaitTermination()

if __name__ == "__main__":
    main()
