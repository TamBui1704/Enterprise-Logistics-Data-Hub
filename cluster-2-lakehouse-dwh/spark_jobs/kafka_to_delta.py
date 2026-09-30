"""
PySpark Structured Streaming Job
Đọc dữ liệu CDC từ Apache Kafka Topic -> Xử lý & Ghi vào MinIO Delta Lake (Bronze / Silver)
và Đồng bộ sang ClickHouse DWH (Gold Layer)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def build_spark_session():
    return SparkSession.builder \
        .appName("Logistics-Kafka-CDC-to-Lakehouse") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.1.0,"
                "com.clickhouse:clickhouse-jdbc:0.6.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadminpassword") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def main():
    print("⚡ Đang khởi tạo Spark Structured Streaming Engine...")
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Schema CDC từ Debezium Kafka
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("status", StringType(), True),
        StructField("updated_at", StringType(), True)
    ])

    print("📥 Đang kết nối tới Apache Kafka Topic 'cdc_logistics.public.orders'...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "cdc_logistics.public.orders") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON payload từ Debezium CDC
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*")

    print("🚀 Ghi luồng Streaming vào MinIO Delta Lake (s3a://logistics-lakehouse/delta/orders)...")
    query = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://logistics-lakehouse/checkpoints/orders") \
        .start("s3a://logistics-lakehouse/delta/orders")

    query.awaitTermination()

if __name__ == "__main__":
    main()
