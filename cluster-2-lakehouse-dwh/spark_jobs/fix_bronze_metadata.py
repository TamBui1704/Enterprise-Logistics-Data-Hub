import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, get_json_object, from_unixtime
from delta.tables import DeltaTable

def main():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio-lakehouse:9000")
    spark = SparkSession.builder \
        .appName("Fix-Bronze-Metadata-Event-Time") \
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

    spark.sparkContext.setLogLevel("WARN")

    bronze_path = "s3a://logistics-lakehouse/bronze/all_tables"
    print(f"🔍 Kiểm tra bảng Bronze Delta tại: {bronze_path}")

    if not DeltaTable.isDeltaTable(spark, bronze_path):
        print("❌ Chưa tìm thấy bảng Delta tại Bronze path.")
        return

    delta_table = DeltaTable.forPath(spark, bronze_path)
    cols = delta_table.toDF().columns

    # Nếu bảng chưa có cột event_time, thực hiện ADD COLUMN
    if "event_time" not in cols:
        print("➕ Đang thêm cột event_time (TIMESTAMP) vào schema bảng Bronze Delta...")
        spark.sql(f"ALTER TABLE delta.`{bronze_path}` ADD COLUMNS (event_time TIMESTAMP)")
        delta_table = DeltaTable.forPath(spark, bronze_path)

    # Đếm số bản ghi cần cập nhật event_time hoặc op
    df = delta_table.toDF()
    null_event_count = df.filter(col("event_time").isNull()).count()
    print(f"📊 Số lượng bản ghi cần cập nhật event_time: {null_event_count}")

    if null_event_count > 0:
        print("🔄 Đang cập nhật event_time từ ts_ms (theo múi giờ Asia/Ho_Chi_Minh)...")
        delta_table.update(
            condition = col("event_time").isNull(),
            set = {
                "event_time": from_unixtime(col("ts_ms") / 1000).cast("timestamp"),
                "op": coalesce(
                    get_json_object(col("raw_value"), "$.payload.op"),
                    get_json_object(col("raw_value"), "$.op")
                ),
                "ts_ms": coalesce(
                    get_json_object(col("raw_value"), "$.payload.ts_ms"),
                    get_json_object(col("raw_value"), "$.ts_ms")
                ).cast("long")
            }
        )

    print("✅ Đã cập nhật thành công toàn bộ cột event_time ở tầng Bronze!")
    print("\n--- 5 bản ghi mẫu sau khi cập nhật event_time ---")
    delta_table.toDF().select("kafka_topic", "op", "ts_ms", "event_time", "kafka_timestamp", "ingested_at").show(5, False)

if __name__ == "__main__":
    main()
