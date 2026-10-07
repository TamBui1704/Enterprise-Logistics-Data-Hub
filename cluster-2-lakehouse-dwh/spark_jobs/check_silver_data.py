import os
from pyspark.sql import SparkSession

def main():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio-lakehouse:9000")
    spark = SparkSession.builder \
        .appName("Check-Silver-Data") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadminpassword") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    tables_to_check = [
        "shipment_bookings",
        "delivery_remunerations",
        "return_remunerations",
        "shipment_event_trackings"
    ]
    
    for table in tables_to_check:
        print(f"\n=========================================")
        print(f"BẢNG: {table.upper()}")
        print(f"=========================================")
        
        # Check Value
        value_path = f"s3a://logistics-lakehouse/silver/value_{table}"
        try:
            df_value = spark.read.format("delta").load(value_path)
            print(f"✅ Đọc thành công thư mục Value: {value_path}")
            print(f"Tổng số dòng: {df_value.count():,}\n")
            print("--- 3 dòng dữ liệu mẫu (Value) ---")
            df_value.show(3, truncate=False)
        except Exception as e:
            print(f"❌ Lỗi khi đọc thư mục Value ({value_path}): {e}")
            
        # Check History
        history_path = f"s3a://logistics-lakehouse/silver/history_{table}"
        try:
            df_history = spark.read.format("delta").load(history_path)
            print(f"✅ Đọc thành công thư mục History: {history_path}")
            print(f"Tổng số dòng: {df_history.count():,}\n")
            print("--- 3 dòng dữ liệu mẫu (History) ---")
            df_history.show(3, truncate=False)
        except Exception as e:
            print(f"❌ Lỗi khi đọc thư mục History ({history_path}): {e}")

if __name__ == "__main__":
    main()
