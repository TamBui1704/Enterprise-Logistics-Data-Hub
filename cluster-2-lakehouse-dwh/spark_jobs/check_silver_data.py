"""
Script Kiểm Tra & Đối Soát Toàn Diện Tầng Silver (Value & History SCD Type 2)
Kiểm tra số lượng bản ghi trên 11 bảng Medallion Lakehouse.
"""

import os
from pyspark.sql import SparkSession

def main():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio-lakehouse:9000")
    spark = SparkSession.builder \
        .appName("Check-Silver-Reconciliation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadminpassword") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.master", "local[2]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Danh sách 11 bảng theo DWH Bus Matrix
    tables = [
        "DIM_CUSTOMERS",
        "DIM_SERVICES",
        "DIM_POS_LOCATIONS",
        "DIM_WEIGHT_TIERS",
        "DIM_ROUTING_TYPES",
        "DIM_DELIVERY_STATUSES",
        "DIM_FAILURE_REASONS",
        "SHIPMENT_BOOKINGS",
        "DELIVERY_REMUNERATIONS",
        "RETURN_REMUNERATIONS",
        "SHIPMENT_EVENT_TRACKINGS"
    ]

    print("\n" + "=" * 115)
    print("📊 BẢNG TỔNG HỢP ĐỐI SOÁT TẦNG SILVER (VALUE 1:1 SNAPSHOT & HISTORY SCD TYPE 2)")
    print("=" * 115)
    print(f"| {'TÊN BẢNG (TABLE NAME)':<26} | {'VALUE COUNT':<12} | {'HIST TOTAL':<12} | {'HIST ACTIVE (1)':<16} | {'HIST CLOSED (0)':<16} | {'TRẠNG THÁI':<12} |")
    print("-" * 115)

    for t_upper in tables:
        t = t_upper.lower()
        val_path = f"s3a://logistics-lakehouse/silver/value_{t}"
        hist_path = f"s3a://logistics-lakehouse/silver/history_{t}"

        try:
            cnt_val = spark.read.format("delta").load(val_path).count()
        except Exception:
            cnt_val = -1

        try:
            df_hist = spark.read.format("delta").load(hist_path)
            cnt_hist_total = df_hist.count()
            cnt_hist_active = df_hist.filter("is_current = 1").count()
            cnt_hist_closed = df_hist.filter("is_current = 0").count()
        except Exception:
            cnt_hist_total, cnt_hist_active, cnt_hist_closed = -1, -1, -1

        # Trạng thái: Value và History Active phải khớp nhau 1-1
        is_match = (cnt_val == cnt_hist_active) and (cnt_val >= 0)
        status_str = "✅ KHỚP 100%" if is_match else "⚠️ CẦN CHECK"

        print(f"| {t_upper:<26} | {str(cnt_val):<12} | {str(cnt_hist_total):<12} | {str(cnt_hist_active):<16} | {str(cnt_hist_closed):<16} | {status_str:<12} |")

    print("=" * 115)
    print("💡 Ghi chú:")
    print(" - 'VALUE COUNT': Số bản ghi hiện hành (Current State 1:1 với nguồn Oracle).")
    print(" - 'HIST ACTIVE (1)': Bản ghi đang có hiệu lực trong bảng SCD Type 2 (Phải luôn = VALUE COUNT).")
    print(" - 'HIST CLOSED (0)': Các phiên bản dữ liệu lịch sử cũ đã bị đóng lại khi có sự kiện cập nhật/thay đổi.\n")

    spark.stop()

if __name__ == "__main__":
    main()
