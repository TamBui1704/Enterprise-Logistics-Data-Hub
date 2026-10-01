# Enterprise Logistics Data Hub (POC Architecture)

Hệ thống Data Hub & Data Lakehouse thử nghiệm theo kiến trúc Modern Data Stack dành cho doanh nghiệp Logistics (EMS).

---

## 📐 Kiến trúc Tổng thể (Architecture)

1. **Lớp Nguồn Dữ liệu (Data Sources)**: Oracle DB (Integration Hub / Core ERP), CRM, Master Data.
2. **Lớp Tích hợp & Thu thập (Ingestion)**: Debezium (CDC), Apache Kafka, Python `dlt`.
3. **Lớp Kho Dữ liệu & Xử lý (Lakehouse & DWH)**: MinIO (S3 Storage), Delta Lake (Lakehouse Bronze/Silver), Apache Spark, ClickHouse (Serving DWH OLAP - Gold), `dbt`.
4. **Lớp Ngữ nghĩa & Điều phối (Semantic & Orchestration)**: Cube.dev (Metrics Layer), Apache Airflow (DAGs Orchestrator).
5. **Lớp Trực quan hóa & Phân tích (Visualization)**: Apache Superset (Self-service BI & Executive Dashboards).

---

## 🛠️ Chiến lược Thử nghiệm Cụm (Modular Testing Strategy - RAM 12GB)

Để đáp ứng tài nguyên máy tính local (RAM ~12GB - 15GB, Disk tối ưu), hệ thống được chia làm **4 Cụm thử nghiệm độc lập**:

| Cụm | Tên Cụm | Các Công nghệ | Mục tiêu Thử nghiệm |
| :--- | :--- | :--- | :--- |
| **Cụm 1** | **Data Source & Real-time CDC** | Oracle / PostgreSQL (Mock), Kafka (KRaft), Debezium CDC | Stream dữ liệu thay đổi (CDC BinLog/RedoLog) vào Kafka Topics |
| **Cụm 2** | **Lakehouse & DWH Processing** | MinIO, Delta Lake, Apache Spark, ClickHouse | Đọc Kafka -> Ghi MinIO Delta Lake -> Transform & Sync vào ClickHouse DWH |
| **Cụm 3** | **Transformation & Orchestration**| `dbt-clickhouse`, Apache Airflow | Biến đổi dữ liệu chuẩn hóa DWH (Dim/Fact) & Điều phối tự động |
| **Cụm 4** | **Semantic Layer & BI Dashboard** | Cube.dev, Apache Superset | Định nghĩa chỉ số Metric tập trung & Dựng Dashboard KPI Logistics |

---

## 📂 Cấu trúc Thư mục Dự án

```text
Enterprise-Logistics-Data-Hub/
├── GUIDE_END_TO_END_TESTING.md   # Hướng dẫn chi tiết kiểm thử thông luồng End-to-End
├── cluster-1-ingestion/          # Cụm 1: CDC Ingestion (Oracle DB + Debezium + Kafka)
│   ├── docker-compose.yml
│   ├── debezium/
│   └── scripts/
├── cluster-2-lakehouse-dwh/      # Cụm 2: Lakehouse & DWH (MinIO + Delta Lake + Spark + ClickHouse)
│   ├── docker-compose.yml
│   └── spark_jobs/
├── cluster-3-dbt-airflow/        # Cụm 3: Transform & Orchestrate (dbt + Airflow)
│   ├── dbt_logistics/
│   └── airflow/
└── cluster-4-bi/                 # Cụm 4: Metrics Layer & BI (Cube.dev + Superset)
    ├── cube/
    └── superset/
```

---

## 📖 Hướng Dẫn Kiểm Thử

Xem tài liệu chi tiết quy trình chạy và kiểm thử thông luồng dữ liệu tại: [GUIDE_END_TO_END_TESTING.md](file:///c:/Users/buith/OneDrive/Desktop/Enterprise-Logistics-Data-Hub/GUIDE_END_TO_END_TESTING.md)

