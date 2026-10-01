# 🚀 Hướng Dẫn Kiểm Thử Thông Luồng Dữ Liệu End-to-End (Data Pipeline Guide)

Tài liệu hướng dẫn chi tiết quy trình khởi chạy và kiểm thử toàn bộ luồng dữ liệu từ **Oracle Database (OLTP Nguồn)** $\rightarrow$ **Debezium CDC** $\rightarrow$ **Apache Kafka** $\rightarrow$ **PySpark Structured Streaming** $\rightarrow$ **MinIO S3 Delta Lakehouse** $\rightarrow$ **ClickHouse DWH**.

---

## 📐 1. Kiến Trúc & Cổng Dịch Vụ Hệ Thống (Ports Mapping)

| Dịch vụ | Tên Container | Cổng Host (Port) | Chức năng / Địa chỉ Web UI |
| :--- | :--- | :--- | :--- |
| **Oracle Database** | `source_oracle_db` | `1521`, `5500` | Oracle 23c XE (LogMiner CDC pre-configured) |
| **Apache Kafka** | `kafka_broker` | `9092` | Event Broker (KRaft Mode) |
| **Debezium Connect** | `debezium_cdc` | `8083` | REST API Đăng ký Connector |
| **Kafka UI** | `kafka_ui` | `8080` | Giao diện Web xem Topics & CDC Messages (`http://localhost:8080`) |
| **MinIO Storage** | `minio_lakehouse` | `9000`, `9001` | S3 Object Storage Console (`http://localhost:9001`) |
| **ClickHouse DWH** | `clickhouse_dwh` | `8123`, `9009` | Serving Data Warehouse OLAP |
| **PySpark Master** | `spark_runner` | `8081`, `7077` | Spark Cluster Web UI (`http://localhost:8081`) |

---

## 🧠 2. KHIẾN TRÚC CHUYÊN SÂU: DEBEZIUM CDC & MEDALLION LAKEHOUSE

```text
ORACLE DB ──► DEBEZIUM CDC ──► KAFKA ──► BRONZE LAYER (Append-Only CDC Payloads)
                                               │
                            ┌──────────────────┴──────────────────┐
                            ▼                                     ▼
                 SILVER VALUE (Current 1:1)             SILVER HISTORY (SCD Type 2)
                 (MERGE INTO, Ingested_at)             (valid_from, valid_to, is_current)
```

### 🔹 Tách 2 Connectors Độc Lập Cho Bảng Nhỏ (Dims) & Bảng Lớn (Facts)
* **Connector Bảng Nhỏ (Dimensions)**: `debezium/register-dim-connector.json` (`snapshot.mode: initial`).
* **Connector Bảng Lớn (Facts)**: `debezium/register-fact-connector.json` (`snapshot.mode: schema_only`). PySpark Initial Bulk Load trực tiếp nạp lịch sử vào Silver, Debezium chỉ chốt mốc SCN và stream dữ liệu mới.

---

## 🏭 3. QUY TRÌNH KIỂM THỬ THÔNG LUỒNG CHI TIẾT TỪNG BƯỚC (STEP-BY-STEP)

> 💡 **Ghi chú về Môi trường Python (khi chọn chạy trực tiếp trên máy Local)**:
> * **Với Cluster 1 scripts**: Cài đặt thư viện bằng `pip install -r cluster-1-ingestion/requirements.txt` (cần gói `oracledb`).
> * **Với Cluster 2 scripts**: Cài đặt thư viện bằng `pip install -r cluster-2-lakehouse-dwh/requirements.txt` (cần `pyspark`, `delta-spark`, `oracledb`).
> * *(Nếu bạn chạy trong Container `spark_runner` thì không cần cài pip trên máy local).*

---

### BƯỚC 1: Khởi Động Hạ Tầng Container (Cluster 1 & Cluster 2)

Mở PowerShell / Terminal tại máy của bạn:

```powershell
# 1. Khởi chạy Cluster 1 (Oracle DB, Kafka, Debezium, Kafka UI)
cd cluster-1-ingestion
docker compose up -d

# 2. Khởi chạy Cluster 2 (MinIO S3, ClickHouse DWH, PySpark Runner)
cd ../cluster-2-lakehouse-dwh
docker compose up -d
```

> 🔍 **Kiểm tra trạng thái**: Gõ `docker ps`. Tất cả 7 container phải ở trạng thái `Up` (hoặc `healthy`).

---

### BƯỚC 2: PySpark Initial Bulk Load Bảng Lớn Từ Oracle Vào Silver Layer

Nạp dữ liệu ban đầu từ Oracle DB sang MinIO S3 Silver Layer:

#### 🔹 Cách A: Chạy trực tiếp trên máy Local (Dành cho Dev / Test nhanh)
```powershell
cd cluster-2-lakehouse-dwh
pip install -r requirements.txt
python spark_jobs/oracle_bulk_initial_load.py
```

#### 🔹 Cách B: Chạy bên trong Spark Container (Dành cho Airflow Production)
```powershell
docker exec -it spark_runner spark-submit /opt/bitnami/spark/spark_jobs/oracle_bulk_initial_load.py
```

---

### BƯỚC 3: Đăng Ký 2 Debezium Connectors (Dim & Fact) Bắt Luồng CDC

Chuyển sang thư mục `cluster-1-ingestion` để gửi REST API tới Debezium Connect:

```powershell
cd cluster-1-ingestion

# 1. Đăng ký Connector Bảng Nhỏ (Snapshot Full + Incremental)
Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Post -ContentType "application/json" -InFile "debezium/register-dim-connector.json"

# 2. Đăng ký Connector Bảng Lớn (Schema Only - Incremental Only)
Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Post -ContentType "application/json" -InFile "debezium/register-fact-connector.json"
```

> 🔍 **Kiểm tra trạng thái**: Mở trình duyệt xem Kafka UI tại `http://localhost:8080` hoặc gõ:
> `Invoke-RestMethod -Uri "http://localhost:8083/connectors/oracle-logistics-fact-connector/status"`

---

### BƯỚC 4: Phát Sinh Giao Dịch Realtime Mới Vào Oracle DB (Simulator)

Mở một cửa sổ Terminal mới để chạy script phát sinh đơn hàng EMS giao dịch realtime:

#### 🔹 Cách A: Chạy trực tiếp trên máy Local
```powershell
cd cluster-1-ingestion
pip install -r requirements.txt
python scripts/seed_realtime_events.py
```
> 📦 Script sẽ tạo liên tục các sự kiện `INSERT` đơn hàng mới và `UPDATE` trạng thái giao hàng trong Oracle DB. Debezium LogMiner sẽ lập tức bắt các sự kiện này và đẩy vào Kafka topic `cdc_logistics_oracle.DEBEZIUM.SHIPMENT_BOOKINGS`.

---

### BƯỚC 5: PySpark Streaming Đọc Kafka CDC Về Bronze Layer (MinIO Delta Lake)

Mở một cửa sổ Terminal khác để chạy PySpark Streaming job tiếp nhận dữ liệu CDC từ Kafka ghi vào Bronze Layer:

#### 🔹 Cách A: Chạy trực tiếp trên máy Local
```powershell
cd cluster-2-lakehouse-dwh
pip install -r requirements.txt
python spark_jobs/kafka_to_delta.py
```

#### 🔹 Cách B: Chạy bên trong Spark Container
```powershell
docker exec -it spark_runner spark-submit /opt/bitnami/spark/spark_jobs/kafka_to_delta.py
```

---

### BƯỚC 6: Thực Thi Medallion Batch Transformation (Bronze -> Silver Value & History)

Sau khi dữ liệu thô CDC đã tích tụ tại Bronze Layer, chạy script chuyển đổi Medallion để thực hiện `MERGE INTO` (Upsert / Delete / SCD Type 2):

#### 🔹 Cách A: Chạy trực tiếp trên máy Local
```powershell
cd cluster-2-lakehouse-dwh
pip install -r requirements.txt
python spark_jobs/bronze_to_silver_medallion.py
```

#### 🔹 Cách B: Chạy bên trong Spark Container
```powershell
docker exec -it spark_runner spark-submit /opt/bitnami/spark/spark_jobs/bronze_to_silver_medallion.py
```

---

## 🔍 4. QUY TRÌNH KIỂM TRA & KIỂM THU DỮ LIỆU ĐÃ ĐỔ VỀ S3 LAKEHOUSE

### 1. Kiểm tra qua MinIO Web Console UI (Trực quan nhất 🌐)
1. Truy cập `http://localhost:9001` (User: `minioadmin` | Password: `minioadminpassword`).
2. Vào **Object Browser** $\rightarrow$ chọn Bucket **`logistics-lakehouse`**:
   * Kiểm tra thư mục `bronze/shipment_bookings/`: Các file `.parquet` thô + thư mục `_delta_log/`.
   * Kiểm tra thư mục `silver/value_shipment_bookings/`: Bản ghi hiện tại 1:1.
   * Kiểm tra thư mục `silver/history_shipment_bookings/`: Bản ghi lưu vết SCD Type 2 (`valid_from`, `valid_to`, `is_current`).

### 2. Kiểm tra qua DBeaver + ClickHouse SQL
Mở DBeaver kết nối tới ClickHouse (`localhost:8123`) và gõ SQL đọc trực tiếp dữ liệu từ MinIO S3:
```sql
SELECT * 
FROM s3('http://minio:9000/logistics-lakehouse/silver/value_shipment_bookings/*.parquet', 'minioadmin', 'minioadminpassword')
LIMIT 10;
```

---

## 📌 5. BỘ LỆNH QUẢN LÝ DEBEZIUM API THƯỜNG DÙNG

```powershell
# 1. Xem danh sách tất cả Connector đang chạy
Invoke-RestMethod -Uri "http://localhost:8083/connectors"

# 2. Kiểm tra chi tiết trạng thái Fact Connector
Invoke-RestMethod -Uri "http://localhost:8083/connectors/oracle-logistics-fact-connector/status"

# 3. Xóa Connector (khi cần làm lại snapshot)
Invoke-RestMethod -Uri "http://localhost:8083/connectors/oracle-logistics-fact-connector" -Method Delete
```
