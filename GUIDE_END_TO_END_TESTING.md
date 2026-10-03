# 🚀 Hướng Dẫn Kiểm Thử Thông Luồng Dữ Liệu End-to-End (Data Pipeline Guide)

Tài liệu hướng dẫn chi tiết quy trình khởi chạy và kiểm thử toàn bộ luồng dữ liệu từ **Oracle Database (OLTP Nguồn)** $\rightarrow$ **Debezium CDC** $\rightarrow$ **Apache Kafka** $\rightarrow$ **PySpark Structured Streaming (Master/Worker Cluster)** $\rightarrow$ **MinIO S3 Delta Lakehouse** $\rightarrow$ **ClickHouse DWH**.

---

## 📐 1. Kiến Trúc & Cổng Dịch Vụ Hệ Thống (Ports Mapping)

| Dịch vụ | Tên Container | Cổng Host (Port) | Chức năng / Địa chỉ Web UI |
| :--- | :--- | :--- | :--- |
| **Oracle Database** | `source_oracle_db` | `1521`, `5500` | Oracle 23c XE (LogMiner CDC pre-configured) |
| **Apache Kafka** | `kafka_broker` | `9092` | Event Broker (KRaft Mode) |
| **Debezium Connect** | `debezium_cdc` | `8083` | REST API Đăng ký Connector |
| **Kafka UI** | `kafka_ui` | `8080` | Giao diện Web xem Topics & CDC Messages (`http://localhost:8080`) |
| **MinIO Storage** | `minio_lakehouse` | `9000`, `9001` | S3 Object Storage Console (`http://localhost:9001`) |
| **ClickHouse DWH** | `clickhouse_dwh` | `8123`, `9009` | Serving Data Warehouse OLAP (Default user: `default`, pass: rỗng) |
| **PySpark Master** | `spark_runner` | `8081`, `7077` | Spark Master Web UI (`http://localhost:8081`) |
| **PySpark Worker** | `spark_worker` | - | Spark Worker Node (Connected to Master `spark_runner:7077`) |

---

## 🧠 2. KIẾN TRÚC CHUYÊN SÂU: DEBEZIUM CDC & MEDALLION LAKEHOUSE

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

# 2. Khởi chạy Cluster 2 (MinIO S3, ClickHouse DWH, PySpark Master & Worker)
cd ../cluster-2-lakehouse-dwh
docker compose up -d
```

> 🔍 **Kiểm tra trạng thái**: Gõ `docker ps`. Tất cả 8 container phải ở trạng thái `Up` (hoặc `healthy`).
> Kiểm tra Spark Master Web UI tại `http://localhost:8081` phải hiển thị **Alive Workers: 1** (`spark_worker`).

---

### BƯỚC 2: PySpark Initial Bulk Load Bảng Lớn Từ Oracle Vào Silver Layer

Nạp dữ liệu ban đầu từ Oracle DB sang MinIO S3 Silver Layer:

#### 🔹 Cách A: Chạy trực tiếp trên máy Local (Dành cho Dev / Test nhanh - Mode Local)
```powershell
cd cluster-2-lakehouse-dwh
pip install -r requirements.txt
python spark_jobs/oracle_bulk_initial_load.py
```

#### 🔹 Cách B: Submit Job lên cụm Spark Standalone Cluster (Dành cho Production / Airflow)
```powershell
docker exec -it spark_runner spark-submit --master spark://spark_runner:7077 /opt/bitnami/spark/spark_jobs/oracle_bulk_initial_load.py
```
> 💡 *Truyền cờ `--master spark://spark_runner:7077` giúp Spark Master (`spark_runner`) ghi nhận job lên Web UI (`http://localhost:8081`) và điều phối cho Spark Worker (`spark_worker`) thực thi.*

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

#### 🔹 Cách B: Submit Job lên cụm Spark Standalone Cluster
```powershell
docker exec -it spark_runner spark-submit --master spark://spark_runner:7077 /opt/bitnami/spark/spark_jobs/kafka_to_delta.py
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

#### 🔹 Cách B: Submit Job lên cụm Spark Standalone Cluster
```powershell
docker exec -it spark_runner spark-submit --master spark://spark_runner:7077 /opt/bitnami/spark/spark_jobs/bronze_to_silver_medallion.py
```

---

### BƯỚC 7: Khởi Động Cluster 3 & Thực Thi dbt Transformations (dbt-clickhouse & Airflow)

#### 1. Khởi động Container Airflow + dbt-clickhouse (Cluster 3)
```powershell
cd cluster-3-dbt-airflow
docker compose up -d --build
```

> 🔍 **Kiểm tra**: Mở Airflow Web UI tại `http://localhost:8085` (User: `admin` | Password: hiển thị trong log hoặc dùng command).

#### 2. Thử nghiệm chạy dbt trực tiếp từ Container `airflow_orchestrator`

Quá trình biến đổi dbt được chia thành **2 project riêng biệt**:
* **`dbt_lakehouse_spark` (Chạy trên cụm Spark)**: Đọc dữ liệu Silver từ MinIO nạp thành các bảng Star Schema và tạo sẵn bảng OBT trên Delta Lake S3.
* **`dbt_warehouse_clickhouse` (Chạy trên ClickHouse)**: Đọc file OBT Delta từ S3 nạp vào ClickHouse dưới dạng `ReplacingMergeTree` (bảng `obt_shipment_analytics_base`) và tạo Wrapper View `obt_shipment_analytics` siêu tối ưu và chống duplicate cho BI (Superset / Metabase).

```powershell
# 1. Run dbt Spark (Biến đổi S3 Silver -> S3 Gold Delta Lake)
docker exec -it airflow_orchestrator dbt run --project-dir /opt/airflow/dbt_lakehouse_spark --profiles-dir /opt/airflow/dbt_lakehouse_spark

# 2. Run dbt ClickHouse (Nạp S3 Gold OBT -> ClickHouse DWH & tạo View)
docker exec -it airflow_orchestrator dbt run --project-dir /opt/airflow/dbt_warehouse_clickhouse --profiles-dir /opt/airflow/dbt_warehouse_clickhouse

# 3. Run dbt data quality tests
docker exec -it airflow_orchestrator dbt test --project-dir /opt/airflow/dbt_warehouse_clickhouse --profiles-dir /opt/airflow/dbt_warehouse_clickhouse
```

#### 3. Kích hoạt Airflow DAG điều phối tự động E2E
Mở `http://localhost:8085` $\rightarrow$ Chọn DAG `e2e_logistics_data_pipeline` $\rightarrow$ Nhấn **Unpause** & **Trigger DAG**.
DAG sẽ tự động chạy chuỗi 4 bước: `Spark Bulk Load` $\rightarrow$ `Spark Medallion` $\rightarrow$ `dbt run (schema + datamart)` $\rightarrow$ `dbt test`.

---

## 🔍 4. QUY TRÌNH KIỂM TRA & KIỂM THU DỮ LIỆU S3 LAKEHOUSE & CLICKHOUSE DWH

### 1. Kiểm tra qua MinIO Web Console UI (Object Storage 🌐)
1. Truy cập `http://localhost:9001` (User: `minioadmin` | Password: `minioadminpassword`).
2. Vào **Object Browser** $\rightarrow$ chọn Bucket **`logistics-lakehouse`**:
   * Kiểm tra thư mục `bronze/shipment_bookings/`: Các file `.parquet` thô + thư mục `_delta_log/`.
   * Kiểm tra thư mục `silver/value_shipment_bookings/`: Bản ghi hiện tại 1:1.
   * Kiểm tra thư mục `silver/history_shipment_bookings/`: Bản ghi lưu vết SCD Type 2 (`valid_from`, `valid_to`, `is_current`).

### 2. Kiểm tra ClickHouse DWH (Serving OLAP Layer ⚡)

Tài khoản kết nối ClickHouse mặc định:
* **Host**: `localhost` | **HTTP Port**: `8123` | **Native Port**: `9009`
* **Username**: `default` | **Password**: *(bỏ trống / empty)*

#### 🔹 Phương án A: Dùng GUI Tool (DBeaver / DataGrip)
1. Tạo Connection chọn driver **ClickHouse**.
2. Nhập Host: `localhost`, Port: `8123` (HTTP) hoặc `9009` (Native TCP), User: `default`, Password: *(bỏ trống)*.
3. **Chạy các câu lệnh SQL nghiệm thu dữ liệu tầng Schema & Datamart OBT**:

```sql
-- 1. Xem danh sách các bảng vừa được dbt tạo ra trong ClickHouse
SHOW TABLES;
-- 💡 Bạn sẽ thấy 2 bảng: obt_shipment_analytics_base (Bảng vật lý chứa dữ liệu incremental) 
-- và obt_shipment_analytics (Wrapper View tự động gỡ duplicate cho DA)

-- 2. Kiểm tra dữ liệu bảng OBT Analytics Mart qua Wrapper View
-- DA chỉ cần truy vấn View này, mọi duplicate do append đều được giải quyết ngầm
SELECT 
    BOOKING_ID, 
    ITEM_CODE, 
    CUSTOMER_NAME, 
    SERVICE_NAME,
    SENDING_PROVINCE, 
    RECEIVING_PROVINCE, 
    STATUS_NAME,
    TOTAL_REVENUE, 
    COST_AMOUNT, 
    PROFIT_AMOUNT
FROM default.obt_shipment_analytics
LIMIT 10;

-- 3. Truy vấn thống kê tổng hợp Doanh thu & Lợi nhuận theo Tỉnh gửi từ bảng OBT
SELECT 
    SENDING_PROVINCE,
    count() AS TOTAL_BOOKINGS,
    sum(TOTAL_REVENUE) AS REVENUE_VND,
    sum(PROFIT_AMOUNT) AS PROFIT_VND
FROM default.obt_shipment_analytics
GROUP BY SENDING_PROVINCE
ORDER BY REVENUE_VND DESC;
```

#### 🔹 Phương án B: Kết nối trực tiếp bằng CLI trong Container
```powershell
# Vô giao diện dòng lệnh ClickHouse Client
docker exec -it clickhouse_dwh clickhouse-client

# Gõ các lệnh SQL kiểm tra:
SHOW TABLES;
SELECT count() FROM default.obt_shipment_analytics;
```

#### 🔹 Phương án C: Kiểm tra nhanh qua cURL (Terminal / PowerShell)
```powershell
curl "http://localhost:8123/?query=SELECT+count()+FROM+default.obt_shipment_analytics"
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


