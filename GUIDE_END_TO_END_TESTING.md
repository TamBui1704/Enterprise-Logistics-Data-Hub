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
| **PySpark Master** | `spark-runner` | `8081`, `7077` | Spark Master Web UI (`http://localhost:8081`) |
| **PySpark Worker** | `spark-worker` | - | Spark Worker Node (Connected to Master `spark-runner:7077`) |

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

### 💡 Hướng Dẫn Thiết Lập Môi Trường Python Ảo (venv) Trên Máy Local

Nếu bạn muốn chạy các script `.py` (như script sinh dữ liệu) trực tiếp trên máy thay vì vào trong container, hãy thiết lập `venv` theo các bước sau (đảm bảo Docker đã chạy để kết nối qua port mapping):

```bash
# 1. Đứng tại thư mục gốc của dự án
cd ~/Projects/Enterprise-Logistics-Data-Hub

# 2. Tạo môi trường ảo có tên là "venv"
python3 -m venv venv

# 3. Kích hoạt venv (Linux/macOS)
source venv/bin/activate
# (Nếu dùng Windows PowerShell thì chạy: .\venv\Scripts\Activate.ps1)

# 4. Cài đặt thư viện
# - Cho các script sinh dữ liệu (Cluster 1):
pip install oracledb

# - Cho các script xử lý PySpark (Cluster 2):
pip install -r cluster-2-lakehouse-dwh/requirements.txt
```


---

### BƯỚC 1: Khởi Động Hạ Tầng Container (Cluster 1 & Cluster 2)

Mở PowerShell / Terminal tại máy của bạn:

```bash
# 1. Khởi chạy Cluster 1 (Oracle DB, Kafka, Debezium, Kafka UI)
cd cluster-1-ingestion
docker compose up -d

# 2. Khởi chạy Cluster 2 (MinIO S3, ClickHouse DWH, PySpark Master & Worker)
cd ../cluster-2-lakehouse-dwh
docker compose up -d
```

> 🔍 **Kiểm tra trạng thái**: Gõ `docker ps`. Tất cả 8 container phải ở trạng thái `Up` (hoặc `healthy`).
> Kiểm tra Spark Master Web UI tại `http://localhost:8081` phải hiển thị **Alive Workers: 1** (`spark-worker`).

---

### BƯỚC 1.5: Sinh Dữ Liệu Mẫu Về Oracle (Chạy 1 lần)

Trước khi thực hiện Initial Load hay CDC, bạn cần bơm dữ liệu mẫu vào cơ sở dữ liệu Oracle vừa khởi động:

```bash
# Đảm bảo venv đang được kích hoạt (như đã hướng dẫn ở phần trên)
# Đứng từ thư mục gốc của dự án:
cd ~/Projects/Enterprise-Logistics-Data-Hub

# Chạy script Python để sinh dữ liệu:
# (Mẹo: Mặc định sẽ tạo 10 triệu bản ghi mất khoảng 1-2 tiếng. Để test nhanh luồng, hãy thêm tham số --records 500000 để tạo 500 ngàn bản ghi trong ~3 phút)
python cluster-1-ingestion/scripts/generate_bulk_10m_data.py --records 500000
```

---

### BƯỚC 1.6: Tạo Bucket trên MinIO
Trước khi Spark có thể ghi dữ liệu, bạn cần tạo một kho chứa (bucket) trên MinIO.

**Cách 1: Dùng Giao diện Web (UI)**
1. Mở trình duyệt và truy cập vào [http://localhost:9001](http://localhost:9001)
2. Đăng nhập với Username: `minioadmin` và Password: `minioadminpassword`
3. Ở menu bên trái, chọn **Buckets** -> Nhấn **Create Bucket**
4. Nhập tên bucket là `logistics-lakehouse` và nhấn **Create Bucket**.

**Cách 2: Dùng lệnh Terminal (CLI - Thường được dùng ở các công ty lớn)**
Bạn có thể dùng công cụ `mc` (MinIO Client) để tạo bucket trực tiếp bên trong container:
```bash
docker exec minio_lakehouse mc alias set myminio http://localhost:9000 minioadmin minioadminpassword
docker exec minio_lakehouse mc mb myminio/logistics-lakehouse
```

### BƯỚC 2: PySpark Initial Bulk Load Bảng Lớn Từ Oracle Vào Silver Layer

Nạp dữ liệu ban đầu từ Oracle DB sang MinIO S3 Silver Layer bằng cách Submit Job lên cụm Spark Standalone Cluster (Dành cho Production / Airflow):

```bash
docker exec -it spark-runner spark-submit \
  --master spark://spark-runner:7077 \
  --executor-memory 2G \
  --executor-cores 2 \
  /opt/bitnami/spark/spark_jobs/oracle_bulk_initial_load.py
```
> 💡 *Truyền cờ `--master spark://spark-runner:7077` giúp Spark Master (`spark-runner`) ghi nhận job lên Web UI (`http://localhost:8081`) và điều phối cho Spark Worker (`spark-worker`) thực thi. Cờ `--executor-memory` và `--executor-cores` giúp tận dụng tối đa tài nguyên để tăng tốc.*

### BƯỚC 2.1: Kiểm Tra Dữ Liệu Bằng ClickHouse (DBeaver)
Bạn đã nạp xong dữ liệu thô vào tầng Silver (MinIO). Nhờ kiến trúc Lakehouse, ClickHouse có thể "đọc xuyên thấu" (Zero-copy) dữ liệu định dạng Delta Lake nằm trên MinIO mà không cần copy dữ liệu sang ổ cứng của ClickHouse!

**Thực hành Query trực tiếp bằng DBeaver:**
1. Mở DBeaver, kết nối vào ClickHouse (Port `8123`, User `default`, không pass).
2. Mở cửa sổ gõ SQL (SQL Editor) và chạy lệnh sau để đọc bảng `shipment_bookings` từ tầng Silver:

```sql
-- Đỉnh cao của Production: Không cần lộ mật khẩu!
-- Dùng Named Collection (minio_silver) đã được cấu hình sẵn trong ruột ClickHouse:
SELECT * 
FROM deltaLake(
    minio_silver, 
    url='http://minio-lakehouse:9000/logistics-lakehouse/silver/value_shipment_bookings/'
)
LIMIT 10;
```

*(Lưu ý: ClickHouse sẽ tải dữ liệu cực nhanh. Hàm `deltaLake` sẽ tự động đọc thư mục `_delta_log` để biết file Parquet nào là mới nhất).*

**Thực hành Query nhanh bằng Terminal (clickhouse-client):**
Nếu bạn đang ở màn hình Terminal và lười mở DBeaver, bạn có thể gọi thẳng client của ClickHouse để đếm số dòng:
```bash
docker exec -it clickhouse_dwh clickhouse-client -q "SELECT count() FROM deltaLake(minio_silver, url='http://minio-lakehouse:9000/logistics-lakehouse/silver/value_shipment_bookings/')"
```

---

### BƯỚC 2.2: Kiểm Tra Dữ Liệu Sau Khi Bulk Load (Bằng Spark)
Sau khi Spark chạy xong, bạn cần xác nhận dữ liệu đã được ghi đúng định dạng Delta Lake xuống MinIO.

**Cách 1: Kiểm tra cấu trúc thư mục bằng MinIO Client (CLI)**
Mở Terminal và gõ lệnh sau để xem dữ liệu đã được ghi vào đúng các thư mục `silver/value_...` và `silver/history_...` chưa:
```bash
docker exec minio_lakehouse mc ls myminio/logistics-lakehouse/silver/
```

**Cách 2: Đọc trực tiếp dữ liệu từ MinIO bằng Spark (Khuyên dùng)**
Do việc mở PySpark Shell cần phải truyền tay rất nhiều cấu hình (S3, Delta Lake), tôi đã chuẩn bị sẵn một script nhỏ để bạn kiểm tra cho lẹ.
Chỉ cần chạy lệnh sau:
```bash
docker exec -it spark-runner spark-submit /opt/bitnami/spark/spark_jobs/check_silver_data.py
```

---

### BƯỚC 3: Đăng Ký 2 Debezium Connectors (Dim & Fact) Bắt Luồng CDC

Chuyển sang thư mục `cluster-1-ingestion` để gửi REST API tới Debezium Connect:

```bash
cd cluster-1-ingestion

# 1. Đăng ký Connector Bảng Nhỏ (Snapshot Full + Incremental)
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium/register-dim-connector.json

# 2. Đăng ký Connector Bảng Lớn (Schema Only - Incremental Only)
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium/register-fact-connector.json
```

> 🔍 **Kiểm tra trạng thái**: Mở trình duyệt xem Kafka UI tại `http://localhost:8080` hoặc gõ:
> `curl -X GET http://localhost:8083/connectors/oracle-logistics-fact-connector/status`

---

### BƯỚC 4: Phát Sinh Giao Dịch Realtime Mới Vào Oracle DB (Simulator)

Mở một cửa sổ Terminal mới để chạy script phát sinh đơn hàng EMS giao dịch realtime:

#### 🔹 Cách A: Chạy trực tiếp trên máy Local
```bash
cd cluster-1-ingestion
pip install -r requirements.txt
python scripts/seed_realtime_events.py
```
> 📦 Script sẽ tạo liên tục các sự kiện `INSERT` đơn hàng mới và `UPDATE` trạng thái giao hàng trong Oracle DB. Debezium LogMiner sẽ lập tức bắt các sự kiện này và đẩy vào Kafka topic `cdc_logistics_oracle.DEBEZIUM.SHIPMENT_BOOKINGS`.

---

### BƯỚC 5: PySpark Streaming Đọc Kafka CDC Về Bronze Layer (MinIO Delta Lake)

Mở một cửa sổ Terminal khác để chạy PySpark Streaming job tiếp nhận dữ liệu CDC từ Kafka ghi vào Bronze Layer:

#### 🔹 Cách A: Chạy trực tiếp trên máy Local
```bash
cd cluster-2-lakehouse-dwh
pip install -r requirements.txt
python spark_jobs/kafka_to_delta.py
```

#### 🔹 Cách B: Submit Job lên cụm Spark Standalone Cluster
```bash
docker exec -it spark-runner spark-submit --master spark://spark-runner:7077 /opt/bitnami/spark/spark_jobs/kafka_to_delta.py
```

---

### BƯỚC 6: Thực Thi Medallion Batch Transformation (Bronze -> Silver Value & History)

Sau khi dữ liệu thô CDC đã tích tụ tại Bronze Layer, chạy script chuyển đổi Medallion để thực hiện `MERGE INTO` (Upsert / Delete / SCD Type 2):

#### 🔹 Cách A: Chạy trực tiếp trên máy Local
```bash
cd cluster-2-lakehouse-dwh
pip install -r requirements.txt
python spark_jobs/bronze_to_silver_medallion.py
```

#### 🔹 Cách B: Submit Job lên cụm Spark Standalone Cluster
```bash
docker exec -it spark-runner spark-submit --master spark://spark-runner:7077 /opt/bitnami/spark/spark_jobs/bronze_to_silver_medallion.py
```

---

### BƯỚC 7: Khởi Động Cluster 3 & Thực Thi dbt Transformations (dbt-clickhouse & Airflow)

#### 1. Khởi động Container Airflow + dbt-clickhouse (Cluster 3)
```bash
cd cluster-3-dbt-airflow
docker compose up -d --build
```

> 🔍 **Kiểm tra**: Mở Airflow Web UI tại `http://localhost:8085` (User: `admin` | Password: hiển thị trong log hoặc dùng command).

#### 2. Thử nghiệm chạy dbt trực tiếp từ Container `airflow_orchestrator`

Quá trình biến đổi dbt được chia thành **2 project riêng biệt**:
* **`dbt_lakehouse_spark` (Chạy trên cụm Spark)**: Đọc dữ liệu Silver từ MinIO nạp thành các bảng Star Schema và tạo sẵn bảng OBT trên Delta Lake S3.
* **`dbt_warehouse_clickhouse` (Chạy trên ClickHouse)**: Đọc file OBT Delta từ S3 nạp vào ClickHouse dưới dạng `ReplacingMergeTree` (bảng `obt_shipment_analytics_base`) và tạo Wrapper View `obt_shipment_analytics` siêu tối ưu và chống duplicate cho BI (Superset / Metabase).

```bash
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
```bash
# Vô giao diện dòng lệnh ClickHouse Client
docker exec -it clickhouse_dwh clickhouse-client

# Gõ các lệnh SQL kiểm tra:
SHOW TABLES;
SELECT count() FROM default.obt_shipment_analytics;
```

#### 🔹 Phương án C: Kiểm tra nhanh qua cURL (Terminal / PowerShell)
```bash
curl "http://localhost:8123/?query=SELECT+count()+FROM+default.obt_shipment_analytics"
```

---

## 📌 5. BỘ LỆNH QUẢN LÝ DEBEZIUM API THƯỜNG DÙNG

```bash
# 1. Xem danh sách tất cả Connector đang chạy
curl -X GET http://localhost:8083/connectors

# 2. Kiểm tra chi tiết trạng thái Fact Connector
curl -X GET http://localhost:8083/connectors/oracle-logistics-fact-connector/status

# 3. Xóa Connector (khi cần làm lại snapshot)
curl -X DELETE http://localhost:8083/connectors/oracle-logistics-fact-connector
```



### 3. Kiểm tra dữ liệu gốc trên Oracle DB bằng DBeaver
Bạn có thể kết nối công cụ DBeaver vào Oracle đang chạy trong Docker bằng thông số sau:
- **Host**: `localhost`
- **Port**: `1521`
- **Database/Service Name**: `FREEPDB1`
- **Username**: `c##dbzuser` (hoặc dùng tài khoản quản trị `sys`)
- **Password**: `dbz` (hoặc `top_secret` nếu đăng nhập bằng `sys`)
- **Role** (nếu dùng `sys`): chọn `SYSDBA`

> **Lưu ý**: Hãy tải driver **Oracle (ojdbc8)** trong DBeaver nếu phần mềm yêu cầu.
