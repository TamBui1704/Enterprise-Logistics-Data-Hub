# 🚀 Hướng Dẫn Kiểm Thử Thông Luồng Dữ Liệu End-to-End (Data Pipeline Guide)

Tài liệu hướng dẫn chi tiết quy trình khởi chạy và kiểm thử toàn bộ luồng dữ liệu từ **Oracle Database (OLTP Nguồn)** $\rightarrow$ **Debezium CDC** $\rightarrow$ **Apache Kafka** $\rightarrow$ **PySpark Structured Streaming** $\rightarrow$ **MinIO Delta Lakehouse**.

---

## 📐 1. Kiến Trúc & Cổng Dịch Vụ Hệ Thống (Ports Mapping)

| Dịch vụ | Tên Container | Cổng Host (Port) | Chức năng / Địa chỉ Web UI |
| :--- | :--- | :--- | :--- |
| **Oracle Database** | `source_oracle_db` | `1521`, `5500` | Oracle 23c XE (LogMiner CDC pre-configured) |
| **Apache Kafka** | `kafka_broker` | `9092` | Event Broker (KRaft Mode) |
| **Debezium Connect** | `debezium_cdc` | `8083` | REST API Đăng ký Connector |
| **Kafka UI** | `kafka_ui` | `8080` | Giao diện Web xem Topics & CDC Messages (`http://localhost:8080`) |
| **MinIO Storage** | `minio_lakehouse` | `9000`, `9001` | S3 Object Storage (`http://localhost:9001`) |
| **ClickHouse DWH** | `clickhouse_dwh` | `8123`, `9009` | Serving Data Warehouse OLAP |
| **PySpark Master** | `spark_runner` | `8081`, `7077` | Spark Cluster Web UI (`http://localhost:8081`) |

---

## 🧠 2. CHUYÊN SÂU: ĐỘNG CƠ DEBEZIUM CDC, SCN & MULTI-CONNECTOR OFFSETS

### 🔹 So sánh Kiến trúc CDC giữa SQL Server và Oracle Debezium LogMiner

| Tiêu chí | SQL Server CDC | Oracle Debezium LogMiner CDC |
| :--- | :--- | :--- |
| **Cơ chế đọc nhật ký** | SQL Server Agent đọc `sys.fn_dblog` / Transaction Log | Debezium LogMiner đọc Redo Log / Archive Log |
| **Chỉ số Checkpoint** | **LSN** (Log Sequence Number) | **SCN** (System Change Number) |
| **Nơi lưu Checkpoint** | Lưu trong bảng hệ thống `cdc.lsn_time_mapping` của DB | Lưu trong Kafka Topic đặc biệt: `my_connect_offsets` |
| **Đơn vị lưu Offset** | Theo từng bảng bật CDC | Theo từng **Connector Name** trong `my_connect_offsets` |

---

### 🔹 Tách 2 Connectors Độc Lập Cho Bảng Nhỏ (Dims) & Bảng Lớn (Facts)

Trên Production, để các bảng Fact hàng trăm triệu dòng không làm treo việc đọc các bảng Dim nhỏ, chúng ta tách làm **2 Connector đăng ký độc lập**:

1. **Connector Bảng Nhỏ (Dimensions)**: `debezium/register-dim-connector.json`
   - Cấu hình `"snapshot.mode": "initial"`. Debezium tự động đọc FULL table 1 giây đầu tiên, sau đó tự chốt SCN và stream biến động.
2. **Connector Bảng Lớn (Facts)**: `debezium/register-fact-connector.json`
   - Cấu hình `"snapshot.mode": "schema_only"`. Full Initial Load do **PySpark JDBC nạp thẳng vào Silver**, Debezium chỉ chốt mốc SCN $T_0$ và stream dữ liệu mới.

> 💡 **Tính Độc Lập Offsets**: Vì 2 connector có tên khác nhau (`oracle-logistics-dim-connector` và `oracle-logistics-fact-connector`), Kafka Connect sẽ lưu **2 OFFSET HOÀN TOÀN ĐỘC LẬP** trong `my_connect_offsets`. Bạn có thể dừng, sửa hay restart 1 connector mà không làm ảnh hưởng tới connector còn lại!

---

## 🏗️ 3. KIẾN TRÚC MEDALLION LAKEHOUSE & CHIẾN LƯỢC XỬ LÝ LỖI (REPLAY/REWIND)

```text
ORACLE DB ──► KAFKA ──► BRONZE LAYER (Append-Only CDC Payloads)
                               │
            ┌──────────────────┴──────────────────┐
            ▼                                     ▼
 SILVER VALUE (Current 1:1)             SILVER HISTORY (SCD Type 2)
 (MERGE INTO, Ingested_at)             (valid_from, valid_to, is_current)
```

### 🔹 Xử lý mã Thao Tác Debezium CDC (`op`) trong Spark MERGE INTO:
* `op = 'r'` (Read / Snapshot) & `op = 'c'` (Create) & `op = 'u'` (Update) $\rightarrow$ Thực hiện Upsert (`WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`).
* `op = 'd'` (Delete) $\rightarrow$ Thực hiện Soft Delete (`is_deleted = true`) hoặc Delete khỏi Silver Value.

### 🔹 Quy trình Xử lý khi 1 Bảng gặp lỗi (Rewind / Re-processing):
* **Nguyên tắc**: Tầng **Bronze Layer** lưu trữ **Immutable Append-Only Raw CDC**, đóng vai trò là "Bảo tàng Dữ liệu". 
* **Giải pháp**: Bạn **KHÔNG CẦN chạm vào Oracle DB hay làm phiền DBA**. Bạn chỉ cần lọc lại dữ liệu thô từ **Bronze Layer** theo mốc thời gian bị lỗi và chạy lại Job PySpark `bronze_to_silver_medallion.py` để tái thiết lập tầng Silver Value & Silver History!

---

## 🗂️ 4. Danh Sách 11 Bảng Theo Ma Trận Bus Matrix Logistics EMS

Tất cả 11 bảng dưới đây đã được khởi tạo sẵn trong Oracle DB (`PDB: ORCLPDB1`, `Schema: DEBEZIUM`):

* **Dimension Tables (Master Data)**: `dim_customers`, `dim_services`, `dim_pos_locations`, `dim_weight_tiers`, `dim_routing_types`, `dim_delivery_statuses`, `dim_failure_reasons`.
* **Fact Tables (Transactions & Events)**: `shipment_bookings`, `delivery_remunerations`, `return_remunerations`, `shipment_event_trackings`.

---

## 🏭 5. KỊCH BẢN PRODUCTION VỚI SPARK FULL LOAD & DEBEZIUM INCREMENTAL

### 🔹 BƯỚC A: Chạy PySpark Initial Bulk Load Bảng Lớn Vào Silver Layer

Mở PowerShell tại `cluster-2-lakehouse-dwh`:

```powershell
cd cluster-2-lakehouse-dwh

# Spark đọc trực tiếp từ Oracle qua JDBC và nạp thẳng vào MinIO Silver Value & History
python spark_jobs/oracle_bulk_initial_load.py
```

---

### 🔹 BƯỚC B: Đăng Ký 2 Debezium Connectors (Dim & Fact)

Chuyển sang PowerShell tại `cluster-1-ingestion`:

```powershell
cd cluster-1-ingestion

# 1. Đăng ký Connector Bảng Nhỏ (Snapshot Full + Incremental)
Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Post -ContentType "application/json" -InFile "debezium/register-dim-connector.json"

# 2. Đăng ký Connector Bảng Lớn (Schema Only - Incremental Only)
Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Post -ContentType "application/json" -InFile "debezium/register-fact-connector.json"
```

---

### 🔹 BƯỚC C: Phát Sinh Giao Dịch Realtime Mới Vào Oracle DB

```powershell
python scripts/seed_realtime_events.py
```

---

### 🔹 BƯỚC D: Chạy PySpark Streaming Tiếp Nận Luồng Incremental CDC Từ Kafka Vào Bronze

Mở PowerShell tại `cluster-2-lakehouse-dwh`:

```powershell
cd cluster-2-lakehouse-dwh

# PySpark đọc luồng Kafka CDC mới và Append vào Delta Lake Bronze Layer
python spark_jobs/kafka_to_delta.py
```

---

### 🔹 BƯỚC E: Biến Đổi Medallion (Bronze -> Silver Value 1:1 & Silver History SCD Type 2)

Mở PowerShell tại `cluster-2-lakehouse-dwh`:

```powershell
python spark_jobs/bronze_to_silver_medallion.py
```

---

## 📌 6. Bộ Lệnh Quản Lý Debezium API Thường Dùng

```powershell
# 1. Xem danh sách Connector
Invoke-RestMethod -Uri "http://localhost:8083/connectors"

# 2. Xem trạng thái các Connector
Invoke-RestMethod -Uri "http://localhost:8083/connectors/oracle-logistics-dim-connector/status"
Invoke-RestMethod -Uri "http://localhost:8083/connectors/oracle-logistics-fact-connector/status"

# 3. Xóa Connector
Invoke-RestMethod -Uri "http://localhost:8083/connectors/oracle-logistics-fact-connector" -Method Delete
```
