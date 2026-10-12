# 🚀 Hướng Dẫn Kiểm Thử Toàn Diện Luồng Dữ Liệu End-to-End (E2E Data Pipeline Guide)

Tài liệu chuẩn hóa toàn bộ quy trình thiết lập, sinh dữ liệu nguồn, dọn dẹp và kiểm thử thông luồng dữ liệu từ:
**Oracle Database (OLTP Nguồn)** → **Debezium CDC** → **Apache Kafka** → **PySpark Structured Streaming** → **MinIO S3 (Delta Lakehouse: Bronze/Silver)** → **ClickHouse DWH**.

---

## 📐 1. Kiến Trúc & Cổng Dịch Vụ Hệ Thống (Port Mapping)

| Dịch vụ | Tên Container | Cổng Host | Thông tin kết nối / Web UI |
| :--- | :--- | :--- | :--- |
| **Oracle Database** | `source_oracle_db` | `1521` | **Service:** `FREEPDB1`, **User:** `debezium`, **Pass:** `dbz` |
| **Apache Kafka** | `kafka_broker` | `9092` | Event Streaming Broker (KRaft Mode, 3 Partitions) |
| **Debezium Connect** | `debezium_cdc` | `8083` | Kafka Connect Distributed (REST API quản lý Connectors) |
| **Kafka UI** | `kafka_ui` | `8080` | Giao diện Web xem Topics & CDC Messages (`http://localhost:8080`) |
| **MinIO Storage** | `minio_lakehouse` | `9000`, `9001` | S3 API: `9000` | Console Web UI: `http://localhost:9001` (`minioadmin`/`minioadminpassword`) |
| **ClickHouse DWH** | `clickhouse_dwh` | `8123`, `9009` | HTTP Interface: `8123` | Native TCP: `9009` |
| **PySpark Master** | `spark-runner` | `8081`, `7077` | Master Web UI (`http://localhost:8081`) | Cluster URL: `spark://spark-runner:7077` |
| **PySpark Worker** | `spark-worker` | - | Spark Worker Cluster Node |

---

## 🧠 2. CHIẾN LƯỢC NẠP DỮ LIỆU ĐỈNH CAO (ENTERPRISE PATTERN)

```text
               ┌─────────────────────── BẢNG NHỎ (DIMS) ────────────────────────┐
               ▼ (Debezium snapshot.mode: initial)                             │
ORACLE DB ────► DEBEZIUM CDC ──► KAFKA ──► BRONZE LAYER (Append-Only CDC)      │
    │          ▲ (Debezium snapshot.mode: schema_only)      │                  ▼
    │          └─────────────── BẢNG LỚN (FACTS) ───────────┘          CLICKHOUSE DWH
    │                                                       │        (Zero-Copy Query)
    ▼                                                       ▼                 ▲
[PYSPARK BULK LOAD JDBC] ──────────────────────────► SILVER VALUE & HISTORY ──┘
 (Baseline Historical Data)                        (MERGE INTO / SCD Type 2)
```

1. **Bảng Danh mục (Dimensions)**: Kích thước nhỏ → Dùng Debezium `snapshot.mode: initial` snapshot toàn bộ vào Kafka ngay từ đầu.
2. **Bảng Giao dịch (Facts)**: Kích thước lớn → Dùng **PySpark Bulk Load** đọc trực tiếp qua JDBC nạp Baseline vào Silver Layer (tránh nghẽn Kafka). Sau đó bật Debezium `snapshot.mode: schema_only` để chỉ bắt biến động mới (Incremental CDC).
3. **Tầng Bronze (Delta Lake)**: Lưu Append-Only toàn bộ sự kiện CDC từ Kafka kèm metadata và timestamp `ingested_at`.
4. **Tầng Silver (Delta Lake)**: 
   - `silver/value_<table_name>`: Bản ghi hiện tại 1:1 (Merge/Upsert).
   - `silver/history_<table_name>`: Lịch sử biến đổi SCD Type 2 (`valid_from`, `valid_to`, `is_current`).

---

## 🏭 3. QUY TRÌNH KIỂM THỬ THÔNG LUỒNG TỪNG BƯỚC (STEP-BY-STEP)

### BƯỚC 0: Dọn dẹp & Reset môi trường để chạy lại từ đầu (Clean Start)

Nếu bạn vừa thử nghiệm và muốn dọn dẹp sạch sẽ toàn bộ connector và dữ liệu lakehouse để chạy lại từ đầu:

```bash
# 1. Xóa các Debezium Connectors cũ
curl -X DELETE http://localhost:8083/connectors/oracle-logistics-fact-connector
curl -X DELETE http://localhost:8083/connectors/oracle-logistics-dim-connector
curl -X DELETE http://localhost:8083/connectors/oracle-logistics-fact-connector-v4
curl -X DELETE http://localhost:8083/connectors/oracle-logistics-dim-connector-v2

# 2. Xóa sạch dữ liệu cũ trong MinIO Bucket (để nạp mới)
docker exec minio_lakehouse mc alias set myminio http://localhost:9000 minioadmin minioadminpassword
docker exec minio_lakehouse mc mb myminio/logistics-lakehouse --ignore-existing
docker exec minio_lakehouse mc rm --recursive --force myminio/logistics-lakehouse/bronze/
docker exec minio_lakehouse mc rm --recursive --force myminio/logistics-lakehouse/silver/
docker exec minio_lakehouse mc rm --recursive --force myminio/logistics-lakehouse/checkpoints/
docker exec minio_lakehouse mc rm --recursive --force myminio/logistics-lakehouse/control/
```

> 💡 **Lưu ý**: Bạn **KHÔNG CẦN** xóa hoặc dựng lại container Oracle Database! Nếu muốn làm mới dữ liệu nguồn Oracle, hãy chạy script sinh dữ liệu ở **BƯỚC 2**, script sẽ tự động làm mới các bảng.

---

### BƯỚC 1: Đảm bảo Hạ Tầng Container (Cluster 1 & Cluster 2) Đang Chạy

Kiểm tra trạng thái các container:

```bash
docker ps --format "table {{.Names}}	{{.Status}}	{{.Ports}}"
```
*Đảm bảo các container sau đang chạy và healthy:*
- Cluster 1: `source_oracle_db` (healthy), `kafka_broker` (healthy), `debezium_cdc`, `kafka_ui`
- Cluster 2: `minio_lakehouse`, `clickhouse_dwh`, `spark-runner`, `spark-worker`

Nếu có container chưa chạy, hãy khởi động (đảm bảo đứng ở thư mục gốc dự án):
```bash
# Cluster 1 (Ingestion & Source)
cd cluster-1-ingestion && docker compose up -d && cd ..

# Cluster 2 (Lakehouse & DWH)
cd cluster-2-lakehouse-dwh && docker compose up -d && cd ..
```

#### ⚠️ Lưu ý bắt buộc: Khởi tạo Bucket `logistics-lakehouse` trên MinIO
Khi bạn vừa dựng mới cụm hoặc sau khi xóa trắng thư mục volume `/data/minio_data`, MinIO sẽ chưa có bucket. Bạn **bắt buộc phải tạo bucket `logistics-lakehouse`** trước khi chạy Spark (để tránh lỗi `NoSuchBucket - 404`):

```bash
# Tạo alias và tạo bucket (tự động bỏ qua nếu đã tồn tại)
docker exec minio_lakehouse mc alias set myminio http://localhost:9000 minioadmin minioadminpassword
docker exec minio_lakehouse mc mb myminio/logistics-lakehouse --ignore-existing
```
*(Hoặc vào giao diện Web UI tại `http://localhost:9001` với user `minioadmin` / `minioadminpassword` -> mục **Buckets** -> nhấn **Create Bucket** -> nhập `logistics-lakehouse`).*

---

### BƯỚC 2: Sinh Dữ Liệu Nguồn Ban Đầu Vào Oracle DB (Initial Data Generation)

Để kiểm thử quy trình Initial Bulk Load và CDC, Oracle Database cần có sẵn tập dữ liệu ban đầu gồm các bảng Danh mục (Dimensions) và bảng Giao dịch (Facts: đơn hàng, lịch sử bưu gửi, thù lao phát/hoàn).

Dự án cung cấp sẵn script `generate_bulk_10m_data.py` sử dụng thư viện Python `oracledb` chạy đa tiến trình (multiprocessing):

```bash
# 1. Tại thư mục gốc dự án, kích hoạt môi trường ảo (tạo venv nếu chưa có)
# python3 -m venv venv
source venv/bin/activate

# 2. Cài đặt thư viện nếu chưa có
pip install -r cluster-1-ingestion/requirements.txt

# 3. Chạy script sinh dữ liệu (Tùy chọn quy mô dữ liệu):

# 🚀 Tùy chọn A (Khuyến nghị cho Test nhanh / máy cá nhân): Sinh 50,000 đơn hàng (~15-20s)
python cluster-1-ingestion/scripts/generate_bulk_10m_data.py --records 50000 --batch 10000 --workers 2

# 🚀 Tùy chọn B (Kiểm thử tải lớn hơn): Sinh 500,000 hoặc 1,000,000 đơn hàng
# python cluster-1-ingestion/scripts/generate_bulk_10m_data.py --records 500000 --batch 25000 --workers 4
```

🔍 **Cơ chế script thực thi**:
1. **Khởi tạo Master Dimensions**: Tự động reset và nạp mới 200 khách hàng (`DIM_CUSTOMERS`), 200 bưu cục bưu điện (`DIM_POS_LOCATIONS`), cùng các dịch vụ EMS (`DIM_SERVICES`), nấc cân nặng (`DIM_WEIGHT_TIERS`), hình thức tuyến (`DIM_ROUTING_TYPES`),...
2. **Sinh Giao dịch Facts**: Sinh song song các bản ghi vận chuyển (`SHIPMENT_BOOKINGS`), các mốc tracking sự kiện bưu gửi (`SHIPMENT_EVENT_TRACKINGS`), thù lao phát (`DELIVERY_REMUNERATIONS`) và thù lao chuyển hoàn (`RETURN_REMUNERATIONS`).

---

### BƯỚC 3: Kết Nối DBeaver Để Kiểm Tra Dữ Liệu Nguồn (Oracle Verification)

DBeaver là công cụ GUI trực quan giúp kiểm tra dữ liệu trong Oracle DB trước khi đưa vào pipeline.

#### 1. Các bước thiết lập kết nối trong DBeaver:
1. Mở DBeaver → Chọn menu **Database** → **New Database Connection**.
2. Chọn loại CSDL: **Oracle** → Nhấn **Next**.
3. Tại tab **Main**, điền thông số chính xác như sau:
   - **Connect by**: Chọn `Service Name` *(⚠️ Cực kỳ quan trọng: KHÔNG chọn `SID` vì Oracle 23c Free sử dụng Pluggable Database `FREEPDB1`)*.
   - **Host**: `localhost` (hoặc IP máy tính của bạn)
   - **Port**: `1521`
   - **Database**: `FREEPDB1`
   - **Username**: `debezium`
   - **Password**: `dbz`
4. Nhấn nút **Test Connection ...**:
   - Nếu DBeaver chưa có driver, hộp thoại sẽ hiển thị gợi ý tải *Oracle Database JDBC Driver* → Chọn **Download**.
   - Khi hiện thông báo `Connected - Oracle Database 23ai Free ...` màu xanh là kết nối thành công.
5. Nhấn **Finish** để hoàn tất.

> 💡 **Mẹo**: Nếu muốn kết nối với user quản trị Container Database (CDB→ROOT), chọn Service Name là `FREE` (hoặc `ORCLCDB`), Username `sys`, Password `top_secret`, và chọn Role là `SYSDBA`.

#### 2. Câu lệnh SQL kiểm tra dữ liệu nguồn:
Mở một SQL Editor trong DBeaver trên kết nối vừa tạo và chạy các truy vấn sau:

```sql
-- 1. Kiểm tra số lượng bản ghi các bảng trong schema DEBEZIUM
SELECT 'DIM_CUSTOMERS' AS table_name, COUNT(*) AS record_count FROM DEBEZIUM.DIM_CUSTOMERS
UNION ALL
SELECT 'DIM_POS_LOCATIONS', COUNT(*) FROM DEBEZIUM.DIM_POS_LOCATIONS
UNION ALL
SELECT 'SHIPMENT_BOOKINGS', COUNT(*) FROM DEBEZIUM.SHIPMENT_BOOKINGS
UNION ALL
SELECT 'SHIPMENT_EVENT_TRACKINGS', COUNT(*) FROM DEBEZIUM.SHIPMENT_EVENT_TRACKINGS
UNION ALL
SELECT 'DELIVERY_REMUNERATIONS', COUNT(*) FROM DEBEZIUM.DELIVERY_REMUNERATIONS
UNION ALL
SELECT 'RETURN_REMUNERATIONS', COUNT(*) FROM DEBEZIUM.RETURN_REMUNERATIONS;

-- 2. Xem 10 đơn hàng bưu gửi EMS mới nhất
SELECT BOOKING_ID, ITEM_CODE, BOOKING_DATE, CUSTOMER_ID, SERVICE_ID, 
       SENDING_POS_CODE, RECEIVING_POS_CODE, WEIGHT_GRAM, TOTAL_REVENUE, STATUS_ID 
FROM DEBEZIUM.SHIPMENT_BOOKINGS 
ORDER BY BOOKING_DATE DESC 
FETCH FIRST 10 ROWS ONLY;

-- 3. Xem các mốc trạng thái hành trình của 1 bưu gửi
SELECT EVENT_ID, BOOKING_ID, STATUS_ID, EVENT_TIME, EVENT_POS_CODE, OPERATOR_USER, NOTES 
FROM DEBEZIUM.SHIPMENT_EVENT_TRACKINGS 
ORDER BY EVENT_TIME ASC 
FETCH FIRST 10 ROWS ONLY;
```

---

### BƯỚC 4: PySpark Batch Initial Bulk Load Nạp Bảng Lớn Vào Silver Layer

> 💡 **Tiền điều kiện**: Đảm bảo bucket `logistics-lakehouse` đã được tạo trên MinIO ở BƯỚC 1 (`docker exec minio_lakehouse mc mb myminio/logistics-lakehouse --ignore-existing`). Nếu bucket chưa tồn tại, Spark sẽ báo lỗi `NoSuchBucket`.

Chạy job Spark nạp toàn bộ dữ liệu lịch sử từ Oracle DB qua JDBC vào MinIO Silver Layer:

```bash
docker exec -it spark-runner spark-submit   --master spark://spark-runner:7077   --executor-memory 1024M --driver-memory 768M   --executor-cores 2   /opt/bitnami/spark/spark_jobs/oracle_bulk_initial_load.py
```

🔍 **Kiểm tra kết quả**:
Sau khi chạy xong, kiểm tra dữ liệu tầng Silver đã xuất hiện trên MinIO:
```bash
docker exec minio_lakehouse mc ls myminio/logistics-lakehouse/silver/
```
*(Bạn sẽ thấy các thư mục `value_shipment_bookings`, `history_shipment_bookings`,...)*

---

### BƯỚC 5: Đăng Ký 2 Debezium Connectors Độc Lập Chạy Song Song Ổn Định

Cấu hình 2 Connector theo chuẩn Production độc lập (Cách B) để chống triệt để lỗi `ORA-01368` và xung đột JMX MBean:
- **Dim Connector (`oracle-logistics-dim-connector`)**: Snapshot danh mục (`snapshot.mode: initial`), dùng `topic.prefix: cdc_dim` + SMT RegexRouter để định tuyến về topic `cdc_logistics_oracle.*` (tránh đụng JMX metrics) và kích hoạt Heartbeat ID 1 để giữ SCN luôn cập nhật theo thời gian thực.
- **Fact Connector (`oracle-logistics-fact-connector`)**: Chốt mốc SCN và stream dữ liệu mới (`snapshot.mode: schema_only`), dùng `topic.prefix: cdc_logistics_oracle` và kích hoạt Heartbeat ID 2.

```bash
cd cluster-1-ingestion

# 1. Đăng ký Dim Connector (Snapshot Bảng Danh Mục)
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium/register-dim-connector.json

# 2. Đăng ký Fact Connector (Incremental Bảng Giao Dịch)
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium/register-fact-connector.json
```

🔍 **Kiểm tra trạng thái**:
```bash
curl -s http://localhost:8083/connectors?expand=status | jq .
```
*Cả 2 connector và tasks đều phải hiển thị trạng thái `"state": "RUNNING"`.*

---

### BƯỚC 6: Kích Hoạt PySpark Streaming Đọc Kafka CDC Ghi Vào Bronze Layer (Zero-Touch Ingestion)

Khởi chạy PySpark Streaming để lắng nghe toàn bộ các bảng CDC từ Kafka (thông qua Wildcard Regex) và ghi Append-Only vào thư mục gộp trên MinIO Delta Lake (Partition tự động theo tên Topic). Bạn có thể lựa chọn 1 trong 2 chế độ chạy:

#### Lựa chọn A: Chế độ Real-time Streaming liên tục 24/7 (Continuous Streaming)
Lắng nghe liên tục trong thời gian thực, có message mới là nạp ngay vào Delta Lake:
```bash
docker exec -it spark-runner spark-submit \
  --master spark://spark-runner:7077 \
  /opt/bitnami/spark/spark_jobs/kafka_to_delta.py
```
> 💡 *Job này giữ terminal mở liên tục để đón dữ liệu real-time. Bạn hãy để cửa sổ Terminal này mở.*

#### Lựa chọn B: Chế độ Batch AvailableNow `--once` (Khuyên dùng khi Test hoặc Tiết kiệm RAM)
Kéo toàn bộ dữ liệu mới tích tụ từ mốc checkpoint lần trước đến nay, ghi xong vào Bronze rồi **tự động thoát hoàn toàn để giải phóng RAM/CPU**:
```bash
docker exec -it spark-runner spark-submit \
  --master spark://spark-runner:7077 \
  /opt/bitnami/spark/spark_jobs/kafka_to_delta.py --once
```
> 💡 *Nhờ tính năng `Trigger.AvailableNow` và cơ chế Checkpoint thông minh của Spark, job sẽ tự nhớ offset đã đọc, không bao giờ đọc trùng lặp hay sót dữ liệu. Rất lý tưởng khi chạy thử nghiệm trên máy local hoặc lên lịch định kỳ theo mẻ (Micro-batch) qua Airflow/Cron.*

🔍 **Kiểm tra dữ liệu tầng Bronze từ ClickHouse (Zero-Copy Query)**:
Sau khi Spark nạp dữ liệu vào Delta Lake, bạn có thể kiểm tra ngay tầng Bronze trực tiếp từ ClickHouse mà không cần copy dữ liệu:

```bash
# 1. Thống kê số lượng bản ghi CDC đã nạp vào Bronze theo từng bảng nguồn:
docker exec -it clickhouse_dwh clickhouse-client -q \
  "SELECT extract(_path, 'kafka_topic=([^/]+)') AS table_name, count() AS total_records, min(event_time) AS min_event_time, max(event_time) AS max_event_time FROM deltaLake(minio_bronze) GROUP BY table_name ORDER BY total_records DESC FORMAT PrettyCompact"

# 2. Xem 5 bản ghi CDC Raw mới nhất (kèm op c/u/d, offset, partition và JSON payload):
docker exec -it clickhouse_dwh clickhouse-client -q \
  "SELECT extract(_path, 'kafka_topic=([^/]+)') AS table_name, op, kafka_offset, event_time, substring(raw_value, 1, 100) AS raw_sample FROM deltaLake(minio_bronze) ORDER BY kafka_offset DESC LIMIT 5 FORMAT PrettyCompact"

# 3. Lọc riêng dữ liệu bảng SHIPMENT_BOOKINGS tầng Bronze:
docker exec -it clickhouse_dwh clickhouse-client -q \
  "SELECT op, kafka_partition, kafka_offset, event_time, substring(raw_value, 1, 120) AS raw_preview FROM deltaLake(minio_bronze) WHERE _path LIKE '%SHIPMENT_BOOKINGS%' ORDER BY kafka_offset DESC LIMIT 5 FORMAT PrettyCompact"
```
> 💡 **Lưu ý khi chạy trên Terminal vs DBeaver**:
> - Trên **Terminal (`clickhouse-client`)**: Giữ `FORMAT PrettyCompact` để terminal in ra bảng ASCII căn lề đẹp mắt.
> - Trên **DBeaver (hoặc GUI)**: **BỎ** phần `FORMAT PrettyCompact` ở cuối câu truy vấn. Driver JDBC của DBeaver tự quản lý format để render dữ liệu lên lưới (Grid). Nếu để `FORMAT PrettyCompact`, DBeaver sẽ báo lỗi: `Only RowBinaryWithNameAndTypes and JSONEachRow are supported for output format`.

---

### BƯỚC 7: Phát Sinh Giao Dịch Realtime Mới Vào Oracle DB (Simulator)

Mở một cửa sổ Terminal mới để chạy script phát sinh đơn hàng EMS:

```bash
# Tại thư mục gốc dự án:
source venv/bin/activate
cd cluster-1-ingestion
python scripts/seed_realtime_events.py
```

🔍 **Quan sát luồng dữ liệu thời gian thực**:
1. Terminal script in ra các icon `📦 [INSERT BOOKING]` và `🔄 [UPDATE BOOKING]`.
2. Mở trình duyệt vào **Kafka UI**: `http://localhost:8080` → Vào topic `cdc_logistics_oracle.DEBEZIUM.SHIPMENT_BOOKINGS` → Số lượng **Message Count nhảy tăng liên tục**.
3. Terminal Spark Streaming (Bước 6) liên tục commit các micro-batches ghi vào thư mục `bronze/all_tables/` (được tự động phân vùng theo partition `kafka_topic`) trên MinIO.

---

### BƯỚC 8: Thực Thi Medallion Batch Transformation (Bronze -> Silver Value & History)

Sau khi dữ liệu CDC tích tụ ở tầng Bronze, chạy job biến đổi linh động (Dynamic Metadata-driven, Tự động hiểu Schema, Idempotent MERGE SCD Type 2 & Cách ly DLQ).

#### 1. Lệnh thực thi cơ bản (Tự động đọc Watermark Cutoff):
```bash
# Xử lý bảng SHIPMENT_BOOKINGS (Fact)
docker exec -it spark-runner spark-submit   --master spark://spark-runner:7077   /opt/bitnami/spark/spark_jobs/bronze_to_silver_medallion.py --table SHIPMENT_BOOKINGS

# Xử lý bảng DIM_CUSTOMERS (Dimension)
docker exec -it spark-runner spark-submit   --master spark://spark-runner:7077   /opt/bitnami/spark/spark_jobs/bronze_to_silver_medallion.py --table DIM_CUSTOMERS

# Hoặc xử lý TẤT CẢ các bảng trong 1 lượt chạy:
docker exec -it spark-runner spark-submit   --master spark://spark-runner:7077   /opt/bitnami/spark/spark_jobs/bronze_to_silver_medallion.py --table ALL
```

#### 2. Điều khiển khung thời gian xử lý (Cutoff & Watermark Controls tương tự SSIS):
Bạn có thể chủ động chỉ định điểm bắt đầu và kết thúc:
```bash
# Chỉ định mốc bắt đầu và kết thúc cụ thể:
docker exec -it spark-runner spark-submit   --master spark://spark-runner:7077   /opt/bitnami/spark/spark_jobs/bronze_to_silver_medallion.py   --table SHIPMENT_BOOKINGS   --from-ts "2026-10-08 00:00:00"   --to-ts "2026-10-08 23:59:59"

# Quét lại toàn bộ Bronze từ mốc khởi thủy (Idempotent MERGE không mất Baseline Bulk Load):
docker exec -it spark-runner spark-submit \
  --master spark://spark-runner:7077 \
  /opt/bitnami/spark/spark_jobs/bronze_to_silver_medallion.py \
  --table SHIPMENT_BOOKINGS \
  --from-ts "1970-01-01 00:00:00"

# Cờ Full Refresh (chạy lại và ghi đè từ đầu, khuyên dùng cho Dimensions):
docker exec -it spark-runner spark-submit \
  --master spark://spark-runner:7077 \
  /opt/bitnami/spark/spark_jobs/bronze_to_silver_medallion.py \
  --table DIM_CUSTOMERS --full-refresh

> 💡 **Lưu ý quan trọng về cờ `--full-refresh`**:
> - Đối với các bảng **Dimensions**: Cả dữ liệu ban đầu và CDC đều nằm trọn vẹn trong Bronze, cờ `--full-refresh` sẽ tính toán lại chuỗi SCD Type 2 từ đầu rất tốt.
> - Đối với các bảng **Facts**: Do 50,000 bản ghi lịch sử ban đầu được nạp bằng **Bulk Load JDBC (Bước 4)**, bạn nên chạy mặc định (không cờ) hoặc dùng `--from-ts "1970-01-01 00:00:00"` để script tự động hòa trộn (MERGE) các sự kiện CDC mà không ghi đè mất Baseline lịch sử.
```

#### 3. Các tính năng chuyên nghiệp tự động:
- **Tự hiểu Schema động**: Tự động parse cấu trúc JSON từ CDC payload của từng bảng, tự động tiến hóa schema (`mergeSchema = true`) khi bảng nguồn thêm cột.
- **Idempotency tuyệt đối**: Dù bạn chạy lại script bao nhiêu lần, dữ liệu trên `silver/value_[table]` (1:1 Snapshot) và `silver/history_[table]` (SCD Type 2) **hoàn toàn không bị trùng lặp (Zero Duplicates)** nhờ cơ chế Anti-Join trên `(PK, valid_from)`.
- **Bảo vệ luồng bằng Dead Letter Queue (DLQ)**: Bản ghi lỗi schema, lỗi parse hoặc thiếu Primary Key được tự động đẩy vào `s3a://logistics-lakehouse/quarantine/dlq_[table]` kèm lý do lỗi, không bao giờ làm crash luồng.
- **Bảng Watermark & Lineage Audit**: 
  - `control/cutoff_watermarks`: Quản lý mốc High Watermark của từng bảng.
  - `control/etl_batch_control`: Lưu vết từng Batch ID, thời gian chạy, số bản ghi xử lý, bản ghi DLQ, trạng thái.

---

### BƯỚC 9: Kiểm Tra & Nghiệm Thu Dữ Liệu (Reconciliation & Zero-Copy Query)

#### 1. Kiểm tra & Đối soát tự động 11 bảng bằng Spark Audit Script:
Chạy script đối soát tự động để kiểm tra số lượng bản ghi của toàn bộ 11 bảng (7 Dimensions + 4 Facts), đảm bảo dữ liệu tầng Silver Value khớp 1:1 với nguồn Oracle và History SCD Type 2 phản ánh chuẩn xác:
```bash
docker exec -it spark-runner python3 /opt/bitnami/spark/spark_jobs/check_silver_data.py
```
*(Bảng ma trận kết quả sẽ hiển thị cột `VALUE COUNT` và `HIST ACTIVE (1)` đạt trạng thái `✅ KHỚP 100%` trên toàn bộ 11 bảng).*

#### 2. Truy vấn dữ liệu tầng Bronze (Raw Ingestion) bằng ClickHouse:
Thống kê đối soát toàn bộ bản ghi raw CDC đã lưu trữ trên Delta Lake Bronze trước khi chạy qua ETL Silver:

```bash
# Thống kê phân bổ dữ liệu Bronze theo từng Topic / Bảng:
docker exec -it clickhouse_dwh clickhouse-client -q \
  "SELECT extract(_path, 'kafka_topic=([^/]+)') AS table_name, count() AS total_records FROM deltaLake(minio_bronze) GROUP BY table_name ORDER BY total_records DESC FORMAT PrettyCompact"
```

#### 3. Truy vấn trực tiếp dữ liệu tầng Silver bằng ClickHouse (Zero-Copy Query):
Truy vấn trực tiếp dữ liệu tầng Silver trên MinIO S3 thông qua ClickHouse mà không cần copy dữ liệu:

```bash
# 1. Đếm tổng số đơn hàng hiện tại trong Silver Value
docker exec -it clickhouse_dwh clickhouse-client -q   "SELECT count() FROM deltaLake(minio_silver, url='http://minio-lakehouse:9000/logistics-lakehouse/silver/value_shipment_bookings/')"

# 2. Xem 5 đơn hàng mới nhất vừa được CDC cập nhật
docker exec -it clickhouse_dwh clickhouse-client -q   "SELECT BOOKING_ID, ITEM_CODE, TOTAL_REVENUE, STATUS_ID, ingested_at FROM deltaLake(minio_silver, url='http://minio-lakehouse:9000/logistics-lakehouse/silver/value_shipment_bookings/') ORDER BY ingested_at DESC LIMIT 5 FORMAT PrettyCompact"

# 3. Kiểm tra bảng lưu vết lịch sử SCD Type 2
docker exec -it clickhouse_dwh clickhouse-client -q   "SELECT BOOKING_ID, STATUS_ID, is_current, valid_from, valid_to FROM deltaLake(minio_silver, url='http://minio-lakehouse:9000/logistics-lakehouse/silver/history_shipment_bookings/') WHERE is_current = 0 LIMIT 5 FORMAT PrettyCompact"
```

---

### BƯỚC 10: Vận Hành & Thông Luồng Cụm 3 (Airflow Orchestrator + dbt Transform Mesh qua SSHOperator)

Cụm 3 đóng vai trò điều phối (Data Orchestration) và chuyển đổi dữ liệu phân tích (Transformations & Modeling) chuẩn kiến trúc Production:
- **Kiến trúc An toàn Production**: Airflow cách ly hoàn toàn môi trường, **không mount `docker.sock`**. Mọi Spark Job nặng được điều phối từ xa qua **`SSHOperator`** tới cụm Spark (`spark_ssh_conn`: `spark-runner:22`).
- **Giám sát thời gian thực**: Toàn bộ `stdout` / `stderr` từ `spark-submit` được stream trực tiếp về Airflow Task Logs. Đồng thời Application ID hiển thị rõ ràng để theo dõi trên Spark Master UI (`http://localhost:8081`).
- **Tầng Lakehouse (`dbt-spark`)**: Đọc từ MinIO Silver (`silver/value_*`) → Pre-join OBT (One Big Table) và làm sạch → Ghi vào MinIO Gold Delta (`gold/datamart/obt_shipment_analytics/`). Spark Thrift Server (cổng 10000) tự động chạy ở chế độ local engine với cấu hình Delta Lake & S3, bảo toàn 100% cores cho cụm Worker.
- **Tầng Data Warehouse (`dbt-clickhouse`)**: Nạp gia tăng (Incremental) từ S3 Gold vào bảng ClickHouse OLAP siêu tốc `default.obt_shipment_analytics` qua bảng đệm `ReplacingMergeTree` → Sẵn sàng cho Cube.js & Apache Superset phục vụ BI Dashboard.

#### 1. Khởi động Cụm 3 (Airflow PostgreSQL Metadata + Airflow Orchestrator):
> [!NOTE]
> Spark HiveThriftServer2 (cổng 10000) và OpenSSH Daemon (cổng 22) đã được tích hợp tự động 100% trong `cluster-2-lakehouse-dwh/entrypoint.sh`. Bạn **không cần phải chạy lệnh `docker exec` thủ công nào để bật Thrift Server**.

```bash
# 1. Đảm bảo Cụm 2 đang hoạt động (Spark Master, Worker, MinIO, ClickHouse):
cd cluster-2-lakehouse-dwh && docker compose up -d && cd ..

# 2. Khởi chạy Cụm 3 (PostgreSQL 15 Metadata DB + Airflow Orchestrator):
cd cluster-3-dbt-airflow && docker compose up -d && cd ..
```
* **Web UI Airflow**: `http://localhost:8085`
* **Tài khoản cố định tự động**: `admin` / `admin` *(tự động khởi tạo cố định khi container khởi chạy, không bao giờ bị mất)*.
* **Tự động cấu hình SSH Connection**: `spark_ssh_conn` (`ssh://spark:spark@spark-runner:22`) được khởi tạo tự động trong metadata Airflow.

#### 2. Kiểm tra thông suốt dịch vụ (Health Check):
```bash
# 1. Kiểm tra kết nối SSH từ Airflow sang Spark:
docker exec -it airflow_orchestrator python -c "from airflow.providers.ssh.hooks.ssh import SSHHook; hook = SSHHook(ssh_conn_id='spark_ssh_conn'); client = hook.get_conn(); print('✅ SSH Connected to:', client.get_transport().getpeername())"

# 2. Kiểm thử thông suốt kết nối dbt Spark (cổng 10000):
docker exec -it airflow_orchestrator dbt debug --project-dir /opt/airflow/dbt_lakehouse_spark --profiles-dir /opt/airflow/dbt_lakehouse_spark

# 3. Kiểm thử thông suốt kết nối dbt ClickHouse (cổng 8123):
docker exec -it airflow_orchestrator dbt debug --project-dir /opt/airflow/dbt_warehouse_clickhouse --profiles-dir /opt/airflow/dbt_warehouse_clickhouse
```
*(Tất cả lệnh đều trả về `All checks passed!` và `Connection test: [OK connection ok]`).*

#### 3. Kích hoạt Pipeline End-to-End:

##### Lựa chọn A: Kích hoạt tự động qua Airflow Web UI:
1. Mở trình duyệt vào `http://localhost:8085` → Đăng nhập với tài khoản `admin` / `admin`.
2. Bật toggle **`ON`** cho DAG: **`e2e_logistics_data_pipeline`**.
3. **Lưu ý quan trọng khi Cụm 1 (Oracle) đang tắt để tiết kiệm RAM**:
   * Nhấn chuột vào task đầu tiên `spark_oracle_bulk_initial_load` → Chọn **Mark State as Success** (để bỏ qua do dữ liệu lịch sử đã nạp ở Bước 4).
   * Airflow sẽ tự động điều phối chạy tiếp các task trong chuỗi:
     `spark_bronze_to_silver_medallion` (SSH) → `dbt_spark_lakehouse_run` → `dbt_clickhouse_warehouse_run` → `dbt_clickhouse_test`.
   * Click vào task bất kỳ và chọn **Log** để xem tiến độ streaming theo thời gian thực.

##### Lựa chọn B: Thực thi kiểm thử từng lệnh qua CLI (Nghiệm thu độc lập):
```bash
# 1. Chạy Medallion Bronze -> Silver qua SSHOperator:
docker exec -it airflow_orchestrator airflow tasks test e2e_logistics_data_pipeline spark_bronze_to_silver_medallion 2026-01-01

# 2. Biến đổi dữ liệu Silver sang Gold OBT bằng dbt Spark (qua ThriftServer 10000):
docker exec -it airflow_orchestrator dbt run --project-dir /opt/airflow/dbt_lakehouse_spark --profiles-dir /opt/airflow/dbt_lakehouse_spark

# 3. Nạp dữ liệu Gold OBT vào ClickHouse DWH bằng dbt ClickHouse:
docker exec -it airflow_orchestrator dbt run --project-dir /opt/airflow/dbt_warehouse_clickhouse --profiles-dir /opt/airflow/dbt_warehouse_clickhouse
```

#### 4. Nghiệm thu dữ liệu Gold Data Mart trên ClickHouse:
Truy vấn bảng OBT Data Mart đã được vật lý hóa vào ClickHouse:
```bash
docker exec -it clickhouse_dwh clickhouse-client -q   "SELECT count() AS total_obt, countDistinct(CUSTOMER_ID) AS unique_customers, sum(TOTAL_REVENUE) AS total_revenue, sum(PROFIT_AMOUNT) AS total_profit FROM default.obt_shipment_analytics FORMAT PrettyCompact"
```
*Kết quả mẫu đạt chuẩn:*
```text
┌─total_obt─┬─unique_customers─┬─────total_revenue─┬───────total_profit─┐
│     50101 │              200 │ 7196612636.749999 │ 2519660794.1200004 │
└───────────┴──────────────────┴───────────────────┴────────────────────┘
```

## 🎯 BẢNG TỔNG KẾT KIỂM THỬ THÀNH CÔNG

| Hạng mục kiểm thử | Tiêu chuẩn thành công | Kết quả thực tế |
| :--- | :--- | :--- |
| **Data Generation** | Sinh thành công Dimensions & Facts vào Oracle PDB `FREEPDB1` | ✅ ĐẠT |
| **DBeaver Inspection** | Kết nối Service Name `FREEPDB1` truy vấn bảng dữ liệu nguồn | ✅ ĐẠT |
| **Initial Bulk Load** | Toàn bộ dữ liệu lịch sử từ Oracle vào Silver Layer trong < 2 phút | ✅ ĐẠT |
| **CDC Capture** | Debezium bắt đầy đủ INSERT, UPDATE, DELETE vào Kafka | ✅ ĐẠT |
| **Bronze Streaming** | Spark Streaming ghi Append-Only vào MinIO Delta với Checkpoint | ✅ ĐẠT |
| **Bronze ClickHouse Inspection** | Truy vấn trực tiếp Delta Lake Bronze từ ClickHouse phân nhóm theo Topic | ✅ ĐẠT |
| **Silver Medallion** | Tự động Dedup, MERGE INTO và quản lý SCD Type 2 | ✅ ĐẠT |
| **ClickHouse Serving** | Truy vấn trực tiếp Delta Lake trên S3 với tốc độ mili-giây | ✅ ĐẠT |
| **Airflow Orchestrator** | Giao diện Web UI Port 8085 hoạt động với tài khoản cố định admin/admin | ✅ ĐẠT |
| **dbt Spark Gold Mart** | Pre-join Silver tạo Gold OBT Delta Lake qua Spark Thrift Server | ✅ ĐẠT |
| **dbt ClickHouse DWH** | Materialize OBT Incremental vào ClickHouse & Pass Quality Tests | ✅ ĐẠT |
