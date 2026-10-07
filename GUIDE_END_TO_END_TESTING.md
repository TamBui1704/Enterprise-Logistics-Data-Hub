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

> 💡 **Mẹo**: Nếu muốn kết nối với user quản trị Container Database (CDB$ROOT), chọn Service Name là `FREE` (hoặc `ORCLCDB`), Username `sys`, Password `top_secret`, và chọn Role là `SYSDBA`.

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

### BƯỚC 5: Đăng Ký 2 Debezium Connectors Bắt Luồng CDC

Đăng ký 2 Connector theo đúng chuẩn:
1. **Dim Connector**: Snapshot danh mục vào Kafka (`snapshot.mode: initial`).
2. **Fact Connector**: Chốt mốc SCN và chỉ stream dữ liệu mới (`snapshot.mode: schema_only`).

```bash
cd cluster-1-ingestion

# 1. Đăng ký Dim Connector (Snapshot Bảng Nhỏ)
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium/register-dim-connector.json

# 2. Đăng ký Fact Connector (Incremental Bảng Lớn)
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium/register-fact-connector.json
```

🔍 **Kiểm tra trạng thái**:
```bash
curl -s http://localhost:8083/connectors?expand=status
```
*Cả 2 connector và tasks đều phải hiển thị trạng thái `"state": "RUNNING"`.*

---

### BƯỚC 6: Kích Hoạt PySpark Streaming Đọc Kafka CDC Ghi Vào Bronze Layer (Zero-Touch Ingestion)

Khởi chạy PySpark Streaming để lắng nghe toàn bộ các bảng CDC từ Kafka (thông qua Wildcard Regex) và ghi Append-Only vào thư mục gộp trên MinIO Delta Lake (Partition tự động theo tên Topic):

```bash
docker exec -it spark-runner spark-submit   --master spark://spark-runner:7077   /opt/bitnami/spark/spark_jobs/kafka_to_delta.py
```

> 💡 *Job này chạy ở chế độ Real-time Streaming liên tục. Nhờ cấu hình Metadata-driven, nó tự động hút dữ liệu của TẤT CẢ các bảng mà không cần phải gọi nhiều lần. Bạn hãy để cửa sổ Terminal này mở.*

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
3. Terminal Spark Streaming (Bước 6) liên tục commit các micro-batches ghi vào thư mục `bronze/shipment_bookings/` trên MinIO.

---

### BƯỚC 8: Thực Thi Medallion Batch Transformation (Bronze -> Silver Value & History)

Sau khi dữ liệu CDC của các bảng tích tụ ở thư mục Bronze gộp, bạn mở Terminal mới để chạy Job biến đổi linh động (Dynamic Metadata-driven). Job này nhận tham số `--table` để biến đổi cho bất kỳ bảng cụ thể nào:

**Ví dụ 1: Xử lý bảng SHIPMENT_BOOKINGS (Fact)**
```bash
docker exec -it spark-runner spark-submit   --master spark://spark-runner:7077   /opt/bitnami/spark/spark_jobs/bronze_to_silver_medallion.py --table SHIPMENT_BOOKINGS
```

**Ví dụ 2: Xử lý bảng DIM_CUSTOMERS (Dimension)**
```bash
docker exec -it spark-runner spark-submit   --master spark://spark-runner:7077   /opt/bitnami/spark/spark_jobs/bronze_to_silver_medallion.py --table DIM_CUSTOMERS
```

**Job sẽ tự động:**
- Lấy thông tin Primary Keys động từ file `tables_config.json`.
- Lấy bản ghi mới nhất cho từng bộ khóa chính (`dedup`).
- Thực hiện `MERGE INTO` (Upsert / Delete) linh động vào thư mục Silver tương ứng (`silver/value/[table_name]`).
- Quản lý phiên bản SCD Type 2 (`valid_from`, `valid_to`, `is_current = 0/1`) vào `silver/history/[table_name]`.
- Ghi nhật ký tiến trình vào bảng `control/etl_batch_control`.

---

### BƯỚC 9: Kiểm Tra & Nghiệm Thu Dữ Liệu Bằng ClickHouse (Zero-Copy Query)

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

## 🎯 BẢNG TỔNG KẾT KIỂM THỬ THÀNH CÔNG

| Hạng mục kiểm thử | Tiêu chuẩn thành công | Kết quả thực tế |
| :--- | :--- | :--- |
| **Data Generation** | Sinh thành công Dimensions & Facts vào Oracle PDB `FREEPDB1` | ✅ ĐẠT |
| **DBeaver Inspection** | Kết nối Service Name `FREEPDB1` truy vấn bảng dữ liệu nguồn | ✅ ĐẠT |
| **Initial Bulk Load** | Toàn bộ dữ liệu lịch sử từ Oracle vào Silver Layer trong < 2 phút | ✅ ĐẠT |
| **CDC Capture** | Debezium bắt đầy đủ INSERT, UPDATE, DELETE vào Kafka | ✅ ĐẠT |
| **Bronze Streaming** | Spark Streaming ghi Append-Only vào MinIO Delta với Checkpoint | ✅ ĐẠT |
| **Silver Medallion** | Tự động Dedup, MERGE INTO và quản lý SCD Type 2 | ✅ ĐẠT |
| **ClickHouse Serving** | Truy vấn trực tiếp Delta Lake trên S3 với tốc độ mili-giây | ✅ ĐẠT |
