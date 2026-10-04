# 📊 Hướng Dẫn Thông Luồng BI (Cube Semantic Layer & Apache Superset)

Tài liệu này hướng dẫn cách kết nối hệ thống Business Intelligence (Cluster 4) với ClickHouse Data Warehouse, sử dụng Cube.dev làm Tầng Ngữ Nghĩa (Semantic Layer) và Superset làm Dashboard.

---

## 1. Nguyên Lý Hoạt Động Cốt Lõi của Cube

Nhiều người thường nhầm lẫn Cube với dbt. Dưới đây là sự khác biệt để bạn dễ hình dung:

* **1 File YAML = 1 (hoặc nhiều) Cube:** Trong Cube, 1 khối `name: ...` đại diện cho 1 Cube (tương đương 1 View/Bảng). 
* **KHÔNG CẦN CHẠY LỆNH BUILD (như `dbt build`):** Cube hoạt động hoàn toàn **Động (Dynamic)**. Khi bạn lưu file `.yml`, Cube Server (đang chạy bằng Docker) sẽ lập tức phát hiện thay đổi (nhờ chế độ `CUBEJS_DEV_MODE: true`) và nạp thẳng cấu hình mới vào RAM. Bạn không cần gõ bất kỳ lệnh nào cả. Cứ lưu file xong là sang Superset query được luôn!

### Xử lý mô hình Chòm Sao Sự Kiện (Constellation Schema)
Nếu hệ thống có 2 Fact (Ví dụ: 1 Fact Đơn Hàng `Shipments` và 1 Fact Ca Chạy Tài Xế `DriverShifts`) cùng dùng chung các Dims (Khách hàng, Tỉnh thành, Bưu cục). Có 2 cách giải quyết:
1. **Cách 1 (The OBT Way - Khuyên dùng 100% cho ClickHouse):** Bạn dùng dbt build ra **2 bảng OBT riêng biệt** trên ClickHouse (`obt_shipment_analytics` và `obt_driver_shifts`). Cả 2 OBT này đều chứa sẵn các cột Tỉnh thành. Sau đó, bên Cube bạn tạo **2 file `.yml` tương ứng** (1 cho Shipments, 1 cho DriverShifts). Cách này làm cho ClickHouse chạy nhanh như điện xẹt vì không phải JOIN.
2. **Cách 2 (The Joins Way - Cách cũ):** Bạn đẩy nguyên dàn Fact và Dims thô lên ClickHouse. Trong Cube, bạn tạo 1 file `.yml` cho Fact và nhiều file cho Dims, rồi định nghĩa cú pháp `joins: ...` trong Cube. Khi Superset gọi, Cube sẽ tự ghép thành lệnh SQL JOIN dài ngoằng đẩy xuống ClickHouse. *(Tuyệt đối tránh cách này vì ClickHouse xử lý JOIN lớn trên nhiều bảng rất tệ).*

Đó là lý do tôi vừa tạo thêm file `DriverShifts.yml` (minh họa) để bạn thấy cách cấu hình 2 Cube OBT riêng biệt!

---

## 2. Cách bật / tắt và "Chạy" Pre-aggregations trong Cube
* **Chạy thông thường (Real-time):** Bạn chỉ cần xóa hoặc comment (`#`) khối `pre_aggregations` trong file `Shipments.yml`. Khi đó, mọi lượt truy cập từ Superset đều được Cube dịch thành SQL và chọc thẳng xuống ClickHouse để lấy dữ liệu mới nhất (độ trễ = thời gian ClickHouse xử lý, thường < 1 giây).
* **Chạy tính sẵn (Pre-aggregations):** Bỏ comment khối `pre_aggregations`. Cube sẽ tự động đọc cấu hình này.

### Cơ chế xử lý ngầm (Zero-Downtime Refresh)
Nếu bạn bật Pre-aggregations, Cube sẽ xử lý theo cơ chế **không bao giờ làm gián đoạn người dùng**:
1. Lần đầu tiên, Cube sẽ ra lệnh ClickHouse tính toán. Quá trình này có thể mất vài giây.
2. Từ đó trở đi, Cube sẽ lập lịch (VD: `every: 1 hour`) để chạy ngầm (Background Worker) cập nhật dữ liệu mới.
3. **Trong lúc hệ thống đang chạy ngầm cập nhật**, nếu người dùng (DA/Giám đốc) mở Dashboard, Cube **vẫn trả về dữ liệu cũ ngay lập tức (trong 10ms)**. Khi background job chạy xong, Cube sẽ tráo đổi (hot-swap) dữ liệu mới vào một cách mượt mà. Người dùng hoàn toàn không cảm nhận được hệ thống đang quá tải hay đang cập nhật.

### Ước lượng thời gian ClickHouse build Pre-aggregations (Tùy phần cứng)
* **10 Triệu dòng:** < 0.5 giây.
* **100 Triệu dòng:** 1 - 2 giây.
* **1 Tỷ dòng:** 5 - 15 giây. 
*(Đây là tốc độ scan của ClickHouse để tạo ra bảng rollup vài ngàn dòng gửi cho Cube Store lưu trữ).*

### Làm sao để kích hoạt/chạy Pre-aggregation?
Cũng giống như tạo Cube, bạn **không cần gõ lệnh gì cả**. 
Ngay khi bạn bỏ comment khối `pre_aggregations` trong file `.yml` (hoặc xem ví dụ mẫu trong file `DriverShifts.yml` tôi vừa tạo), Cube sẽ tự nhận diện. 
* **Lần truy cập đầu tiên:** Khi người dùng mở Dashboard Superset trùng với các dimension trong pre-aggregation, Cube sẽ "khựng" lại vài giây (hoặc nó đã chạy ngầm trước đó) để yêu cầu ClickHouse gửi dữ liệu, nạp vào Cube Store. 
* **Từ đó về sau:** Worker ngầm sẽ bám theo `every: 1 hour` tự động làm mới. Bạn rảnh tay hoàn toàn!

---

## 2. Kiểm tra dữ liệu trên Cube (Developer Playground)

Trước khi vào Superset, ta cần xác nhận Cube đã đọc hiểu ClickHouse thành công.
1. Chạy cụm BI: `cd cluster-4-bi && docker compose up -d`
2. Mở trình duyệt truy cập: **`http://localhost:4000`** (Cube Developer Playground).
3. Vào tab **Build**, bạn sẽ thấy danh sách các Measures và Dimensions đã cấu hình trong `Shipments.yml`.
4. Chọn Measure: `Tổng Doanh Thu (VND)`, Dimension: `Tỉnh Gửi`.
5. Bấm nút **Run**. Cube sẽ hiển thị biểu đồ và bảng dữ liệu. Bạn có thể bấm vào tab **SQL** để xem câu lệnh ClickHouse tuyệt đẹp mà Cube vừa tự động sinh ra.

---

## 3. Đăng nhập Superset & Kết nối Cube

1. Mở trình duyệt truy cập: **`http://localhost:8088`**
2. Đăng nhập bằng tài khoản mặc định (Thường Superset khởi tạo mặc định là `admin` / `admin`. Nếu chưa tạo admin, bạn chạy lệnh: `docker exec -it superset_bi superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin`)
3. **Kết nối Database:**
   * Chọn `Settings` (biểu tượng bánh răng góc phải) $\rightarrow$ `Database Connections` $\rightarrow$ `+ Database`.
   * Chọn **PostgreSQL** (Đừng chọn ClickHouse, vì ta đang giả lập qua Cube).
   * Điền thông số URI: `postgresql://default@cube_semantic_layer:15432/default`
   * Bấm **Connect** và lưu lại.

---

## 4. Thiết kế Dashboard Cơ Bản cho Logistics

1. **Thêm Dataset:** 
   * Trên thanh menu chọn `Datasets` $\rightarrow$ `+ Dataset`.
   * Chọn Database vừa tạo $\rightarrow$ Schema: `public` $\rightarrow$ Bảng: `Shipments`.
   * (Superset sẽ tự nhận diện đây là bảng Shipments được Cube "bơm" cho).

2. **Tạo Biểu đồ 1: Tổng Doanh thu theo Tỉnh (Bar Chart)**
   * Tạo Chart mới $\rightarrow$ Chọn Dataset `Shipments`.
   * Chart Type: `Bar Chart`.
   * X-Axis: Kéo cột `Tỉnh Gửi` (sendingProvince) vào.
   * Metric: Chọn `Tổng Doanh Thu (VND)` (totalRevenue).
   * Bấm **Create Chart** $\rightarrow$ Lưu lại với tên "Doanh thu theo tỉnh".

3. **Tạo Biểu đồ 2: Tỷ lệ Trạng thái Đơn Hàng (Pie Chart)**
   * Chart Type: `Pie Chart`.
   * Group By: `Trạng Thái Đơn` (statusName).
   * Metric: Chọn `Tổng Số Đơn Hàng` (totalBookings).
   * Bấm **Create Chart** $\rightarrow$ Lưu lại.

4. **Lên Dashboard:**
   * Vào menu `Dashboards` $\rightarrow$ `+ Dashboard`.
   * Đặt tên "Logistics Overview".
   * Kéo thả 2 biểu đồ vừa tạo từ panel bên phải vào màn hình.
   * Chỉnh sửa kích thước (Resize), thêm bộ lọc (Filter) theo `Ngày Gửi Hàng` và Bấm **Save**.

---

## 5. Chuyên đề nâng cao: Chuyển đổi tư duy từ SSAS sang Modern Data Stack (Cube + Superset)

Nếu bạn đã quen với **Microsoft SSAS (Tabular / MOLAP)**, bạn sẽ thấy sự khác biệt về triết lý thiết kế.

### 5.1. Thiết kế Mô hình (Joins vs OBT)
* **Trong SSAS:** Bạn thiết kế chuẩn Star Schema, kéo thả Relationship (Join) giữa Fact và Dim. SSAS nạp tất cả vào RAM và xử lý Join cực mượt.
* **Trong Cube / ClickHouse:** ClickHouse là RDBMS dạng Cột (Columnar). Điểm yếu chí mạng của nó là thực hiện JOIN khi Query. Do đó, giới Data Engineer ưu tiên **Denormalization (Gộp bảng - OBT)**.
  👉 *Trùng lặp Dim có đáng sợ không?* Không! Storage cực rẻ, và ClickHouse nén dữ liệu cực tốt. Việc lấp lại cột Tỉnh Thành trên 2 OBT đổi lại tốc độ truy vấn nhanh gấp 10 lần so với JOIN.
  👉 *Lưu ý:* Cube **VẪN HỖ TRỢ JOIN** y hệt SSAS (bằng thuộc tính `joins:` trong file YAML). Nếu lượng dữ liệu Dim của bạn không quá lớn, bạn hoàn toàn có thể định nghĩa 1 file `DimLocations.yml` và Join nó vào các Fact. Nhưng để đạt hiệu năng tối đa (hàng tỷ dòng), OBT vẫn là vua.

### 5.2. Quản trị Người dùng (Users) & Phân quyền dữ liệu (RLS - Row Level Security)

Tư duy của bạn hoàn toàn chính xác! Nếu Semantic Layer là trái tim phục vụ cho cả Superset, Excel Pivot và các AI Agents (như ChatGPT gọi API hỏi số liệu), thì **việc phân quyền RLS BẮT BUỘC PHẢI DIỄN RA Ở CUBE**. Nếu ta phân quyền trên Superset, một nhân viên dùng Excel hoặc một con AI gọi API tới Cube sẽ dễ dàng vượt mặt (bypass) quy định bảo mật đó.

**Kiến trúc Đề Xuất (Keycloak + ClickHouse + Cube.js):**
Thay vì dùng AD/LDAP truyền thống, Modern Data Stack sử dụng **Identity Provider (IdP)** như **Keycloak** kết hợp với bảng phân quyền lưu trên Data Warehouse (ClickHouse).

1. **Centralized IAM với Keycloak:** Keycloak đóng vai trò trung tâm quản lý tài khoản duy nhất (SSO). Bạn tạo tài khoản, đổi mật khẩu ở đây. Keycloak có thể quản lý đăng nhập (OIDC/SAML) cho toàn bộ hệ sinh thái của bạn, bao gồm Superset, MinIO, Airflow, dbt, (tương tự như AWS IAM quản lý S3, Athena...).
2. **Lưu trữ Bảng Phân Quyền tại ClickHouse:** Cấu hình ma trận phân quyền (Ai được xem tỉnh nào, ngành hàng nào) sẽ được thiết kế thành một bảng `dim_user_permissions` lưu trên ClickHouse. Điều này giúp Data Engineer dễ dàng bảo trì và cập nhật quyền hàng loạt bằng các luồng ETL/dbt.
3. **Thực thi quyền động (Dynamic RLS) qua Cube.js:** 
   * Khi Superset hoặc AI Agent truy xuất dữ liệu từ Cube, chúng phải đính kèm một **JWT (JSON Web Token)** do Keycloak cấp.
   * File `cube.js` (trong hàm `checkAuth`) sẽ giải mã và xác thực Token này để lấy username. Tiếp theo, nó sẽ gọi một truy vấn (Async) xuống bảng `dim_user_permissions` ở ClickHouse để lấy danh sách tỉnh thành mà user được xem.
   * Danh sách này được đưa vào biến môi trường `securityContext`.
   * Tại các file `.yml` định nghĩa Cube (ví dụ `Shipments.yml`), tính năng `query_rewrite` (hoặc data access rules) sẽ đọc mảng từ `securityContext` và tự động kẹp điều kiện (ví dụ `WHERE SENDING_PROVINCE IN (...)`) vào câu lệnh SQL trước khi đẩy xuống ClickHouse.

👉 *Kết quả:* Kiến trúc này an toàn tuyệt đối và mở rộng vô hạn. Bạn chỉ cần tạo User 1 lần ở Keycloak. Mọi hệ thống trong Cluster đều dùng Keycloak để xác thực. Khi truy vấn dữ liệu, lưới lọc Row-Level Security tự động lấy rules từ ClickHouse và khóa chặt phạm vi dữ liệu ở tầng Cube. Sếp tổng thấy toàn quốc, quản lý miền Nam chỉ thấy số liệu miền Nam một cách hoàn toàn tự động!

### 5.3. Khả năng Ad-hoc Query (Kỳ vọng thay thế Excel nối SSAS qua IIS)
Superset đáp ứng hoàn hảo nhu cầu Ad-hoc Query của bạn qua 2 công cụ:
1. **Superset SQL Lab:** Giao diện cho phép DAs gõ lệnh SQL trực tiếp vào Cube, lấy kết quả và xuất ra Excel/CSV.
2. **Superset Explore (Giao diện Pivot):** Giống hệt Pivot Table của Excel. Kéo thả Dimension vào Row/Column, Metric vào Value. Dữ liệu sẽ tự động xoay chiều.

🔥 **TUYỆT CHIÊU CUỐI CÙNG (Dùng thẳng Excel):** 
Bạn nhớ cổng `15432` của Cube chứ? Cube tự giả lập mình là một Postgres Server. Nếu DAs của bạn vẫn "yêu" Excel / PowerBI, họ chỉ cần vào Excel $\rightarrow$ **Get Data from PostgreSQL** $\rightarrow$ Nhập IP máy chủ và cổng `15432`. Họ có thể dùng thẳng Pivot Table của Excel kéo thả dữ liệu tỷ dòng y hệt như đang cắm vào SSAS ngày xưa!

🎉 **Hoàn tất!** Giờ đây bạn đã có một Data Platform E2E toàn diện.

---

## 6. Hướng dẫn Tích hợp Keycloak & Row-Level Security trên ClickHouse

Để đưa hệ thống lên chuẩn Enterprise bảo mật, chúng ta bổ sung **Keycloak** làm SSO (Single Sign-On) và tạo bảng lưu cấu hình quyền hạn (RLS - Row-Level Security) trên **ClickHouse**.

### 6.1. Khởi chạy cụm IAM (Keycloak)
Tôi đã tạo sẵn file cấu hình `docker-compose.yml` trong thư mục `cluster-0-iam/`.
1. Mở Terminal, di chuyển vào thư mục và chạy:
   ```bash
   cd cluster-0-iam && docker compose up -d
   ```
2. Truy cập Keycloak Admin Console tại: `http://localhost:8080` (User: `admin`, Pass: `admin`).
3. Tại đây bạn có thể tạo Realm, tạo Client (cho Cube và Superset), và tạo các Users (VD: `nguyenvana`, `admin`).

### 6.2. Thiết kế bảng Phân Quyền (Mapping Table) trên ClickHouse
Thay vì dùng Code DAX hoặc SQL Server để lưu quyền như cũ, trên ClickHouse bạn tạo một bảng danh mục (Dimension) chứa thông tin User và phạm vi dữ liệu họ được phép xem.

*Kịch bản:* User truy cập Ad-hoc (Excel) hoặc qua Superset. Cube.js sẽ đọc bảng này để khóa dữ liệu.

```sql
-- Chạy trên ClickHouse DB
CREATE TABLE IF NOT EXISTS dim_user_permissions (
    username String,           -- Tên đăng nhập lấy từ Keycloak (VD: nguyenvana)
    role String,               -- Vai trò (admin, manager, staff)
    province_access Array(String), -- Danh sách tỉnh thành được phép xem
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY username;

-- Insert dữ liệu mẫu:
INSERT INTO dim_user_permissions (username, role, province_access) VALUES 
('admin', 'admin', ['ALL']),
('nguyenvana', 'manager', ['Hồ Chí Minh', 'Đồng Nai']);
```
*(Bạn có thể dùng dbt để tạo và cập nhật bảng này tự động từ các nguồn dữ liệu HR/CRM).*

### 6.3. Giải thích Code RLS trên Cube.js (cube.js & .yml)
Tôi đã cập nhật file `cluster-4-bi/cube_conf/cube.js` để bao phủ cả 2 luồng truy cập:

*   **Hàm `checkSqlAuth`:** Khi người dùng mở Excel hoặc Superset, họ kết nối vào giao thức Postgres giả lập của Cube (Cổng 15432). Cube lấy User/Pass, gọi lên Keycloak để xác thực. Nếu đúng, Cube gọi Query xuống bảng `dim_user_permissions` trên ClickHouse lấy mảng `['Hồ Chí Minh', 'Đồng Nai']` và nạp vào **`securityContext`**.
*   **Hàm `checkAuth`:** Khi AI Agent hoặc ứng dụng Web gọi REST API của Cube, chúng gửi kèm mã JWT. Cube giải mã JWT lấy username, và cũng nạp mảng phân quyền vào **`securityContext`**.

**Cách Cube ép buộc (Enforce) RLS vào câu SQL:**
Trong file định nghĩa Cube (VD: `Shipments.yml`), bạn chỉ cần thêm đoạn mã `query_rewrite` (hoặc data_access_rules) như sau:

```yaml
cubes:
  - name: Shipments
    sql: SELECT * FROM obt_shipments
    
    # Kích hoạt Row-Level Security
    query_rewrite:
      # Nếu province_access có chữ ALL -> Bỏ qua không filter.
      # Nếu không, kẹp câu điều kiện tự động: WHERE SENDING_PROVINCE IN ('Hồ Chí Minh', 'Đồng Nai')
      sql: >
        {% if COMPILE_CONTEXT.securityContext.province_access != 'ALL' %}
          SELECT * FROM (${COMPILE_CONTEXT.sql}) AS tbl 
          WHERE tbl.sending_province IN ({{ COMPILE_CONTEXT.securityContext.province_access | join: ", " | quote }})
        {% else %}
          ${COMPILE_CONTEXT.sql}
        {% endif %}
```

👉 **Tổng kết Luồng:** 
Keycloak quản lý thông tin User $\rightarrow$ ClickHouse giữ logic phân quyền $\rightarrow$ Cube.js đóng vai trò "cửa khẩu" kẹp điều kiện WHERE vào mọi câu Query $\rightarrow$ Superset và Ad-hoc Excel hiển thị kết quả an toàn.
