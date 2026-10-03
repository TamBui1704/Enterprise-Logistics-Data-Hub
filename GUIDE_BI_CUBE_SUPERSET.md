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

Đây chính xác là triết lý của SSAS ngày xưa, và Cube sinh ra để làm điều này một cách cực kỳ thanh lịch thông qua **Security Context**.

**Cách làm trên Cube (Tích hợp AD và Bảng Phân Quyền):**
1. **Quản lý quyền động qua Database (Dynamic RLS):** File `cube.js` thực chất là một môi trường Node.js hoàn chỉnh. Bạn hoàn toàn có thể biến hàm `checkSqlAuth` thành hàm `async`. Khi User nhập tài khoản, Cube sẽ thực hiện 2 việc:
   * Gọi lên **Active Directory (AD/LDAP)** để xác thực mật khẩu.
   * Query xuống một bảng tên là `Dim_User_Permissions` trên CSDL (Postgres hoặc ClickHouse) để lấy danh sách các tỉnh mà user đó được cấp quyền.
2. **Nạp Security Context & Kích hoạt RLS trong YML:** Sau khi lấy được dữ liệu từ bảng phân quyền, hàm JavaScript đẩy mảng các tỉnh đó vào biến `securityContext`. Ở file `Shipments.yml`, khối `query_rewrite` sẽ tự động đọc mảng này và kẹp điều kiện `OR SENDING_PROVINCE IN (...)` vào lệnh SQL để khóa chặt phạm vi dữ liệu.

*(Tôi đã cập nhật lại file [`cube.js`](file:///home/tambt/Desktop/Enterprise-Logistics-Data-Hub/cluster-4-bi/cube_conf/cube.js) mẫu với đoạn code mô phỏng việc gọi hàm Async xuống Database / AD. Bạn có thể mở ra xem kiến trúc code thực tế).*

👉 *Kết quả:* Kiến trúc này y hệt mô hình chuẩn của SSAS: Không ai biết password của ai, dữ liệu quyền được sửa trên Database là có tác dụng ngay lập tức, và bất kể truy cập từ Excel hay AI, mọi truy vấn đều bị lọc cứng ngắc ở tầng Semantic Layer!

👉 *Kết quả:* Bây giờ sếp tổng nối Excel bằng tài khoản `admin_tong` sẽ thấy toàn quốc. Quản lý miền Nam nối Superset bằng tài khoản `quan_ly_mn` (khai báo tài khoản này lúc tạo kết nối Database trên Superset) thì mọi biểu đồ chỉ hiện số liệu miền Nam. Tuyệt đối bảo mật ở cấp độ Core!

### 5.3. Khả năng Ad-hoc Query (Kỳ vọng thay thế Excel nối SSAS qua IIS)
Superset đáp ứng hoàn hảo nhu cầu Ad-hoc Query của bạn qua 2 công cụ:
1. **Superset SQL Lab:** Giao diện cho phép DAs gõ lệnh SQL trực tiếp vào Cube, lấy kết quả và xuất ra Excel/CSV.
2. **Superset Explore (Giao diện Pivot):** Giống hệt Pivot Table của Excel. Kéo thả Dimension vào Row/Column, Metric vào Value. Dữ liệu sẽ tự động xoay chiều.

🔥 **TUYỆT CHIÊU CUỐI CÙNG (Dùng thẳng Excel):** 
Bạn nhớ cổng `15432` của Cube chứ? Cube tự giả lập mình là một Postgres Server. Nếu DAs của bạn vẫn "yêu" Excel / PowerBI, họ chỉ cần vào Excel $\rightarrow$ **Get Data from PostgreSQL** $\rightarrow$ Nhập IP máy chủ và cổng `15432`. Họ có thể dùng thẳng Pivot Table của Excel kéo thả dữ liệu tỷ dòng y hệt như đang cắm vào SSAS ngày xưa!

🎉 **Hoàn tất!** Giờ đây bạn đã có một Data Platform E2E toàn diện.
