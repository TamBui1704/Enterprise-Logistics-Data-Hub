import os
import time
import random
import uuid
from datetime import datetime
import oracledb

# Kết nối tới Oracle DB (Debezium Oracle XE container)
DB_CONFIG = {
    "user": "debezium",
    "password": "dbz",
    "dsn": os.getenv("ORACLE_DSN", "localhost:1521/FREEPDB1")
}

SERVICES = ["SERV-01", "SERV-02", "SERV-03", "SERV-04"]
WEIGHT_TIERS = ["WT-01", "WT-02", "WT-03", "WT-04", "WT-05", "WT-06"]
ROUTING_TYPES = ["RT-01", "RT-02", "RT-03", "RT-04"]
STATUSES = ["STAT-01", "STAT-02", "STAT-03", "STAT-04", "STAT-05", "STAT-06"]
FAILURE_REASONS = ["FAIL-01", "FAIL-02", "FAIL-03", "FAIL-04"]

def generate_live_transactions():
    print("🚀 Đang kết nối tới Oracle Database (Debezium CDC Source)...")
    try:
        conn = oracledb.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        print("✅ Kết nối Oracle DB thành công! Bắt đầu phát sinh giao dịch Real-time theo chuẩn EMS Bus Matrix...")

        # Fetch active master data IDs
        cursor.execute("SELECT customer_id FROM debezium.dim_customers")
        cust_ids = [r[0] for r in cursor.fetchall()] or ["CUST-0001"]
        
        cursor.execute("SELECT pos_code FROM debezium.dim_pos_locations")
        pos_codes = [r[0] for r in cursor.fetchall()] or ["POS-0001"]

        order_count = 1
        while True:
            booking_id = f"BK-{uuid.uuid4().hex[:10].upper()}"
            item_code = f"EA{random.randint(100000000, 999999999)}VN"
            cust_id = random.choice(cust_ids)
            serv_id = random.choice(SERVICES)
            send_pos = random.choice(pos_codes)
            recv_pos = random.choice([p for p in pos_codes if p != send_pos] or pos_codes)
            weight_g = random.randint(100, 10000)
            wt_id = random.choice(WEIGHT_TIERS)
            rt_id = random.choice(ROUTING_TYPES)
            
            main_fee = round(random.uniform(20000, 300000), 2)
            sur_fee = round(main_fee * 0.1, 2)
            discount = round(main_fee * 0.05, 2)
            tot_rev = main_fee + sur_fee - discount
            cost_amt = round(tot_rev * 0.6, 2)
            
            # 1. INSERT SHIPMENT BOOKING
            cursor.execute(
                "INSERT INTO debezium.shipment_bookings (booking_id, item_code, customer_id, service_id, "
                "sending_pos_code, receiving_pos_code, weight_gram, weight_tier_id, routing_type_id, "
                "main_fee, sur_fee, discount_amount, total_revenue, cost_amount, status_id) "
                "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)",
                (booking_id, item_code, cust_id, serv_id, send_pos, recv_pos, weight_g, wt_id, rt_id,
                 main_fee, sur_fee, discount, tot_rev, cost_amt, "STAT-01")
            )
            print(f"📦 [{datetime.now().strftime('%H:%M:%S')}] [INSERT BOOKING] {item_code} | Doanh thu: {tot_rev:,.0f} VNĐ")
            time.sleep(1)

            # 2. INSERT TRACKING EVENT (Khai thác thu gom)
            event_id = f"EVT-{uuid.uuid4().hex[:10].upper()}"
            cursor.execute(
                "INSERT INTO debezium.shipment_event_trackings (event_id, booking_id, event_pos_code, status_id, operator_user, notes) "
                "VALUES (:1, :2, :3, :4, :5, :6)",
                (event_id, booking_id, send_pos, "STAT-01", "KTV_ACCEPT", "Chấp nhận bưu gửi tại bưu cục")
            )
            print(f"🚚 [{datetime.now().strftime('%H:%M:%S')}] [EVENT TRACKING] {item_code} -> Đã thu gom tại {send_pos}")
            time.sleep(1)

            # 3. UPDATE STATUS & RECORD REMUNERATION (Mô phỏng hoàn thành giao hàng / chuyển hoàn)
            if random.random() > 0.3:
                new_status = random.choice(["STAT-05", "STAT-06"])
                cursor.execute(
                    "UPDATE debezium.shipment_bookings SET status_id = :1, updated_at = CURRENT_TIMESTAMP WHERE booking_id = :2",
                    (new_status, booking_id)
                )
                print(f"🔄 [{datetime.now().strftime('%H:%M:%S')}] [UPDATE BOOKING] {item_code} -> Trạng thái: {new_status}")
                
                # Thù lao phát / Thù lao hoàn
                if new_status == "STAT-05":
                    cursor.execute(
                        "INSERT INTO debezium.delivery_remunerations (remuneration_id, booking_id, courier_id, delivery_pos_code, delivery_status_id, delivery_fee_amount) "
                        "VALUES (:1, :2, :3, :4, :5, :6)",
                        (f"REM-{uuid.uuid4().hex[:10].upper()}", booking_id, "COU-001", recv_pos, "STAT-05", 5000.0)
                    )
                else:
                    cursor.execute(
                        "INSERT INTO debezium.return_remunerations (return_id, booking_id, courier_id, return_pos_code, failure_reason_id, return_fee_amount) "
                        "VALUES (:1, :2, :3, :4, :5, :6)",
                        (f"RET-{uuid.uuid4().hex[:10].upper()}", booking_id, "COU-001", send_pos, random.choice(FAILURE_REASONS), 3000.0)
                    )

            time.sleep(2)
            order_count += 1
            if order_count > 50:
                print("🏁 Đã tạo đủ 50 giao dịch mô phỏng realtime.")
                break

    except Exception as e:
        print(f"❌ Lỗi phát sinh dữ liệu Oracle: {e}")

if __name__ == "__main__":
    generate_live_transactions()
