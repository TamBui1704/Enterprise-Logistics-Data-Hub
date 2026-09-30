import time
import random
import uuid
from datetime import datetime
import psycopg2

DB_CONFIG = {
    "dbname": "logistics_db",
    "user": "oracle_user",
    "password": "oracle_password",
    "host": "localhost",
    "port": "5432"
}

STATUSES = ["CREATED", "PROCESSING", "SHIPPED", "DELIVERED"]
CARRIERS = ["ViettelPost", "VNPost", "GiaoHangNhanh", "NinjaVan"]
CITIES = ["Hà Nội", "Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ", "Nha Trang"]

def generate_live_transactions():
    print("🚀 Đang kết nối tới Nguồn Dữ liệu Logistics (Oracle/Postgres Mock)...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        print("✅ Kết nối thành công! Bắt đầu phát sinh giao dịch CDC Real-time...")

        order_count = 1
        while True:
            order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
            cust_id = f"CUST-00{random.randint(1, 3)}"
            wh_id = random.choice(["WH-HN-01", "WH-HCM-01", "WH-DN-01"])
            amount = round(random.uniform(50000, 5000000), 2)
            
            # 1. INSERT ORDER
            cursor.execute(
                "INSERT INTO orders (order_id, customer_id, warehouse_id, total_amount, status) VALUES (%s, %s, %s, %s, %s)",
                (order_id, cust_id, wh_id, amount, "CREATED")
            )
            print(f"📦 [{datetime.now().strftime('%H:%M:%S')}] [INSERT ORDER] {order_id} | Giá trị: {amount:,.0f} VNĐ")
            time.sleep(1)

            # 2. INSERT SHIPMENT
            shipment_id = f"SHIP-{uuid.uuid4().hex[:8].upper()}"
            tracking_num = f"TK{random.randint(100000000, 999999999)}"
            carrier = random.choice(CARRIERS)
            origin = random.choice(CITIES)
            dest = random.choice([c for c in CITIES if c != origin])
            
            cursor.execute(
                "INSERT INTO shipments (shipment_id, order_id, carrier_name, tracking_number, origin_city, destination_city, status) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (shipment_id, order_id, carrier, tracking_num, origin, dest, "IN_TRANSIT")
            )
            print(f"🚚 [{datetime.now().strftime('%H:%M:%S')}] [INSERT SHIPMENT] {shipment_id} | Đơn: {order_id} | Hãng: {carrier}")
            time.sleep(1)

            # 3. UPDATE ORDER STATUS (Mô phỏng thay đổi trạng thái CDC)
            if random.random() > 0.3:
                new_status = random.choice(["PROCESSING", "SHIPPED", "DELIVERED"])
                cursor.execute(
                    "UPDATE orders SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE order_id = %s",
                    (new_status, order_id)
                )
                print(f"🔄 [{datetime.now().strftime('%H:%M:%S')}] [UPDATE ORDER] {order_id} -> Trạng thái mới: {new_status}")

            time.sleep(2)
            order_count += 1
            if order_count > 50:
                print("🏁 Đã tạo đủ 50 giao dịch mô phỏng.")
                break

    except Exception as e:
        print(f"❌ Lỗi phát sinh dữ liệu: {e}")

if __name__ == "__main__":
    generate_live_transactions()
