import os
import time
import random
import uuid
import argparse
from datetime import datetime, timedelta
import oracledb

# Cấu hình kết nối Oracle Database (Debezium Oracle XE container)
DB_CONFIG = {
    "user": "debezium",
    "password": "dbz",
    "dsn": os.getenv("ORACLE_DSN", "localhost:1521/ORCLPDB1")
}

PROVINCES = [
    ("10", "Hà Nội", "Miền Bắc"), ("20", "Hồ Chí Minh", "Miền Nam"), ("48", "Đà Nẵng", "Miền Trung"),
    ("31", "Hải Phòng", "Miền Bắc"), ("92", "Cần Thơ", "Miền Nam"), ("56", "Khánh Hòa", "Miền Trung"),
    ("24", "Bắc Ninh", "Miền Bắc"), ("60", "Đồng Nai", "Miền Nam"), ("75", "Thừa Thiên Huế", "Miền Trung")
]

SERVICES = ["SERV-01", "SERV-02", "SERV-03", "SERV-04"]
WEIGHT_TIERS = ["WT-01", "WT-02", "WT-03", "WT-04", "WT-05", "WT-06"]
ROUTING_TYPES = ["RT-01", "RT-02", "RT-03", "RT-04"]
STATUSES = ["STAT-01", "STAT-02", "STAT-03", "STAT-04", "STAT-05", "STAT-06", "STAT-07"]
FAILURE_REASONS = ["FAIL-01", "FAIL-02", "FAIL-03", "FAIL-04", "FAIL-05"]

def seed_dimensions(cursor):
    print("🌱 [1/3] Đang khởi tạo Dữ liệu Master Dimensions (Khách hàng, Bưu cục)...")
    
    # 1. Seed 200 Customers
    customers_data = []
    for i in range(1, 201):
        prov_code, prov_name, region = random.choice(PROVINCES)
        cust_type = random.choice(["B2B", "RETAIL", "ENTERPRISE"])
        customers_data.append((
            f"CUST-{i:04d}",
            f"Khách hàng Doanh nghiệp / Cá nhân EMS {i}",
            cust_type,
            f"010{random.randint(1000000, 9999999)}",
            prov_name,
            region
        ))
    cursor.executemany(
        "INSERT INTO debezium.dim_customers (customer_id, customer_name, customer_type, tax_code, province, region) "
        "VALUES (:1, :2, :3, :4, :5, :6)",
        customers_data
    )

    # 2. Seed 200 POS Locations (Bưu cục)
    pos_data = []
    for i in range(1, 201):
        prov_code, prov_name, region = random.choice(PROVINCES)
        pos_level = random.choice(["PROVINCIAL", "DISTRICT", "COMMUNE"])
        pos_data.append((
            f"POS-{i:04d}",
            f"Bưu cục EMS {prov_name} số {i}",
            prov_code,
            prov_name,
            region,
            pos_level
        ))
    cursor.executemany(
        "INSERT INTO debezium.dim_pos_locations (pos_code, pos_name, province_code, province_name, region, pos_level) "
        "VALUES (:1, :2, :3, :4, :5, :6)",
        pos_data
    )
    print("✅ Hoàn tất khởi tạo 200 Customers & 200 POS Locations!")

def generate_bulk_facts(target_records=10000000, batch_size=25000):
    print(f"🚀 [2/3] Bắt đầu sinh ngẫu nhiên {target_records:,} bản ghi Facts (Logistics EMS)...")
    start_time = time.time()

    conn = oracledb.connect(**DB_CONFIG)
    conn.autocommit = False
    cursor = conn.cursor()

    # Seed Master Data first
    try:
        seed_dimensions(cursor)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"⚠️ Dữ liệu Master đã tồn tại hoặc có lỗi nhỏ: {e}")

    customer_ids = [f"CUST-{i:04d}" for i in range(1, 201)]
    pos_codes = [f"POS-{i:04d}" for i in range(1, 201)]
    courier_ids = [f"COU-{i:03d}" for i in range(1, 101)]

    total_inserted = 0
    start_date = datetime.now() - timedelta(days=365)

    print(f"⚡ Đang thực thi chèn khối lượng lớn (Batch size: {batch_size:,} dòng/lần)...")

    while total_inserted < target_records:
        current_batch_size = min(batch_size, target_records - total_inserted)
        
        bookings_batch = []
        delivery_remun_batch = []
        return_remun_batch = []
        events_batch = []

        for _ in range(current_batch_size):
            b_id = f"BK-{uuid.uuid4().hex[:12].upper()}"
            item_code = f"EA{random.randint(100000000, 999999999)}VN"
            booking_dt = start_date + timedelta(seconds=random.randint(0, 31536000))
            cust_id = random.choice(customer_ids)
            serv_id = random.choice(SERVICES)
            send_pos = random.choice(pos_codes)
            recv_pos = random.choice([p for p in pos_codes if p != send_pos])
            weight_g = random.randint(100, 15000)
            wt_id = random.choice(WEIGHT_TIERS)
            rt_id = random.choice(ROUTING_TYPES)
            
            main_fee = round(random.uniform(15000, 250000), 2)
            sur_fee = round(main_fee * 0.1, 2)
            discount = round(main_fee * 0.05, 2) if random.random() > 0.7 else 0
            tot_rev = main_fee + sur_fee - discount
            cost_amt = round(tot_rev * 0.65, 2)
            
            status_id = random.choice(STATUSES)

            bookings_batch.append((
                b_id, item_code, booking_dt, cust_id, serv_id, send_pos, recv_pos,
                weight_g, wt_id, rt_id, main_fee, sur_fee, discount, tot_rev, cost_amt, status_id
            ))

            # Fact Remuneration & Events
            courier = random.choice(courier_ids)
            if status_id in ["STAT-05", "STAT-04"]: # Successful / Delivering
                delivery_remun_batch.append((
                    f"REM-{uuid.uuid4().hex[:12].upper()}", b_id, courier, recv_pos,
                    booking_dt + timedelta(hours=random.randint(12, 72)), status_id,
                    5000.0, 1000.0 if random.random() > 0.8 else 0
                ))
            elif status_id == "STAT-06": # Returned
                return_remun_batch.append((
                    f"RET-{uuid.uuid4().hex[:12].upper()}", b_id, courier, send_pos,
                    booking_dt + timedelta(hours=random.randint(48, 120)), random.choice(FAILURE_REASONS),
                    3000.0
                ))

            # Fact Event Tracking (3 tracking events per booking)
            events_batch.append((
                f"EVT-{uuid.uuid4().hex[:12].upper()}", b_id, send_pos, booking_dt, "STAT-01", "KTV_THU_GOM", "Chấp nhận bưu gửi"
            ))
            events_batch.append((
                f"EVT-{uuid.uuid4().hex[:12].upper()}", b_id, send_pos, booking_dt + timedelta(hours=6), "STAT-02", "KTV_DONG_GOI", "Đóng chuyến thư trung chuyển"
            ))
            events_batch.append((
                f"EVT-{uuid.uuid4().hex[:12].upper()}", b_id, recv_pos, booking_dt + timedelta(hours=24), status_id, "KTV_PHAT", "Phát bưu gửi"
            ))

        # Bulk INSERT into Oracle
        cursor.executemany(
            "INSERT INTO debezium.shipment_bookings (booking_id, item_code, booking_date, customer_id, service_id, "
            "sending_pos_code, receiving_pos_code, weight_gram, weight_tier_id, routing_type_id, main_fee, sur_fee, "
            "discount_amount, total_revenue, cost_amount, status_id) "
            "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)",
            bookings_batch
        )

        if delivery_remun_batch:
            cursor.executemany(
                "INSERT INTO debezium.delivery_remunerations (remuneration_id, booking_id, courier_id, delivery_pos_code, "
                "delivery_date, delivery_status_id, delivery_fee_amount, bonus_amount) "
                "VALUES (:1, :2, :3, :4, :5, :6, :7, :8)",
                delivery_remun_batch
            )

        if return_remun_batch:
            cursor.executemany(
                "INSERT INTO debezium.return_remunerations (return_id, booking_id, courier_id, return_pos_code, "
                "return_date, failure_reason_id, return_fee_amount) "
                "VALUES (:1, :2, :3, :4, :5, :6, :7)",
                return_remun_batch
            )

        cursor.executemany(
            "INSERT INTO debezium.shipment_event_trackings (event_id, booking_id, event_pos_code, event_time, "
            "status_id, operator_user, notes) "
            "VALUES (:1, :2, :3, :4, :5, :6, :7)",
            events_batch
        )

        conn.commit()
        total_inserted += current_batch_size
        elapsed = time.time() - start_time
        speed = total_inserted / elapsed if elapsed > 0 else 0
        percent = (total_inserted / target_records) * 100

        print(f"⏳ Tiến độ: {total_inserted:,} / {target_records:,} bản ghi [{percent:.1f}%] - Tốc độ: {speed:,.0f} recs/sec")

    cursor.close()
    conn.close()

    total_time = time.time() - start_time
    print(f"\n🎉 [3/3] HOÀN THÀNH TẠO DỮ LIỆU CỠ LỚN!")
    print(f"📊 Tổng số bản ghi Shipment Bookings: {total_inserted:,}")
    print(f"📊 Tổng số bản ghi Event Tracking: {total_inserted * 3:,}")
    print(f"⏱️ Tổng thời gian thực thi: {total_time:.2f} giây ({total_inserted / total_time:,.0f} bản ghi/giây)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sinh dữ liệu mô phỏng Logistics EMS cỡ lớn cho Oracle CDC")
    parser.add_argument("--records", type=int, default=10000000, help="Số lượng bản ghi Booking cần sinh (Mặc định: 10,000,000)")
    parser.add_argument("--batch", type=int, default=25000, help="Kích thước batch ghi Oracle (Mặc định: 25,000)")
    args = parser.parse_args()

    generate_bulk_facts(target_records=args.records, batch_size=args.batch)
