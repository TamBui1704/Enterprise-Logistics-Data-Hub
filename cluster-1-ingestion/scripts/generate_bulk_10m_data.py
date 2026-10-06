import os
import time
import random
import uuid
import argparse
from datetime import datetime, timedelta
import oracledb
import multiprocessing as mp

DB_CONFIG = {
    "user": "debezium",
    "password": "dbz",
    "dsn": os.getenv("ORACLE_DSN", "localhost:1521/FREEPDB1")
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

customer_ids = [f"CUST-{i:04d}" for i in range(1, 201)]
pos_codes = [f"POS-{i:04d}" for i in range(1, 201)]
courier_ids = [f"COU-{i:03d}" for i in range(1, 101)]

def seed_dimensions():
    conn = oracledb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("🌱 Đang khởi tạo Dữ liệu Master Dimensions...")
    
    try:
        cursor.execute("TRUNCATE TABLE debezium.delivery_remunerations")
        cursor.execute("TRUNCATE TABLE debezium.return_remunerations")
        cursor.execute("TRUNCATE TABLE debezium.shipment_event_trackings")
        cursor.execute("TRUNCATE TABLE debezium.shipment_bookings")
        cursor.execute("DELETE FROM debezium.dim_customers")
        cursor.execute("DELETE FROM debezium.dim_pos_locations")
        conn.commit()
    except Exception as e:
        pass

    customers_data = []
    for i in range(1, 201):
        prov_code, prov_name, region = random.choice(PROVINCES)
        cust_type = random.choice(["B2B", "RETAIL", "ENTERPRISE"])
        customers_data.append((f"CUST-{i:04d}", f"Khách hàng {i}", cust_type, f"010{random.randint(1000000, 9999999)}", prov_name, region))
    cursor.executemany("INSERT INTO debezium.dim_customers (customer_id, customer_name, customer_type, tax_code, province, region) VALUES (:1, :2, :3, :4, :5, :6)", customers_data)

    pos_data = []
    for i in range(1, 201):
        prov_code, prov_name, region = random.choice(PROVINCES)
        pos_level = random.choice(["PROVINCIAL", "DISTRICT", "COMMUNE"])
        pos_data.append((f"POS-{i:04d}", f"Bưu cục {i}", prov_code, prov_name, region, pos_level))
    cursor.executemany("INSERT INTO debezium.dim_pos_locations (pos_code, pos_name, province_code, province_name, region, pos_level) VALUES (:1, :2, :3, :4, :5, :6)", pos_data)
    
    conn.commit()
    cursor.close()
    conn.close()

def worker_generate(worker_id, target_records, batch_size, offset):
    conn = oracledb.connect(**DB_CONFIG)
    conn.autocommit = False
    cursor = conn.cursor()
    
    start_date = datetime(2023, 1, 1)
    total_inserted = 0
    
    while total_inserted < target_records:
        current_batch = min(batch_size, target_records - total_inserted)
        bookings_batch = []
        events_batch = []
        deliv_remun_batch = []
        return_remun_batch = []
        
        for i in range(current_batch):
            global_idx = offset + total_inserted + i
            b_id = f"BK-{uuid.uuid4().hex[:12].upper()}"
            item_code = f"EA{100000000 + global_idx}VN"
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

            bookings_batch.append((b_id, item_code, booking_dt, cust_id, serv_id, send_pos, recv_pos, weight_g, wt_id, rt_id, main_fee, sur_fee, discount, tot_rev, cost_amt, status_id))

            for step in range(1, 4):
                evt_id = f"EVT-{uuid.uuid4().hex[:12].upper()}"
                evt_dt = booking_dt + timedelta(hours=step*6)
                pos = send_pos if step == 1 else recv_pos
                events_batch.append((evt_id, b_id, status_id, evt_dt, pos, "AUTO_SYS", f"Ghi nhận trạng thái {step}"))

            if status_id == "STAT-05":
                deliv_remun_batch.append((f"DREM-{uuid.uuid4().hex[:10].upper()}", b_id, random.choice(courier_ids), recv_pos, booking_dt + timedelta(days=2), status_id, round(tot_rev * 0.1, 2), 0))
            elif status_id == "STAT-07":
                return_remun_batch.append((f"RREM-{uuid.uuid4().hex[:10].upper()}", b_id, random.choice(courier_ids), send_pos, booking_dt + timedelta(days=5), round(tot_rev * 0.05, 2), random.choice(FAILURE_REASONS)))

        cursor.executemany("INSERT INTO debezium.shipment_bookings (booking_id, item_code, booking_date, customer_id, service_id, sending_pos_code, receiving_pos_code, weight_gram, weight_tier_id, routing_type_id, main_fee, sur_fee, discount_amount, total_revenue, cost_amount, status_id) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)", bookings_batch)
        cursor.executemany("INSERT INTO debezium.shipment_event_trackings (event_id, booking_id, status_id, event_time, event_pos_code, operator_user, notes) VALUES (:1, :2, :3, :4, :5, :6, :7)", events_batch)
        if deliv_remun_batch: cursor.executemany("INSERT INTO debezium.delivery_remunerations (remuneration_id, booking_id, courier_id, delivery_pos_code, delivery_date, delivery_status_id, delivery_fee_amount, bonus_amount) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)", deliv_remun_batch)
        if return_remun_batch: cursor.executemany("INSERT INTO debezium.return_remunerations (return_id, booking_id, courier_id, return_pos_code, return_date, return_fee_amount, failure_reason_id) VALUES (:1, :2, :3, :4, :5, :6, :7)", return_remun_batch)
        
        conn.commit()
        total_inserted += current_batch
        print(f"[{worker_id}] Tiến độ: {total_inserted:,} / {target_records:,}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=10000000)
    parser.add_argument("--batch", type=int, default=25000)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    seed_dimensions()
    
    print(f"🚀 Bắt đầu sinh {args.records:,} bản ghi với {args.workers} workers...")
    start_time = time.time()
    
    records_per_worker = args.records // args.workers
    processes = []
    
    for w in range(args.workers):
        offset = w * records_per_worker
        tr = records_per_worker if w < args.workers - 1 else args.records - offset
        p = mp.Process(target=worker_generate, args=(f"Worker-{w+1}", tr, args.batch, offset))
        processes.append(p)
        p.start()
        
    for p in processes:
        p.join()
        
    total_time = time.time() - start_time
    print(f"\n🎉 HOÀN THÀNH! Tổng thời gian: {total_time:.2f}s ({args.records / total_time:,.0f} bản ghi/s)")
