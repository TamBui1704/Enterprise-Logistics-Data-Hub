-- ====================================================================
-- Enterprise Logistics Data Hub (EMS Logistics Architecture)
-- Oracle Initialization Script matching DWH Bus Matrix
-- Schema: DEBEZIUM in PDB: ORCLPDB1
-- ====================================================================

ALTER SESSION SET CONTAINER = ORCLPDB1;
ALTER SESSION SET CURRENT_SCHEMA = DEBEZIUM;

-- --------------------------------------------------------------------
-- 1. DROP EXISTING TABLES (Safely for re-runs)
-- --------------------------------------------------------------------
BEGIN EXECUTE IMMEDIATE 'DROP TABLE debezium.debezium_signal CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END; /
BEGIN EXECUTE IMMEDIATE 'DROP TABLE debezium.shipment_event_trackings CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END; /
BEGIN EXECUTE IMMEDIATE 'DROP TABLE debezium.return_remunerations CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END; /
BEGIN EXECUTE IMMEDIATE 'DROP TABLE debezium.delivery_remunerations CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END; /
BEGIN EXECUTE IMMEDIATE 'DROP TABLE debezium.shipment_bookings CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END; /
BEGIN EXECUTE IMMEDIATE 'DROP TABLE debezium.dim_failure_reasons CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END; /
BEGIN EXECUTE IMMEDIATE 'DROP TABLE debezium.dim_delivery_statuses CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END; /
BEGIN EXECUTE IMMEDIATE 'DROP TABLE debezium.dim_routing_types CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END; /
BEGIN EXECUTE IMMEDIATE 'DROP TABLE debezium.dim_weight_tiers CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END; /
BEGIN EXECUTE IMMEDIATE 'DROP TABLE debezium.dim_pos_locations CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END; /
BEGIN EXECUTE IMMEDIATE 'DROP TABLE debezium.dim_services CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END; /
BEGIN EXECUTE IMMEDIATE 'DROP TABLE debezium.dim_customers CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END; /

-- DEBEZIUM SIGNAL TABLE (Dùng cho Ad-hoc Incremental Snapshot)
CREATE TABLE debezium.debezium_signal (
    id VARCHAR2(64) PRIMARY KEY,
    type VARCHAR2(32) NOT NULL,
    data VARCHAR2(2048)
);
ALTER TABLE debezium.debezium_signal ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;


-- --------------------------------------------------------------------
-- 2. CREATE DIMENSION SOURCE TABLES (Master Data)
-- --------------------------------------------------------------------

-- DIM_CUSTOMER (Khách hàng)
CREATE TABLE debezium.dim_customers (
    customer_id VARCHAR2(50) PRIMARY KEY,
    customer_name VARCHAR2(100) NOT NULL,
    customer_type VARCHAR2(50) DEFAULT 'B2B', -- B2B, RETAIL, ENTERPRISE
    tax_code VARCHAR2(30),
    province VARCHAR2(50) NOT NULL,
    region VARCHAR2(50) NOT NULL, -- Miền Bắc, Miền Trung, Miền Nam
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM_SERVICE (Dịch vụ EMS)
CREATE TABLE debezium.dim_services (
    service_id VARCHAR2(50) PRIMARY KEY,
    service_code VARCHAR2(20) UNIQUE NOT NULL, -- EMS_EXPRESS, EMS_FAST, EMS_SAVING, EMS_INTL
    service_name VARCHAR2(100) NOT NULL,
    is_international NUMBER(1) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM_POS_LOCATION (Bưu cục / Tỉnh thành)
CREATE TABLE debezium.dim_pos_locations (
    pos_code VARCHAR2(50) PRIMARY KEY, -- Mã Bưu cục (vd: BC-100000)
    pos_name VARCHAR2(100) NOT NULL,
    province_code VARCHAR2(20) NOT NULL,
    province_name VARCHAR2(50) NOT NULL,
    region VARCHAR2(50) NOT NULL,
    pos_level VARCHAR2(20) DEFAULT 'DISTRICT', -- PROVINCIAL, DISTRICT, COMMUNE
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM_WEIGHT_TIER (Nấc khối lượng)
CREATE TABLE debezium.dim_weight_tiers (
    weight_tier_id VARCHAR2(50) PRIMARY KEY,
    tier_name VARCHAR2(50) NOT NULL, -- 0-250g, 250g-500g, 500g-1kg, 1kg-2kg, 2kg-5kg, >5kg
    min_weight_gram NUMBER(10) NOT NULL,
    max_weight_gram NUMBER(10) NOT NULL
);

-- DIM_ROUTING_TYPE (Luồng vận chuyển)
CREATE TABLE debezium.dim_routing_types (
    routing_type_id VARCHAR2(50) PRIMARY KEY,
    routing_code VARCHAR2(20) UNIQUE NOT NULL, -- INTRA_PROVINCE, INTRA_REGION, INTER_REGION, INTL
    routing_name VARCHAR2(100) NOT NULL
);

-- DIM_DELIVERY_STATUS (Trạng thái phát)
CREATE TABLE debezium.dim_delivery_statuses (
    status_id VARCHAR2(50) PRIMARY KEY,
    status_code VARCHAR2(30) UNIQUE NOT NULL, -- ACCEPTED, IN_TRANSIT, ARRIVED_POS, DELIVERING, SUCCESSFUL, RETURNED, LOST
    status_name VARCHAR2(100) NOT NULL,
    status_group VARCHAR2(50) NOT NULL -- PENDING, IN_PROGRESS, COMPLETED, FAILED
);

-- DIM_FAILURE_REASON (Lý do chuyển hoàn)
CREATE TABLE debezium.dim_failure_reasons (
    failure_reason_id VARCHAR2(50) PRIMARY KEY,
    reason_code VARCHAR2(30) UNIQUE NOT NULL,
    reason_name VARCHAR2(150) NOT NULL
);

-- --------------------------------------------------------------------
-- 3. CREATE TRANSACTIONAL TABLES (Facts Source)
-- --------------------------------------------------------------------

-- SHIPMENT_BOOKINGS (Tiền thân FACT_SHIPMENT_BOOKING & FACT_REVENUE_COST)
CREATE TABLE debezium.shipment_bookings (
    booking_id VARCHAR2(50) PRIMARY KEY,
    item_code VARCHAR2(50) UNIQUE NOT NULL, -- Mã bưu gửi EMS (vd: EA123456789VN)
    booking_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    customer_id VARCHAR2(50) REFERENCES debezium.dim_customers(customer_id),
    service_id VARCHAR2(50) REFERENCES debezium.dim_services(service_id),
    sending_pos_code VARCHAR2(50) REFERENCES debezium.dim_pos_locations(pos_code),
    receiving_pos_code VARCHAR2(50) REFERENCES debezium.dim_pos_locations(pos_code),
    weight_gram NUMBER(10) NOT NULL,
    weight_tier_id VARCHAR2(50) REFERENCES debezium.dim_weight_tiers(weight_tier_id),
    routing_type_id VARCHAR2(50) REFERENCES debezium.dim_routing_types(routing_type_id),
    main_fee NUMBER(15,2) NOT NULL,
    sur_fee NUMBER(15,2) DEFAULT 0,
    discount_amount NUMBER(15,2) DEFAULT 0,
    total_revenue NUMBER(15,2) NOT NULL,
    cost_amount NUMBER(15,2) NOT NULL,
    status_id VARCHAR2(50) REFERENCES debezium.dim_delivery_statuses(status_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DELIVERY_REMUNERATIONS (Tiền thân FACT_DELIVERY_REMUNERATION - Thù lao bưu tá phát)
CREATE TABLE debezium.delivery_remunerations (
    remuneration_id VARCHAR2(50) PRIMARY KEY,
    booking_id VARCHAR2(50) REFERENCES debezium.shipment_bookings(booking_id),
    courier_id VARCHAR2(50) NOT NULL, -- Mã bưu tá
    delivery_pos_code VARCHAR2(50) REFERENCES debezium.dim_pos_locations(pos_code),
    delivery_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    delivery_status_id VARCHAR2(50) REFERENCES debezium.dim_delivery_statuses(status_id),
    delivery_fee_amount NUMBER(15,2) NOT NULL, -- Thù lao phát (vd: 5,000 VNĐ)
    bonus_amount NUMBER(15,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- RETURN_REMUNERATIONS (Tiền thân FACT_RETURN_REMUNERATION - Thù lao chuyển hoàn)
CREATE TABLE debezium.return_remunerations (
    return_id VARCHAR2(50) PRIMARY KEY,
    booking_id VARCHAR2(50) REFERENCES debezium.shipment_bookings(booking_id),
    courier_id VARCHAR2(50) NOT NULL,
    return_pos_code VARCHAR2(50) REFERENCES debezium.dim_pos_locations(pos_code),
    return_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    failure_reason_id VARCHAR2(50) REFERENCES debezium.dim_failure_reasons(failure_reason_id),
    return_fee_amount NUMBER(15,2) NOT NULL, -- Thù lao chuyển hoàn (vd: 3,000 VNĐ)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SHIPMENT_EVENT_TRACKINGS (Tiền thân FACT_SHIPMENT_EVENT_TRACKING - Nhật ký hành trình)
CREATE TABLE debezium.shipment_event_trackings (
    event_id VARCHAR2(50) PRIMARY KEY,
    booking_id VARCHAR2(50) REFERENCES debezium.shipment_bookings(booking_id),
    event_pos_code VARCHAR2(50) REFERENCES debezium.dim_pos_locations(pos_code),
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status_id VARCHAR2(50) REFERENCES debezium.dim_delivery_statuses(status_id),
    operator_user VARCHAR2(50) NOT NULL, -- Khai thác viên
    notes VARCHAR2(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- --------------------------------------------------------------------
-- 4. ENABLE SUPPLEMENTAL LOGGING FOR DEBEZIUM CDC
-- --------------------------------------------------------------------
ALTER TABLE debezium.dim_customers ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE debezium.dim_services ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE debezium.dim_pos_locations ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE debezium.dim_weight_tiers ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE debezium.dim_routing_types ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE debezium.dim_delivery_statuses ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE debezium.dim_failure_reasons ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE debezium.shipment_bookings ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE debezium.delivery_remunerations ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE debezium.return_remunerations ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE debezium.shipment_event_trackings ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- --------------------------------------------------------------------
-- 5. SEED DIMENSION MASTER DATA (Chèn sẵn dữ liệu Danh mục chuẩn)
-- --------------------------------------------------------------------

-- Services
INSERT INTO debezium.dim_services VALUES ('SERV-01', 'EMS_EXPRESS', 'EMS Chuyển phát nhanh', 0, CURRENT_TIMESTAMP);
INSERT INTO debezium.dim_services VALUES ('SERV-02', 'EMS_FAST', 'EMS Hỏa tốc đường không', 0, CURRENT_TIMESTAMP);
INSERT INTO debezium.dim_services VALUES ('SERV-03', 'EMS_SAVING', 'EMS Tiết kiệm đường bộ', 0, CURRENT_TIMESTAMP);
INSERT INTO debezium.dim_services VALUES ('SERV-04', 'EMS_INTL', 'EMS Quốc tế Chuyển phát nhanh', 1, CURRENT_TIMESTAMP);

-- Weight Tiers
INSERT INTO debezium.dim_weight_tiers VALUES ('WT-01', '0g - 250g', 0, 250);
INSERT INTO debezium.dim_weight_tiers VALUES ('WT-02', '250g - 500g', 250, 500);
INSERT INTO debezium.dim_weight_tiers VALUES ('WT-03', '500g - 1000g', 500, 1000);
INSERT INTO debezium.dim_weight_tiers VALUES ('WT-04', '1kg - 2kg', 1000, 2000);
INSERT INTO debezium.dim_weight_tiers VALUES ('WT-05', '2kg - 5kg', 2000, 5000);
INSERT INTO debezium.dim_weight_tiers VALUES ('WT-06', '> 5kg', 5000, 50000);

-- Routing Types
INSERT INTO debezium.dim_routing_types VALUES ('RT-01', 'INTRA_PROVINCE', 'Luồng Vận chuyển Nội tỉnh');
INSERT INTO debezium.dim_routing_types VALUES ('RT-02', 'INTRA_REGION', 'Luồng Vận chuyển Nội miền');
INSERT INTO debezium.dim_routing_types VALUES ('RT-03', 'INTER_REGION', 'Luồng Vận chuyển Liên miền (Bắc-Nam)');
INSERT INTO debezium.dim_routing_types VALUES ('RT-04', 'INTL', 'Luồng Vận chuyển Quốc tế');

-- Delivery Statuses
INSERT INTO debezium.dim_delivery_statuses VALUES ('STAT-01', 'ACCEPTED', 'Đã thu gom / Chấp nhận', 'PENDING');
INSERT INTO debezium.dim_delivery_statuses VALUES ('STAT-02', 'IN_TRANSIT', 'Đang vận chuyển trung chuyển', 'IN_PROGRESS');
INSERT INTO debezium.dim_delivery_statuses VALUES ('STAT-03', 'ARRIVED_POS', 'Đã đến bưu cục phát', 'IN_PROGRESS');
INSERT INTO debezium.dim_delivery_statuses VALUES ('STAT-04', 'DELIVERING', 'Bưu tá đang đi phát', 'IN_PROGRESS');
INSERT INTO debezium.dim_delivery_statuses VALUES ('STAT-05', 'SUCCESSFUL', 'Giao hàng thành công', 'COMPLETED');
INSERT INTO debezium.dim_delivery_statuses VALUES ('STAT-06', 'RETURNED', 'Giao thất bại - Chuyển hoàn', 'FAILED');
INSERT INTO debezium.dim_delivery_statuses VALUES ('STAT-07', 'LOST', 'Thất lạc / Hư hỏng', 'FAILED');

-- Failure Reasons
INSERT INTO debezium.dim_failure_reasons VALUES ('FAIL-01', 'NO_ANSWER', 'Khách hàng không nghe máy (3 lần)');
INSERT INTO debezium.dim_failure_reasons VALUES ('FAIL-02', 'WRONG_ADDRESS', 'Sai địa chỉ người nhận / Không tìm thấy nhà');
INSERT INTO debezium.dim_failure_reasons VALUES ('FAIL-03', 'REFUSED_ORDER', 'Người nhận từ chối nhận (Không đúng hàng)');
INSERT INTO debezium.dim_failure_reasons VALUES ('FAIL-04', 'RESCHEDULED', 'Người nhận hẹn ngày phát khác vượt quá thời gian');
INSERT INTO debezium.dim_failure_reasons VALUES ('FAIL-05', 'DAMAGED_PACKAGE', 'Bưu gửi bị hư hỏng trong quá trình vận chuyển');

COMMIT;
