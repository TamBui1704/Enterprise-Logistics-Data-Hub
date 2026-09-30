-- Schema Khởi tạo Dữ liệu Logistics (Enterprise Logistics Hub)

CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    customer_type VARCHAR(50) DEFAULT 'B2B',
    region VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE warehouses (
    warehouse_id VARCHAR(50) PRIMARY KEY,
    warehouse_name VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    capacity INT DEFAULT 10000,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(customer_id),
    warehouse_id VARCHAR(50) REFERENCES warehouses(warehouse_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(15, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'CREATED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE shipments (
    shipment_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id),
    carrier_name VARCHAR(50) NOT NULL,
    tracking_number VARCHAR(100) UNIQUE NOT NULL,
    origin_city VARCHAR(50) NOT NULL,
    destination_city VARCHAR(50) NOT NULL,
    status VARCHAR(50) DEFAULT 'IN_TRANSIT',
    estimated_delivery TIMESTAMP,
    actual_delivery TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed Initial Master Data
INSERT INTO warehouses (warehouse_id, warehouse_name, city, capacity) VALUES
('WH-HN-01', 'Kho Trung Chuyển Hà Nội', 'Hà Nội', 50000),
('WH-HCM-01', 'Kho Tổng Hồ Chí Minh', 'Hồ Chí Minh', 80000),
('WH-DN-01', 'Kho Miền Trung Đà Nẵng', 'Đà Nẵng', 30000);

INSERT INTO customers (customer_id, customer_name, customer_type, region) VALUES
('CUST-001', 'Công ty EMS Express', 'ENTERPRISE', 'Miền Bắc'),
('CUST-002', 'Tập đoàn Bán lẻ Viettel', 'RETAIL', 'Miền Nam'),
('CUST-003', 'Chuỗi Kho Vận Logistics A', 'B2B', 'Miền Trung');
