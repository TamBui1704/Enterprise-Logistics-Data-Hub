#!/bin/bash
set -e

echo "=== KHỞI TẠO APACHE SUPERSET BAN ĐẦU ==="
echo "1. Tạo tài khoản Admin (admin / admin)..."
docker exec -it superset_bi superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname Logistics \
  --email admin@logistics.com \
  --password admin || true

echo "2. Chạy migration metadata database..."
docker exec -it superset_bi superset db upgrade

echo "3. Khởi tạo roles và permissions mặc định..."
docker exec -it superset_bi superset init

echo "4. Tạo các tài khoản người dùng mẫu (manager_north, manager_south, driver_hcm_01)..."
docker exec -it superset_bi superset fab create-user \
  --role Gamma \
  --username manager_north \
  --firstname North \
  --lastname Manager \
  --email manager_north@logistics.com \
  --password password123 || true

docker exec -it superset_bi superset fab create-user \
  --role Gamma \
  --username manager_south \
  --firstname South \
  --lastname Manager \
  --email manager_south@logistics.com \
  --password password123 || true

docker exec -it superset_bi superset fab create-user \
  --role Gamma \
  --username driver_hcm_01 \
  --firstname Driver \
  --lastname HCM \
  --email driver01@logistics.com \
  --password password123 || true

echo "=== HOÀN TẤT! Đăng nhập Superset tại: http://localhost:8088 (admin / admin) ==="
