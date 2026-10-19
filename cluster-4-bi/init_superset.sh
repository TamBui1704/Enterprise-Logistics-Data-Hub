#!/bin/bash
set -e

echo "=========================================================="
echo "    KHỞI TẠO APACHE SUPERSET METADATA & PERMISSIONS       "
echo "=========================================================="

echo "1. Tạo tài khoản Local Admin dự phòng (admin / admin)..."
# Giữ tài khoản local admin để quản trị khẩn cấp khi Keycloak bảo trì
docker exec -it superset_bi superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname Logistics \
  --email admin@logistics.com \
  --password admin || true

echo "2. Chạy migration metadata database..."
docker exec -it superset_bi superset db upgrade

echo "3. Khởi tạo roles và permissions mặc định (Admin, Alpha, Gamma, SQL Lab)..."
docker exec -it superset_bi superset init

echo "----------------------------------------------------------"
echo "LƯU Ý VỀ TÀI KHOẢN NGƯỜI DÙNG TRONG HỆ THỐNG ENTERPRISE:"
echo "Toàn bộ tài khoản (manager_north, manager_south, driver_hcm_01)"
echo "hiện được quản trị TẬP TRUNG trên Keycloak Realm 'logistics'."
echo "Khi người dùng bấm 'Sign in with keycloak', Superset sẽ TỰ ĐỘNG"
echo "đồng bộ danh tính (JIT Provisioning) và gán role mặc định."
echo "----------------------------------------------------------"

# Nếu chạy chế độ Standalone không có Keycloak (ENABLE_KEYCLOAK=false),
# bỏ comment các dòng dưới đây để tạo user local:
#
# docker exec -it superset_bi superset fab create-user \
#   --role Gamma --username manager_north --firstname North --lastname Manager \
#   --email manager_north@logistics.com --password password123 || true
#
# docker exec -it superset_bi superset fab create-user \
#   --role Gamma --username manager_south --firstname South --lastname Manager \
#   --email manager_south@logistics.com --password password123 || true
#
# docker exec -it superset_bi superset fab create-user \
#   --role Gamma --username driver_hcm_01 --firstname Driver --lastname HCM \
#   --email driver01@logistics.com --password password123 || true

echo "=== HOÀN TẤT! Superset sẵn sàng tích hợp Keycloak SSO! ==="
