-- ===================================================================
-- Ma Trận Phân Quyền Dữ Liệu Tầng Ngữ Nghĩa (Data Governance Matrix)
-- Được đọc động bởi Cube.js Semantic Layer để thực thi Dynamic RLS
-- ===================================================================

CREATE TABLE IF NOT EXISTS default.dim_user_permissions (
    username String,                -- Username đăng nhập (khớp với Superset / Keycloak / App)
    role String,                    -- Vai trò: ADMIN, REGIONAL_MANAGER, DRIVER, STAFF
    province_access Array(String),  -- Danh sách các tỉnh được phép xem (hoặc ['ALL'] nếu toàn quyền)
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY username;

-- Thêm dữ liệu phân quyền mẫu
INSERT INTO default.dim_user_permissions (username, role, province_access) VALUES 
('admin', 'ADMIN', ['ALL']),
('admin@logistics.com', 'ADMIN', ['ALL']),
('manager_north', 'REGIONAL_MANAGER', ['Hà Nội', 'Hải Phòng', 'Bắc Ninh']),
('manager_south', 'REGIONAL_MANAGER', ['Hồ Chí Minh', 'Đồng Nai', 'Bình Dương']),
('driver_hcm_01', 'DRIVER', ['Hồ Chí Minh']);
