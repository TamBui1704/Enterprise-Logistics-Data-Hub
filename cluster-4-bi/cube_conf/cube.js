/**
 * Cube.js Configuration Module - Enterprise Dynamic RLS & RBAC
 * 
 * Phân quyền đa chiều:
 * 1. Role-Based Access Control (RBAC theo vai trò: ADMIN, REGIONAL_MANAGER, DRIVER)
 * 2. Row-Level Security (RLS theo địa bàn tỉnh thành: province_access)
 * Dữ liệu phân quyền được nạp trực tiếp từ ClickHouse `default.dim_user_permissions`.
 */

const CLICKHOUSE_HOST = process.env.CUBEJS_DB_HOST || 'clickhouse_dwh';
const CLICKHOUSE_PORT = process.env.CUBEJS_DB_PORT || '8123';
const CLICKHOUSE_USER = process.env.CUBEJS_DB_USER || 'default';
const CLICKHOUSE_PASS = process.env.CUBEJS_DB_PASS || '';

/**
 * HÀM 1: getUserPermissionsFromClickHouse(username)
 * -------------------------------------------------------------
 * Nhiệm vụ: Gọi HTTP API sang ClickHouse để truy vấn ma trận phân quyền
 * Input: username (ví dụ: 'manager_north', 'driver_hcm_01')
 * Output: { role: '...', province_access: ['...'] }
 */
async function getUserPermissionsFromClickHouse(username) {
  try {
    const cleanUser = String(username).replace(/'/g, "''");
    const sql = `SELECT role, province_access FROM default.dim_user_permissions WHERE username = '${cleanUser}' LIMIT 1 FORMAT JSON`;
    const url = `http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/?query=${encodeURIComponent(sql)}`;
    
    const headers = {};
    if (CLICKHOUSE_USER) {
      headers['X-ClickHouse-User'] = CLICKHOUSE_USER;
      headers['X-ClickHouse-Key'] = CLICKHOUSE_PASS;
    }

    const res = await fetch(url, { headers });
    if (res.ok) {
      const json = await res.json();
      if (json.data && json.data.length > 0) {
        const row = json.data[0];
        let access = row.province_access;
        if (typeof access === 'string') {
          try { access = JSON.parse(access); } catch (e) { access = [access]; }
        }
        return {
          role: row.role || 'USER',
          province_access: Array.isArray(access) ? access : [access]
        };
      }
    }
  } catch (err) {
    console.error(`[Cube RLS] Lỗi truy vấn dim_user_permissions từ ClickHouse:`, err.message);
  }

  // Nếu user không tồn tại trong ClickHouse -> Mặc định coi là GUEST không có quyền
  return { role: 'GUEST', province_access: [] };
}

module.exports = {
  /**
   * HÀM 2: checkAuth(req, auth)
   * -----------------------------------------------------------
   * Cửa khẩu bảo vệ REST API & GraphQL (Cổng 4000)
   * Phục vụ: AI Agent, Web Application (React/Vue), Script Curl
   * Nhiệm vụ:
   *   1. Xác định danh tính user (qua Header 'x-user', Token JWT, hoặc query param)
   *   2. Đọc quyền từ ClickHouse và gắn vào req.securityContext
   */
  checkAuth: async (req, auth) => {
    let username = req.headers?.['x-user'] || req.query?.user;

    // Trích xuất từ JWT Bearer token nếu có
    if (!username && req.headers?.authorization) {
      const token = req.headers.authorization.replace(/^Bearer\s+/i, '');
      const payloadBase64 = token.split('.')[1];
      if (payloadBase64) {
        let payloadJson;
        try {
          payloadJson = JSON.parse(Buffer.from(payloadBase64, 'base64').toString('utf8'));
        } catch (e) {
          throw new Error('InvalidToken: Malformed JWT');
        }

        // 1. Kiểm tra thời hạn hết hạn của Token (exp)
        if (payloadJson.exp && Math.floor(Date.now() / 1000) > payloadJson.exp) {
          throw new Error('TokenExpiredError: jwt expired');
        }

        username = payloadJson.preferred_username || payloadJson.sub || payloadJson.username;
      }
    }

    // Mặc định cho Developer Playground UI
    if (!username) {
      username = 'admin';
    }

    // Đọc quyền thực tế từ ClickHouse
    const perm = await getUserPermissionsFromClickHouse(username);

    req.securityContext = {
      username: username,
      role: perm.role,
      province_access: perm.province_access
    };
  },

  /**
   * HÀM 3: checkSqlAuth(req, user, password)
   * -----------------------------------------------------------
   * Cửa khẩu bảo vệ SQL API (Cổng 5432 / 15432 giả lập PostgreSQL)
   * Phục vụ: Apache Superset, PowerBI, Tableau, DBeaver, Excel
   * Nhiệm vụ:
   *   1. Nhận username & password từ client kết nối Postgres
   *   2. Tra cứu ClickHouse để gán quyền vào session.securityContext
   */
  checkSqlAuth: async (req, user, password) => {
    if (!user) throw new Error('Unauthorized: Thiếu Username kết nối!');

    // 1. Kiểm tra Mật khẩu bắt buộc (Không cho phép bypass/skip)
    const REQUIRED_SQL_PASSWORD = process.env.CUBEJS_SQL_USER_PASSWORD || 'logistics-super-secret-sql-pass';
    if (!password || password !== REQUIRED_SQL_PASSWORD) {
      throw new Error(`Unauthorized: Sai mật khẩu kết nối SQL API cho user '${user}'!`);
    }

    // 2. Tra cứu ma trận phân quyền từ ClickHouse
    const perm = await getUserPermissionsFromClickHouse(user);

    return {
      password: REQUIRED_SQL_PASSWORD,
      skipPasswordCheck: false, // Bắt buộc kiểm tra mật khẩu trong môi trường Production
      securityContext: {
        username: user,
        role: perm.role,
        province_access: perm.province_access
      }
    };
  },

  /**
   * HÀM 4: queryRewrite(query, { securityContext })
   * -----------------------------------------------------------
   * Bộ Não Điều Khiển Thực Thi RLS & RBAC Động
   * Kích hoạt: Ngay trước khi câu lệnh được dịch ra SQL gửi xuống ClickHouse.
   * Nhiệm vụ:
   *   - Kiểm tra vai trò (Role): ADMIN vs REGIONAL_MANAGER vs DRIVER
   *   - Tự động chèn các bộ lọc `filters` phù hợp với từng Cube
   */
  queryRewrite: async (query, { securityContext }) => {
    if (!securityContext) return query;

    const { role, province_access } = securityContext;

    // ---------------------------------------------------------
    // VAI TRÒ 1: ADMIN (Toàn quyền xem tất cả, không bị lọc gì)
    // ---------------------------------------------------------
    if (role === 'ADMIN' || province_access.includes('ALL')) {
      return query;
    }

    query.filters = query.filters || [];

    // Kiểm tra xem câu query này đang truy cập vào Cube nào
    const touchesShipments = (query.measures || []).some(m => m.startsWith('Shipments.')) ||
                            (query.dimensions || []).some(d => d.startsWith('Shipments.'));
    const touchesDriverShifts = (query.measures || []).some(m => m.startsWith('DriverShifts.')) ||
                               (query.dimensions || []).some(d => d.startsWith('DriverShifts.'));

    // ---------------------------------------------------------
    // VAI TRÒ 2: DRIVER (Tài xế giao hàng)
    // ---------------------------------------------------------
    if (role === 'DRIVER') {
      // CẤM tài xế xem báo cáo Doanh thu & Lợi nhuận của công ty
      if (touchesShipments) {
        throw new Error('Access Denied: Tài xế không có quyền truy cập dữ liệu Doanh thu & Đơn hàng (Cube Shipments)!');
      }

      // Chỉ cho phép xem ca chạy thuộc địa bàn tỉnh của mình
      if (touchesDriverShifts) {
        query.filters.push({
          member: 'DriverShifts.sendingProvince',
          operator: 'equals',
          values: province_access.length > 0 ? province_access : ['__NO_ACCESS__']
        });
      }
      return query;
    }

    // ---------------------------------------------------------
    // VAI TRÒ 3: REGIONAL_MANAGER (Trưởng vùng quản lý)
    // ---------------------------------------------------------
    if (role === 'REGIONAL_MANAGER') {
      // Được xem Shipments nhưng bị lọc theo tỉnh thành phụ trách
      if (touchesShipments) {
        query.filters.push({
          member: 'Shipments.sendingProvince',
          operator: 'equals',
          values: province_access.length > 0 ? province_access : ['__NO_ACCESS__']
        });
      }

      // Được xem Ca chạy nhưng bị lọc theo tỉnh thành phụ trách
      if (touchesDriverShifts) {
        query.filters.push({
          member: 'DriverShifts.sendingProvince',
          operator: 'equals',
          values: province_access.length > 0 ? province_access : ['__NO_ACCESS__']
        });
      }
      return query;
    }

    // ---------------------------------------------------------
    // VAI TRÒ 4: NGƯỜI LẠ / GUEST (Chặn không cho ra dữ liệu)
    // ---------------------------------------------------------
    query.filters.push({
      member: touchesShipments ? 'Shipments.sendingProvince' : 'DriverShifts.sendingProvince',
      operator: 'equals',
      values: ['__NO_ACCESS__']
    });

    return query;
  }
};
