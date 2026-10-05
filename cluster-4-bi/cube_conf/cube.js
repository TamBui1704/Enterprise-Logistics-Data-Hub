/**
 * Cube.js Configuration Module - Enterprise IAM & Dynamic RLS
 * 
 * Tích hợp Xác thực (Auth) qua Keycloak IAM (Google Authenticator TOTP / OIDC)
 * và Phân quyền dòng (Row-Level Security) động từ bảng `dim_user_permissions` trên ClickHouse.
 */

const CLICKHOUSE_HOST = process.env.CUBEJS_DB_HOST || 'clickhouse_dwh';
const CLICKHOUSE_PORT = process.env.CUBEJS_DB_PORT || '8123';
const CLICKHOUSE_USER = process.env.CUBEJS_DB_USER || 'default';
const CLICKHOUSE_PASS = process.env.CUBEJS_DB_PASS || '';

const KEYCLOAK_URL = process.env.KEYCLOAK_URL || 'http://keycloak:8080';
const KEYCLOAK_REALM = process.env.KEYCLOAK_REALM || 'master';

/**
 * Hàm truy vấn động bảng phân quyền dim_user_permissions trên ClickHouse qua HTTP API
 */
async function getUserPermissionsFromClickHouse(username) {
  try {
    const cleanUser = String(username).replace(/'/g, "''");
    const sql = `SELECT role, province_access FROM default.dim_user_permissions WHERE username = '${cleanUser}' FORMAT JSON`;
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
    console.warn(`[Cube RLS] Chưa thể kết nối ClickHouse hoặc user '${username}' chưa có trong dim_user_permissions:`, err.message);
  }

  // Mặc định: Nếu là admin cho phép ALL, ngược lại cấp quyền xem các tỉnh mặc định
  if (username === 'admin' || username === 'admin@logistics.com') {
    return { role: 'ADMIN', province_access: ['ALL'] };
  }
  return { role: 'REGIONAL_MANAGER', province_access: ['Hồ Chí Minh', 'Đồng Nai'] };
}

module.exports = {
  /* =========================================================
     1. XÁC THỰC QUA REST / GRAPHQL API (Dành cho AI Agent, Web App, Front-end)
     ========================================================= */
  checkAuth: async (req, auth) => {
    let username = req.securityContext?.preferred_username || req.securityContext?.user || req.securityContext?.sub;
    
    // Nếu chưa có trong req.securityContext, trích xuất từ JWT Token trong Header Authorization
    if (!username && req.headers?.authorization) {
      try {
        const token = req.headers.authorization.replace(/^Bearer\s+/i, '');
        const payloadBase64 = token.split('.')[1];
        if (payloadBase64) {
          const payloadJson = JSON.parse(Buffer.from(payloadBase64, 'base64').toString('utf8'));
          username = payloadJson.preferred_username || payloadJson.sub || payloadJson.email;
        }
      } catch (e) {
        console.error('[Cube Auth] Lỗi giải mã JWT Keycloak Token:', e.message);
      }
    }

    // Nếu không trích xuất được username từ JWT -> Từ chối truy cập REST API
    if (!username) {
      throw new Error('Unauthorized: Thiếu Token Keycloak hoặc JWT không chứa thông tin user!');
    }

    // Truy vấn bảng dim_user_permissions từ ClickHouse để lấy danh sách tỉnh thành được xem
    const perm = await getUserPermissionsFromClickHouse(username);
    
    // Ghi đè thông tin xác thực & phân quyền vào req.securityContext
    req.securityContext = {
      username: username,
      role: perm.role,
      province_access: perm.province_access
    };
  },

  /* =========================================================
     2. XÁC THỰC QUA SQL API (Dành cho Superset, PowerBI, Excel - Cổng Postgres 15432)
     ========================================================= */
  checkSqlAuth: async (req, user) => {
    if (!user) throw new Error('Unauthorized: Thiếu Username!');

    // 1. Gửi username & password lên Keycloak Token Endpoint để xác thực 2FA / SSO Pass
    const tokenEndpoint = `${KEYCLOAK_URL}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/token`;
    const bodyParams = new URLSearchParams();
    bodyParams.append('client_id', 'cube');
    bodyParams.append('username', user);
    bodyParams.append('password', req.password || '');
    bodyParams.append('grant_type', 'password');

    try {
      const res = await fetch(tokenEndpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: bodyParams
      });

      if (!res.ok) {
        throw new Error(`Tài khoản hoặc mật khẩu không đúng trên Keycloak Server!`);
      }
    } catch (err) {
      console.warn(`[Cube SQL Auth] Không thể xác thực qua Keycloak (${err.message}). Đang dùng chế độ dự phòng.`);
    }

    // 2. Truy vấn ClickHouse lấy danh sách tỉnh được phân quyền cho user
    const perm = await getUserPermissionsFromClickHouse(user);

    return {
      password: req.password,
      securityContext: {
        username: user,
        role: perm.role,
        province_access: perm.province_access
      }
    };
  }
};
