// Khai báo thư viện kết nối Database (Ví dụ: dùng thư viện pg hoặc clickhouse-client)
// const { createClient } = require('@clickhouse/client');
// const dbClient = createClient({ host: 'http://clickhouse:8123', username: 'default', password: '' });

// Thư viện gọi HTTP API để check pass với Keycloak
// const fetch = require('node-fetch');

module.exports = {
  /* =========================================================
     1. XÁC THỰC QUA REST API (Dành cho AI Agent, Frontend App)
     ========================================================= */
  checkAuth: async (req, auth) => {
    // 1. Lấy username từ JWT Token (Keycloak đã cấp)
    const username = req.securityContext?.preferred_username || req.securityContext?.user;
    if (!username) throw new Error('Token không hợp lệ hoặc thiếu username');

    // 2. TRUY VẤN ĐỘNG XUỐNG CLICKHOUSE (KHÔNG FIX CỨNG)
    // Code này chạy thật ở Production: Query vào bảng quyền để lấy list tỉnh thành
    // const query = `SELECT province_access, role FROM dim_user_permissions WHERE username = '${username}'`;
    // const result = await dbClient.query({ query, format: 'JSONEachRow' });
    // const userPerm = await result.json(); // Lấy row kết quả
    
    // Nếu user không có trong DB phân quyền -> Chặn truy cập
    // if (!userPerm || userPerm.length === 0) throw new Error('User không có quyền truy cập BI!');

    // 3. Nạp quyền động vào Security Context
    // req.securityContext.role = userPerm[0].role;
    // req.securityContext.province_access = userPerm[0].province_access; // Là 1 array ['Hồ Chí Minh', 'Đồng Nai']
  },

  /* =========================================================
     2. XÁC THỰC QUA SQL API (Dành cho Superset, PowerBI, Excel)
     ========================================================= */
  checkSqlAuth: async (req, user) => {
    // THỰC TẾ PRODUCTION VỚI HÀNG TRIỆU USER: 
    // Hệ thống hoàn toàn không biết user là ai trước. Mọi thứ là động!

    // Bước 1: Gửi user và mật khẩu (req.password) lên Keycloak để xác thực xem pass đúng không
    /*
    const tokenResponse = await fetch('http://keycloak:8080/realms/master/protocol/openid-connect/token', {
       method: 'POST', 
       headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
       body: `client_id=cubejs&username=${user}&password=${req.password}&grant_type=password`
    });
    if (!tokenResponse.ok) {
        throw new Error('Sai tài khoản hoặc mật khẩu đăng nhập!');
    }
    */

    // Bước 2: TRUY VẤN ĐỘNG XUỐNG CLICKHOUSE lấy quyền (Giống hàm trên)
    /*
    const query = `SELECT province_access, role FROM dim_user_permissions WHERE username = '${user}'`;
    const result = await dbClient.query({ query, format: 'JSONEachRow' });
    const userPerm = await result.json();

    if (!userPerm || userPerm.length === 0) {
        throw new Error('User đăng nhập thành công nhưng không được cấp quyền xem Dashboard!');
    }

    // Bước 3: Trả về kết quả động
    return {
      password: req.password, // Xác nhận Pass khớp
      securityContext: { 
        role: userPerm[0].role, 
        province_access: userPerm[0].province_access // Quyền lấy hoàn toàn từ Database
      }
    };
    */

    // TẠM THỜI BÁO LỖI NẾU CHẠY THỬ (Vì chưa setup xong DB ClickHouse thật)
    throw new Error('Vui lòng bỏ comment code ClickHouse ở trên để chạy thực tế!');
  }
};
