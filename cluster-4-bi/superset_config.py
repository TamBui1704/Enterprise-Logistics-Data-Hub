# Superset Enterprise Configuration
import os
import jwt
from flask_appbuilder.security.manager import AUTH_DB, AUTH_OAUTH
from superset.security import SupersetSecurityManager

# -------------------------------------------------------------------
# 1. Feature Flags
# -------------------------------------------------------------------
FEATURE_FLAGS = {
    # Kích hoạt phân quyền Dashboard theo Role (Tương tự Power BI Workspaces)
    "DASHBOARD_RBAC": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "GENERIC_CHART_AXES": True,
}

# -------------------------------------------------------------------
# 2. Cấu hình Xác thực OIDC qua Keycloak (IAM)
# -------------------------------------------------------------------
ENABLE_KEYCLOAK = os.getenv("ENABLE_KEYCLOAK", "true").lower() == "true"

if ENABLE_KEYCLOAK:
    AUTH_TYPE = AUTH_OAUTH
    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = "Gamma"  # Role mặc định cho nhân viên mới nếu chưa gán role

    # Bật đồng bộ Role từ Keycloak sang Superset mỗi lần đăng nhập
    AUTH_ROLES_SYNC_AT_LOGIN = True

    # Bảng ánh xạ Role: "Role trên Keycloak" -> ["Role trên Superset"]
    AUTH_ROLES_MAPPING = {
        "admin": ["Admin"],                # Realm role 'admin' -> Quản trị viên tối cao Superset
        "superset_admin": ["Admin"],       # Role tùy chỉnh
        "superset_alpha": ["Alpha"],       # Role phân tích viên (được tạo chart, dashboard)
        "superset_gamma": ["Gamma"],       # Role người xem báo cáo
        "manager": ["Gamma"],
    }

    # URL nội bộ giữa các container Docker
    KEYCLOAK_INTERNAL_URL = os.getenv("KEYCLOAK_INTERNAL_URL", "http://keycloak:8080")
    # URL bên ngoài mà trình duyệt của người dùng truy cập
    KEYCLOAK_EXTERNAL_URL = os.getenv("KEYCLOAK_EXTERNAL_URL", "http://localhost:8080")
    KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "logistics")
    KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "superset")
    KEYCLOAK_CLIENT_SECRET = os.getenv("KEYCLOAK_CLIENT_SECRET", "super-secret-keycloak-client-secret")

    OAUTH_PROVIDERS = [
        {
            "name": "keycloak",
            "icon": "fa-key",
            "token_key": "access_token",
            "remote_app": {
                "client_id": KEYCLOAK_CLIENT_ID,
                "client_secret": KEYCLOAK_CLIENT_SECRET,
                "client_kwargs": {
                    "scope": "openid email profile roles"
                },
                "api_base_url": f"{KEYCLOAK_INTERNAL_URL}/realms/{KEYCLOAK_REALM}/protocol/",
                "access_token_url": f"{KEYCLOAK_INTERNAL_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token",
                "authorize_url": f"{KEYCLOAK_EXTERNAL_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/auth",
                "server_metadata_url": f"{KEYCLOAK_INTERNAL_URL}/realms/{KEYCLOAK_REALM}/.well-known/openid-configuration",
            }
        }
    ]

    # Custom Security Manager để bóc tách Role từ Token JWT của Keycloak
    class CustomSecurityManager(SupersetSecurityManager):
        def get_oauth_user_info(self, provider: str, resp: dict):
            userinfo = super().get_oauth_user_info(provider, resp)
            if provider == "keycloak":
                token = resp.get("access_token")
                if token:
                    try:
                        # Giải mã JWT Token (Keycloak chứa roles ở realm_access và resource_access)
                        decoded = jwt.decode(token, options={"verify_signature": False})
                        realm_roles = decoded.get("realm_access", {}).get("roles", [])
                        client_roles = decoded.get("resource_access", {}).get("superset", {}).get("roles", [])
                        userinfo["role_keys"] = list(set(realm_roles + client_roles))
                    except Exception:
                        pass
            return userinfo

    CUSTOM_SECURITY_MANAGER = CustomSecurityManager

else:
    AUTH_TYPE = AUTH_DB
