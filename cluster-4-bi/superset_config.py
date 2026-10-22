# Superset Enterprise Configuration
import os
import jwt
from flask import redirect, session, request
from urllib.parse import quote
from flask_appbuilder import expose
from flask_appbuilder.security.manager import AUTH_DB, AUTH_OAUTH
from flask_appbuilder.security.views import AuthOAuthView
from flask_login import logout_user
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

    # URL bên ngoài mà trình duyệt của người dùng truy cập
    KEYCLOAK_EXTERNAL_URL = os.getenv("KEYCLOAK_EXTERNAL_URL", "http://sso.192.168.63.106.nip.io:8080")
    # URL nội bộ (đồng nhất với external để token issuer và userinfo khớp 100%)
    KEYCLOAK_INTERNAL_URL = os.getenv("KEYCLOAK_INTERNAL_URL", KEYCLOAK_EXTERNAL_URL)
    KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "logistics")
    KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "superset")
    KEYCLOAK_CLIENT_SECRET = os.getenv("KEYCLOAK_CLIENT_SECRET", "super-secret-keycloak-client-secret")

    # Đường dẫn đăng xuất SSO OIDC
    # Đường dẫn đăng xuất SSO OIDC
    LOGOUT_REDIRECT_URL = f"{KEYCLOAK_EXTERNAL_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/logout?client_id={KEYCLOAK_CLIENT_ID}"

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

    # Custom OAuth View để hỗ trợ OIDC Single Sign-Out (RP-Initiated Logout)
    class CustomAuthOAuthView(AuthOAuthView):
        @expose("/logout/")
        def logout(self):
            logout_user()
            session.clear()
            post_uri = quote(request.host_url + 'login/')
            keycloak_logout_url = (
                f"{KEYCLOAK_EXTERNAL_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/logout"
                f"?client_id={KEYCLOAK_CLIENT_ID}&post_logout_redirect_uri={post_uri}"
            )
            return redirect(keycloak_logout_url)

    # Custom Security Manager để bóc tách Role từ Token JWT của Keycloak và quản lý phiên đăng xuất SSO
    class CustomSecurityManager(SupersetSecurityManager):
        authoauthview = CustomAuthOAuthView

        def __init__(self, appbuilder):
            super().__init__(appbuilder)
            if self.auth_type == AUTH_OAUTH and "keycloak" in self.oauth_remotes:
                keycloak_remote = self.oauth_remotes["keycloak"]
                orig_parse_id_token = keycloak_remote.parse_id_token

                def relaxed_parse_id_token(token, nonce, claims_options=None, claims_cls=None, leeway=120):
                    if claims_options is None:
                        claims_options = {}
                    # Chấp nhận mọi Issuer kết thúc bằng /realms/{KEYCLOAK_REALM} (hỗ trợ cả internal docker, localhost, nip.io, LAN IP)
                    claims_options["iss"] = {
                        "validate": lambda claims, value: value.endswith(f"/realms/{KEYCLOAK_REALM}")
                    }
                    return orig_parse_id_token(token, nonce, claims_options=claims_options, claims_cls=claims_cls, leeway=leeway)

                keycloak_remote.parse_id_token = relaxed_parse_id_token

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

# Cookie & Security settings for HTTP LAN access
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'

TALISMAN_CONFIG = {
    'content_security_policy': {
        'default-src': ["'self'"],
        'img-src': ["'self'", 'blob:', 'data:'],
        'worker-src': ["'self'", 'blob:'],
        'connect-src': ["'self'", 'https://api.mapbox.com', 'https://events.mapbox.com'],
        'object-src': "'none'",
        'style-src': ["'self'", "'unsafe-inline'"],
        'script-src': ["'self'", "'strict-dynamic'"],
    },
    'content_security_policy_nonce_in': ['script-src'],
    'force_https': False,
    'session_cookie_secure': False,
}
