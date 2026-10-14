# Superset Enterprise Configuration
import os

FEATURE_FLAGS = {
    # Kích hoạt phân quyền Dashboard theo Role (Tương tự Power BI Workspaces)
    "DASHBOARD_RBAC": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "GENERIC_CHART_AXES": True,
}
