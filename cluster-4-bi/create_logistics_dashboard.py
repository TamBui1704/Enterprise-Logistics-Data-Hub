#!/usr/bin/env python3
"""
create_logistics_dashboard.py
--------------------------------------------------------------------------
TẠO TỰ ĐỘNG 100% DASHBOARD ĐIỀU HÀNH LOGISTICS ENTERPRISE TRÊN APACHE SUPERSET:
1. Tạo & tối ưu hóa 15 biểu đồ (KPI Cards, Time-series, Bar charts, Donut charts, Table):
   - Đơn vị tiền tệ chuẩn VNĐ, làm tròn đến hàng đơn vị (,.0f).
   - Sửa lỗi định dạng % trên thẻ KPI (,.2f) giúp số hiển thị to rõ, trực quan.
   - Cấu hình đầy đủ granularity_sqla & x_axis cho các biểu đồ phân tích ECharts Bar.
   - Tắt tính năng thanh màu thanh tiến độ ngẫu nhiên trên bảng chi tiết (show_cell_bars: False).
2. Thiết kế bố cục chuẩn 2 Tab:
   - Tab 1: 📊 Tổng Quan Điều Hành (Executive Overview)
   - Tab 2: 🚚 Chi Tiết Vận Hành & Dịch Vụ (Operations & Drilldown)
3. Cấu hình Native Filters lọc động (Dịch vụ, Tỉnh thành, Vùng miền, Trạng thái)
   với scope áp dụng cho toàn bộ 15 biểu đồ.
4. Phân quyền Public Dashboard cho Role 'Gamma' để test Row-Level Security (RLS).
--------------------------------------------------------------------------
"""

import json
import uuid
import sys
from superset.app import create_app

def generate_id(prefix=""):
    return f"{prefix}{uuid.uuid4().hex[:10]}"

def build_logistics_dashboard():
    print("\n==================================================================")
    print("\n🚀 BẮT ĐẦU TẠO / CẬP NHẬT DASHBOARD ĐIỀU HÀNH LOGISTICS ENTERPRISE")
    print("\n==================================================================")

    app = create_app()
    with app.app_context():
        from superset import db
        from superset.models.slice import Slice
        from superset.models.dashboard import Dashboard
        from superset.connectors.sqla.models import SqlaTable
        from flask_appbuilder.security.sqla.models import User, Role

        admin_user = db.session.query(User).filter_by(username="admin").first()
        if not admin_user:
            admin_user = db.session.query(User).first()

        table = db.session.query(SqlaTable).filter_by(table_name="Shipments").first()
        if not table:
            print("\n❌ Không tìm thấy Dataset 'Shipments'! Hãy chạy sync_cube_to_superset.py trước.")
            sys.exit(1)

        datasource_str = f"{table.id}__table"
        print(f"📦 Sử dụng Dataset: '{table.table_name}' (ID: {table.id})")

        # -------------------------------------------------------------
        # 1. ĐỊNH NGHĨA 15 BIỂU ĐỒ (SLICES) VỚI FORMAT & THIẾT LẬP CHUẨN
        # -------------------------------------------------------------
        chart_configs = [
            # === TAB 1: TỔNG QUAN (KPIs + Trends + Breakdown) ===
            {
                "key": "kpi_bookings",
                "name": "Tổng Số Đơn Hàng",
                "old_names": ["Tổng Số Đơn Hàng"],
                "viz": "big_number_total",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "big_number_total",
                    "metric": "totalBookings",
                    "header_font_size": 0.45,
                    "subheader_font_size": 0.15,
                    "y_axis_format": ",.0f",
                    "subheader": "Toàn bộ đơn hàng logistics"
                }
            },
            {
                "key": "kpi_revenue",
                "name": "Tổng Doanh Thu (VNĐ)",
                "old_names": ["Tổng Doanh Thu", "Tổng Doanh Thu (VNĐ)"],
                "viz": "big_number_total",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "big_number_total",
                    "metric": "totalRevenue",
                    "header_font_size": 0.45,
                    "subheader_font_size": 0.15,
                    "y_axis_format": ",.0f",
                    "subheader": "Doanh thu cước vận chuyển (VNĐ)"
                }
            },
            {
                "key": "kpi_profit",
                "name": "Tổng Lợi Nhuận Gộp (VNĐ)",
                "old_names": ["Tổng Lợi Nhuận Gộp", "Tổng Lợi Nhuận Gộp (VNĐ)"],
                "viz": "big_number_total",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "big_number_total",
                    "metric": "totalProfit",
                    "header_font_size": 0.45,
                    "subheader_font_size": 0.15,
                    "y_axis_format": ",.0f",
                    "subheader": "Doanh thu - Chi phí vận hành (VNĐ)"
                }
            },
            {
                "key": "kpi_margin",
                "name": "Biên Lợi Nhuận Gộp (%)",
                "old_names": ["Biên Lợi Nhuận Gộp", "Biên Lợi Nhuận Gộp (%)"],
                "viz": "big_number_total",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "big_number_total",
                    "metric": "profitMarginPct",
                    "header_font_size": 0.45,
                    "subheader_font_size": 0.15,
                    "y_axis_format": ",.2f",
                    "subheader": "Tỷ suất sinh lời gộp (%)"
                }
            },
            {
                "key": "kpi_delivery_rate",
                "name": "Tỷ Lệ Giao Thành Công (%)",
                "old_names": ["Tỷ Lệ Giao Thành Công", "Tỷ Lệ Giao Thành Công (%)"],
                "viz": "big_number_total",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "big_number_total",
                    "metric": "deliverySuccessRate",
                    "header_font_size": 0.45,
                    "subheader_font_size": 0.15,
                    "y_axis_format": ",.2f",
                    "subheader": "Chỉ tiêu chất lượng SLA (%)"
                }
            },
            {
                "key": "trend_daily",
                "name": "Xu Hướng Đơn Hàng & Doanh Thu Theo Ngày",
                "old_names": ["Xu Hướng Đơn Hàng & Doanh Thu Theo Ngày"],
                "viz": "echarts_timeseries_bar",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "echarts_timeseries_bar",
                    "granularity_sqla": "bookingDate",
                    "time_grain_sqla": "P1D",
                    "time_range": "2023-01-01 : 2024-01-01",
                    "metrics": ["totalBookings", "totalRevenue"],
                    "y_axis_format": ",.0f",
                    "order_desc": False,
                    "row_limit": 500,
                    "show_legend": True,
                    "zoomable": True
                }
            },
            {
                "key": "pie_region_revenue",
                "name": "Cơ Cấu Doanh Thu Theo Vùng Miền",
                "old_names": ["Cơ Cấu Doanh Thu Theo Vùng Miền"],
                "viz": "pie",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "pie",
                    "groupby": ["sendingRegion"],
                    "metric": "totalRevenue",
                    "donut": True,
                    "show_labels": True,
                    "number_format": ",.0f"
                }
            },
            {
                "key": "bar_province_finance",
                "name": "Doanh Thu & Lợi Nhuận Theo Tỉnh Gửi",
                "old_names": ["Doanh Thu & Lợi Nhuận Theo Tỉnh Gửi"],
                "viz": "echarts_timeseries_bar",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "echarts_timeseries_bar",
                    "granularity_sqla": "bookingDate",
                    "time_range": "No filter",
                    "x_axis": "sendingProvince",
                    "xAxisLabelRotation": 45,
                    "truncateXAxis": False,
                    "groupby": [],
                    "metrics": ["totalRevenue", "totalProfit"],
                    "orientation": "vertical",
                    "y_axis_format": ",.0f",
                    "show_legend": True,
                    "row_limit": 20
                }
            },
            {
                "key": "bar_quality_region",
                "name": "Chất Lượng Giao Hàng: Thành Công vs Thất Bại Theo Vùng",
                "old_names": ["Chất Lượng Giao Hàng: Thành Công vs Thất Bại Theo Vùng"],
                "viz": "echarts_timeseries_bar",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "echarts_timeseries_bar",
                    "granularity_sqla": "bookingDate",
                    "time_range": "No filter",
                    "x_axis": "sendingRegion",
                    "groupby": [],
                    "metrics": ["completedBookings", "failedBookings"],
                    "y_axis_format": ",.0f",
                    "show_legend": True
                }
            },

            # === TAB 2: CHI TIẾT VẬN HÀNH & DỊCH VỤ ===
            {
                "key": "kpi_aov",
                "name": "Giá Trị Đơn Trung Bình - AOV (VNĐ)",
                "old_names": ["Giá Trị Đơn Trung Bình (AOV)", "Giá Trị Đơn Trung Bình - AOV (VNĐ)"],
                "viz": "big_number_total",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "big_number_total",
                    "metric": "avgOrderValue",
                    "header_font_size": 0.45,
                    "subheader_font_size": 0.15,
                    "y_axis_format": ",.0f",
                    "subheader": "Quy mô trung bình mỗi vận đơn (VNĐ)"
                }
            },
            {
                "key": "kpi_weight",
                "name": "Tổng Khối Lượng Vận Chuyển (kg)",
                "old_names": ["Tổng Khối Lượng Vận Chuyển", "Tổng Khối Lượng Vận Chuyển (kg)"],
                "viz": "big_number_total",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "big_number_total",
                    "metric": "totalWeightKg",
                    "header_font_size": 0.45,
                    "subheader_font_size": 0.15,
                    "y_axis_format": ",.0f",
                    "subheader": "Tổng trọng lượng hàng (kg)"
                }
            },
            {
                "key": "kpi_customers",
                "name": "Số Khách Hàng Hoạt Động",
                "old_names": ["Số Khách Hàng Hoạt Động"],
                "viz": "big_number_total",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "big_number_total",
                    "metric": "uniqueCustomers",
                    "header_font_size": 0.45,
                    "subheader_font_size": 0.15,
                    "y_axis_format": ",.0f",
                    "subheader": "Khách hàng phát sinh đơn"
                }
            },
            {
                "key": "bar_service_perf",
                "name": "Hiệu Suất & Số Đơn Theo Đơn Vị Dịch Vụ",
                "old_names": ["Hiệu Suất & Số Đơn Theo Đơn Vị Dịch Vụ"],
                "viz": "echarts_timeseries_bar",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "echarts_timeseries_bar",
                    "granularity_sqla": "bookingDate",
                    "time_range": "No filter",
                    "x_axis": "serviceName",
                    "xAxisLabelRotation": 25,
                    "truncateXAxis": False,
                    "groupby": [],
                    "metrics": ["totalBookings", "completedBookings"],
                    "y_axis_format": ",.0f",
                    "show_legend": True
                }
            },
            {
                "key": "pie_status_dist",
                "name": "Cơ Cấu Đơn Hàng Theo Nhóm Trạng Thái",
                "old_names": ["Cơ Cấu Đơn Hàng Theo Nhóm Trạng Thái"],
                "viz": "pie",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "pie",
                    "groupby": ["statusGroup"],
                    "metric": "totalBookings",
                    "donut": True,
                    "show_labels": True,
                    "number_format": ",.0f"
                }
            },
            {
                "key": "table_drilldown",
                "name": "Bảng Chi Tiết Vận Đơn Theo Tuyến Vận Chuyển",
                "old_names": ["Bảng Chi Tiết Vận Đơn Theo Tuyến Vận Chuyển"],
                "viz": "table",
                "params": {
                    "datasource": datasource_str,
                    "viz_type": "table",
                    "groupby": ["sendingProvince", "receivingProvince", "serviceName", "customerType"],
                    "metrics": [
                        "totalBookings",
                        "completedBookings",
                        "failedBookings",
                        "totalRevenue",
                        "totalProfit",
                        "profitMarginPct",
                        "deliverySuccessRate"
                    ],
                    "page_length": 10,
                    "order_desc": True,
                    "include_search": True,
                    "table_timestamp_format": "smart_date",
                    "show_cell_bars": False,
                    "align_pn": False,
                    "color_pn": False,
                    "column_config": {
                        "totalRevenue": {"d3NumberFormat": ",.0f"},
                        "totalProfit": {"d3NumberFormat": ",.0f"},
                        "totalBookings": {"d3NumberFormat": ",.0f"},
                        "completedBookings": {"d3NumberFormat": ",.0f"},
                        "failedBookings": {"d3NumberFormat": ",.0f"},
                        "profitMarginPct": {"d3NumberFormat": ",.2f"},
                        "deliverySuccessRate": {"d3NumberFormat": ",.2f"}
                    }
                }
            }
        ]

        slices_map = {}
        for cfg in chart_configs:
            # Tìm kiếm slice theo tên mới hoặc tên cũ
            slc = None
            for name_cand in [cfg["name"]] + cfg.get("old_names", []):
                slc = db.session.query(Slice).filter_by(slice_name=name_cand).first()
                if slc:
                    break

            if not slc:
                slc = Slice(
                    slice_name=cfg["name"],
                    datasource_id=table.id,
                    datasource_type="table",
                    datasource_name=table.table_name,
                    viz_type=cfg["viz"],
                    params=json.dumps(cfg["params"]),
                    created_by_fk=admin_user.id
                )
                db.session.add(slc)
                db.session.flush()
                print(f"  ✨ Đã tạo biểu đồ mới: '{cfg['name']}' (ID: {slc.id})")
            else:
                slc.slice_name = cfg["name"]
                slc.viz_type = cfg["viz"]
                slc.params = json.dumps(cfg["params"])
                db.session.flush()
                print(f"  🔄 Cập nhật biểu đồ: '{cfg['name']}' (ID: {slc.id})")
            slices_map[cfg["key"]] = slc

        # -------------------------------------------------------------
        # 2. XÂY DỰNG BỐ CỤC DASHBOARD (POSITION_JSON VỚI 2 TABS & METADATA CHUẨN)
        # -------------------------------------------------------------
        tabs_root_id = "TABS-ROOT"
        tab_overview_id = "TAB-OVERVIEW"
        tab_operations_id = "TAB-OPERATIONS"
        
        row_kpi_1_id = "ROW-KPI-1"
        row_charts_1_id = "ROW-CHARTS-1"
        row_charts_2_id = "ROW-CHARTS-2"

        row_kpi_2_id = "ROW-KPI-2"
        row_charts_3_id = "ROW-CHARTS-3"
        row_table_id = "ROW-TABLE"

        pos = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {
                "id": "ROOT_ID",
                "type": "ROOT",
                "children": ["GRID_ID"]
            },
            "GRID_ID": {
                "id": "GRID_ID",
                "type": "GRID",
                "children": [tabs_root_id],
                "parents": ["ROOT_ID"]
            },
            tabs_root_id: {
                "id": tabs_root_id,
                "type": "TABS",
                "children": [tab_overview_id, tab_operations_id],
                "parents": ["ROOT_ID", "GRID_ID"],
                "meta": {}
            },

            # === TAB 1: TỔNG QUAN ===
            tab_overview_id: {
                "id": tab_overview_id,
                "type": "TAB",
                "children": [row_kpi_1_id, row_charts_1_id, row_charts_2_id],
                "parents": ["ROOT_ID", "GRID_ID", tabs_root_id],
                "meta": {"text": "📊 Tổng Quan Điều Hành"}
            },
            row_kpi_1_id: {
                "id": row_kpi_1_id,
                "type": "ROW",
                "children": [
                    f"CHART-{slices_map['kpi_bookings'].id}",
                    f"CHART-{slices_map['kpi_revenue'].id}",
                    f"CHART-{slices_map['kpi_profit'].id}",
                    f"CHART-{slices_map['kpi_margin'].id}",
                    f"CHART-{slices_map['kpi_delivery_rate'].id}"
                ],
                "parents": ["ROOT_ID", "GRID_ID", tabs_root_id, tab_overview_id],
                "meta": {"background": "BACKGROUND_TRANSPARENT"}
            },
            row_charts_1_id: {
                "id": row_charts_1_id,
                "type": "ROW",
                "children": [
                    f"CHART-{slices_map['trend_daily'].id}",
                    f"CHART-{slices_map['pie_region_revenue'].id}"
                ],
                "parents": ["ROOT_ID", "GRID_ID", tabs_root_id, tab_overview_id],
                "meta": {"background": "BACKGROUND_TRANSPARENT"}
            },
            row_charts_2_id: {
                "id": row_charts_2_id,
                "type": "ROW",
                "children": [
                    f"CHART-{slices_map['bar_province_finance'].id}",
                    f"CHART-{slices_map['bar_quality_region'].id}"
                ],
                "parents": ["ROOT_ID", "GRID_ID", tabs_root_id, tab_overview_id],
                "meta": {"background": "BACKGROUND_TRANSPARENT"}
            },

            # === TAB 2: CHI TIẾT VẬN HÀNH ===
            tab_operations_id: {
                "id": tab_operations_id,
                "type": "TAB",
                "children": [row_kpi_2_id, row_charts_3_id, row_table_id],
                "parents": ["ROOT_ID", "GRID_ID", tabs_root_id],
                "meta": {"text": "🚚 Chi Tiết Vận Hành & Dịch Vụ"}
            },
            row_kpi_2_id: {
                "id": row_kpi_2_id,
                "type": "ROW",
                "children": [
                    f"CHART-{slices_map['kpi_aov'].id}",
                    f"CHART-{slices_map['kpi_weight'].id}",
                    f"CHART-{slices_map['kpi_customers'].id}"
                ],
                "parents": ["ROOT_ID", "GRID_ID", tabs_root_id, tab_operations_id],
                "meta": {"background": "BACKGROUND_TRANSPARENT"}
            },
            row_charts_3_id: {
                "id": row_charts_3_id,
                "type": "ROW",
                "children": [
                    f"CHART-{slices_map['bar_service_perf'].id}",
                    f"CHART-{slices_map['pie_status_dist'].id}"
                ],
                "parents": ["ROOT_ID", "GRID_ID", tabs_root_id, tab_operations_id],
                "meta": {"background": "BACKGROUND_TRANSPARENT"}
            },
            row_table_id: {
                "id": row_table_id,
                "type": "ROW",
                "children": [
                    f"CHART-{slices_map['table_drilldown'].id}"
                ],
                "parents": ["ROOT_ID", "GRID_ID", tabs_root_id, tab_operations_id],
                "meta": {"background": "BACKGROUND_TRANSPARENT"}
            }
        }

        # Thiết lập kích thước width (tổng 12 grid) và height cho từng Chart
        chart_layout_specs = {
            "kpi_bookings": {"width": 2, "height": 26, "row": row_kpi_1_id, "tab": tab_overview_id},
            "kpi_revenue": {"width": 3, "height": 26, "row": row_kpi_1_id, "tab": tab_overview_id},
            "kpi_profit": {"width": 3, "height": 26, "row": row_kpi_1_id, "tab": tab_overview_id},
            "kpi_margin": {"width": 2, "height": 26, "row": row_kpi_1_id, "tab": tab_overview_id},
            "kpi_delivery_rate": {"width": 2, "height": 26, "row": row_kpi_1_id, "tab": tab_overview_id},

            "trend_daily": {"width": 7, "height": 48, "row": row_charts_1_id, "tab": tab_overview_id},
            "pie_region_revenue": {"width": 5, "height": 48, "row": row_charts_1_id, "tab": tab_overview_id},

            "bar_province_finance": {"width": 7, "height": 48, "row": row_charts_2_id, "tab": tab_overview_id},
            "bar_quality_region": {"width": 5, "height": 48, "row": row_charts_2_id, "tab": tab_overview_id},

            "kpi_aov": {"width": 4, "height": 26, "row": row_kpi_2_id, "tab": tab_operations_id},
            "kpi_weight": {"width": 4, "height": 26, "row": row_kpi_2_id, "tab": tab_operations_id},
            "kpi_customers": {"width": 4, "height": 26, "row": row_kpi_2_id, "tab": tab_operations_id},

            "bar_service_perf": {"width": 7, "height": 48, "row": row_charts_3_id, "tab": tab_operations_id},
            "pie_status_dist": {"width": 5, "height": 48, "row": row_charts_3_id, "tab": tab_operations_id},

            "table_drilldown": {"width": 12, "height": 60, "row": row_table_id, "tab": tab_operations_id}
        }

        all_chart_ids = [slc.id for slc in slices_map.values()]

        for key, spec in chart_layout_specs.items():
            slc = slices_map[key]
            chart_id_str = f"CHART-{slc.id}"
            pos[chart_id_str] = {
                "id": chart_id_str,
                "type": "CHART",
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", tabs_root_id, spec["tab"], spec["row"]],
                "meta": {
                    "chartId": slc.id,
                    "sliceName": slc.slice_name,
                    "width": spec["width"],
                    "height": spec["height"]
                }
            }

        # -------------------------------------------------------------
        # 3. CẤU HÌNH BỘ LỌC ĐỘNG (NATIVE FILTERS)
        # -------------------------------------------------------------
        native_filters = [
            {
                "id": "NATIVE_FILTER-service",
                "name": "Dịch Vụ Vận Chuyển",
                "filterType": "filter_select",
                "targets": [{"datasetId": table.id, "column": {"name": "serviceName"}}],
                "defaultDataMask": {
                    "extraFormData": {},
                    "filterState": {},
                    "ownState": {}
                },
                "controlValues": {
                    "enableEmptyFilter": False,
                    "defaultToFirstItem": False,
                    "multiSelect": True,
                    "searchAllOptions": False,
                    "inverseSelection": False
                },
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "type": "NATIVE_FILTER",
                "cascadeParentIds": [],
                "description": "",
                "chartsInScope": all_chart_ids,
                "tabsInScope": []
            },
            {
                "id": "NATIVE_FILTER-province",
                "name": "Tỉnh Thành Xuất Phát",
                "filterType": "filter_select",
                "targets": [{"datasetId": table.id, "column": {"name": "sendingProvince"}}],
                "defaultDataMask": {
                    "extraFormData": {},
                    "filterState": {},
                    "ownState": {}
                },
                "controlValues": {
                    "enableEmptyFilter": False,
                    "defaultToFirstItem": False,
                    "multiSelect": True,
                    "searchAllOptions": False,
                    "inverseSelection": False
                },
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "type": "NATIVE_FILTER",
                "cascadeParentIds": [],
                "description": "",
                "chartsInScope": all_chart_ids,
                "tabsInScope": []
            },
            {
                "id": "NATIVE_FILTER-region",
                "name": "Vùng Miền Gửi",
                "filterType": "filter_select",
                "targets": [{"datasetId": table.id, "column": {"name": "sendingRegion"}}],
                "defaultDataMask": {
                    "extraFormData": {},
                    "filterState": {},
                    "ownState": {}
                },
                "controlValues": {
                    "enableEmptyFilter": False,
                    "defaultToFirstItem": False,
                    "multiSelect": True,
                    "searchAllOptions": False,
                    "inverseSelection": False
                },
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "type": "NATIVE_FILTER",
                "cascadeParentIds": [],
                "description": "",
                "chartsInScope": all_chart_ids,
                "tabsInScope": []
            },
            {
                "id": "NATIVE_FILTER-status",
                "name": "Nhóm Trạng Thái",
                "filterType": "filter_select",
                "targets": [{"datasetId": table.id, "column": {"name": "statusGroup"}}],
                "defaultDataMask": {
                    "extraFormData": {},
                    "filterState": {},
                    "ownState": {}
                },
                "controlValues": {
                    "enableEmptyFilter": False,
                    "defaultToFirstItem": False,
                    "multiSelect": True,
                    "searchAllOptions": False,
                    "inverseSelection": False
                },
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "type": "NATIVE_FILTER",
                "cascadeParentIds": [],
                "description": "",
                "chartsInScope": all_chart_ids,
                "tabsInScope": []
            }
        ]

        chart_config_map = {}
        for cid in all_chart_ids:
            other_ids = [oid for oid in all_chart_ids if oid != cid]
            chart_config_map[str(cid)] = {
                "id": cid,
                "crossFilters": {
                    "scope": "global",
                    "chartsInScope": other_ids
                }
            }

        json_metadata = {
            "native_filter_configuration": native_filters,
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "refresh_frequency": 0,
            "color_scheme": "supersetColors",
            "cross_filters_enabled": True,
            "chart_configuration": chart_config_map,
            "global_chart_configuration": {
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "chartsInScope": all_chart_ids
            }
        }

        # -------------------------------------------------------------
        # 4. TẠO HOẶC CẬP NHẬT DASHBOARD & PHÂN QUYỀN PUBLIC (GAMMA ROLE)
        # -------------------------------------------------------------
        DASHBOARD_TITLE = "Logistics Executive Overview"
        dashboard = db.session.query(Dashboard).filter_by(dashboard_title=DASHBOARD_TITLE).first()
        if not dashboard:
            dashboard = Dashboard(
                dashboard_title=DASHBOARD_TITLE,
                slug="logistics-executive-overview",
                published=True,
                created_by_fk=admin_user.id
            )
            db.session.add(dashboard)
            db.session.flush()

        dashboard.slices = list(slices_map.values())
        dashboard.position_json = json.dumps(pos)
        dashboard.owners = [admin_user]  # ✅ BẢO VỆ DASHBOARD: Chỉ Admin mới có quyền Sửa

        CUSTOM_CSS = """
/* Chỉ áp dụng viền xanh cho KHUNG NGOÀI CÙNG của từng Card biểu đồ */
.dashboard-component-chart-holder {
    border-radius: 12px !important;
    border: 2px solid #2563eb !important;
    background-color: #ffffff !important;
    box-shadow: 0 3px 10px rgba(37, 99, 235, 0.1) !important;
    overflow: hidden !important;
}

/* Hover: giữ nguyên viền xanh sáng, KHÔNG đổi màu viền, không biến dạng */
.dashboard-component-chart-holder:hover {
    border: 2px solid #2563eb !important;
    border-color: #2563eb !important;
    box-shadow: 0 4px 14px rgba(37, 99, 235, 0.18) !important;
    transform: none !important;
}

/* Bo góc cho lớp viền ảo :after của Superset */
.dashboard-component-chart-holder:after {
    border-radius: 12px !important;
    border-color: #2563eb !important;
}

/* BỎ TOÀN BỘ viền trong và bo góc bên trong từng biểu đồ */
.dashboard-component-chart-holder .dashboard-chart,
.dashboard-component-chart-holder .chart-container,
.dashboard-component-chart-holder .chart-slice,
.dashboard-component-chart-holder .slice_container,
.dashboard-component-chart-holder .slice_cell,
.dashboard-component-chart-holder .panel-body,
.dashboard-component-chart-holder .text-container,
.dashboard-chart,
.chart-container,
.chart-slice,
.slice_container,
.slice_cell {
    border-radius: 0 !important;
    border: none !important;
    box-shadow: none !important;
    outline: none !important;
}

/* Tiêu đề biểu đồ sắc nét, chuyên nghiệp */
.dashboard-component-chart-holder .header-title,
.dashboard-component-chart-holder .header-large {
    font-weight: 700 !important;
    color: #1e293b !important;
    font-size: 15px !important;
}

/* Số liệu thẻ KPI to rõ & đậm nét */
.dashboard-component-chart-holder .header-line {
    font-weight: 800 !important;
    letter-spacing: -0.02em !important;
    color: #0f172a !important;
}

/* Tab điều hướng thiết kế thanh lịch */
.ant-tabs-nav-wrap {
    font-size: 15px !important;
    font-weight: 600 !important;
    padding-bottom: 6px !important;
}

.ant-tabs-tab {
    border-radius: 8px !important;
    padding: 8px 18px !important;
    transition: all 0.2s ease !important;
}

.ant-tabs-tab-active {
    background-color: #eff6ff !important;
    color: #2563eb !important;
    font-weight: 700 !important;
}

/* Nút APPLY FILTERS khi chưa chọn hoặc chưa có thay đổi (disabled): màu xám thanh lịch */
button[data-test="filter-bar__apply-button"]:disabled,
button[data-test="filter-bar__apply-button"][disabled],
.filter-apply-button:disabled,
.filter-apply-button[disabled] {
    background-color: #e2e8f0 !important;
    border-color: #cbd5e1 !important;
    color: #94a3b8 !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    box-shadow: none !important;
    cursor: not-allowed !important;
    opacity: 0.8 !important;
}

/* Nút APPLY FILTERS khi có lựa chọn cần lọc (active): sáng màu xanh nổi bật */
button[data-test="filter-bar__apply-button"]:not(:disabled),
button[data-test="filter-bar__apply-button"]:not([disabled]),
.filter-apply-button:not(:disabled),
.filter-apply-button:not([disabled]) {
    background-color: #2563eb !important;
    border-color: #2563eb !important;
    color: #ffffff !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    box-shadow: 0 4px 12px rgba(37, 99, 235, 0.25) !important;
    cursor: pointer !important;
}

button[data-test="filter-bar__apply-button"]:not(:disabled):hover,
.filter-apply-button:not(:disabled):hover {
    background-color: #1d4ed8 !important;
    border-color: #1d4ed8 !important;
    box-shadow: 0 6px 16px rgba(37, 99, 235, 0.35) !important;
}
"""
        dashboard.json_metadata = json.dumps(json_metadata)
        dashboard.css = CUSTOM_CSS
        dashboard.published = True   # ✅ BẬT PUBLIC
        dashboard.description = "Báo cáo điều hành Logistics toàn diện tích hợp Cube Semantic Layer & Row-Level Security."

        # Cấp quyền cho Role 'Gamma' để tất cả user (manager_north, manager_south, driver) xem được
        gamma_role = db.session.query(Role).filter_by(name="Gamma").first()
        if gamma_role and gamma_role not in dashboard.roles:
            dashboard.roles.append(gamma_role)

        db.session.commit()

        print("")
        print(f"🎉 HOÀN TẤT TẠO / CẬP NHẬT DASHBOARD '{DASHBOARD_TITLE}' THÀNH CÔNG!")
        print(f"  🔗 URL: http://localhost:8088/superset/dashboard/{dashboard.id}/")
        print(f"  📑 Bao gồm 2 Tab: 'Tổng Quan' và 'Chi Tiết Vận Hành'")
        print(f"  📊 Tổng cộng: {len(slices_map)} biểu đồ đã được liên kết")
        print(f"  🔍 Bộ lọc Native Filters: 4 bộ lọc tương tác với đầy đủ charts in scope")
        print(f"  🌐 Trạng thái: Published = True (Public cho Role Gamma & Admin)")
        print("\n==================================================================")

if __name__ == '__main__':
    build_logistics_dashboard()
