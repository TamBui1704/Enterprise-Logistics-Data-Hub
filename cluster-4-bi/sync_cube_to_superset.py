#!/usr/bin/env python3
"""
sync_cube_to_superset.py
--------------------------------------------------------------------------
ĐỒNG BỘ TỰ ĐỘNG & ĐỘNG 100% TỪ CUBE SEMANTIC LAYER SANG APACHE SUPERSET:
1. Đọc Metadata từ Cube REST API (/cubejs-api/v1/meta).
2. Tự động nhận diện và phân loại chuẩn xác Dimension & Measure:
   - DIMENSION: Bật 'Is dimension' (groupby=True), 'Is filterable' (filterable=True),
     nhận diện cột thời gian (is_dttm=True).
   - MEASURE: Tắt 'Is dimension' (groupby=False) để bảo vệ người dùng không kéo
     nhầm vào ô Group By / Dimensions; giữ 'Is filterable' (filterable=True).
   - TECHNICAL COLUMNS (__user, __cubeJoinField): Ẩn hoàn toàn (is_active=False).
3. Chuẩn hóa Saved Metrics với cơ chế UPSERT:
   - Giữ nguyên ID của metric đã tồn tại, tránh lỗi 'One or more metrics do not exist'.
   - Ánh xạ chính xác hàm Aggregation (SUM, AVG, COUNT, MAX) dựa trên aggType & schema.
   - Tự động gán D3 Format (Tiền tệ $, Tỷ lệ %, Số nguyên có dấu phẩy).
4. Áp dụng chứng chỉ dữ liệu tin cậy (Dataset Certification).
--------------------------------------------------------------------------
"""

import json
import urllib.request
import sys

from superset.app import create_app

CUBE_API_URL = "http://cube_semantic_layer:4000/cubejs-api/v1/meta"

def fetch_cube_metadata():
    """Lấy toàn bộ schema và metadata từ Cube REST API."""
    try:
        req = urllib.request.Request(CUBE_API_URL, headers={"x-user": "admin"})
        with urllib.request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as e:
        print(f"❌ Lỗi kết nối Cube API ({CUBE_API_URL}): {e}")
        sys.exit(1)

def determine_expression_and_format(measure_meta):
    """
    Xác định công thức SQL (expression) và định dạng hiển thị (d3format)
    dựa trên metadata của Cube (aggType, type, format, tên).
    """
    m_name = measure_meta.get("name", "")
    short_name = m_name.split(".")[-1]
    lower = short_name.lower()
    agg_type = measure_meta.get("aggType")
    fmt = measure_meta.get("format")
    
    # 1. Xác định Expression (Hàm tổng hợp SQL tương thích với Cube SQL API):
    if short_name == "count":
        expression = "COUNT(*)"
    elif agg_type == "count":
        # Trong Cube SQL API, measure dạng count khi truy vấn từ Superset phải dùng SUM
        expression = f'SUM("{short_name}")'
    elif agg_type == "sum":
        expression = f'SUM("{short_name}")'
    elif agg_type == "avg":
        expression = f'AVG("{short_name}")'
    elif agg_type == "min":
        expression = f'MIN("{short_name}")'
    elif agg_type == "max":
        expression = f'COUNT(DISTINCT "{short_name}")'
    elif agg_type in ["countDistinct", "countDistinctApprox"]:
        expression = f'COUNT(DISTINCT "{short_name}")'
    else:
        # agg_type là "number" (calculated measure) hoặc không xác định:
        if any(k in lower for k in ["pct", "rate", "margin", "percent", "ratio", "avg", "aov"]):
            expression = f'AVG("{short_name}")'
        else:
            expression = f'SUM("{short_name}")'

    # 2. Xác định D3 Format:
    if fmt == "currency" or (any(k in lower for k in ["revenue", "profit", "cost", "fee", "price", "amount", "value", "aov"]) and not any(k in lower for k in ["pct", "margin", "rate"])):
        d3format = "$,.0f"
    elif fmt == "percent" or any(k in lower for k in ["pct", "rate", "margin", "percent", "ratio"]):
        d3format = ",.2f%"
    elif any(k in lower for k in ["distance", "km", "weight"]):
        d3format = ",.2f"
    elif any(k in lower for k in ["booking", "order", "shift", "count", "customer", "failed", "completed"]):
        d3format = ",.0f"
    else:
        d3format = ",.2f" if "avg" in lower else ",.0f"

    return expression, d3format

def sync_cube_to_superset():
    print("==================================================================")
    print("🔄 BẮT ĐẦU ĐỒNG BỘ TỰ ĐỘNG CUBE METADATA SANG APACHE SUPERSET")
    print("==================================================================")
    
    cube_meta = fetch_cube_metadata()
    cubes_list = cube_meta.get("cubes", [])
    print(f"📦 Tìm thấy {len(cubes_list)} Cube(s): {[c['name'] for c in cubes_list]}")

    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable, TableColumn, SqlMetric
        from superset.models.core import Database

        # 1. Đảm bảo kết nối Database 'Cube Semantic Layer' tồn tại
        cube_db = db.session.query(Database).filter_by(database_name="Cube Semantic Layer").first()
        if not cube_db:
            cube_db = Database(
                database_name="Cube Semantic Layer",
                sqlalchemy_uri="postgresql://admin:admin@cube_semantic_layer:5432/default",
                impersonate_user=True
            )
            db.session.add(cube_db)
            db.session.commit()
            print("  ✅ Đã tạo mới kết nối Database 'Cube Semantic Layer'")

        # 2. Xử lý từng Cube
        for cube in cubes_list:
            cube_name = cube["name"]
            print(f"\n🚀 Đang xử lý Cube: '{cube_name}'...")

            table = db.session.query(SqlaTable).filter_by(table_name=cube_name, database_id=cube_db.id).first()
            if not table:
                table = SqlaTable(table_name=cube_name, schema="public", database_id=cube_db.id)
                db.session.add(table)
                db.session.commit()
                print(f"  ✨ Đã tạo dataset mới '{cube_name}' trong Superset")

            # Cập nhật cấu trúc cột vật lý từ Cube SQL API
            table.fetch_metadata()
            db.session.commit()

            # Phân tách Dimensions và Measures từ Cube Metadata
            dim_dict = {}
            for d in cube.get("dimensions", []):
                short_d = d["name"].split(".")[-1]
                title = d.get("title", short_d)
                if title.startswith(f"{cube_name} "):
                    title = title[len(cube_name) + 1:]
                dim_dict[short_d] = {
                    "title": title,
                    "description": d.get("description") or f"Chiều phân tích {title}",
                    "type": d.get("type", "string")
                }

            measure_dict = {}
            for m in cube.get("measures", []):
                short_m = m["name"].split(".")[-1]
                title = m.get("title", short_m)
                if title.startswith(f"{cube_name} "):
                    title = title[len(cube_name) + 1:]
                measure_dict[short_m] = {
                    "raw": m,
                    "title": title,
                    "description": m.get("description") or f"Chỉ số {title}"
                }

            print(f"  📊 Cube có {len(dim_dict)} Dimensions và {len(measure_dict)} Measures.")

            # --- A. PHÂN LOẠI & ĐÁNH DẤU CHUẨN XÁC TAB COLUMNS ---
            count_dims = 0
            count_measures = 0
            count_hidden = 0

            for col in table.columns:
                col_name = col.column_name

                if col_name in dim_dict:
                    # ✅ 1. ĐÂY LÀ DIMENSION THỰC SỰ:
                    col_info = dim_dict[col_name]
                    col.verbose_name = col_info["title"]
                    col.description = col_info["description"]
                    col.is_dttm = (col_info["type"] == "time")
                    col.groupby = True         # ✅ TÍCH CHỌN: 'Is dimension'
                    col.filterable = True      # ✅ TÍCH CHỌN: 'Is filterable'
                    col.is_active = True       # ✅ HIỂN THỊ
                    count_dims += 1

                elif col_name in measure_dict:
                    # ❌ 2. ĐÂY LÀ MEASURE (Chỉ số số học):
                    m_info = measure_dict[col_name]
                    col.verbose_name = m_info["title"]
                    col.description = f"{m_info['description']} (Sử dụng tại mục Saved Metrics)"
                    col.is_dttm = False
                    col.groupby = False        # ❌ BỎ TÍCH: Không phải Dimension, chặn kéo nhầm vào Group By!
                    col.filterable = True      # ✅ Vẫn cho phép lọc điều kiện số (> , <)
                    col.is_active = True       # ✅ Cột vẫn active để truy vấn dữ liệu
                    count_measures += 1

                elif col_name.startswith("__"):
                    # 🚫 3. CỘT KỸ THUẬT NỘI BỘ (__user, __cubeJoinField):
                    col.is_active = False      # Ẩn hoàn toàn khỏi sidebar và UI
                    col.groupby = False
                    col.filterable = False
                    count_hidden += 1

                else:
                    # Các cột phát sinh khác:
                    col_type_str = str(col.type).upper()
                    if any(num_t in col_type_str for num_t in ["INT", "NUMERIC", "FLOAT", "DOUBLE", "DECIMAL"]):
                        col.groupby = False
                    else:
                        col.groupby = True
                    col.filterable = True
                    col.is_active = True

            print(f"  🏷️  Tab COLUMNS: Đã đánh dấu {count_dims} Dimensions, {count_measures} Measures (groupby=False), ẩn {count_hidden} cột kỹ thuật.")

            # --- B. ĐỒNG BỘ TAB METRICS VỚI CƠ CHẾ UPSERT (KHÔNG LÀM THAY ĐỔI ID) ---
            existing_metrics = {m.metric_name: m for m in table.metrics}
            synced_metrics_count = 0
            valid_cube_metric_names = set(measure_dict.keys())

            for short_m, m_info in measure_dict.items():
                m_raw = m_info["raw"]
                title = m_info["title"]
                description = m_info["description"]
                expression, d3format = determine_expression_and_format(m_raw)

                if short_m in existing_metrics:
                    # UPSERT - Cập nhật metric đã có, GIỮ NGUYÊN ID!
                    metric_obj = existing_metrics[short_m]
                    metric_obj.verbose_name = title
                    metric_obj.expression = expression
                    metric_obj.description = description
                    metric_obj.d3format = d3format
                else:
                    # Thêm mới nếu chưa có
                    new_m = SqlMetric(
                        table_id=table.id,
                        metric_name=short_m,
                        verbose_name=title,
                        expression=expression,
                        description=description,
                        d3format=d3format
                    )
                    db.session.add(new_m)
                
                synced_metrics_count += 1

            # Xóa các metric cũ rác (nếu có) mà không còn trong Cube Schema
            for old_name, old_m in existing_metrics.items():
                if old_name not in valid_cube_metric_names:
                    db.session.delete(old_m)

            print(f"  📈 Tab METRICS: Đã đồng bộ an toàn (UPSERT) {synced_metrics_count} Saved Metrics.")

            # --- C. GÁN CHỨNG CHỈ DỮ LIỆU TIN CẬY (CERTIFIED) ---
            extra_dict = {}
            if table.extra:
                try: extra_dict = json.loads(table.extra)
                except: pass
            
            extra_dict["certification"] = {
                "certified_by": "Data Platform Engineering Team",
                "details": f"Dataset {cube_name} được chuẩn hóa tự động 100% từ Cube Semantic Layer & ClickHouse DWH."
            }
            table.extra = json.dumps(extra_dict)
            table.description = f"Bảng dữ liệu phân tích chuẩn hóa '{cube_name}' quản lý tập trung bởi Cube Semantic Layer."

            db.session.commit()
            print(f"  ✅ Hoàn tất đồng bộ Cube '{cube_name}'!")

        print("\n🎉 TẤT CẢ CUBES ĐÃ ĐƯỢC ĐỒNG BỘ THÀNH CÔNG VỚI SUPERSET!")
        print("💡 Lưu ý: Nếu đang mở modal Edit Dataset trên trình duyệt, hãy ấn CANCEL và nhấn F5 để giao diện tải lại metadata mới nhất.")

if __name__ == "__main__":
    sync_cube_to_superset()
