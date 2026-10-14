import json
from superset import create_app, db

app = create_app()
with app.app_context():
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

    dash = db.session.query(Dashboard).filter_by(id=1).first()
    if not dash:
        print("❌ Dashboard 1 không tồn tại!")
        exit(1)
        
    export_data = {
        "dashboard_title": dash.dashboard_title,
        "slug": dash.slug,
        "description": dash.description,
        "css": dash.css,
        "position_json": json.loads(dash.position_json) if dash.position_json else {},
        "json_metadata": json.loads(dash.json_metadata) if dash.json_metadata else {},
        "charts": []
    }
    
    for slc in dash.slices:
        export_data["charts"].append({
            "id": slc.id,
            "slice_name": slc.slice_name,
            "viz_type": slc.viz_type,
            "params": json.loads(slc.params) if slc.params else {}
        })
        
    out_path = "/tmp/dashboard_state.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(export_data, f, ensure_ascii=False, indent=2)
    total_charts = len(export_data["charts"])
    print(f"✅ Đã xuất thành công trạng thái Dashboard sang {out_path} ({total_charts} charts)!")
