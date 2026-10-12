"""Add two reproducible operational questions to the supplied local dashboard."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from scripts.setup_metabase import MB, USER, call

QUESTIONS = [
    ("Which trips lose the most frames?", """select drive_id, trip_id, frames, frames_kept,
round(100.0 * (frames - frames_kept) / nullif(frames, 0), 1) as dropped_pct,
avg_speed_kmh, lighting, split
from marts.mart_trip_summary
order by dropped_pct desc, trip_id"""),
    ("Bonus: is lighting represented in every split?", """select split, lighting,
count(*) as trips, sum(frames_kept) as kept_frames,
round(sum(frames_kept) * 100.0 / nullif(sum(sum(frames_kept)) over (partition by split), 0), 1) as share_pct
from marts.mart_trip_summary
group by split, lighting
order by split, lighting"""),
]


def main():
    token = call("POST", "/api/session", {"username": USER["email"], "password": USER["password"]})["id"]
    databases = call("GET", "/api/database", token=token)
    databases = databases.get("data", []) if isinstance(databases, dict) else databases
    database = next(d for d in databases if d["name"] == "warehouse")
    dashboard = next(d for d in call("GET", "/api/dashboard", token=token) if d["name"] == "Day 9: video data pipeline")
    dash = call("GET", f"/api/dashboard/{dashboard['id']}", token=token)
    cards = dash["dashcards"]
    evidence = {"student_id": "2A202602422", "dashboard_url": f"{MB}/dashboard/{dashboard['id']}", "questions": []}
    for i, (name, sql) in enumerate(QUESTIONS):
        card = next((d["card"] for d in cards if d.get("card", {}).get("name") == name), None)
        query = {"type": "native", "database": database["id"], "native": {"query": sql}}
        if card is None:
            card = call("POST", "/api/card", {"name": name, "display": "table", "visualization_settings": {}, "dataset_query": query}, token=token)
            bottom = max((d["row"] + d["size_y"] for d in cards), default=0)
            cards.append({"id": -(i + 1), "card_id": card["id"], "row": bottom, "col": 0, "size_x": 24, "size_y": 8,
                          "visualization_settings": {}, "parameter_mappings": []})
        result = call("POST", "/api/dataset", query, token=token)
        if result.get("status") != "completed":
            raise RuntimeError(result)
        evidence["questions"].append({"name": name, "card_id": card["id"], "sql": sql,
                                       "columns": [c["name"] for c in result["data"]["cols"]], "rows": result["data"]["rows"]})
    call("PUT", f"/api/dashboard/{dashboard['id']}", {"dashcards": cards}, token=token)
    (ROOT / "report/evidence/metabase_questions.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(evidence["dashboard_url"])
    for q in evidence["questions"]:
        print(q["name"], q["rows"][:6])


if __name__ == "__main__":
    main()
