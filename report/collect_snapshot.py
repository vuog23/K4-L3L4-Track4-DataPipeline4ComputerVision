"""Collect observed lake/SQL details using the lab Python inside Airflow."""
import json
import os
import sys
from pathlib import Path

from pipeline import config as C, store
from pipeline.experiments import _fingerprint
from pipeline.steps import _latest_version


def main():
    phase = sys.argv[1]
    con = store.duck(C.DUCKDB_PATH)
    def query(sql):
        cursor = con.execute(sql)
        return [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]
    version = _latest_version()
    manifest = json.loads(store.s3().get_object(Bucket=C.BUCKET, Key=f"gold/datasets/{version}/manifest.json")["Body"].read())
    count, fingerprint = _fingerprint()
    data = {
        "student_id": os.environ["LAB_STUDENT_ID"], "phase": phase,
        "dataset_version": version, "silver_frame_rows": count, "silver_frame_fingerprint": fingerprint,
        "telemetry": query("select drive_id, count(*) as clean_rows, min(ts) as first_ts, max(speed_mps) as max_speed_mps from silver.silver_telemetry group by drive_id order by drive_id"),
        "sync": query("select drive_id, round(avg(sync_gap_ms)) as avg_gap_ms, max(sync_gap_ms) as max_gap_ms from silver.silver_frame_telemetry group by drive_id order by drive_id"),
        "dark_retained": query("select f.drive_id,count(*) as dark_flagged,count(*) filter(where g.frame_id is not null) as dark_in_gold from silver.silver_frame_telemetry f left join gold.gold_dataset_frames g using(frame_id) where f.is_dark group by f.drive_id order by f.drive_id"),
        "recovery_trip": query("select * from marts.mart_trip_summary where trip_id='trip-07774'"),
        "manifest": manifest,
    }
    path = Path("report/evidence") / f"{phase}_snapshot.json"
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    if phase == "final":
        Path("report/evidence/dataset_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps({k: data[k] for k in ("dataset_version", "silver_frame_rows", "silver_frame_fingerprint", "dark_retained")}))


if __name__ == "__main__":
    main()
