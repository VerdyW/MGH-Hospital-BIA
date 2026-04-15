"""
Before vs After sample report — for assignment documentation.
Run after main.py. Reads raw CSVs and staged CSVs, outputs a simple HTML report.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from config.paths import (
    RAW_PATIENTS, RAW_ENCOUNTERS, RAW_PROCEDURES,
    STAGING_PATIENTS, STAGING_ENCOUNTERS, STAGING_PROCEDURES,
    OUTPUT_DIR,
)

SAMPLE_N = 5

SECTIONS = [
    ("Patients",   RAW_PATIENTS,   STAGING_PATIENTS,
     ["Id","BIRTHDATE","DEATHDATE","GENDER","MARITAL","RACE","ZIP"],
     ["patient_id","birth_date","death_date","gender","marital_status","race","age","age_group","is_deceased"]),

    ("Encounters", RAW_ENCOUNTERS, STAGING_ENCOUNTERS,
     ["Id","START","STOP","ENCOUNTERCLASS","REASONCODE","REASONDESCRIPTION"],
     ["encounter_id","start_time","stop_time","encounter_class","los_minutes","reason_code","reason_description"]),

    ("Procedures", RAW_PROCEDURES, STAGING_PROCEDURES,
     ["START","STOP","CODE","DESCRIPTION","REASONCODE"],
     ["start_time","stop_time","code","description","procedure_category","duration_minutes","reason_code"]),
]

CSS = """
<style>
  body { font-family: Arial, sans-serif; padding: 30px; background: #f5f5f5; }
  h1   { color: #2c3e50; }
  h2   { color: #34495e; border-bottom: 2px solid #3498db; padding-bottom: 6px; margin-top: 40px; }
  h3   { color: #7f8c8d; margin-bottom: 4px; }
  table{ border-collapse: collapse; width: 100%; margin-bottom: 20px; font-size: 13px; background: white; }
  th   { background: #3498db; color: white; padding: 8px 10px; text-align: left; }
  td   { padding: 6px 10px; border-bottom: 1px solid #ddd; }
  tr:hover td { background: #eaf4fb; }
  .badge-before { background:#e74c3c; color:white; padding:2px 8px; border-radius:4px; font-size:11px; }
  .badge-after  { background:#27ae60; color:white; padding:2px 8px; border-radius:4px; font-size:11px; }
  .meta { color:#888; font-size:12px; margin-bottom: 6px; }
</style>
"""

def df_to_html(df):
    return df.to_html(index=False, border=0, classes="")


def build_report():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    html_parts = [f"<html><head><meta charset='utf-8'><title>ETL Report</title>{CSS}</head><body>"]
    html_parts.append("<h1>Hospital ETL — Before &amp; After Transformation Report</h1>")

    for name, raw_path, staged_path, raw_cols, clean_cols in SECTIONS:
        raw   = pd.read_csv(raw_path)
        clean = pd.read_csv(staged_path)

        html_parts.append(f"<h2>{name}</h2>")
        html_parts.append(
            f"<p class='meta'>Raw: {len(raw)} rows, {len(raw.columns)} cols &nbsp;→&nbsp; "
            f"Cleaned: {len(clean)} rows, {len(clean.columns)} cols</p>"
        )

        html_parts.append("<h3><span class='badge-before'>BEFORE</span> — Raw sample</h3>")
        html_parts.append(df_to_html(raw[raw_cols].head(SAMPLE_N)))

        html_parts.append("<h3><span class='badge-after'>AFTER</span> — Transformed sample</h3>")
        html_parts.append(df_to_html(clean[clean_cols].head(SAMPLE_N)))

    html_parts.append("</body></html>")

    out = OUTPUT_DIR / "before_after_report.html"
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(html_parts))

    print(f"Report saved → {out}")


if __name__ == "__main__":
    build_report()
