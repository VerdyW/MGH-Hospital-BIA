import pandas as pd
from src.utils.helpers import parse_datetime, duration_minutes
from src.utils.logger import logger
from src.transform.business_rules import classify_procedure


def clean_procedures(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] Cleaning procedures...")

    df = df.rename(columns={
        "PATIENT": "patient_id", "ENCOUNTER": "encounter_id",
        "CODE": "code", "DESCRIPTION": "description",
        "START": "start_time", "STOP": "stop_time",
        "BASE_COST": "base_cost",
        "REASONCODE": "reason_code", "REASONDESCRIPTION": "reason_description",
    })

    df["start_time"]       = parse_datetime(df["start_time"])
    df["stop_time"]        = parse_datetime(df["stop_time"])
    df["duration_minutes"] = duration_minutes(df["start_time"], df["stop_time"])

    # Classify first (uses original description with tags intact)
    df["procedure_category"] = df["description"].apply(classify_procedure)

    # Then strip trailing parenthetical tags e.g. "(procedure)", "(regime/therapy)"
    # classification is now captured as booleans in DIM_PROCEDURE
    df["description"] = df["description"].str.replace(r"\s*\(.*?\)\s*$", "", regex=True).str.strip()

    df["reason_code"]        = df["reason_code"].fillna(0).astype(int)
    df["reason_description"] = df["reason_description"].fillna("Not Specified")

    return df[[
        "patient_id", "encounter_id", "code", "description",
        "procedure_category", "start_time", "stop_time",
        "duration_minutes", "base_cost",
        "reason_code", "reason_description",
    ]]
