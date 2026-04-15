import pandas as pd
from src.utils.helpers import parse_datetime, duration_minutes
from src.utils.logger import logger


def clean_encounters(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] Cleaning encounters...")

    df = df.rename(columns={
        "Id": "encounter_id", "PATIENT": "patient_id",
        "ORGANIZATION": "organization_id", "PAYER": "payer_id",
        "ENCOUNTERCLASS": "encounter_class", "CODE": "code",
        "DESCRIPTION": "description", "START": "start_time", "STOP": "stop_time",
        "BASE_ENCOUNTER_COST": "base_encounter_cost",
        "TOTAL_CLAIM_COST": "total_claim_cost",
        "PAYER_COVERAGE": "payer_coverage",
        "REASONCODE": "reason_code", "REASONDESCRIPTION": "reason_description",
    })

    df["start_time"] = parse_datetime(df["start_time"])
    df["stop_time"]  = parse_datetime(df["stop_time"])
    df["los_minutes"] = duration_minutes(df["start_time"], df["stop_time"])

    df["encounter_class"] = df["encounter_class"].str.lower().str.strip()

    # reason nulls are valid — fill for readability
    df["reason_code"]        = df["reason_code"].fillna(0).astype(int)
    df["reason_description"] = df["reason_description"].fillna("Not Specified")

    return df[[
        "encounter_id", "patient_id", "organization_id", "payer_id",
        "encounter_class", "code", "description",
        "start_time", "stop_time", "los_minutes",
        "base_encounter_cost", "total_claim_cost", "payer_coverage",
        "reason_code", "reason_description",
    ]]
