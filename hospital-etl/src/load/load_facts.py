import pandas as pd
from src.utils.logger import logger


def _time_key(dt_series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(dt_series)
    return dt.dt.hour * 100 + dt.dt.minute


def build_fact_encounter(encounters_df: pd.DataFrame,
                         dim_encounter_class: pd.DataFrame,
                         dim_clinical_code: pd.DataFrame,
                         dim_date: pd.DataFrame) -> pd.DataFrame:

    enc_class_map = dict(zip(
        dim_encounter_class["encounter_class"],
        dim_encounter_class["encounter_class_id"]
    ))
    clinical_map = dict(zip(
        dim_clinical_code["clinical_code"].astype(str),
        dim_clinical_code["clinical_code_id"]
    ))

    fact = encounters_df.copy()
    fact["encounter_class_id"] = fact["encounter_class"].map(enc_class_map)
    fact["clinical_code_id"]   = fact["reason_code"].astype(str).map(clinical_map)
    fact["date_id"]            = pd.to_datetime(fact["start_time"]).dt.strftime("%Y%m%d").astype(int)
    fact["start_time_id"]      = _time_key(fact["start_time"])
    fact["stop_time_id"]       = _time_key(fact["stop_time"])

    fact = fact[[
        "encounter_id", "patient_id", "organization_id", "payer_id",
        "encounter_class_id", "clinical_code_id", "date_id",
        "start_time_id", "stop_time_id",
        "description", "los_minutes",
        "base_encounter_cost", "total_claim_cost", "payer_coverage",
        "reason_code", "reason_description",
    ]]
    logger.info(f"[FACT_ENCOUNTER] Built {len(fact)} records")
    return fact


def build_fact_procedures(procedures_df: pd.DataFrame,
                          dim_procedure: pd.DataFrame,
                          dim_clinical_code: pd.DataFrame) -> pd.DataFrame:

    proc_map = dict(zip(
        dim_procedure["code"].astype(str),
        dim_procedure["procedure_id"]
    ))
    clinical_map = dict(zip(
        dim_clinical_code["clinical_code"].astype(str),
        dim_clinical_code["clinical_code_id"]
    ))

    fact = procedures_df.copy()
    fact["procedure_id"]      = fact["code"].astype(str).map(proc_map)
    fact["clinical_code_id"]  = fact["reason_code"].astype(str).map(clinical_map)
    fact["date_id"]           = pd.to_datetime(fact["start_time"]).dt.strftime("%Y%m%d").astype(int)
    fact["start_time_id"]     = _time_key(fact["start_time"])
    fact["stop_time_id"]      = _time_key(fact["stop_time"])

    fact = fact[[
        "patient_id", "encounter_id", "procedure_id", "clinical_code_id",
        "date_id", "start_time_id", "stop_time_id",
        "duration_minutes", "base_cost", "reason_code", "reason_description",
    ]]
    fact.insert(0, "procedure_sk", range(1, len(fact) + 1))
    logger.info(f"[FACT_PROCEDURES] Built {len(fact)} records")
    return fact


def build_fact_diagnosis(encounters_df: pd.DataFrame,
                         dim_clinical_code: pd.DataFrame,
                         dim_date: pd.DataFrame) -> pd.DataFrame:

    clinical_map = dict(zip(
        dim_clinical_code["clinical_code"].astype(str),
        dim_clinical_code["clinical_code_id"]
    ))

    fact = encounters_df[encounters_df["reason_code"] != 0].copy()
    fact["clinical_code_id"] = fact["reason_code"].astype(str).map(clinical_map)
    fact["date_id"]          = pd.to_datetime(fact["start_time"]).dt.strftime("%Y%m%d").astype(int)
    fact["is_deceased"]      = 0  # enriched in main.py

    fact = fact[[
        "patient_id", "encounter_id", "clinical_code_id",
        "date_id", "reason_code", "reason_description", "is_deceased",
    ]]
    fact.insert(0, "diagnosis_sk", range(1, len(fact) + 1))
    logger.info(f"[FACT_DIAGNOSIS] Built {len(fact)} records")
    return fact


def build_fact_billing(encounters_df: pd.DataFrame,
                       dim_date: pd.DataFrame) -> pd.DataFrame:

    fact = encounters_df.copy()
    fact["date_id"]      = pd.to_datetime(fact["start_time"]).dt.strftime("%Y%m%d").astype(int)
    fact["patient_cost"] = (fact["total_claim_cost"] - fact["payer_coverage"]).round(2)

    fact = fact[[
        "encounter_id", "patient_id", "payer_id", "date_id",
        "base_encounter_cost", "total_claim_cost",
        "payer_coverage", "patient_cost",
    ]]
    fact.insert(0, "billing_sk", range(1, len(fact) + 1))
    logger.info(f"[FACT_BILLING] Built {len(fact)} records")
    return fact
