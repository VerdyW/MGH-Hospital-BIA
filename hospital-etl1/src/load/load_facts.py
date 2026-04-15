import pandas as pd
from src.utils.logger import logger


def build_fact_encounter(encounters_df: pd.DataFrame,
                         dim_encounter_class: pd.DataFrame,
                         dim_date: pd.DataFrame) -> pd.DataFrame:

    # FK: encounter_class_id
    enc_class_map = dict(zip(
        dim_encounter_class["encounter_class"],
        dim_encounter_class["encounter_class_id"]
    ))

    # FK: date_id from start_time
    date_id = pd.to_datetime(encounters_df["start_time"]).dt.strftime("%Y%m%d").astype(int)

    fact = encounters_df.copy()
    fact["encounter_class_id"] = fact["encounter_class"].map(enc_class_map)
    fact["date_id"]            = date_id

    fact = fact[[
        "encounter_id", "patient_id", "organization_id", "payer_id",
        "encounter_class_id", "date_id", "code", "description",
        "los_minutes", "base_encounter_cost", "total_claim_cost",
        "payer_coverage", "reason_code", "reason_description",
    ]]
    logger.info(f"[FACT_ENCOUNTER] Built {len(fact)} records")
    return fact


def build_fact_procedures(procedures_df: pd.DataFrame,
                          dim_procedure_type: pd.DataFrame,
                          dim_date: pd.DataFrame) -> pd.DataFrame:

    proc_type_map = dict(zip(
        dim_procedure_type["procedure_category"],
        dim_procedure_type["procedure_type_id"]
    ))

    fact = procedures_df.copy()
    fact["procedure_type_id"] = fact["procedure_category"].map(proc_type_map)
    fact["date_id"]           = pd.to_datetime(fact["start_time"]).dt.strftime("%Y%m%d").astype(int)

    fact = fact[[
        "patient_id", "encounter_id", "procedure_type_id", "date_id",
        "duration_minutes", "base_cost", "reason_code", "reason_description",
    ]]
    fact.insert(0, "procedure_sk", range(1, len(fact) + 1))
    logger.info(f"[FACT_PROCEDURES] Built {len(fact)} records")
    return fact


def build_fact_diagnosis(encounters_df: pd.DataFrame,
                         dim_diagnosis: pd.DataFrame,
                         dim_date: pd.DataFrame) -> pd.DataFrame:

    diag_map = dict(zip(dim_diagnosis["reason_code"],
                        dim_diagnosis["diagnosis_id"]))

    fact = encounters_df[encounters_df["reason_code"] != 0].copy()
    fact["diagnosis_id"] = fact["reason_code"].map(diag_map)
    fact["date_id"]      = pd.to_datetime(fact["start_time"]).dt.strftime("%Y%m%d").astype(int)

    # join is_deceased from patients — passed via encounters left-joined at main
    # for now default 0; will be enriched in main.py
    fact["is_deceased"] = 0

    fact = fact[[
        "patient_id", "encounter_id", "diagnosis_id",
        "date_id", "is_deceased",
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
