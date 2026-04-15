import pandas as pd
from src.utils.logger import logger


def build_dim_date(encounters_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate DIM_DATE from all unique dates found in encounter start times.
    """
    dates = pd.to_datetime(
        encounters_df["start_time"].dropna()
    ).dt.normalize().drop_duplicates().sort_values()

    dim_date = pd.DataFrame({"full_date": dates})
    dim_date["date_id"]   = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"]      = dim_date["full_date"].dt.year
    dim_date["quarter"]   = dim_date["full_date"].dt.quarter
    dim_date["month"]     = dim_date["full_date"].dt.month
    dim_date["month_name"]= dim_date["full_date"].dt.strftime("%B")
    dim_date["week"]      = dim_date["full_date"].dt.isocalendar().week.astype(int)
    dim_date["day"]       = dim_date["full_date"].dt.day
    dim_date["day_name"]  = dim_date["full_date"].dt.strftime("%A")
    dim_date["full_date"] = dim_date["full_date"].dt.strftime("%Y-%m-%d")

    logger.info(f"[DIM_DATE] Built {len(dim_date)} date records")
    return dim_date[[
        "date_id", "full_date", "year", "quarter",
        "month", "month_name", "week", "day", "day_name"
    ]]


def build_dim_encounter_class(encounters_df: pd.DataFrame) -> pd.DataFrame:
    classes = (
        encounters_df["encounter_class"]
        .dropna().unique()
    )
    dim = pd.DataFrame({
        "encounter_class_id": range(1, len(classes) + 1),
        "encounter_class":    sorted(classes),
    })
    logger.info(f"[DIM_ENCOUNTER_CLASS] Built {len(dim)} records")
    return dim


def build_dim_procedure_type(procedures_df: pd.DataFrame) -> pd.DataFrame:
    dim = (
        procedures_df[["procedure_category"]]
        .drop_duplicates()
        .sort_values("procedure_category")
        .reset_index(drop=True)
    )
    dim.insert(0, "procedure_type_id", range(1, len(dim) + 1))
    logger.info(f"[DIM_PROCEDURE_TYPE] Built {len(dim)} records")
    return dim


def build_dim_clinical_code(encounters_df: pd.DataFrame, procedures_df: pd.DataFrame) -> pd.DataFrame:
    enc_codes = encounters_df[["code", "description"]].rename(
        columns={"code": "clinical_code", "description": "clinical_description"}
    )
    proc_codes = procedures_df[["code", "description"]].rename(
        columns={"code": "clinical_code", "description": "clinical_description"}
    )
    dim = (
        pd.concat([enc_codes, proc_codes], ignore_index=True)
        .drop_duplicates(subset=["clinical_code"])
        .sort_values("clinical_code")
        .reset_index(drop=True)
    )
    dim.insert(0, "clinical_code_id", range(1, len(dim) + 1))
    logger.info(f"[DIM_CLINICAL_CODE] Built {len(dim)} records")
    return dim
