import pandas as pd
from src.utils.logger import logger


def build_dim_date(encounters_df: pd.DataFrame) -> pd.DataFrame:
    dates = (
        pd.to_datetime(encounters_df["start_time"].dropna())
        .dt.normalize()
        .drop_duplicates()
        .sort_values()
    )
    dim = pd.DataFrame({"full_date": dates})
    dim["date_id"]    = dim["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim["year"]       = dim["full_date"].dt.year
    dim["quarter"]    = dim["full_date"].dt.quarter
    dim["month"]      = dim["full_date"].dt.month
    dim["month_name"] = dim["full_date"].dt.strftime("%B")
    dim["week"]       = dim["full_date"].dt.isocalendar().week.astype(int)
    dim["day"]        = dim["full_date"].dt.day
    dim["day_name"]   = dim["full_date"].dt.strftime("%A")
    dim["full_date"]  = dim["full_date"].dt.strftime("%Y-%m-%d")
    logger.info(f"[DIM_DATE] Built {len(dim)} records")
    return dim[["date_id","full_date","year","quarter","month","month_name","week","day","day_name"]]


def build_dim_time() -> pd.DataFrame:
    """
    Generate one row per minute of the day — 1,440 rows total.
    time_key is HHMM integer e.g. 09:30 → 930
    """
    rows = []
    for h in range(24):
        for m in range(60):
            time_key    = h * 100 + m
            full_time   = f"{h:02d}:{m:02d}"
            hour12      = h % 12 or 12
            am_pm       = "AM" if h < 12 else "PM"

            # 30-min interval label
            m30_start   = (m // 30) * 30
            m30_end     = m30_start + 30
            interval_30 = f"{h:02d}:{m30_start:02d}-{h:02d}:{m30_end:02d}"

            # 1-hour interval label
            interval_1h = f"{h:02d}:00-{(h+1)%24:02d}:00"

            # Time of day
            if h < 6:    tod = "Early Morning"
            elif h < 12: tod = "Morning"
            elif h < 18: tod = "Afternoon"
            else:        tod = "Evening"

            is_biz = (h >= 8) and (h < 17)

            rows.append({
                "time_key":         time_key,
                "full_time":        full_time,
                "hour24":           h,
                "hour12":           hour12,
                "am_pm":            am_pm,
                "minute":           m,
                "interval_30min":   interval_30,
                "interval_1hour":   interval_1h,
                "time_of_day":      tod,
                "is_business_hours": is_biz,
            })

    dim = pd.DataFrame(rows)
    logger.info(f"[DIM_TIME] Built {len(dim)} records")
    return dim


def build_dim_encounter_class(encounters_df: pd.DataFrame) -> pd.DataFrame:
    classes = sorted(encounters_df["encounter_class"].dropna().unique())
    dim = pd.DataFrame({
        "encounter_class_id": range(1, len(classes) + 1),
        "encounter_class":    classes,
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


def build_dim_clinical_code(encounters_df: pd.DataFrame,
                             procedures_df: pd.DataFrame) -> pd.DataFrame:
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
    logger.info(f"[DIM_CLINICAL_CODES] Built {len(dim)} records")
    return dim
