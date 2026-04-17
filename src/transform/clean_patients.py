import pandas as pd
from config.settings import AGE_BINS, AGE_LABELS
from src.utils.helpers import zip_to_str
from src.utils.logger import logger


def clean_patients(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] Cleaning patients...")

    df = df.rename(columns={
        "Id": "patient_id", "BIRTHDATE": "birth_date", "DEATHDATE": "death_date",
        "PREFIX": "prefix", "FIRST": "first_name", "LAST": "last_name",
        "SUFFIX": "suffix", "MAIDEN": "maiden_name", "MARITAL": "marital_status",
        "RACE": "race", "ETHNICITY": "ethnicity", "GENDER": "gender",
        "BIRTHPLACE": "birth_place", "CITY": "city", "COUNTY": "county",
    })

    df["birth_date"] = pd.to_datetime(df["birth_date"], errors="coerce")
    df["death_date"] = pd.to_datetime(df["death_date"], errors="coerce")

    df["is_deceased"] = df["death_date"].notna().astype(bool)

    ref = pd.Timestamp("today").normalize()
    df["age"] = df.apply(
        lambda r: int(
            ((r["death_date"] if pd.notna(r["death_date"]) else ref) - r["birth_date"]).days // 365
        ), axis=1
    )
    df["age_group"] = pd.cut(df["age"], bins=AGE_BINS, labels=AGE_LABELS, right=False).astype(str)

    df["gender"]         = df["gender"].map({"F": "Female", "M": "Male"})
    df["marital_status"] = df["marital_status"].fillna("Unknown").map(
        {"M": "Married", "S": "Single", "Unknown": "Unknown"}
    )
    df["race"]      = df["race"].str.title()
    df["ethnicity"] = df["ethnicity"].str.title()
    df["suffix"]      = df["suffix"].fillna("")
    df["maiden_name"] = df["maiden_name"].fillna("")

    return df[[
        "patient_id", "birth_date", "death_date", "prefix",
        "first_name", "last_name", "suffix", "maiden_name",
        "marital_status", "race", "ethnicity", "gender",
        "birth_place", "city", "county", "age", "age_group", "is_deceased",
    ]]
