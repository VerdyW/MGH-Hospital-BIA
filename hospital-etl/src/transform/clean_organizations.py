import pandas as pd
from src.utils.logger import logger


def clean_organizations(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] Cleaning organizations...")

    df = df.rename(columns={
        "Id": "organization_id", "NAME": "name", "ADDRESS": "address",
        "CITY": "city", "STATE": "state", "ZIP": "zip",
        "LAT": "lat", "LON": "lon",
    })

    df["name"]  = df["name"].str.title()
    df["zip"]   = df["zip"].astype(str).str.zfill(5)
    df["state"] = df["state"].str.upper()

    return df[["organization_id", "name", "address", "city", "state", "zip"]]
